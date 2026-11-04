// Package gateway is the public HTTP/JSON API in front of the gRPC services. Routes come from the google.api.http
// options in the protos, so an RPC without one, such as every service-to-service call, has no public route at all.
package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

const (
	// maxBody bounds a request body; the largest real one is a few hundred bytes.
	maxBody = 64 << 10
	// unaryTimeout bounds a non-streaming call, so a hung service can't hold a client and a goroutine forever.
	unaryTimeout = 10 * time.Second
)

// Backends are connections to the services behind the gateway.
type Backends struct {
	User, Ride, Location, Matching, Payment *grpc.ClientConn
}

type Options struct {
	CORSOrigins []string
	Limits      Limits
}

// New returns the public API handler.
func New(lg *zap.Logger, b Backends, limiter *Limiter, opts Options) http.Handler {
	mux := runtime.NewServeMux(
		// Authorization is always forwarded. Nothing else is: a client mustn't be able to set metadata a service
		// might trust as coming from inside.
		runtime.WithIncomingHeaderMatcher(func(string) (string, bool) { return "", false }),
		runtime.WithMiddlewares(observe(lg)),
	)

	ctx := context.Background()
	for _, register := range []func() error{
		func() error {
			return userv1.RegisterAuthServiceHandlerClient(ctx, mux, userv1.NewAuthServiceClient(b.User))
		},
		func() error {
			return userv1.RegisterUserServiceHandlerClient(ctx, mux, userv1.NewUserServiceClient(b.User))
		},
		func() error {
			return ridev1.RegisterRideServiceHandlerClient(ctx, mux, ridev1.NewRideServiceClient(b.Ride))
		},
		func() error {
			return locationv1.RegisterLocationServiceHandlerClient(ctx, mux, locationv1.NewLocationServiceClient(b.Location))
		},
		func() error {
			return matchingv1.RegisterMatchingServiceHandlerClient(ctx, mux, matchingv1.NewMatchingServiceClient(b.Matching))
		},
		func() error {
			return paymentv1.RegisterPaymentServiceHandlerClient(ctx, mux, paymentv1.NewPaymentServiceClient(b.Payment))
		},
	} {
		// Registering only adds routes to the mux; it fails only on a route conflict, which is a programming error.
		if err := register(); err != nil {
			panic(err)
		}
	}

	var h http.Handler = mux
	h = withTimeout(h)
	h = limiter.Middleware(opts.Limits)(h)
	h = withBodyLimit(h)
	h = withCORS(opts.CORSOrigins)(h)
	// A server span per request; the gRPC client spans below it carry the trace into the services.
	h = otelhttp.NewHandler(h, "gateway", otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
		return r.Method + " " + r.URL.Path
	}))
	return untracedHeartbeats(h)
}

// untracedHeartbeats keeps driver location updates out of tracing, here and in the location service. Every online
// driver sends one every few seconds, so they would be most traces by far and bury the rest; their latency is still
// measured, in gateway_request_duration_seconds.
func untracedHeartbeats(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/v1/driver/location" {
			r.Header.Del("Traceparent") // a client can't opt back in
			r = r.WithContext(telemetry.Untraced(r.Context()))
		}
		next.ServeHTTP(w, r)
	})
}

// isStream reports whether the route is a server stream, which must not get a deadline.
func isStream(path string) bool {
	return strings.HasSuffix(path, ":watch")
}

func withTimeout(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isStream(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), unaryTimeout)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func withBodyLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxBody {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		next.ServeHTTP(w, r)
	})
}

// withCORS lets the listed browser origins call the API. Others get no CORS headers, so browsers block them; non-browser
// clients aren't affected either way.
func withCORS(origins []string) func(http.Handler) http.Handler {
	allowed := map[string]bool{}
	for _, o := range origins {
		allowed[o] = true
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" || !allowed[origin] {
				next.ServeHTTP(w, r)
				return
			}
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Add("Vary", "Origin")
			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE")
				h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
				h.Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// writeError answers in the same JSON shape grpc-gateway uses for errors, with the matching gRPC code.
func writeError(w http.ResponseWriter, status int, message string) {
	code := map[int]codes.Code{http.StatusTooManyRequests: codes.ResourceExhausted, http.StatusRequestEntityTooLarge: codes.InvalidArgument}[status]
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"code": code, "message": message, "details": []any{}})
}
