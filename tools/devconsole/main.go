// Command devconsole is a local development UI for the ride-hailing services: it creates riders and drivers,
// keeps their sessions, drives them through the gateway, and shows calls plus live database state.
// Development only: it has narrow administrative service grants and reads every service's database directly.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/lifecycle"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	"github.com/rickyroynardson/ride-hailing/pkg/workloadtls"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	"google.golang.org/grpc"
)

//go:embed index.html
var indexHTML []byte

// env returns the variable or a default for `go run` against the compose ports on localhost.
func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()
	shutdownTracing, err := telemetry.Setup(ctx, "devconsole")
	if err != nil {
		log.Fatal(err)
	}
	defer lifecycle.Flush(shutdownTracing)
	serviceTokens := make(map[string]*jwtauth.TokenCredentials)
	for _, audience := range []string{"matching", "payment"} {
		credentials, err := jwtauth.CredentialsFromEnv(audience)
		if err != nil {
			log.Fatal(err)
		}
		serviceTokens[audience] = credentials
	}

	pool := func(url string) *pgxpool.Pool {
		p, err := pgxpool.New(ctx, url)
		if err != nil {
			log.Fatalf("postgres %s: %v", url, err)
		}
		return p
	}
	dial := func(addr, audience string) *grpc.ClientConn {
		// The console's calls start traces, so each call in its log can link to the whole journey in Jaeger. Its
		// own database and Redis reads stay untraced: polled every 1.5s, they would bury the real traffic.
		transport, err := workloadtls.ClientFromEnv(audience)
		if err != nil {
			log.Fatal(err)
		}
		conn, err := grpcserver.NewClient(addr, grpc.WithTransportCredentials(transport),
			grpc.WithPerRPCCredentials(serviceTokens[audience]), grpcserver.ClientTracing())
		if err != nil {
			log.Fatalf("grpc %s: %v", addr, err)
		}
		return conn
	}
	redisOpts, err := redis.ParseURL(env("REDIS_URL", "redis://localhost:6379/0"))
	if err != nil {
		log.Fatal(err)
	}
	redisOpts.ContextTimeoutEnabled = true

	c := &console{
		jaegerURL:  env("JAEGER_UI_URL", "http://localhost:16686"),
		gateway:    newGatewayClient(env("GATEWAY_URL", "http://localhost:8000")),
		matching:   matchingv1.NewMatchingServiceClient(dial(env("MATCHING_ADDR", "localhost:50054"), "matching")),
		payment:    paymentv1.NewPaymentServiceClient(dial(env("PAYMENT_ADDR", "localhost:50055"), "payment")),
		usersDB:    pool(env("USERS_DB_URL", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")),
		ridesDB:    pool(env("RIDES_DB_URL", "postgres://postgres:postgres@localhost:5432/rides?sslmode=disable")),
		matchingDB: pool(env("MATCHING_DB_URL", "postgres://postgres:postgres@localhost:5432/matching?sslmode=disable")),
		paymentsDB: pool(env("PAYMENTS_DB_URL", "postgres://postgres:postgres@localhost:5432/payments?sslmode=disable")),
		rdb:        redis.NewClient(redisOpts),
		byID:       map[string]*actor{},
		pages:      map[chan struct{}]struct{}{},
	}
	life := lifecycle.New(func(ctx context.Context) error {
		if err := c.rdb.Ping(ctx).Err(); err != nil {
			return err
		}
		for _, db := range []*pgxpool.Pool{c.usersDB, c.ridesDB, c.matchingDB, c.paymentsDB} {
			if err := db.Ping(ctx); err != nil {
				return err
			}
		}
		return nil
	})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	mux.HandleFunc("GET /api/state", c.handleState)
	mux.HandleFunc("GET /api/events", c.handleEvents)
	mux.HandleFunc("POST /api/actors", c.handleCreateActor)
	mux.HandleFunc("POST /api/actors/{id}/location", c.handleLocation)
	mux.HandleFunc("POST /api/actors/{id}/offline", c.handleOffline)
	mux.HandleFunc("POST /api/actors/{id}/rides", c.handleRequestRide)
	mux.HandleFunc("POST /api/actors/{id}/rides/{ride}/cancel", c.handleCancelRide)
	mux.HandleFunc("POST /api/actors/{id}/rides/{ride}/retry-charge", c.handleRetryCharge)
	mux.HandleFunc("POST /api/actors/{id}/card", c.handleSetCard)
	mux.HandleFunc("POST /api/actors/{id}/estimate", c.handleEstimateFare)
	mux.HandleFunc("POST /api/actors/{id}/charges/{ride}", c.handleGetCharge)
	mux.HandleFunc("POST /api/actors/{id}/earnings", c.handleGetEarnings)
	mux.HandleFunc("POST /api/actors/{id}/rides/{ride}/{step}", c.handleTripStep)
	mux.HandleFunc("POST /api/rides/{id}/start-matching", c.handleStartMatching)
	mux.HandleFunc("POST /api/rides/{id}/refund", c.handleRefund)
	mux.HandleFunc("POST /api/actors/{id}/offer", c.handleGetOffer)
	mux.HandleFunc("POST /api/actors/{id}/offers/{offer}/{answer}", c.handleAnswerOffer)
	mux.HandleFunc("POST /api/actors/{id}/remove", c.handleRemoveActor)
	mux.HandleFunc("POST /api/calls/clear", c.handleClearCalls)
	mux.HandleFunc("POST /api/reset", c.handleReset)

	addr := ":" + env("PORT", "9000")
	log.Printf("devconsole on http://localhost%s", addr)
	srv := &http.Server{Addr: addr, Handler: life.Health(life.HTTP(mux)), ReadHeaderTimeout: 5 * time.Second}
	if err := life.Run(lifecycle.Config{HTTP: []*http.Server{srv}, OnReady: func() { log.Print("server ready") }, OnDrain: func() { log.Print("server draining") }}); err != nil {
		if err != lifecycle.ErrDrainDeadline {
			log.Fatal(err)
		}
		log.Print(err)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeErr reports a failed action to the page; the gRPC detail is also in the call log.
func writeErr(w http.ResponseWriter, err error) {
	code := http.StatusBadGateway
	var e *gatewayError
	if errors.As(err, &e) && e.httpStatus >= 400 {
		code = e.httpStatus
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
