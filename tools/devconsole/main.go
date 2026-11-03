// Command devconsole is a local development UI for the ride-hailing services: it creates riders and drivers,
// keeps their sessions, drives them from a map, and shows every gRPC call it makes plus live database state.
// Development only: it holds the JWT secret and reads every service's database directly.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	defer shutdownTracing(context.Background())

	pool := func(url string) *pgxpool.Pool {
		p, err := pgxpool.New(ctx, url)
		if err != nil {
			log.Fatalf("postgres %s: %v", url, err)
		}
		return p
	}
	dial := func(addr string) *grpc.ClientConn {
		// The console's calls start traces, so each call in its log can link to the whole journey in Jaeger. Its
		// own database and Redis reads stay untraced: polled every 1.5s, they would bury the real traffic.
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpcserver.ClientTracing())
		if err != nil {
			log.Fatalf("grpc %s: %v", addr, err)
		}
		return conn
	}
	redisOpts, err := redis.ParseURL(env("REDIS_URL", "redis://localhost:6379/0"))
	if err != nil {
		log.Fatal(err)
	}

	userConn := dial(env("USER_ADDR", "localhost:50051"))
	c := &console{
		jwtSecret:  []byte(env("JWT_SECRET", "dev-secret-change-me")),
		jaegerURL:  env("JAEGER_UI_URL", "http://localhost:16686"),
		auth:       userv1.NewAuthServiceClient(userConn),
		user:       userv1.NewUserServiceClient(userConn),
		ride:       ridev1.NewRideServiceClient(dial(env("RIDE_ADDR", "localhost:50052"))),
		location:   locationv1.NewLocationServiceClient(dial(env("LOCATION_ADDR", "localhost:50053"))),
		matching:   matchingv1.NewMatchingServiceClient(dial(env("MATCHING_ADDR", "localhost:50054"))),
		payment:    paymentv1.NewPaymentServiceClient(dial(env("PAYMENT_ADDR", "localhost:50055"))),
		usersDB:    pool(env("USERS_DB_URL", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")),
		ridesDB:    pool(env("RIDES_DB_URL", "postgres://postgres:postgres@localhost:5432/rides?sslmode=disable")),
		matchingDB: pool(env("MATCHING_DB_URL", "postgres://postgres:postgres@localhost:5432/matching?sslmode=disable")),
		paymentsDB: pool(env("PAYMENTS_DB_URL", "postgres://postgres:postgres@localhost:5432/payments?sslmode=disable")),
		rdb:        redis.NewClient(redisOpts),
		byID:       map[string]*actor{},
		pages:      map[chan struct{}]struct{}{},
	}

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
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeErr reports a failed action to the page; the gRPC detail is also in the call log.
func writeErr(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
}
