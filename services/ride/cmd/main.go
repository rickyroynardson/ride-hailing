package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/lifecycle"
	"github.com/rickyroynardson/ride-hailing/pkg/logger"
	"github.com/rickyroynardson/ride-hailing/pkg/loop"
	"github.com/rickyroynardson/ride-hailing/pkg/postgres"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	"github.com/rickyroynardson/ride-hailing/pkg/workloadtls"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/config"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/database"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/outbox"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/ride"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	lg, err := logger.New(cfg.ServiceName, cfg.AppEnv)
	if err != nil {
		log.Fatalf("failed to init logger: %v", err)
	}
	defer lg.Sync()
	serverTLS, err := workloadtls.ServerFromEnv()
	if err != nil {
		lg.Fatal("invalid server TLS configuration", zap.Error(err))
	}
	transport := func(name string) grpc.DialOption {
		creds, err := workloadtls.ClientFromEnv(name)
		if err != nil {
			lg.Fatal("invalid client TLS configuration", zap.Error(err))
		}
		return grpc.WithTransportCredentials(creds)
	}
	verifier, err := jwtauth.VerifierFromEnv("ride")
	if err != nil {
		lg.Fatal("invalid auth trust configuration", zap.Error(err))
	}
	credentials := func(audience string) *jwtauth.TokenCredentials {
		creds, err := jwtauth.CredentialsFromEnv(audience)
		if err != nil {
			lg.Fatal("invalid service identity configuration", zap.Error(err))
		}
		return creds
	}

	ctx := context.Background()

	shutdownTracing, err := telemetry.Setup(ctx, cfg.ServiceName)
	if err != nil {
		lg.Fatal("failed to set up tracing", zap.Error(err))
	}
	defer lifecycle.Flush(shutdownTracing)

	dbpool, err := postgres.Connect(ctx, cfg.PostgresURL)
	if err != nil {
		lg.Fatal("failed to connect postgres", zap.Error(err))
	}
	defer dbpool.Close()

	if err := dbpool.Ping(ctx); err != nil {
		lg.Fatal("failed to ping postgres", zap.Error(err))
	}
	if err := postgres.CheckSchema(ctx, dbpool, database.SchemaVersion); err != nil {
		lg.Fatal("refusing to start", zap.Error(err),
			zap.String("fix", `goose -dir services/ride/migrations postgres "<postgres_url>" up`))
	}
	postgres.RegisterPoolMetrics(dbpool)
	life := lifecycle.New(dbpool.Ping)

	// WatchRide reads the driver's position from location. grpcserver.NewClient connects lazily, so ride starts even if
	// location isn't up; streams just go without a position until it is.
	locationConn, err := grpcserver.NewClient(cfg.LocationAddr,
		transport("location"),
		grpc.WithPerRPCCredentials(credentials("location")),
		grpcserver.ClientTracing(),
	)
	if err != nil {
		lg.Fatal("failed to create location client", zap.Error(err))
	}
	defer locationConn.Close()

	rideRepo := ride.NewRepository(dbpool)
	hub := ride.NewHub(dbpool)
	rideService := ride.NewService(rideRepo, hub, locationv1.NewLocationServiceClient(locationConn))

	gs := server.NewGRPCServer(lg, verifier, rideService, serverTLS, life.UnaryOption(), life.StreamOption())

	// The relay delivers outbox messages with centrally issued, cached, audience-specific service tokens.
	// grpcserver.NewClient connects lazily, so ride starts even if a receiver isn't up; messages wait in the outbox until it is.
	matchingConn, err := grpcserver.NewClient(cfg.MatchingAddr,
		transport("matching"),
		grpc.WithPerRPCCredentials(credentials("matching")),
		grpcserver.ClientTracing(),
	)
	if err != nil {
		lg.Fatal("failed to create matching client", zap.Error(err))
	}
	defer matchingConn.Close()
	paymentConn, err := grpcserver.NewClient(cfg.PaymentAddr,
		transport("payment"),
		grpc.WithPerRPCCredentials(credentials("payment")),
		grpcserver.ClientTracing(),
	)
	if err != nil {
		lg.Fatal("failed to create payment client", zap.Error(err))
	}
	defer paymentConn.Close()
	relay := outbox.NewRelay(dbpool, lg, matchingv1.NewMatchingServiceClient(matchingConn), paymentv1.NewPaymentServiceClient(paymentConn))

	// Every replica runs the relay; SKIP LOCKED splits the work, and receivers are idempotent anyway. The listener
	// wakes it the moment a message commits; the one-second poll picks up retries as their backoff ends, and anything
	// a notification missed.
	life.Go(func(loopCtx context.Context) {
		loop.Run(loopCtx, lg, "outbox listener", time.Second, 30*time.Second, relay.Listen)
	})
	life.Go(func(loopCtx context.Context) {
		loop.RunWake(loopCtx, lg, "outbox relay", time.Second, 30*time.Second, relay.Wake(), relay.Run)
	})
	life.Go(func(loopCtx context.Context) {
		loop.Run(loopCtx, lg, "ride change listener", time.Second, 30*time.Second, hub.Listen)
	})

	r := chi.NewRouter()

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("pong!"))
	})
	r.Handle("/metrics", promhttp.Handler())

	sv := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           life.Health(r),
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := life.Run(lifecycle.Config{GRPC: gs, GRPCAddress: ":" + cfg.GRPCPort, Admin: sv, OnReady: func() { lg.Info("server ready") }, OnDrain: func() { lg.Info("server draining") }}); err != nil {
		if err != lifecycle.ErrDrainDeadline {
			lg.Fatal("server failed", zap.Error(err))
		}
		lg.Warn("forced shutdown", zap.Error(err))
	}
	lg.Info("server shutdown success")
}
