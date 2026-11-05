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
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/config"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/database"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/matching"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/server"
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
	verifier, err := jwtauth.VerifierFromEnv("matching")
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
			zap.String("fix", `goose -dir services/matching/migrations postgres "<postgres_url>" up`))
	}
	postgres.RegisterPoolMetrics(dbpool)
	life := lifecycle.New(dbpool.Ping)

	// Calls carry centrally issued, cached, audience-specific service tokens. grpcserver.NewClient connects lazily, so
	// matching starts even if location or ride is not up yet.
	locationConn, err := grpcserver.NewClient(cfg.LocationAddr, transport("location"), grpcserver.ClientTracing(), grpc.WithPerRPCCredentials(credentials("location")))
	if err != nil {
		lg.Fatal("failed to create location client", zap.Error(err))
	}
	defer locationConn.Close()
	rideConn, err := grpcserver.NewClient(cfg.RideAddr, transport("ride"), grpcserver.ClientTracing(), grpc.WithPerRPCCredentials(credentials("ride")))
	if err != nil {
		lg.Fatal("failed to create ride client", zap.Error(err))
	}
	defer rideConn.Close()

	matchingRepo := matching.NewRepository(dbpool)
	hub := postgres.NewHub(dbpool, matching.OfferChangedChannel)
	matchingService := matching.NewService(matchingRepo, locationv1.NewLocationServiceClient(locationConn), ridev1.NewRideServiceClient(rideConn), hub)

	gs := server.NewGRPCServer(lg, verifier, matchingService, serverTLS, life.UnaryOption(), life.StreamOption())

	// Every replica runs this; that is safe, since expiry, giving up and offer creation are each guarded by a
	// single SQL statement or a unique index.
	life.Go(func(tickCtx context.Context) {
		loop.Run(tickCtx, lg, "matching tick", time.Second, 30*time.Second, matchingService.Tick)
	})
	life.Go(func(tickCtx context.Context) {
		loop.Run(tickCtx, lg, "offer change listener", time.Second, 30*time.Second, hub.Listen)
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
