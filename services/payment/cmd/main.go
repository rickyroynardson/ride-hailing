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
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/config"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/database"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/payment"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/provider"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/server"
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
	clientTLS, err := workloadtls.ClientFromEnv("ride")
	if err != nil {
		lg.Fatal("invalid client TLS configuration", zap.Error(err))
	}
	verifier, err := jwtauth.VerifierFromEnv("payment")
	if err != nil {
		lg.Fatal("invalid auth trust configuration", zap.Error(err))
	}
	credentials, err := jwtauth.CredentialsFromEnv("ride")
	if err != nil {
		lg.Fatal("invalid service identity configuration", zap.Error(err))
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
			zap.String("fix", `goose -dir services/payment/migrations postgres "<postgres_url>" up`))
	}
	postgres.RegisterPoolMetrics(dbpool)
	life := lifecycle.New(dbpool.Ping)

	// grpcserver.NewClient connects lazily, so payment starts even if ride isn't up; outcomes wait on the charge until it is.
	rideConn, err := grpcserver.NewClient(cfg.RideAddr,
		grpc.WithTransportCredentials(clientTLS),
		grpc.WithPerRPCCredentials(credentials),
		grpcserver.ClientTracing(),
	)
	if err != nil {
		lg.Fatal("failed to create ride client", zap.Error(err))
	}
	defer rideConn.Close()

	paymentRepo := payment.NewRepository(dbpool)
	paymentService := payment.NewService(paymentRepo, provider.NewPersistentFake(dbpool), ridev1.NewRideServiceClient(rideConn), lg)

	gs := server.NewGRPCServer(lg, verifier, paymentService, serverTLS, life.UnaryOption(), life.StreamOption())

	// Every replica runs this; SKIP LOCKED splits the charges, and the provider's idempotency key covers the rest.
	life.Go(func(tickCtx context.Context) {
		loop.Run(tickCtx, lg, "payment tick", time.Second, 30*time.Second, paymentService.Tick)
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
