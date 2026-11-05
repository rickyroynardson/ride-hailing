package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/lifecycle"
	"github.com/rickyroynardson/ride-hailing/pkg/logger"
	"github.com/rickyroynardson/ride-hailing/pkg/postgres"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	"github.com/rickyroynardson/ride-hailing/pkg/workloadtls"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/auth"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/config"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/database"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/server"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/user"
	"go.uber.org/zap"
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
	verifier, err := jwtauth.VerifierFromEnv("user")
	if err != nil {
		lg.Fatal("invalid auth trust configuration", zap.Error(err))
	}
	signer, err := jwtauth.UserSignerFromEnv()
	if err != nil {
		lg.Fatal("invalid user signing configuration", zap.Error(err))
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
			zap.String("fix", `goose -dir services/user/migrations postgres "<postgres_url>" up`))
	}
	postgres.RegisterPoolMetrics(dbpool)
	life := lifecycle.New(dbpool.Ping)

	authRepo := auth.NewRepository(dbpool)
	authService := auth.NewServiceWithSigner(authRepo, signer)

	userRepo := user.NewRepository(dbpool)
	userService := user.NewService(userRepo)

	gs := server.NewGRPCServer(lg, verifier, authService, userService, serverTLS, life.UnaryOption(), life.StreamOption())

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
