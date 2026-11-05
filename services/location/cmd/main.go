package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/lifecycle"
	"github.com/rickyroynardson/ride-hailing/pkg/logger"
	"github.com/rickyroynardson/ride-hailing/pkg/loop"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	"github.com/rickyroynardson/ride-hailing/pkg/workloadtls"
	"github.com/rickyroynardson/ride-hailing/services/location/internal/config"
	"github.com/rickyroynardson/ride-hailing/services/location/internal/database"
	"github.com/rickyroynardson/ride-hailing/services/location/internal/location"
	"github.com/rickyroynardson/ride-hailing/services/location/internal/server"
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
	verifier, err := jwtauth.VerifierFromEnv("location")
	if err != nil {
		lg.Fatal("invalid auth trust configuration", zap.Error(err))
	}

	ctx := context.Background()

	shutdownTracing, err := telemetry.Setup(ctx, cfg.ServiceName)
	if err != nil {
		lg.Fatal("failed to set up tracing", zap.Error(err))
	}
	defer lifecycle.Flush(shutdownTracing)

	rdb, err := database.ConnectRedis(ctx, cfg.RedisURL)
	if err != nil {
		lg.Fatal("failed to connect redis", zap.Error(err))
	}
	defer rdb.Close()
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		lg.Fatal("failed to trace redis", zap.Error(err))
	}

	locationRepo := location.NewRepository(rdb)
	locationService := location.NewService(locationRepo)
	life := lifecycle.New(func(ctx context.Context) error { return rdb.Ping(ctx).Err() })

	gs := server.NewGRPCServer(lg, verifier, locationService, serverTLS, life.UnaryOption(), life.StreamOption())

	// Every replica runs this; that is harmless, since pruning re-checks each driver and is idempotent.
	life.Go(func(cleanupCtx context.Context) {
		loop.Run(cleanupCtx, lg, "stale driver cleanup", time.Minute, 5*time.Minute, func(ctx context.Context) error {
			n, err := locationService.PruneStale(ctx)
			if n > 0 {
				lg.Debug("pruned stale drivers", zap.Int("count", n))
			}
			return err
		})
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
