package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/logger"
	"github.com/rickyroynardson/ride-hailing/pkg/loop"
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

	ctx := context.Background()

	rdb, err := database.ConnectRedis(ctx, cfg.RedisURL)
	if err != nil {
		lg.Fatal("failed to connect redis", zap.Error(err))
	}
	defer rdb.Close()

	locationRepo := location.NewRepository(rdb)
	locationService := location.NewService(locationRepo)

	gs := server.NewGRPCServer(lg, jwtauth.NewVerifier(cfg.JWTSecret), locationService)

	// Every replica runs this; that is harmless, since pruning re-checks each driver and is idempotent.
	cleanupCtx, stopCleanup := context.WithCancel(context.Background())
	go loop.Run(cleanupCtx, lg, "stale driver cleanup", time.Minute, 5*time.Minute, func(ctx context.Context) error {
		n, err := locationService.PruneStale(ctx)
		if n > 0 {
			lg.Debug("pruned stale drivers", zap.Int("count", n))
		}
		return err
	})

	r := chi.NewRouter()

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("pong!"))
	})
	r.Handle("/metrics", promhttp.Handler())

	sv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: r,
	}

	grpcLn, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		lg.Fatal("failed to listen grpc", zap.Error(err))
	}

	go func() {
		if err := gs.Serve(grpcLn); err != nil {
			lg.Fatal("grpc server stopped", zap.Error(err))
		}
	}()
	go func() {
		if err := sv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			lg.Fatal("failed to start server", zap.Error(err))
		}
	}()
	lg.Info("server is running", zap.String("http", ":"+cfg.HTTPPort), zap.String("grpc", ":"+cfg.GRPCPort))

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop
	stopCleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gs.GracefulStop()
	if err := sv.Shutdown(ctx); err != nil {
		lg.Fatal("failed to shutdown server", zap.Error(err))
	}
	lg.Info("server shutdown success")
}
