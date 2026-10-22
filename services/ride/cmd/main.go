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
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/auth"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/config"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/database"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/logger"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/server"
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

	dbpool, err := database.ConnectPgx(ctx, cfg.PostgresURL)
	if err != nil {
		lg.Fatal("failed to connect postgres", zap.Error(err))
	}
	defer dbpool.Close()

	if err := dbpool.Ping(ctx); err != nil {
		lg.Fatal("failed to ping postgres", zap.Error(err))
	}
	database.RegisterPoolMetrics(dbpool)

	gs := server.NewGRPCServer(lg, auth.NewVerifier(cfg.JWTSecret))

	r := chi.NewRouter()

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("pong!"))
	})
	r.Handle("/metrics", promhttp.Handler())

	sv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	grpcLn, err := net.Listen("tcp", ":50051")
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
	lg.Info("server is running", zap.String("http", ":8080"), zap.String("grpc", ":50051"))

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gs.GracefulStop()
	if err := sv.Shutdown(ctx); err != nil {
		lg.Fatal("failed to shutdown server", zap.Error(err))
	}
	lg.Info("server shutdown success")
}
