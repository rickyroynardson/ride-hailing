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
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/logger"
	"github.com/rickyroynardson/ride-hailing/pkg/loop"
	"github.com/rickyroynardson/ride-hailing/pkg/postgres"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/config"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/database"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/matching"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	shutdownTracing, err := telemetry.Setup(ctx, cfg.ServiceName)
	if err != nil {
		lg.Fatal("failed to set up tracing", zap.Error(err))
	}
	defer shutdownTracing(context.Background())

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

	// Calls to other services carry a fresh service token each time. grpc.NewClient connects lazily, so
	// matching starts even if location or ride is not up yet.
	clientOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(jwtauth.NewServiceCredentials(cfg.ServiceName, cfg.JWTSecret)),
		grpcserver.ClientTracing(),
	}
	locationConn, err := grpc.NewClient(cfg.LocationAddr, clientOpts...)
	if err != nil {
		lg.Fatal("failed to create location client", zap.Error(err))
	}
	defer locationConn.Close()
	rideConn, err := grpc.NewClient(cfg.RideAddr, clientOpts...)
	if err != nil {
		lg.Fatal("failed to create ride client", zap.Error(err))
	}
	defer rideConn.Close()

	matchingRepo := matching.NewRepository(dbpool)
	matchingService := matching.NewService(matchingRepo, locationv1.NewLocationServiceClient(locationConn), ridev1.NewRideServiceClient(rideConn))

	gs := server.NewGRPCServer(lg, jwtauth.NewVerifier(cfg.JWTSecret), matchingService)

	// Every replica runs this; that is safe, since expiry, giving up and offer creation are each guarded by a
	// single SQL statement or a unique index.
	tickCtx, stopTick := context.WithCancel(context.Background())
	go loop.Run(tickCtx, lg, "matching tick", time.Second, 30*time.Second, matchingService.Tick)

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
	stopTick()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	gs.GracefulStop()
	if err := sv.Shutdown(ctx); err != nil {
		lg.Fatal("failed to shutdown server", zap.Error(err))
	}
	lg.Info("server shutdown success")
}
