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
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/config"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/database"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/outbox"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/ride"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/server"
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
			zap.String("fix", `goose -dir services/ride/migrations postgres "<postgres_url>" up`))
	}
	postgres.RegisterPoolMetrics(dbpool)

	// WatchRide reads the driver's position from location. grpcserver.NewClient connects lazily, so ride starts even if
	// location isn't up; streams just go without a position until it is.
	locationConn, err := grpcserver.NewClient(cfg.LocationAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(jwtauth.NewServiceCredentials(cfg.ServiceName, cfg.JWTSecret)),
		grpcserver.ClientTracing(),
	)
	if err != nil {
		lg.Fatal("failed to create location client", zap.Error(err))
	}
	defer locationConn.Close()

	rideRepo := ride.NewRepository(dbpool)
	hub := ride.NewHub(dbpool)
	rideService := ride.NewService(rideRepo, hub, locationv1.NewLocationServiceClient(locationConn))

	gs := server.NewGRPCServer(lg, jwtauth.NewVerifier(cfg.JWTSecret), rideService)

	// The relay delivers outbox messages (e.g. start matching, charge a ride) with a fresh service token per call.
	// grpcserver.NewClient connects lazily, so ride starts even if a receiver isn't up; messages wait in the outbox until it is.
	matchingConn, err := grpcserver.NewClient(cfg.MatchingAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(jwtauth.NewServiceCredentials(cfg.ServiceName, cfg.JWTSecret)),
		grpcserver.ClientTracing(),
	)
	if err != nil {
		lg.Fatal("failed to create matching client", zap.Error(err))
	}
	defer matchingConn.Close()
	paymentConn, err := grpcserver.NewClient(cfg.PaymentAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(jwtauth.NewServiceCredentials(cfg.ServiceName, cfg.JWTSecret)),
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
	loopCtx, stopLoops := context.WithCancel(context.Background())
	go loop.Run(loopCtx, lg, "outbox listener", time.Second, 30*time.Second, relay.Listen)
	go loop.RunWake(loopCtx, lg, "outbox relay", time.Second, 30*time.Second, relay.Wake(), relay.Run)
	go loop.Run(loopCtx, lg, "ride change listener", time.Second, 30*time.Second, hub.Listen)

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
	stopLoops()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// GracefulStop waits for every RPC, and a WatchRide stream may never end by itself. Past the deadline, Stop ends
	// what is left; those clients reconnect to another replica.
	stopped := make(chan struct{})
	go func() {
		gs.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-ctx.Done():
		gs.Stop()
	}
	if err := sv.Shutdown(ctx); err != nil {
		lg.Fatal("failed to shutdown server", zap.Error(err))
	}
	lg.Info("server shutdown success")
}
