package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/lifecycle"
	"github.com/rickyroynardson/ride-hailing/pkg/logger"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	"github.com/rickyroynardson/ride-hailing/pkg/workloadtls"
	"github.com/rickyroynardson/ride-hailing/services/gateway/internal/config"
	"github.com/rickyroynardson/ride-hailing/services/gateway/internal/gateway"
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

	shutdownTracing, err := telemetry.Setup(context.Background(), cfg.ServiceName)
	if err != nil {
		lg.Fatal("failed to set up tracing", zap.Error(err))
	}
	defer lifecycle.Flush(shutdownTracing)

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		lg.Fatal("invalid redis_url", zap.Error(err))
	}
	redisOpts.ContextTimeoutEnabled = true
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()
	life := lifecycle.New(func(ctx context.Context) error { return rdb.Ping(ctx).Err() })

	// grpcserver.NewClient connects lazily, so the gateway starts even if a service isn't up; its routes answer
	// UNAVAILABLE (503) until it is. Calls carry the user's token and the gateway's own TLS workload identity.
	dial := func(addr, name string) *grpc.ClientConn {
		transport, err := workloadtls.ClientFromEnv(name)
		if err != nil {
			lg.Fatal("invalid client TLS configuration", zap.Error(err))
		}
		conn, err := grpcserver.NewClient(addr, grpc.WithTransportCredentials(transport), grpcserver.ClientTracing())
		if err != nil {
			lg.Fatal("failed to create client", zap.String("addr", addr), zap.Error(err))
		}
		return conn
	}
	backends := gateway.Backends{
		User: dial(cfg.UserAddr, "user"), Ride: dial(cfg.RideAddr, "ride"), Location: dial(cfg.LocationAddr, "location"),
		Matching: dial(cfg.MatchingAddr, "matching"), Payment: dial(cfg.PaymentAddr, "payment"),
	}
	for _, c := range []*grpc.ClientConn{backends.User, backends.Ride, backends.Location, backends.Matching, backends.Payment} {
		defer c.Close()
	}

	api := &http.Server{
		Addr: ":" + cfg.APIPort,
		Handler: life.HTTP(gateway.New(lg, backends, gateway.NewLimiter(rdb, lg), gateway.Options{
			CORSOrigins: cfg.CORSOrigins,
			Limits:      gateway.Limits{Auth: cfg.AuthLimit, User: cfg.UserLimit, Anon: cfg.AnonLimit},
		})),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    16 << 10,
		// No WriteTimeout: it would cut off the watch streams. Unary calls get a deadline in the handler instead.
	}

	r := chi.NewRouter()
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("pong!"))
	})
	r.Handle("/metrics", promhttp.Handler())
	admin := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: life.Health(r), ReadHeaderTimeout: 5 * time.Second}
	if err := life.Run(lifecycle.Config{HTTP: []*http.Server{api}, Admin: admin, OnReady: func() { lg.Info("server ready") }, OnDrain: func() { lg.Info("server draining") }}); err != nil {
		if err != lifecycle.ErrDrainDeadline {
			lg.Fatal("server failed", zap.Error(err))
		}
		lg.Warn("forced shutdown", zap.Error(err))
	}
	lg.Info("server shutdown success")
}
