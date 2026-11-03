package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/logger"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	"github.com/rickyroynardson/ride-hailing/services/gateway/internal/config"
	"github.com/rickyroynardson/ride-hailing/services/gateway/internal/gateway"
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

	shutdownTracing, err := telemetry.Setup(context.Background(), cfg.ServiceName)
	if err != nil {
		lg.Fatal("failed to set up tracing", zap.Error(err))
	}
	defer shutdownTracing(context.Background())

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		lg.Fatal("invalid redis_url", zap.Error(err))
	}
	rdb := redis.NewClient(redisOpts)
	defer rdb.Close()

	// grpc.NewClient connects lazily, so the gateway starts even if a service isn't up; its routes answer
	// UNAVAILABLE (503) until it is. Calls carry the client's own token: the gateway holds no secrets.
	dial := func(addr string) *grpc.ClientConn {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpcserver.ClientTracing())
		if err != nil {
			lg.Fatal("failed to create client", zap.String("addr", addr), zap.Error(err))
		}
		return conn
	}
	backends := gateway.Backends{
		User: dial(cfg.UserAddr), Ride: dial(cfg.RideAddr), Location: dial(cfg.LocationAddr),
		Matching: dial(cfg.MatchingAddr), Payment: dial(cfg.PaymentAddr),
	}
	for _, c := range []*grpc.ClientConn{backends.User, backends.Ride, backends.Location, backends.Matching, backends.Payment} {
		defer c.Close()
	}

	api := &http.Server{
		Addr: ":" + cfg.APIPort,
		Handler: gateway.New(lg, backends, gateway.NewLimiter(rdb, lg), gateway.Options{
			CORSOrigins: cfg.CORSOrigins,
			Limits:      gateway.Limits{Auth: cfg.AuthLimit, User: cfg.UserLimit, Anon: cfg.AnonLimit},
		}),
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
	admin := &http.Server{Addr: ":" + cfg.HTTPPort, Handler: r, ReadHeaderTimeout: 5 * time.Second}

	for _, sv := range []*http.Server{api, admin} {
		go func() {
			if err := sv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				lg.Fatal("failed to start server", zap.String("addr", sv.Addr), zap.Error(err))
			}
		}()
	}
	lg.Info("server is running", zap.String("api", api.Addr), zap.String("http", admin.Addr))

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Shutdown waits for open requests, and a watch stream never ends by itself. Past the deadline, Close ends what is
	// left; those clients reconnect to another replica.
	if err := api.Shutdown(ctx); err != nil {
		api.Close()
	}
	admin.Shutdown(ctx)
	lg.Info("server shutdown success")
}
