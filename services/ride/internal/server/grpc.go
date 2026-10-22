package server

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/auth"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/ride"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func NewGRPCServer(lg *zap.Logger, verifier *auth.Verifier, rideService *ride.RideService) *grpc.Server {
	metrics := grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram())
	prometheus.MustRegister(metrics)

	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(
		metrics.UnaryServerInterceptor(),
		loggingInterceptor(lg),
		authInterceptor(verifier),
	))

	ridev1.RegisterRideServiceServer(gs, NewRideServer(rideService))
	reflection.Register(gs)
	metrics.InitializeMetrics(gs)

	return gs
}
