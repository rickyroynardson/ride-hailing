package server

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/auth"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func NewGRPCServer(lg *zap.Logger, verifier *auth.Verifier) *grpc.Server {
	metrics := grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram())
	prometheus.MustRegister(metrics)

	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(
		metrics.UnaryServerInterceptor(),
		loggingInterceptor(lg),
		authInterceptor(verifier),
	))

	matchingv1.RegisterMatchingServiceServer(gs, NewMatchingServer())
	reflection.Register(gs)
	metrics.InitializeMetrics(gs)

	return gs
}
