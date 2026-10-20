package server

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func NewGRPCServer(authService *auth.AuthService) *grpc.Server {
	metrics := grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram())
	prometheus.MustRegister(metrics)

	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(metrics.UnaryServerInterceptor()))

	userv1.RegisterAuthServiceServer(gs, NewAuthServer(authService))
	reflection.Register(gs)
	metrics.InitializeMetrics(gs)

	return gs
}
