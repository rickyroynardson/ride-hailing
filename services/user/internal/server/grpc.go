package server

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/auth"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func NewGRPCServer(authService *auth.AuthService, userService *user.UserService) *grpc.Server {
	metrics := grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram())
	prometheus.MustRegister(metrics)

	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(
		metrics.UnaryServerInterceptor(),
		authInterceptor(authService),
	))

	userv1.RegisterAuthServiceServer(gs, NewAuthServer(authService))
	userv1.RegisterUserServiceServer(gs, NewUserServer(userService))
	reflection.Register(gs)
	metrics.InitializeMetrics(gs)

	return gs
}
