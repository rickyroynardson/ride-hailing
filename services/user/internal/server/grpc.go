package server

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/auth"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/user"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// methodAccess: sign-up and session RPCs are public, everything else needs a user token.
var methodAccess = map[string]grpcserver.Access{
	"/user.v1.AuthService/Register": grpcserver.Public,
	"/user.v1.AuthService/Login":    grpcserver.Public,
	"/user.v1.AuthService/Refresh":  grpcserver.Public,
	"/user.v1.AuthService/Logout":   grpcserver.Public,
}

func NewGRPCServer(lg *zap.Logger, verifier *jwtauth.Verifier, authService *auth.AuthService, userService *user.UserService, options ...grpc.ServerOption) *grpc.Server {
	metrics := grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram())
	prometheus.MustRegister(metrics)

	options = append(options, grpcserver.ServerTracing(), grpc.ChainUnaryInterceptor(
		metrics.UnaryServerInterceptor(),
		grpcserver.Logging(lg, errorCodes),
		grpcserver.Auth(verifier, methodAccess),
	))
	gs := grpc.NewServer(options...)

	userv1.RegisterAuthServiceServer(gs, NewAuthServer(authService))
	userv1.RegisterUserServiceServer(gs, NewUserServer(userService))
	reflection.Register(gs)
	metrics.InitializeMetrics(gs)

	return gs
}
