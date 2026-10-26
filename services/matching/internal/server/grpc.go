package server

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/matching"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// methodAccess lists RPCs that accept service tokens; every other RPC is for users only.
var methodAccess = map[string]grpcserver.Access{
	"/matching.v1.MatchingService/StartMatching":  grpcserver.ServicesOnly,
	"/matching.v1.MatchingService/CancelMatching": grpcserver.ServicesOnly,
}

func NewGRPCServer(lg *zap.Logger, verifier *jwtauth.Verifier, matchingService *matching.MatchingService) *grpc.Server {
	metrics := grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram())
	prometheus.MustRegister(metrics)

	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(
		metrics.UnaryServerInterceptor(),
		grpcserver.Logging(lg, errorCodes),
		grpcserver.Auth(verifier, methodAccess),
	))

	matchingv1.RegisterMatchingServiceServer(gs, NewMatchingServer(matchingService))
	reflection.Register(gs)
	metrics.InitializeMetrics(gs)

	return gs
}
