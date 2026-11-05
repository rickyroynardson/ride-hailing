package server

import (
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/matching"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// methodAccess lists RPCs that accept service tokens; every other RPC is for users only.
var methodAccess = map[string]grpcserver.Access{
	"/matching.v1.MatchingService/StartMatching":  grpcserver.ServicesOnly,
	"/matching.v1.MatchingService/CancelMatching": grpcserver.ServicesOnly,
	// Reflection is a stream, so StreamAuth sees it; it lets grpcurl and Postman discover the API.
	"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo":      grpcserver.Public,
	"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo": grpcserver.Public,
}

func NewGRPCServer(lg *zap.Logger, verifier *jwtauth.Verifier, matchingService *matching.MatchingService, options ...grpc.ServerOption) *grpc.Server {
	metrics := grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram())
	prometheus.MustRegister(metrics)

	options = append(options, grpcserver.ServerTracing(), grpc.ChainUnaryInterceptor(
		metrics.UnaryServerInterceptor(),
		grpcserver.Logging(lg, errorCodes),
		grpcserver.Auth(verifier, methodAccess),
	), grpc.ChainStreamInterceptor(
		metrics.StreamServerInterceptor(),
		grpcserver.StreamLogging(lg, errorCodes),
		grpcserver.StreamAuth(verifier, methodAccess),
	), grpc.KeepaliveParams(keepalive.ServerParameters{
		// Ping quiet connections, so a phone that vanished without closing its stream is noticed.
		Time:    30 * time.Second,
		Timeout: 10 * time.Second,
		// A stream's token is checked only when it opens. Ending connections after the access token's lifetime makes
		// clients reconnect with a current token, which also spreads long streams across replicas.
		MaxConnectionAge:      15 * time.Minute,
		MaxConnectionAgeGrace: 30 * time.Second,
	}))
	gs := grpc.NewServer(options...)

	matchingv1.RegisterMatchingServiceServer(gs, NewMatchingServer(matchingService))
	reflection.Register(gs)
	metrics.InitializeMetrics(gs)

	return gs
}
