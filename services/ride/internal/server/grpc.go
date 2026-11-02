package server

import (
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/ride"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// methodAccess lists RPCs that accept service tokens; every other RPC is for users only.
var methodAccess = map[string]grpcserver.Access{
	"/ride.v1.RideService/FilterAvailableDrivers": grpcserver.ServicesOnly,
	"/ride.v1.RideService/AssignDriver":           grpcserver.ServicesOnly,
	"/ride.v1.RideService/MarkNoDriver":           grpcserver.ServicesOnly,
	"/ride.v1.RideService/RecordPayment":          grpcserver.ServicesOnly,
	// Reflection is a stream, so StreamAuth sees it; it lets grpcurl and Postman discover the API.
	"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo":      grpcserver.Public,
	"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo": grpcserver.Public,
}

func NewGRPCServer(lg *zap.Logger, verifier *jwtauth.Verifier, rideService *ride.RideService) *grpc.Server {
	metrics := grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram())
	prometheus.MustRegister(metrics)

	gs := grpc.NewServer(grpcserver.ServerTracing(), grpc.ChainUnaryInterceptor(
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

	ridev1.RegisterRideServiceServer(gs, NewRideServer(rideService))
	reflection.Register(gs)
	metrics.InitializeMetrics(gs)

	return gs
}
