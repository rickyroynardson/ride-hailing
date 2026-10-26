package server

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/ride"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// methodAccess lists RPCs that accept service tokens; every other RPC is for users only.
var methodAccess = map[string]grpcserver.Access{
	"/ride.v1.RideService/FilterAvailableDrivers": grpcserver.ServicesOnly,
	"/ride.v1.RideService/AssignDriver":           grpcserver.ServicesOnly,
	"/ride.v1.RideService/MarkNoDriver":           grpcserver.ServicesOnly,
}

func NewGRPCServer(lg *zap.Logger, verifier *jwtauth.Verifier, rideService *ride.RideService) *grpc.Server {
	metrics := grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram())
	prometheus.MustRegister(metrics)

	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(
		metrics.UnaryServerInterceptor(),
		grpcserver.Logging(lg, errorCodes),
		grpcserver.Auth(verifier, methodAccess),
	))

	ridev1.RegisterRideServiceServer(gs, NewRideServer(rideService))
	reflection.Register(gs)
	metrics.InitializeMetrics(gs)

	return gs
}
