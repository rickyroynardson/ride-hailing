package server

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	"github.com/rickyroynardson/ride-hailing/services/location/internal/location"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// methodAccess lists RPCs that accept service tokens; every other RPC is for users only.
var methodAccess = map[string]grpcserver.Access{
	"/location.v1.LocationService/ListNearbyDrivers": grpcserver.UsersOrServices,
}

func NewGRPCServer(lg *zap.Logger, verifier *jwtauth.Verifier, locationService *location.LocationService) *grpc.Server {
	metrics := grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram())
	prometheus.MustRegister(metrics)

	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(
		metrics.UnaryServerInterceptor(),
		grpcserver.Logging(lg, errorCodes),
		grpcserver.Auth(verifier, methodAccess),
	))

	locationv1.RegisterLocationServiceServer(gs, NewLocationServer(locationService))
	reflection.Register(gs)
	metrics.InitializeMetrics(gs)

	return gs
}
