package server

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	"github.com/rickyroynardson/ride-hailing/services/location/internal/auth"
	"github.com/rickyroynardson/ride-hailing/services/location/internal/location"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// methodAccess lists RPCs that accept service tokens; every other RPC is for users only.
var methodAccess = map[string]access{
	"/location.v1.LocationService/ListNearbyDrivers": usersOrServices,
}

func NewGRPCServer(lg *zap.Logger, verifier *auth.Verifier, locationService *location.LocationService) *grpc.Server {
	metrics := grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram())
	prometheus.MustRegister(metrics)

	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(
		metrics.UnaryServerInterceptor(),
		loggingInterceptor(lg),
		authInterceptor(verifier, methodAccess),
	))

	locationv1.RegisterLocationServiceServer(gs, NewLocationServer(locationService))
	reflection.Register(gs)
	metrics.InitializeMetrics(gs)

	return gs
}
