package server

import (
	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/payment"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// methodAccess lists RPCs that accept service tokens; every other RPC is for users only.
var methodAccess = map[string]grpcserver.Access{
	"/payment.v1.PaymentService/ChargeRide": grpcserver.ServicesOnly,
	// Reflection is a stream, so StreamAuth sees it; it lets grpcurl and Postman discover the API.
	"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo":      grpcserver.Public,
	"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo": grpcserver.Public,
}

func NewGRPCServer(lg *zap.Logger, verifier *jwtauth.Verifier, paymentService *payment.PaymentService) *grpc.Server {
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
	))

	paymentv1.RegisterPaymentServiceServer(gs, NewPaymentServer(paymentService))
	reflection.Register(gs)
	metrics.InitializeMetrics(gs)

	return gs
}
