package server

import (
	"errors"

	"github.com/rickyroynardson/ride-hailing/services/payment/internal/payment"
	"google.golang.org/grpc/codes"
)

// errorCodes maps payment's domain errors to gRPC codes for grpcserver.Logging; anything else becomes Internal.
func errorCodes(err error) (codes.Code, bool) {
	switch {
	case errors.Is(err, payment.ErrInvalidInput):
		return codes.InvalidArgument, true
	case errors.Is(err, payment.ErrChargeNotFound):
		return codes.NotFound, true
	}
	return 0, false
}
