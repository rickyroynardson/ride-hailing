package server

import (
	"errors"

	"github.com/rickyroynardson/ride-hailing/services/ride/internal/ride"
	"google.golang.org/grpc/codes"
)

// errorCodes maps ride's domain errors to gRPC codes for grpcserver.Logging; anything else becomes Internal.
func errorCodes(err error) (codes.Code, bool) {
	switch {
	case errors.Is(err, ride.ErrInvalidInput), errors.Is(err, ride.ErrTooManyIDs):
		return codes.InvalidArgument, true
	case errors.Is(err, ride.ErrActiveRide), errors.Is(err, ride.ErrDriverBusy):
		return codes.AlreadyExists, true
	case errors.Is(err, ride.ErrRideUnavailable), errors.Is(err, ride.ErrPaymentOverdue):
		return codes.FailedPrecondition, true
	case errors.Is(err, ride.ErrRideNotFound):
		return codes.NotFound, true
	}
	return 0, false
}
