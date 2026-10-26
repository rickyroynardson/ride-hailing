package server

import (
	"errors"

	"github.com/rickyroynardson/ride-hailing/services/location/internal/location"
	"google.golang.org/grpc/codes"
)

// errorCodes maps location's domain errors to gRPC codes for grpcserver.Logging; anything else becomes Internal.
func errorCodes(err error) (codes.Code, bool) {
	switch {
	case errors.Is(err, location.ErrInvalidInput):
		return codes.InvalidArgument, true
	case errors.Is(err, location.ErrNotDriver):
		return codes.PermissionDenied, true
	case errors.Is(err, location.ErrRateLimited):
		return codes.ResourceExhausted, true
	}
	return 0, false
}
