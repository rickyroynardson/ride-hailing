package server

import (
	"errors"

	"github.com/rickyroynardson/ride-hailing/services/matching/internal/matching"
	"google.golang.org/grpc/codes"
)

// errorCodes maps matching's domain errors to gRPC codes for grpcserver.Logging; anything else becomes Internal.
func errorCodes(err error) (codes.Code, bool) {
	switch {
	case errors.Is(err, matching.ErrInvalidInput):
		return codes.InvalidArgument, true
	case errors.Is(err, matching.ErrNotDriver):
		return codes.PermissionDenied, true
	case errors.Is(err, matching.ErrOfferUnavailable):
		return codes.FailedPrecondition, true
	case errors.Is(err, matching.ErrAssignPending):
		return codes.Unavailable, true
	}
	return 0, false
}
