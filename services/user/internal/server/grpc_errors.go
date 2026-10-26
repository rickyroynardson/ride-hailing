package server

import (
	"errors"

	"github.com/rickyroynardson/ride-hailing/services/user/internal/auth"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/user"
	"google.golang.org/grpc/codes"
)

// errorCodes maps user's domain errors to gRPC codes for grpcserver.Logging; anything else becomes Internal.
func errorCodes(err error) (codes.Code, bool) {
	switch {
	case errors.Is(err, auth.ErrInvalidInput), errors.Is(err, user.ErrInvalidInput):
		return codes.InvalidArgument, true
	case errors.Is(err, auth.ErrUserExists), errors.Is(err, user.ErrDriverExists):
		return codes.AlreadyExists, true
	case errors.Is(err, auth.ErrInvalidCreds), errors.Is(err, auth.ErrInvalidToken):
		return codes.Unauthenticated, true
	case errors.Is(err, user.ErrUserNotFound):
		return codes.NotFound, true
	}
	return 0, false
}
