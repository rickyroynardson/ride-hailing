package server

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/rickyroynardson/ride-hailing/services/ride/internal/auth"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/ride"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// loggingInterceptor maps domain errors to gRPC statuses in one place, logs every RPC with the real cause of
// Internal errors, and recovers panics, which grpc-go would otherwise let crash the whole process.
func loggingInterceptor(lg *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		start := time.Now()
		method := zap.String("method", info.FullMethod)

		defer func() {
			if p := recover(); p != nil {
				lg.Error("rpc panic", method, zap.Any("panic", p), zap.Stack("stack"))
				resp, err = nil, status.Error(codes.Internal, "internal error")
			}
		}()

		resp, err = handler(ctx, req)
		st := toStatus(err)

		code := status.Code(st)
		fields := []zap.Field{method, zap.String("code", code.String()), zap.Duration("duration", time.Since(start))}
		if err != nil {
			fields = append(fields, zap.Error(err))
		}
		lg.Log(codeLevel(code), "rpc", fields...)

		return resp, st
	}
}

// codeLevel follows go-grpc-middleware's default, except successes are debug: metrics already count them, so
// production (info level) logs only failures. Business events worth keeping are logged explicitly where they happen.
func codeLevel(code codes.Code) zapcore.Level {
	switch code {
	case codes.OK:
		return zap.DebugLevel
	case codes.Canceled, codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.Unauthenticated:
		return zap.InfoLevel
	case codes.Unknown, codes.Unimplemented, codes.Internal, codes.DataLoss:
		return zap.ErrorLevel
	default:
		return zap.WarnLevel
	}
}

// toStatus passes nil and existing statuses through and hides unknown errors behind a generic Internal.
func toStatus(err error) error {
	if _, ok := status.FromError(err); ok {
		return err
	}

	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, auth.ErrInvalidAuth):
		return status.Error(codes.Unauthenticated, err.Error())
	case errors.Is(err, auth.ErrWrongCaller):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, ride.ErrInvalidInput):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ride.ErrActiveRide):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ride.ErrTooManyIDs):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}

type callerKey struct{}

// access says who may call a method. Methods missing from a service's access map are usersOnly.
type access int

const (
	usersOnly access = iota
	servicesOnly
	usersOrServices
)

// authInterceptor requires a valid user or service token on every RPC, and that the kind of caller is allowed
// on the method: users can't reach internal RPCs, and a leaked service token can't act as a user.
func authInterceptor(verifier *auth.Verifier, methodAccess map[string]access) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		token, ok := strings.CutPrefix(strings.Join(md.Get("authorization"), ""), "Bearer ")
		if !ok {
			return nil, auth.ErrInvalidAuth
		}

		caller, err := verifier.Verify(token)
		if err != nil {
			return nil, err
		}

		switch a := methodAccess[info.FullMethod]; {
		case caller.Service != "" && a == usersOnly, caller.Service == "" && a == servicesOnly:
			return nil, auth.ErrWrongCaller
		}

		return handler(context.WithValue(ctx, callerKey{}, caller), req)
	}
}

func callerFromContext(ctx context.Context) auth.Caller {
	c, _ := ctx.Value(callerKey{}).(auth.Caller)
	return c
}
