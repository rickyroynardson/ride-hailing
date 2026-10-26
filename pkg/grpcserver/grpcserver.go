// Package grpcserver holds the interceptors every service runs: logging with panic recovery and one place that
// turns errors into gRPC statuses, and authentication with per-method rules for who may call.
package grpcserver

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ErrorCodes maps a service's domain errors to gRPC codes; the error's message is sent with the code. Return false
// for errors it doesn't know, and they become a generic Internal.
type ErrorCodes func(error) (codes.Code, bool)

// Logging converts errors to gRPC statuses in one place, logs every RPC with the real cause of Internal errors,
// and recovers panics, which grpc-go would otherwise let crash the whole process.
func Logging(lg *zap.Logger, domain ErrorCodes) grpc.UnaryServerInterceptor {
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
		st := toStatus(err, domain)

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

// toStatus passes nil and existing statuses through, maps shared and domain errors, and hides unknown errors
// behind a generic Internal.
func toStatus(err error, domain ErrorCodes) error {
	if _, ok := status.FromError(err); ok {
		return err
	}

	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "deadline exceeded")
	case errors.Is(err, jwtauth.ErrInvalidAuth):
		return status.Error(codes.Unauthenticated, err.Error())
	case errors.Is(err, jwtauth.ErrWrongCaller):
		return status.Error(codes.PermissionDenied, err.Error())
	}
	if domain != nil {
		if code, ok := domain(err); ok {
			return status.Error(code, err.Error())
		}
	}
	return status.Error(codes.Internal, "internal error")
}

// Access says who may call a method. Methods missing from a service's access map are UsersOnly.
type Access int

const (
	UsersOnly Access = iota
	ServicesOnly
	UsersOrServices
	// Public needs no token, e.g. login and registration.
	Public
)

type callerKey struct{}

// Auth requires a valid user or service token on every non-public RPC, and that the kind of caller is allowed on
// the method: users can't reach internal RPCs, and a leaked service token can't act as a user.
func Auth(verifier *jwtauth.Verifier, methodAccess map[string]Access) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		a := methodAccess[info.FullMethod]
		if a == Public {
			return handler(ctx, req)
		}

		md, _ := metadata.FromIncomingContext(ctx)
		token, ok := strings.CutPrefix(strings.Join(md.Get("authorization"), ""), "Bearer ")
		if !ok {
			return nil, jwtauth.ErrInvalidAuth
		}

		caller, err := verifier.Verify(token)
		if err != nil {
			return nil, err
		}
		if (caller.Service != "" && a == UsersOnly) || (caller.Service == "" && a == ServicesOnly) {
			return nil, jwtauth.ErrWrongCaller
		}

		return handler(context.WithValue(ctx, callerKey{}, caller), req)
	}
}

// CallerFrom returns who is behind the request, as set by Auth; empty on public methods.
func CallerFrom(ctx context.Context) jwtauth.Caller {
	c, _ := ctx.Value(callerKey{}).(jwtauth.Caller)
	return c
}
