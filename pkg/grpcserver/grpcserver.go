// Package grpcserver holds what every service's gRPC server and clients use: logging with panic recovery and one
// place that turns errors into gRPC statuses, authentication with per-method rules for who may call, and tracing.
package grpcserver

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
		err = observe(ctx, lg, domain, info.FullMethod, func() (err error) {
			resp, err = handler(ctx, req)
			return err
		})
		return resp, err
	}
}

// StreamLogging is Logging for streaming RPCs. It logs once, when the stream ends, with the stream's whole duration.
func StreamLogging(lg *zap.Logger, domain ErrorCodes) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return observe(ss.Context(), lg, domain, info.FullMethod, func() error { return handler(srv, ss) })
	}
}

// observe runs one RPC's handler; see Logging.
func observe(ctx context.Context, lg *zap.Logger, domain ErrorCodes, fullMethod string, handler func() error) (err error) {
	start := time.Now()
	method := zap.String("method", fullMethod)

	defer func() {
		if p := recover(); p != nil {
			lg.Error("rpc panic", method, zap.Any("panic", p), zap.Stack("stack"))
			err = status.Error(codes.Internal, "internal error")
		}
	}()

	err = handler()
	st := toStatus(err, domain)

	code := status.Code(st)
	fields := []zap.Field{method, zap.String("code", code.String()), zap.Duration("duration", time.Since(start))}
	// The trace ID ties this log line to the request's trace, and to the other services' lines for it. The
	// context field gives the OpenTelemetry log record its trace and span IDs (pkg/logger keeps it off stdout).
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		fields = append(fields, zap.String("trace_id", sc.TraceID().String()), zap.Any("ctx", ctx))
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	lg.Log(codeLevel(code), "rpc", fields...)

	return st
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
		ctx, err := authenticate(ctx, verifier, methodAccess[info.FullMethod])
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamAuth is Auth for streaming RPCs. The token is checked once, when the stream opens, so a server with long
// streams needs to limit how long they live (keepalive's MaxConnectionAge) or they would outlive the token. gRPC
// reflection is a stream too: list it as Public in methodAccess to keep grpcurl and Postman working.
func StreamAuth(verifier *jwtauth.Verifier, methodAccess map[string]Access) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := authenticate(ss.Context(), verifier, methodAccess[info.FullMethod])
		if err != nil {
			return err
		}
		return handler(srv, authedStream{ss, ctx})
	}
}

// authedStream gives a stream's handler the context carrying the caller.
type authedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s authedStream) Context() context.Context { return s.ctx }

// authenticate returns ctx carrying the caller, if the request may reach a method with access level a.
func authenticate(ctx context.Context, verifier *jwtauth.Verifier, a Access) (context.Context, error) {
	if a == Public {
		return ctx, nil
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
	// Who called is the first question when reading a trace: the user, or the service acting for one.
	if caller.Service != "" {
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("caller.service", caller.Service))
	} else {
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("enduser.id", caller.UserID), attribute.Bool("enduser.driver", caller.Driver))
	}

	return context.WithValue(ctx, callerKey{}, caller), nil
}

// CallerFrom returns who is behind the request, as set by Auth; empty on public methods.
func CallerFrom(ctx context.Context) jwtauth.Caller {
	c, _ := ctx.Value(callerKey{}).(jwtauth.Caller)
	return c
}

// ServerTracing records a span for every incoming RPC, continuing the caller's trace from its traceparent header.
func ServerTracing() grpc.ServerOption {
	return grpc.StatsHandler(otelgrpc.NewServerHandler())
}

// ClientTracing records a span for every outgoing RPC and sends the trace along in a traceparent header, so the
// called service continues the same trace.
func ClientTracing() grpc.DialOption {
	return grpc.WithStatsHandler(otelgrpc.NewClientHandler())
}
