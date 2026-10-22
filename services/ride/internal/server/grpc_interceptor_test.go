package server

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rickyroynardson/ride-hailing/services/ride/internal/auth"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLoggingInterceptor(t *testing.T) {
	intercept := loggingInterceptor(zap.NewNop())
	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}

	cases := map[string]struct {
		handler grpc.UnaryHandler
		code    codes.Code
		msg     string
	}{
		"ok":           {func(context.Context, any) (any, error) { return "res", nil }, codes.OK, ""},
		"domain error": {func(context.Context, any) (any, error) { return nil, auth.ErrInvalidAuth }, codes.Unauthenticated, auth.ErrInvalidAuth.Error()},
		"status error": {func(context.Context, any) (any, error) { return nil, status.Error(codes.NotFound, "x") }, codes.NotFound, "x"},
		"canceled":     {func(context.Context, any) (any, error) { return nil, fmt.Errorf("query: %w", context.Canceled) }, codes.Canceled, ""},
		"hidden cause": {func(context.Context, any) (any, error) { return nil, errors.New("db password leaked") }, codes.Internal, "internal error"},
		"panic":        {func(context.Context, any) (any, error) { panic("boom") }, codes.Internal, "internal error"},
	}
	for name, c := range cases {
		_, err := intercept(context.Background(), nil, info, c.handler)
		if st := status.Convert(err); st.Code() != c.code || (c.msg != "" && st.Message() != c.msg) {
			t.Fatalf("%s: got %v %q, want %v %q", name, st.Code(), st.Message(), c.code, c.msg)
		}
	}
}
