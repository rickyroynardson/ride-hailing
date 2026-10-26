package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/auth"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
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

func TestAuthInterceptor(t *testing.T) {
	const secret = "secret"
	intercept := authInterceptor(auth.NewVerifier(secret), map[string]access{
		"/test/Internal": servicesOnly,
		"/test/Shared":   usersOrServices,
	})
	md, err := auth.NewServiceCredentials("matching", secret).GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	serviceToken := md["authorization"]
	userToken := "Bearer " + signUserToken(t, secret, "u1")

	var got auth.Caller
	handler := func(ctx context.Context, _ any) (any, error) { got = callerFromContext(ctx); return nil, nil }
	call := func(method, authorization string) error {
		got = auth.Caller{}
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", authorization))
		_, err := intercept(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
		return err
	}

	cases := []struct {
		method, token string
		wantErr       error
		want          auth.Caller
	}{
		{"/test/Public", userToken, nil, auth.Caller{UserID: "u1"}},
		{"/test/Public", serviceToken, auth.ErrWrongCaller, auth.Caller{}},
		{"/test/Internal", serviceToken, nil, auth.Caller{Service: "matching"}},
		{"/test/Internal", userToken, auth.ErrWrongCaller, auth.Caller{}},
		{"/test/Shared", userToken, nil, auth.Caller{UserID: "u1"}},
		{"/test/Shared", serviceToken, nil, auth.Caller{Service: "matching"}},
		{"/test/Public", "Bearer garbage", auth.ErrInvalidAuth, auth.Caller{}},
		{"/test/Public", "", auth.ErrInvalidAuth, auth.Caller{}},
	}
	for _, c := range cases {
		if err := call(c.method, c.token); !errors.Is(err, c.wantErr) || got != c.want {
			t.Fatalf("%s with %.20q: got %+v, %v; want %+v, %v", c.method, c.token, got, err, c.want, c.wantErr)
		}
	}
}

func signUserToken(t *testing.T, secret, userID string) string {
	t.Helper()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   userID,
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	}).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	return tok
}
