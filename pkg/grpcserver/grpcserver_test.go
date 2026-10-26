package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestLoggingInterceptor(t *testing.T) {
	domain := func(err error) (codes.Code, bool) {
		if err.Error() == "no such ride" {
			return codes.NotFound, true
		}
		return 0, false
	}
	intercept := Logging(zap.NewNop(), domain)
	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}

	cases := map[string]struct {
		handler grpc.UnaryHandler
		code    codes.Code
		msg     string
	}{
		"ok":           {func(context.Context, any) (any, error) { return "res", nil }, codes.OK, ""},
		"shared error": {func(context.Context, any) (any, error) { return nil, jwtauth.ErrInvalidAuth }, codes.Unauthenticated, jwtauth.ErrInvalidAuth.Error()},
		"domain error": {func(context.Context, any) (any, error) { return nil, errors.New("no such ride") }, codes.NotFound, "no such ride"},
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
	intercept := Auth(jwtauth.NewVerifier(secret), map[string]Access{
		"/test/Internal": ServicesOnly,
		"/test/Shared":   UsersOrServices,
		"/test/Login":    Public,
	})
	md, err := jwtauth.NewServiceCredentials("matching", secret).GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	serviceToken := md["authorization"]
	userToken := "Bearer " + signUserToken(t, secret, "u1")

	var got jwtauth.Caller
	handler := func(ctx context.Context, _ any) (any, error) { got = CallerFrom(ctx); return nil, nil }
	call := func(method, authorization string) error {
		got = jwtauth.Caller{}
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", authorization))
		_, err := intercept(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
		return err
	}

	cases := []struct {
		method, token string
		wantErr       error
		want          jwtauth.Caller
	}{
		{"/test/Public", userToken, nil, jwtauth.Caller{UserID: "u1"}},
		{"/test/Public", serviceToken, jwtauth.ErrWrongCaller, jwtauth.Caller{}},
		{"/test/Internal", serviceToken, nil, jwtauth.Caller{Service: "matching"}},
		{"/test/Internal", userToken, jwtauth.ErrWrongCaller, jwtauth.Caller{}},
		{"/test/Shared", userToken, nil, jwtauth.Caller{UserID: "u1"}},
		{"/test/Shared", serviceToken, nil, jwtauth.Caller{Service: "matching"}},
		{"/test/Login", "", nil, jwtauth.Caller{}},
		{"/test/Login", "Bearer garbage", nil, jwtauth.Caller{}},
		{"/test/Public", "Bearer garbage", jwtauth.ErrInvalidAuth, jwtauth.Caller{}},
		{"/test/Public", "", jwtauth.ErrInvalidAuth, jwtauth.Caller{}},
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
