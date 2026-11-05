package grpcserver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestScopedIdentityForUnaryAndStreams(t *testing.T) {
	key := func(id string) (jwtauth.SigningKey, string) {
		pub, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return jwtauth.SigningKey{ID: id, PrivateKey: base64.StdEncoding.EncodeToString(private)}, base64.StdEncoding.EncodeToString(pub)
	}
	userKey, userPublic := key("user")
	serviceKey, servicePublic := key("service")
	bundle := jwtauth.TrustBundle{User: map[string]string{"user": userPublic}, Service: map[string]string{"service": servicePublic}}
	verifier, err := jwtauth.NewTokenVerifier("payment", bundle)
	if err != nil {
		t.Fatal(err)
	}
	secret := strings.Repeat("r", 43)
	issuer, err := jwtauth.NewIssuer(serviceKey, jwtauth.IssuerConfig{Clients: map[string]string{"ride": jwtauth.SecretDigest(secret), "devconsole": jwtauth.SecretDigest(strings.Repeat("d", 43))}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(issuer)
	defer server.Close()
	token := func(id, clientSecret, audience string) string {
		v, err := jwtauth.NewTokenVerifier(audience, bundle)
		if err != nil {
			t.Fatal(err)
		}
		creds, err := jwtauth.NewTokenCredentials(server.URL+"/token", jwtauth.ClientIdentity{ID: id, Secret: clientSecret}, audience, v, true)
		if err != nil {
			t.Fatal(err)
		}
		md, err := creds.GetRequestMetadata(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		return md["authorization"]
	}
	ride := token("ride", secret, "payment")
	wrongAudience := token("ride", secret, "matching")
	admin := token("devconsole", strings.Repeat("d", 43), "payment")
	signer, _ := jwtauth.NewUserSigner(userKey)
	user, err := signer("u1", false, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	const charge = "/payment.v1.PaymentService/ChargeRide"
	const refund = "/payment.v1.PaymentService/RefundCharge"
	const get = "/payment.v1.PaymentService/GetCharge"
	access := map[string]Access{charge: ServicesOnly, refund: ServicesOnly}
	for _, test := range []struct {
		name, method, token string
		code                codes.Code
	}{
		{"ride charges", charge, ride, codes.OK},
		{"ride cannot refund", refund, ride, codes.PermissionDenied},
		{"admin refunds", refund, admin, codes.OK},
		{"admin cannot charge", charge, admin, codes.PermissionDenied},
		{"wrong audience", charge, wrongAudience, codes.Unauthenticated},
		{"service cannot act as user", get, ride, codes.PermissionDenied},
		{"user cannot act as service", charge, "Bearer " + user, codes.PermissionDenied},
		{"user route", get, "Bearer " + user, codes.OK},
		{"missing identity", charge, "", codes.Unauthenticated},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", test.token))
			called := false
			_, err := Auth(verifier, access)(ctx, nil, &grpc.UnaryServerInfo{FullMethod: test.method}, func(ctx context.Context, _ any) (any, error) {
				called = true
				if c := CallerFrom(ctx); c.UserID == "" && c.Service == "" {
					t.Error("caller not propagated")
				}
				return nil, nil
			})
			if got := status.Code(toStatus(err, nil)); got != test.code || called != (test.code == codes.OK) {
				t.Fatalf("unary code=%s called=%v", got, called)
			}
			called = false
			err = StreamAuth(verifier, access)(nil, fakeStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: test.method}, func(_ any, ss grpc.ServerStream) error {
				called = true
				if c := CallerFrom(ss.Context()); c.UserID == "" && c.Service == "" {
					t.Error("stream caller not propagated")
				}
				return nil
			})
			if got := status.Code(toStatus(err, nil)); got != test.code || called != (test.code == codes.OK) {
				t.Fatalf("stream code=%s called=%v", got, called)
			}
		})
	}
}
