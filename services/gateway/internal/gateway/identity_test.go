package gateway

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"net"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type scopedPayment struct {
	paymentv1.UnimplementedPaymentServiceServer
	charges, refunds, reads atomic.Int32
}

func (s *scopedPayment) ChargeRide(context.Context, *paymentv1.ChargeRideRequest) (*paymentv1.ChargeRideResponse, error) {
	s.charges.Add(1)
	return &paymentv1.ChargeRideResponse{}, nil
}
func (s *scopedPayment) RefundCharge(context.Context, *paymentv1.RefundChargeRequest) (*paymentv1.RefundChargeResponse, error) {
	s.refunds.Add(1)
	return &paymentv1.RefundChargeResponse{}, nil
}
func (s *scopedPayment) GetCharge(context.Context, *paymentv1.GetChargeRequest) (*paymentv1.GetChargeResponse, error) {
	s.reads.Add(1)
	return &paymentv1.GetChargeResponse{}, nil
}

func TestCentralIdentityOverGRPC(t *testing.T) {
	newKey := func(id string) (jwtauth.SigningKey, string) {
		pub, key, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return jwtauth.SigningKey{ID: id, PrivateKey: base64.StdEncoding.EncodeToString(key)}, base64.StdEncoding.EncodeToString(pub)
	}
	userKey, userPub := newKey("user")
	serviceKey, servicePub := newKey("service")
	bundle := jwtauth.TrustBundle{User: map[string]string{"user": userPub}, Service: map[string]string{"service": servicePub}}
	verifier, err := jwtauth.NewTokenVerifier("payment", bundle)
	if err != nil {
		t.Fatal(err)
	}
	rideSecret, adminSecret := strings.Repeat("r", 43), strings.Repeat("a", 43)
	issuer, err := jwtauth.NewIssuer(serviceKey, jwtauth.IssuerConfig{Clients: map[string]string{"ride": jwtauth.SecretDigest(rideSecret), "devconsole": jwtauth.SecretDigest(adminSecret)}})
	if err != nil {
		t.Fatal(err)
	}
	identity := httptest.NewServer(issuer)
	defer identity.Close()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(grpcserver.Logging(zap.NewNop(), nil), grpcserver.Auth(verifier, map[string]grpcserver.Access{
		paymentv1.PaymentService_ChargeRide_FullMethodName:   grpcserver.ServicesOnly,
		paymentv1.PaymentService_RefundCharge_FullMethodName: grpcserver.ServicesOnly,
	})))
	backend := &scopedPayment{}
	paymentv1.RegisterPaymentServiceServer(gs, backend)
	go gs.Serve(l)
	t.Cleanup(gs.Stop)
	client := func(id, secret, audience string) paymentv1.PaymentServiceClient {
		opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
		if id != "" {
			v, err := jwtauth.NewTokenVerifier(audience, bundle)
			if err != nil {
				t.Fatal(err)
			}
			creds, err := jwtauth.NewTokenCredentials(identity.URL+"/token", jwtauth.ClientIdentity{ID: id, Secret: secret}, audience, v, true)
			if err != nil {
				t.Fatal(err)
			}
			opts = append(opts, grpc.WithPerRPCCredentials(creds))
		}
		conn, err := grpcserver.NewClient(l.Addr().String(), opts...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return paymentv1.NewPaymentServiceClient(conn)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ride := client("ride", rideSecret, "payment")
	if _, err := ride.ChargeRide(ctx, &paymentv1.ChargeRideRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := ride.RefundCharge(ctx, &paymentv1.RefundChargeRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("ride refund: %v", err)
	}
	if _, err := ride.GetCharge(ctx, &paymentv1.GetChargeRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("service impersonating user: %v", err)
	}
	admin := client("devconsole", adminSecret, "payment")
	if _, err := admin.RefundCharge(ctx, &paymentv1.RefundChargeRequest{}); err != nil {
		t.Fatal(err)
	}
	wrongTarget := client("ride", rideSecret, "matching")
	if _, err := wrongTarget.ChargeRide(ctx, &paymentv1.ChargeRideRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong audience: %v", err)
	}
	userSigner, _ := jwtauth.NewUserSigner(userKey)
	userToken, err := userSigner("rider", false, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	user := client("", "", "")
	userCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+userToken)
	if _, err := user.GetCharge(userCtx, &paymentv1.GetChargeRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := user.ChargeRide(userCtx, &paymentv1.ChargeRideRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("user on internal method: %v", err)
	}
	legacy, _ := jwtauth.SignUser("old-shared-secret", "rider", false, time.Now(), time.Minute)
	legacyCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+legacy)
	if _, err := user.GetCharge(legacyCtx, &paymentv1.GetChargeRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("legacy token accepted: %v", err)
	}
	// Issuer downtime is a dependency failure, not an invalid end-user login (which would trigger HTTP 401).
	identity.Close()
	uncached := client("ride", rideSecret, "payment")
	if _, err := uncached.ChargeRide(ctx, &paymentv1.ChargeRideRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("issuer outage: %v", err)
	}
	if backend.charges.Load() != 1 || backend.refunds.Load() != 1 || backend.reads.Load() != 1 {
		t.Fatalf("unauthorized request reached backend: charges=%d refunds=%d reads=%d", backend.charges.Load(), backend.refunds.Load(), backend.reads.Load())
	}
}
