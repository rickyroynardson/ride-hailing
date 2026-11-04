package gateway

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type slowPayment struct {
	paymentv1.UnimplementedPaymentServiceServer
	entered, finished chan struct{}
	calls             atomic.Int32
}

func (s *slowPayment) GetCharge(ctx context.Context, req *paymentv1.GetChargeRequest) (*paymentv1.GetChargeResponse, error) {
	s.calls.Add(1)
	if req.GetRideId() != "slow" {
		return &paymentv1.GetChargeResponse{}, nil
	}
	s.entered <- struct{}{}
	<-ctx.Done()
	s.finished <- struct{}{}
	return nil, status.FromContextError(ctx.Err()).Err()
}

func awaitBackpressure[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for test backend")
		var zero T
		return zero
	}
}

func TestSlowPaymentBackpressureAndRecovery(t *testing.T) {
	s := &slowPayment{entered: make(chan struct{}, 4), finished: make(chan struct{}, 4)}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	paymentv1.RegisterPaymentServiceServer(gs, s)
	healthpb.RegisterHealthServer(gs, health.NewServer())
	go gs.Serve(l)
	t.Cleanup(gs.Stop)
	limits := grpcserver.ClientLimits{MaxConcurrent: 1, MaxQueued: 0, Timeout: 500 * time.Millisecond}
	conn, err := grpcserver.NewClientWithLimits(l.Addr().String(), limits, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := paymentv1.NewPaymentServiceClient(conn)
	// A separate connection represents another backend's independent bulkhead.
	other, err := grpcserver.NewClientWithLimits(l.Addr().String(), limits, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := client.GetCharge(ctx, &paymentv1.GetChargeRequest{RideId: "slow"}); done <- err }()
	awaitBackpressure(t, s.entered)
	_, err = client.GetCharge(context.Background(), &paymentv1.GetChargeRequest{RideId: "fast"})
	if status.Code(err) != codes.ResourceExhausted || s.calls.Load() != 1 {
		t.Fatalf("overflow error=%v calls=%d", err, s.calls.Load())
	}
	// Use the same generated handler registration as the gateway: overload becomes HTTP 429.
	mux := runtime.NewServeMux()
	if err := paymentv1.RegisterPaymentServiceHandlerClient(context.Background(), mux, client); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/rides/fast/charge", nil))
	if w.Code != http.StatusTooManyRequests || s.calls.Load() != 1 {
		t.Fatalf("HTTP overflow status=%d body=%s calls=%d", w.Code, w.Body.String(), s.calls.Load())
	}
	if _, err := paymentv1.NewPaymentServiceClient(other).GetCharge(context.Background(), &paymentv1.GetChargeRequest{RideId: "fast"}); err != nil {
		t.Fatalf("slow backend blocked independent connection: %v", err)
	}
	// Long-lived streams neither need an active unary slot nor inherit its timeout.
	stream, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	if _, hasDeadline := stream.Context().Deadline(); hasDeadline {
		t.Fatal("unary timeout applied to stream")
	}
	cancel()
	if err := awaitBackpressure(t, done); status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
	awaitBackpressure(t, s.finished)
	if _, err := client.GetCharge(context.Background(), &paymentv1.GetChargeRequest{RideId: "fast"}); err != nil {
		t.Fatalf("capacity did not recover: %v", err)
	}
	// A caller with no deadline still gets the helper's budget, and cancellation reaches the server.
	_, err = client.GetCharge(context.Background(), &paymentv1.GetChargeRequest{RideId: "slow"})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(err)
	}
	awaitBackpressure(t, s.finished)
	if _, err := client.GetCharge(context.Background(), &paymentv1.GetChargeRequest{RideId: "fast"}); err != nil {
		t.Fatal(err)
	}
}
