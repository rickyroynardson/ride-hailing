package grpcserver

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func waitQueued(t *testing.T, b *unaryBulkhead, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for len(b.waiting) != want {
		if time.Now().After(deadline) {
			t.Fatalf("queued=%d, want %d", len(b.waiting), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestUnaryBulkheadBoundsAndCancellation(t *testing.T) {
	b, err := newUnaryBulkhead(ClientLimits{MaxConcurrent: 1, MaxQueued: 1, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	entered := make(chan struct{}, 4)
	invoke := func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		calls.Add(1)
		entered <- struct{}{}
		<-ctx.Done()
		return status.FromContextError(ctx.Err()).Err()
	}
	call := func(ctx context.Context) <-chan error {
		done := make(chan error, 1)
		go func() { done <- b.intercept(ctx, "/payment/slow", nil, nil, nil, invoke) }()
		return done
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	first := call(firstCtx)
	receive(t, entered)
	queuedCtx, cancelQueued := context.WithCancel(context.Background())
	defer cancelQueued()
	queued := call(queuedCtx)
	waitQueued(t, b, 1)
	if err := b.intercept(context.Background(), "/payment/slow", nil, nil, nil, invoke); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("overflow: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("dispatched queued/rejected calls: %d", calls.Load())
	}
	cancelQueued()
	if err := receive(t, queued); status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
	if len(b.waiting) != 0 || len(b.active) != 1 {
		t.Fatal("queued cancellation leaked capacity or released the active slot")
	}
	// Fill the released queue slot; ending the active call must admit this waiter.
	nextCtx, cancelNext := context.WithCancel(context.Background())
	defer cancelNext()
	next := call(nextCtx)
	waitQueued(t, b, 1)
	cancelFirst()
	if err := receive(t, first); status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
	receive(t, entered)
	cancelNext()
	if err := receive(t, next); status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
	if len(b.active) != 0 || len(b.waiting) != 0 || calls.Load() != 2 {
		t.Fatal("capacity was not recovered")
	}
}

func TestUnaryBulkheadQueueDeadline(t *testing.T) {
	b, _ := newUnaryBulkhead(ClientLimits{MaxConcurrent: 1, MaxQueued: 1, Timeout: 40 * time.Millisecond})
	// Hold the active slot independently so its deadline can't free it for the queued request.
	b.active <- struct{}{}
	defer func() { <-b.active }()
	invoked := false
	err := b.intercept(context.Background(), "/payment/slow", nil, nil, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			invoked = true
			return nil
		})
	if status.Code(err) != codes.DeadlineExceeded || invoked || len(b.waiting) != 0 {
		t.Fatalf("error=%v invoked=%v queued=%d", err, invoked, len(b.waiting))
	}
}

func TestUnaryBulkheadPreservesEarlierDeadlineAndReleasesOnError(t *testing.T) {
	b, _ := newUnaryBulkhead(ClientLimits{MaxConcurrent: 1, Timeout: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	want, _ := ctx.Deadline()
	err := b.intercept(ctx, "/payment/fail", nil, nil, nil,
		func(ctx context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			if got, ok := ctx.Deadline(); !ok || !got.Equal(want) {
				t.Errorf("deadline=%v, want %v", got, want)
			}
			return status.Error(codes.Unavailable, "backend failed")
		})
	if status.Code(err) != codes.Unavailable || len(b.active) != 0 {
		t.Fatalf("error=%v active=%d", err, len(b.active))
	}
	cancel()
	err = b.intercept(ctx, "/payment/canceled", nil, nil, nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			t.Error("invoked canceled request")
			return nil
		})
	if status.Code(err) != codes.Canceled || len(b.active) != 0 {
		t.Fatal(err)
	}
}

func TestClientLimitsValidation(t *testing.T) {
	for _, limits := range []ClientLimits{{}, {MaxConcurrent: -1, Timeout: time.Second}, {MaxConcurrent: 1, MaxQueued: -1, Timeout: time.Second}, {MaxConcurrent: 1, Timeout: -time.Second}} {
		conn, err := NewClientWithLimits("127.0.0.1:5000", limits, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			conn.Close()
			t.Errorf("accepted %+v", limits)
		}
	}
}
