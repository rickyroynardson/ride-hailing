package grpcserver

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ClientLimits bounds unary work per client connection, across all its methods. Queue time counts toward Timeout.
// Streams need separate lifetime/capacity policies and do not use these slots. These are local limits, not a
// cluster-wide rate limit or protection against callers that create a new connection for every request.
type ClientLimits struct {
	MaxConcurrent int
	MaxQueued     int
	Timeout       time.Duration
}

func defaultClientLimits() ClientLimits {
	return ClientLimits{MaxConcurrent: 32, MaxQueued: 32, Timeout: 3 * time.Second}
}

type unaryBulkhead struct {
	active  chan struct{}
	waiting chan struct{}
	timeout time.Duration
}

func newUnaryBulkhead(limits ClientLimits) (*unaryBulkhead, error) {
	if limits.MaxConcurrent <= 0 || limits.MaxQueued < 0 || limits.Timeout <= 0 {
		return nil, fmt.Errorf("client limits require positive concurrency and timeout, and nonnegative queue capacity")
	}
	return &unaryBulkhead{
		active: make(chan struct{}, limits.MaxConcurrent), waiting: make(chan struct{}, limits.MaxQueued), timeout: limits.Timeout,
	}, nil
}

func (b *unaryBulkhead) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	select {
	case b.active <- struct{}{}:
		return nil
	default:
	}
	select {
	case b.waiting <- struct{}{}:
		defer func() { <-b.waiting }()
	default:
		return status.Error(codes.ResourceExhausted, "backend concurrency and queue limits reached")
	}
	// No goroutines or unbounded waiter list. Waiting is cancelable and doesn't consume an active RPC slot.
	// This is a bounded queue, not a strict FIFO scheduler: new arrivals may acquire a newly freed active slot.
	select {
	case b.active <- struct{}{}:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	}
}

func (b *unaryBulkhead) intercept(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx, cancel := context.WithTimeout(ctx, b.timeout) // Preserve an earlier caller deadline.
	defer cancel()
	if err := b.acquire(ctx); err != nil {
		return err
	}
	defer func() { <-b.active }()
	// Cancellation can race with a free slot. Never deliberately dispatch an already canceled queued request.
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	return invoke(ctx, method, req, reply, cc, opts...)
}
