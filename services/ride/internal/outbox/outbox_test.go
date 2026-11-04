package outbox

import (
	"context"
	"errors"
	"testing"

	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type conflictingPayment struct{ paymentv1.PaymentServiceClient }

func (conflictingPayment) ChargeRide(context.Context, *paymentv1.ChargeRideRequest, ...grpc.CallOption) (*paymentv1.ChargeRideResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "different amount on this ride")
}

func TestChargeConflictIsSetAsideNotRetried(t *testing.T) {
	r := &Relay{payment: conflictingPayment{}}
	err := r.send(context.Background(), "charge_ride", []byte(`{"ride_id":"r1","amount":100,"currency":"IDR"}`))
	if !errors.Is(err, errChargeConflict) || !permanent(err) {
		t.Fatalf("charge conflict would be retried: %v", err)
	}
	if permanent(status.Error(codes.Unavailable, "down")) {
		t.Fatal("outage became a dead message")
	}
}
