package payment

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrInvalidInput = errors.New("invalid input")
	// ErrChargeNotFound also covers someone else's charge, so a user can't probe which rides were charged.
	ErrChargeNotFound = errors.New("charge not found")
	// ErrNotRefundable, ErrRefundTooLarge and ErrKeyReused are all refusals retrying won't change.
	ErrNotRefundable  = errors.New("only a captured charge can be refunded")
	ErrRefundTooLarge = errors.New("refund is more than what is left of the charge")
	ErrKeyReused      = errors.New("idempotency key reused with different parameters")
)

// Charge statuses and kinds, as stored.
const (
	StatusPending  = "pending"
	StatusCaptured = "captured"
	StatusFailed   = "failed"

	RefundPending   = "pending"
	RefundSucceeded = "succeeded"
	RefundFailed    = "failed"

	KindFare            = "fare"
	KindCancellationFee = "cancellation_fee"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Money is an amount in a currency's smallest unit used here.
type Money struct {
	Amount   int64
	Currency string
}

type ChargeInput struct {
	RideID   string
	RiderID  string
	DriverID string
	Kind     string
	Amount   Money
}

func (in ChargeInput) Validate() error {
	for _, id := range []string{in.RideID, in.RiderID, in.DriverID} {
		if !uuidPattern.MatchString(id) {
			return fmt.Errorf("%w: ride_id, rider_id and driver_id must be UUIDs", ErrInvalidInput)
		}
	}
	if in.Kind != KindFare && in.Kind != KindCancellationFee {
		return fmt.Errorf("%w: unknown charge kind", ErrInvalidInput)
	}
	if in.Amount.Amount <= 0 || in.Amount.Currency == "" {
		return fmt.Errorf("%w: amount must be positive, with a currency", ErrInvalidInput)
	}
	return nil
}

// Charge is a charge as its rider and driver see it.
type Charge struct {
	ID            string
	RideID        string
	Kind          string
	Status        string
	Amount        Money
	DeclineReason string
	// RefundedAmount is what has actually gone back to the rider.
	RefundedAmount int64
	Refunds        []Refund
}

type RefundInput struct {
	RideID         string
	Amount         int64
	IdempotencyKey string
	Reason         string
}

func (in RefundInput) Validate() error {
	if !uuidPattern.MatchString(in.RideID) {
		return fmt.Errorf("%w: ride_id must be a UUID", ErrInvalidInput)
	}
	if in.Amount <= 0 {
		return fmt.Errorf("%w: amount must be positive", ErrInvalidInput)
	}
	if strings.TrimSpace(in.IdempotencyKey) == "" || len(in.IdempotencyKey) > 255 {
		return fmt.Errorf("%w: idempotency_key is required, at most 255 bytes", ErrInvalidInput)
	}
	if strings.TrimSpace(in.Reason) == "" {
		return fmt.Errorf("%w: reason is required", ErrInvalidInput)
	}
	return nil
}

type Refund struct {
	ID            string
	Amount        Money
	Status        string
	Reason        string
	FailureReason string
}

// validToken bounds a token before it is stored; which tokens work is the provider's call.
func validToken(token string) bool {
	return strings.TrimSpace(token) != "" && len(token) <= 255
}
