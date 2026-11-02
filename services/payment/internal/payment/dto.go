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
)

// Charge statuses and kinds, as stored.
const (
	StatusPending  = "pending"
	StatusCaptured = "captured"
	StatusFailed   = "failed"

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
}

// validToken bounds a token before it is stored; which tokens work is the provider's call.
func validToken(token string) bool {
	return strings.TrimSpace(token) != "" && len(token) <= 255
}
