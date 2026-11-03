// Package provider is the boundary to the payment provider: the outside system that actually moves the money.
package provider

import (
	"context"
	"errors"
	"sync"
)

// The fake provider's card tokens, like a real provider's test cards.
const (
	TokenOK       = "tok_ok"
	TokenDeclined = "tok_declined"
	// TokenLostResponse takes the money but loses the first reply, as a timeout does: the caller can't tell whether
	// the charge happened. A repeat with the same key gets the original result.
	TokenLostResponse = "tok_lost_response"
	// TokenRefundRefused pays normally, but the provider refuses to refund the charge.
	TokenRefundRefused = "tok_refund_refused"
	// TokenRefundLost pays normally, but the first reply to each refund is lost after the money went back.
	TokenRefundLost = "tok_refund_lost"
)

// ErrOutcomeUnknown is what a timeout means to a payment: the charge may or may not have happened.
var ErrOutcomeUnknown = errors.New("provider response lost; outcome unknown")

type Request struct {
	// Key makes the call idempotent: the provider charges once per key, and answers a repeat with the first result.
	Key      string
	Token    string
	Amount   int64
	Currency string
}

// Result is a definite answer. A non-empty DeclineReason means no money moved; otherwise Reference identifies the
// charge at the provider.
type Result struct {
	Reference     string
	DeclineReason string
}

type RefundRequest struct {
	// Key makes the call idempotent, as in Request.
	Key string
	// ChargeReference is the Reference of the charge to give money back from.
	ChargeReference string
	Amount          int64
	Currency        string
}

// RefundResult is a definite answer. A non-empty FailureReason means no money moved; otherwise Reference identifies
// the refund at the provider.
type RefundResult struct {
	Reference     string
	FailureReason string
}

// Provider moves money. An error means the outcome is unknown, so the only safe next step is the same request again.
type Provider interface {
	Charge(ctx context.Context, req Request) (Result, error)
	Refund(ctx context.Context, req RefundRequest) (RefundResult, error)
}

// Fake is an in-memory provider for local development and tests. Its memory lasts as long as the process, which is
// enough to exercise retries, but restarting between a lost response and its retry would charge twice, and a charge
// from before a restart can't be refunded (unknown_charge).
type Fake struct {
	mu      sync.Mutex
	results map[string]Result
	// charges remembers what each Reference took, and with which card, so refunds can be checked against it.
	charges map[string]*fakeCharge
	refunds map[string]RefundResult
}

type fakeCharge struct {
	token    string
	amount   int64
	refunded int64
}

func NewFake() *Fake {
	return &Fake{results: map[string]Result{}, charges: map[string]*fakeCharge{}, refunds: map[string]RefundResult{}}
}

func (f *Fake) Charge(ctx context.Context, req Request) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if r, ok := f.results[req.Key]; ok {
		return r, nil
	}
	switch req.Token {
	case TokenOK, TokenLostResponse, TokenRefundRefused, TokenRefundLost, "": // a rider who never set a card uses the fake's default one
		r := Result{Reference: "fake_" + req.Key}
		f.results[req.Key] = r
		f.charges[r.Reference] = &fakeCharge{token: req.Token, amount: req.Amount}
		if req.Token == TokenLostResponse {
			return Result{}, ErrOutcomeUnknown
		}
		return r, nil
	case TokenDeclined:
		return Result{DeclineReason: "card_declined"}, nil
	default:
		return Result{DeclineReason: "invalid_card"}, nil
	}
}

// Refund gives money back, once per key. Like a real provider it refuses more than the charge took, which is what
// would catch a bug in the caller's own accounting.
func (f *Fake) Refund(ctx context.Context, req RefundRequest) (RefundResult, error) {
	if err := ctx.Err(); err != nil {
		return RefundResult{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if r, ok := f.refunds[req.Key]; ok {
		return r, nil
	}
	c, ok := f.charges[req.ChargeReference]
	switch {
	case !ok:
		return RefundResult{FailureReason: "unknown_charge"}, nil
	case c.token == TokenRefundRefused:
		return RefundResult{FailureReason: "refund_refused"}, nil
	case c.refunded+req.Amount > c.amount:
		return RefundResult{FailureReason: "exceeds_charge"}, nil
	}
	c.refunded += req.Amount
	r := RefundResult{Reference: "fake_refund_" + req.Key}
	f.refunds[req.Key] = r
	if c.token == TokenRefundLost {
		return RefundResult{}, ErrOutcomeUnknown
	}
	return r, nil
}

// Refunded totals the money actually given back, for tests to catch a double refund.
func (f *Fake) Refunded() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var total int64
	for _, c := range f.charges {
		total += c.refunded
	}
	return total
}

// Captured counts the charges that actually took money, for tests to catch a double charge.
func (f *Fake) Captured() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.results)
}
