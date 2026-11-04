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

// ErrKeyReused means the caller changed the parameters of an operation whose outcome may already be final.
var ErrKeyReused = errors.New("provider idempotency key used with different parameters")

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
	mu       sync.Mutex
	results  map[string]Result
	requests map[string]Request
	// charges remembers what each Reference took, and with which card, so refunds can be checked against it.
	charges        map[string]*fakeCharge
	refunds        map[string]RefundResult
	refundRequests map[string]RefundRequest
}

type fakeCharge struct {
	token    string
	amount   int64
	refunded int64
	currency string
}

func NewFake() *Fake {
	return &Fake{results: map[string]Result{}, requests: map[string]Request{}, charges: map[string]*fakeCharge{}, refunds: map[string]RefundResult{}, refundRequests: map[string]RefundRequest{}}
}

func (f *Fake) Charge(ctx context.Context, req Request) (Result, error) {
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if r, ok := f.results[req.Key]; ok {
		if f.requests[req.Key] != req {
			return Result{}, ErrKeyReused
		}
		return r, nil
	}
	f.requests[req.Key] = req
	switch req.Token {
	case TokenOK, TokenLostResponse, TokenRefundRefused, TokenRefundLost, "": // a rider who never set a card uses the fake's default one
		r := Result{Reference: "fake_" + req.Key}
		f.results[req.Key] = r
		f.charges[r.Reference] = &fakeCharge{token: req.Token, amount: req.Amount, currency: req.Currency}
		if req.Token == TokenLostResponse {
			return Result{}, ErrOutcomeUnknown
		}
		return r, nil
	case TokenDeclined:
		f.results[req.Key] = Result{DeclineReason: "card_declined"}
	default:
		f.results[req.Key] = Result{DeclineReason: "invalid_card"}
	}
	return f.results[req.Key], nil
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
		if f.refundRequests[req.Key] != req {
			return RefundResult{}, ErrKeyReused
		}
		return r, nil
	}
	f.refundRequests[req.Key] = req
	c, ok := f.charges[req.ChargeReference]
	switch {
	case !ok:
		f.refunds[req.Key] = RefundResult{FailureReason: "unknown_charge"}
	case c.token == TokenRefundRefused:
		f.refunds[req.Key] = RefundResult{FailureReason: "refund_refused"}
	case req.Amount <= 0 || req.Amount > c.amount-c.refunded || req.Currency != c.currency:
		f.refunds[req.Key] = RefundResult{FailureReason: "exceeds_charge"}
	}
	if r, ok := f.refunds[req.Key]; ok {
		return r, nil
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
	n := 0
	for _, r := range f.results {
		if r.Reference != "" {
			n++
		}
	}
	return n
}
