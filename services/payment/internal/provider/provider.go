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

// Provider charges cards. An error means the outcome is unknown, so the only safe next step is the same Request again.
type Provider interface {
	Charge(ctx context.Context, req Request) (Result, error)
}

// Fake is an in-memory provider for local development and tests. Its idempotency memory lasts as long as the process,
// which is enough to exercise retries, but restarting between a lost response and its retry would charge twice.
type Fake struct {
	mu      sync.Mutex
	results map[string]Result
}

func NewFake() *Fake {
	return &Fake{results: map[string]Result{}}
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
	case TokenOK, TokenLostResponse, "": // a rider who never set a card uses the fake's default one
		r := Result{Reference: "fake_" + req.Key}
		f.results[req.Key] = r
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

// Captured counts the charges that actually took money, for tests to catch a double charge.
func (f *Fake) Captured() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.results)
}
