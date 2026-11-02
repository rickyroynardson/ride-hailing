package provider

import (
	"context"
	"errors"
	"testing"
)

func TestFakeLostResponseChargesOnce(t *testing.T) {
	f, ctx := NewFake(), context.Background()
	req := Request{Key: "k", Token: TokenLostResponse, Amount: 100, Currency: "IDR"}

	if _, err := f.Charge(ctx, req); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("first call: got %v", err)
	}
	r, err := f.Charge(ctx, req)
	if err != nil || r.Reference == "" {
		t.Fatalf("repeat: %+v, %v", r, err)
	}
	if f.Captured() != 1 {
		t.Fatalf("captured %d, want 1", f.Captured())
	}
}

func TestFakeDecline(t *testing.T) {
	f, ctx := NewFake(), context.Background()
	for token, reason := range map[string]string{TokenDeclined: "card_declined", "tok_nonsense": "invalid_card"} {
		r, err := f.Charge(ctx, Request{Key: token, Token: token})
		if err != nil || r.DeclineReason != reason {
			t.Fatalf("%s: %+v, %v", token, r, err)
		}
	}
	if f.Captured() != 0 {
		t.Fatalf("a decline took money")
	}
}
