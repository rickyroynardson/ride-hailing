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

func TestFakeIdempotencyRejectsChangedParameters(t *testing.T) {
	f, ctx := NewFake(), context.Background()
	req := Request{Key: "stable", Token: TokenDeclined, Amount: 100, Currency: "IDR"}
	first, err := f.Charge(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.Charge(ctx, req)
	if err != nil || first != again {
		t.Fatalf("decline wasn't cached: %v %v", again, err)
	}
	req.Token = TokenOK
	if _, err := f.Charge(ctx, req); !errors.Is(err, ErrKeyReused) {
		t.Fatalf("changed card on same key: %v", err)
	}
	if f.Captured() != 0 {
		t.Fatal("conflicting retry took money")
	}
	req.Key = "new-intent"
	paid, err := f.Charge(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	refund := RefundRequest{Key: "refund", ChargeReference: paid.Reference, Amount: 50, Currency: "IDR"}
	if _, err := f.Refund(ctx, refund); err != nil {
		t.Fatal(err)
	}
	refund.Amount = 100
	if _, err := f.Refund(ctx, refund); !errors.Is(err, ErrKeyReused) {
		t.Fatalf("changed refund: %v", err)
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

func TestFakeRefund(t *testing.T) {
	f, ctx := NewFake(), context.Background()
	charge := func(key, token string) string {
		t.Helper()
		r, err := f.Charge(ctx, Request{Key: key, Token: token, Amount: 100})
		if err != nil {
			t.Fatal(err)
		}
		return r.Reference
	}

	ok := charge("a", TokenOK)
	if r, err := f.Refund(ctx, RefundRequest{Key: "r1", ChargeReference: ok, Amount: 60}); err != nil || r.Reference == "" {
		t.Fatalf("refund: %+v, %v", r, err)
	}
	if r, _ := f.Refund(ctx, RefundRequest{Key: "r2", ChargeReference: ok, Amount: 60}); r.FailureReason != "exceeds_charge" {
		t.Fatalf("over-refund: %+v", r)
	}
	if r, _ := f.Refund(ctx, RefundRequest{Key: "r3", ChargeReference: charge("b", TokenRefundRefused), Amount: 1}); r.FailureReason != "refund_refused" {
		t.Fatalf("refused: %+v", r)
	}
	if r, _ := f.Refund(ctx, RefundRequest{Key: "r4", ChargeReference: "nope", Amount: 1}); r.FailureReason != "unknown_charge" {
		t.Fatalf("unknown: %+v", r)
	}

	lost := RefundRequest{Key: "r5", ChargeReference: charge("c", TokenRefundLost), Amount: 100}
	if _, err := f.Refund(ctx, lost); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("lost refund: %v", err)
	}
	if r, err := f.Refund(ctx, lost); err != nil || r.Reference == "" {
		t.Fatalf("repeat: %+v, %v", r, err)
	}
	if got := f.Refunded(); got != 160 {
		t.Fatalf("refunded %d, want 160", got)
	}
}
