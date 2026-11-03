package payment

import (
	"errors"
	"strings"
	"testing"
)

func TestChargeInputValidate(t *testing.T) {
	const id = "11111111-1111-1111-1111-111111111111"
	ok := ChargeInput{RideID: id, RiderID: id, DriverID: id, Kind: KindFare, Amount: Money{Amount: 1, Currency: "IDR"}}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}

	invalid := map[string]func(*ChargeInput){
		"bad ride":    func(in *ChargeInput) { in.RideID = "ride-1" },
		"no driver":   func(in *ChargeInput) { in.DriverID = "" },
		"no kind":     func(in *ChargeInput) { in.Kind = "" },
		"zero amount": func(in *ChargeInput) { in.Amount.Amount = 0 },
		"negative":    func(in *ChargeInput) { in.Amount.Amount = -5 },
		"no currency": func(in *ChargeInput) { in.Amount.Currency = "" },
	}
	for name, change := range invalid {
		in := ok
		change(&in)
		if err := in.Validate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("%s: expected ErrInvalidInput, got %v", name, err)
		}
	}
}

func TestRefundInputValidate(t *testing.T) {
	ok := RefundInput{RideID: "11111111-1111-1111-1111-111111111111", Amount: 1, IdempotencyKey: "k", Reason: "r"}
	if err := ok.Validate(); err != nil {
		t.Fatal(err)
	}
	invalid := map[string]func(*RefundInput){
		"bad ride":    func(in *RefundInput) { in.RideID = "ride-1" },
		"zero amount": func(in *RefundInput) { in.Amount = 0 },
		"no key":      func(in *RefundInput) { in.IdempotencyKey = " " },
		"long key":    func(in *RefundInput) { in.IdempotencyKey = strings.Repeat("k", 256) },
		"no reason":   func(in *RefundInput) { in.Reason = "" },
	}
	for name, change := range invalid {
		in := ok
		change(&in)
		if err := in.Validate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("%s: expected ErrInvalidInput, got %v", name, err)
		}
	}
}

// However a refund is cut up, the shares handed back must add up to exactly the shares the capture took.
func TestRefundSharesAddUpToTheCapture(t *testing.T) {
	const amount = 10_007
	wantPlatform, wantDriver := split(amount)
	for _, parts := range [][]int64{{amount}, {1, 1, 10_005}, {3_333, 3_333, 3_341}, {5_000, 5_007}} {
		var before, platform, driver int64
		for _, part := range parts {
			p, d := refundShares(before, part)
			if p+d != part {
				t.Fatalf("refundShares(%d, %d) = %d + %d", before, part, p, d)
			}
			before += part
			platform += p
			driver += d
		}
		if platform != wantPlatform || driver != wantDriver {
			t.Fatalf("%v gave back %d, %d; want %d, %d", parts, platform, driver, wantPlatform, wantDriver)
		}
	}
}

func TestCommissionKeepsEveryRupiah(t *testing.T) {
	for _, amount := range []int64{1, 3, 10_001, 99_999} {
		platform, driver := split(amount)
		if platform+driver != amount {
			t.Fatalf("split(%d) = %d + %d", amount, platform, driver)
		}
	}
	if platform, driver := split(10_000); platform != 2_000 || driver != 8_000 {
		t.Fatalf("split(10000) = %d, %d", platform, driver)
	}
}
