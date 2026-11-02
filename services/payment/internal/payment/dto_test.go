package payment

import (
	"errors"
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
