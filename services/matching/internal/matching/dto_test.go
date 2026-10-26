package matching

import (
	"errors"
	"math"
	"testing"
)

func TestStartInputValidate(t *testing.T) {
	const ride = "11111111-1111-1111-1111-111111111111"
	if err := (StartInput{RideID: ride, Pickup: &Location{}}).Validate(); err != nil {
		t.Fatal(err)
	}

	invalid := map[string]StartInput{
		"no ride id":  {Pickup: &Location{}},
		"bad ride id": {RideID: "ride-1", Pickup: &Location{}},
		"no pickup":   {RideID: ride},
		"lat range":   {RideID: ride, Pickup: &Location{Lat: 86}},
		"NaN lng":     {RideID: ride, Pickup: &Location{Lng: math.NaN()}},
	}
	for name, in := range invalid {
		if err := in.Validate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("%s: expected ErrInvalidInput, got %v", name, err)
		}
	}
}
