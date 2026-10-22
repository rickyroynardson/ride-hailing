package ride

import (
	"errors"
	"testing"
)

func TestCreateRideInputValidate(t *testing.T) {
	a, b := &Location{Lat: -6.2, Lng: 106.8}, &Location{Lat: -6.3, Lng: 106.9}
	if err := (CreateRideInput{Pickup: a, Dropoff: b}).Validate(); err != nil {
		t.Fatal(err)
	}
	// 0,0 is a real coordinate and must be accepted.
	if err := (CreateRideInput{Pickup: &Location{}, Dropoff: b}).Validate(); err != nil {
		t.Fatal(err)
	}

	invalid := map[string]CreateRideInput{
		"no pickup":  {Dropoff: b},
		"no dropoff": {Pickup: a},
		"lat range":  {Pickup: &Location{Lat: 91}, Dropoff: b},
		"lng range":  {Pickup: a, Dropoff: &Location{Lng: -181}},
		"same point": {Pickup: a, Dropoff: &Location{Lat: a.Lat, Lng: a.Lng}},
	}
	for name, in := range invalid {
		if err := in.Validate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("%s: expected ErrInvalidInput, got %v", name, err)
		}
	}
}
