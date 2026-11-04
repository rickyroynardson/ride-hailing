package ride

import (
	"errors"
	"math"
	"testing"
)

func TestCreateRideInputValidate(t *testing.T) {
	a, b := &Location{Lat: -6.2, Lng: 106.8}, &Location{Lat: -6.3, Lng: 106.9}
	const key = "11111111-1111-1111-1111-111111111111"
	if err := (CreateRideInput{RequestID: key, Pickup: a, Dropoff: b}).Validate(); err != nil {
		t.Fatal(err)
	}
	// 0,0 is a real coordinate and must be accepted.
	if err := (CreateRideInput{RequestID: key, Pickup: &Location{}, Dropoff: b}).Validate(); err != nil {
		t.Fatal(err)
	}

	invalid := map[string]CreateRideInput{
		"no pickup":  {Dropoff: b},
		"no dropoff": {Pickup: a},
		"lat range":  {Pickup: &Location{Lat: 91}, Dropoff: b},
		"lng range":  {Pickup: a, Dropoff: &Location{Lng: -181}},
		"same point": {Pickup: a, Dropoff: &Location{Lat: a.Lat, Lng: a.Lng}},
		"nan":        {Pickup: &Location{Lat: math.NaN()}, Dropoff: b},
	}
	for name, in := range invalid {
		in.RequestID = key
		if err := in.Validate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("%s: expected ErrInvalidInput, got %v", name, err)
		}
	}
	for _, id := range []string{"", "not-a-uuid"} {
		if err := (CreateRideInput{RequestID: id, Pickup: a, Dropoff: b}).Validate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("request_id %q accepted: %v", id, err)
		}
	}
}
