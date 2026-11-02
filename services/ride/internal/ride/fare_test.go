package ride

import (
	"math"
	"testing"
)

func TestDistanceKm(t *testing.T) {
	// One degree of latitude is about 111.2 km anywhere on Earth.
	if got := distanceKm(Location{Lat: 0, Lng: 0}, Location{Lat: 1, Lng: 0}); math.Abs(got-111.19) > 0.1 {
		t.Fatalf("got %.2f km", got)
	}
}

func TestPriceFare(t *testing.T) {
	// A hop of a few metres pays the minimum, not just the base fare.
	short := priceFare(Location{Lat: -6.2, Lng: 106.8}, Location{Lat: -6.2001, Lng: 106.8})
	if short.Amount != minFare || short.Currency != "IDR" {
		t.Fatalf("short trip: %+v", short)
	}

	// 1° of latitude: 111.19 km × 1.3 × 3,000 + 8,000 ≈ 441,600.
	long := priceFare(Location{Lat: 0, Lng: 0}, Location{Lat: 1, Lng: 0})
	if long.Amount < 441_000 || long.Amount > 442_000 {
		t.Fatalf("long trip: %+v", long)
	}

	// Same trip, same fare: CreateRide must match EstimateFare.
	if again := priceFare(Location{Lat: 0, Lng: 0}, Location{Lat: 1, Lng: 0}); again != long {
		t.Fatalf("not deterministic: %+v vs %+v", again, long)
	}
}
