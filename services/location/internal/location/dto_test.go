package location

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestUpdateLocationInput(t *testing.T) {
	now := time.Now()
	valid := UpdateLocationInput{Location: &Location{Lat: -6.2, Lng: 106.8}, AccuracyMeters: 10, RecordedAt: now}
	with := func(f func(*UpdateLocationInput)) UpdateLocationInput {
		in := valid
		f(&in)
		return in
	}

	usable := map[string]UpdateLocationInput{
		"valid":            valid,
		"0,0 is real":      with(func(in *UpdateLocationInput) { in.Location = &Location{} }),
		"edge of geo":      with(func(in *UpdateLocationInput) { in.Location = &Location{Lat: maxLat, Lng: -180} }),
		"accuracy at max":  with(func(in *UpdateLocationInput) { in.AccuracyMeters = maxAccuracyMeters }),
		"small clock skew": with(func(in *UpdateLocationInput) { in.RecordedAt = now.Add(30 * time.Second) }),
	}
	for name, in := range usable {
		if err := in.Validate(now); err != nil || in.skipReason(now) != "" {
			t.Fatalf("%s: err %v, skip %q", name, err, in.skipReason(now))
		}
	}

	skipped := map[Outcome]UpdateLocationInput{
		OutcomeIgnoredInaccurate: with(func(in *UpdateLocationInput) { in.AccuracyMeters = 51 }),
		OutcomeIgnoredTooOld:     with(func(in *UpdateLocationInput) { in.RecordedAt = now.Add(-staleAfter - time.Second) }),
	}
	for want, in := range skipped {
		if err := in.Validate(now); err != nil || in.skipReason(now) != want {
			t.Fatalf("%s: err %v, skip %q", want, err, in.skipReason(now))
		}
	}

	invalid := map[string]UpdateLocationInput{
		"no location":    with(func(in *UpdateLocationInput) { in.Location = nil }),
		"lat range":      with(func(in *UpdateLocationInput) { in.Location = &Location{Lat: 86} }),
		"lng range":      with(func(in *UpdateLocationInput) { in.Location = &Location{Lng: -181} }),
		"NaN lat":        with(func(in *UpdateLocationInput) { in.Location = &Location{Lat: math.NaN()} }),
		"no accuracy":    with(func(in *UpdateLocationInput) { in.AccuracyMeters = 0 }),
		"NaN accuracy":   with(func(in *UpdateLocationInput) { in.AccuracyMeters = math.NaN() }),
		"Inf accuracy":   with(func(in *UpdateLocationInput) { in.AccuracyMeters = math.Inf(1) }),
		"no timestamp":   with(func(in *UpdateLocationInput) { in.RecordedAt = time.Time{} }),
		"future reading": with(func(in *UpdateLocationInput) { in.RecordedAt = now.Add(2 * time.Minute) }),
	}
	for name, in := range invalid {
		if err := in.Validate(now); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("%s: expected ErrInvalidInput, got %v", name, err)
		}
	}
}

func TestListNearbyInputValidate(t *testing.T) {
	in := ListNearbyInput{Location: &Location{Lat: -6.2, Lng: 106.8}}
	if err := in.Validate(); err != nil || in.RadiusMeters != defaultRadiusMeters || in.Limit != defaultNearbyLimit {
		t.Fatalf("defaults: got %+v, %v", in, err)
	}

	here := &Location{Lat: -6.2, Lng: 106.8}
	invalid := map[string]ListNearbyInput{
		"no location":     {},
		"lat range":       {Location: &Location{Lat: 86}},
		"NaN lng":         {Location: &Location{Lng: math.NaN()}},
		"negative radius": {Location: here, RadiusMeters: -1},
		"radius too big":  {Location: here, RadiusMeters: maxRadiusMeters + 1},
		"NaN radius":      {Location: here, RadiusMeters: math.NaN()},
		"negative limit":  {Location: here, Limit: -1},
		"limit too big":   {Location: here, Limit: maxNearbyLimit + 1},
	}
	for name, in := range invalid {
		if err := in.Validate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("%s: expected ErrInvalidInput, got %v", name, err)
		}
	}
}
