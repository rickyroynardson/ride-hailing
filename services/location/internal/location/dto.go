package location

import (
	"errors"
	"fmt"
	"math"
	"time"
)

var (
	ErrInvalidInput = errors.New("invalid input")
	ErrNotDriver    = errors.New("only approved drivers can report location")
	ErrRateLimited  = errors.New("location updates too frequent, at most one per second")
)

const (
	// maxLat is the limit of Redis geo indexing (Web Mercator), slightly inside ±90.
	maxLat = 85.05112878
	// maxAccuracyMeters drops GPS fixes too vague to match on, e.g. indoors or between tall buildings.
	maxAccuracyMeters = 50
	// staleAfter is how old a reading can be before the driver no longer counts as online.
	staleAfter = 30 * time.Second
	// maxClockSkew tolerates device clocks running slightly ahead.
	maxClockSkew = time.Minute
)

type Location struct {
	Lat float64
	Lng float64
}

type UpdateLocationInput struct {
	DriverID string
	// Pointer because proto3 reports an unset location as nil, while 0,0 is a real coordinate.
	Location       *Location
	AccuracyMeters float64
	RecordedAt     time.Time
}

// Validate rejects malformed readings. Comparisons are written so NaN fails them.
func (in UpdateLocationInput) Validate(now time.Time) error {
	l := in.Location
	if l == nil {
		return fmt.Errorf("%w: location is required", ErrInvalidInput)
	}
	if !(l.Lat >= -maxLat && l.Lat <= maxLat && l.Lng >= -180 && l.Lng <= 180) {
		return fmt.Errorf("%w: lat must be within ±%v and lng within ±180", ErrInvalidInput, maxLat)
	}
	if !(in.AccuracyMeters > 0) || math.IsInf(in.AccuracyMeters, 0) {
		return fmt.Errorf("%w: accuracy_meters must be positive", ErrInvalidInput)
	}
	if in.RecordedAt.IsZero() {
		return fmt.Errorf("%w: recorded_at is required", ErrInvalidInput)
	}
	if in.RecordedAt.After(now.Add(maxClockSkew)) {
		return fmt.Errorf("%w: recorded_at is in the future", ErrInvalidInput)
	}
	return nil
}

// Usable reports whether a valid reading is good enough to store. Unusable readings are dropped silently:
// the device did nothing wrong, and its next reading will replace this one.
func (in UpdateLocationInput) Usable(now time.Time) bool {
	return in.AccuracyMeters <= maxAccuracyMeters && now.Sub(in.RecordedAt) <= staleAfter
}
