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

	// Nearby search defaults and caps; the caps keep one request from scanning a whole city.
	defaultRadiusMeters = 3000
	maxRadiusMeters     = 10000
	defaultNearbyLimit  = 10
	maxNearbyLimit      = 50
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

// Outcome is what happened to a valid reading. Ignored readings are not errors: the device did nothing wrong,
// and its next reading may be stored.
type Outcome string

const (
	OutcomeStored            Outcome = "stored"
	OutcomeIgnoredInaccurate Outcome = "ignored_inaccurate"
	OutcomeIgnoredTooOld     Outcome = "ignored_too_old"
	OutcomeIgnoredOutOfOrder Outcome = "ignored_out_of_order"
)

// skipReason reports why a valid reading should not be stored, or "" if it should be.
func (in UpdateLocationInput) skipReason(now time.Time) Outcome {
	switch {
	case in.AccuracyMeters > maxAccuracyMeters:
		return OutcomeIgnoredInaccurate
	case now.Sub(in.RecordedAt) > staleAfter:
		return OutcomeIgnoredTooOld
	default:
		return ""
	}
}

type ListNearbyInput struct {
	// Pointer because proto3 reports an unset location as nil, while 0,0 is a real coordinate.
	Location     *Location
	RadiusMeters float64
	Limit        int
}

// Validate fills defaults for unset (zero) radius and limit, then rejects anything out of range.
func (in *ListNearbyInput) Validate() error {
	l := in.Location
	if l == nil {
		return fmt.Errorf("%w: location is required", ErrInvalidInput)
	}
	if !(l.Lat >= -maxLat && l.Lat <= maxLat && l.Lng >= -180 && l.Lng <= 180) {
		return fmt.Errorf("%w: lat must be within ±%v and lng within ±180", ErrInvalidInput, maxLat)
	}
	if in.RadiusMeters == 0 {
		in.RadiusMeters = defaultRadiusMeters
	}
	if !(in.RadiusMeters > 0 && in.RadiusMeters <= maxRadiusMeters) {
		return fmt.Errorf("%w: radius_meters must be within (0, %d]", ErrInvalidInput, maxRadiusMeters)
	}
	if in.Limit == 0 {
		in.Limit = defaultNearbyLimit
	}
	if in.Limit < 1 || in.Limit > maxNearbyLimit {
		return fmt.Errorf("%w: limit must be within [1, %d]", ErrInvalidInput, maxNearbyLimit)
	}
	return nil
}

type NearbyDriver struct {
	DriverID       string
	Location       Location
	DistanceMeters float64
}
