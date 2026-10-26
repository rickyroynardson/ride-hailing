package matching

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

var (
	ErrInvalidInput = errors.New("invalid input")
	// errRideHasOffer and errDriverTaken come from the offers unique indexes; StartMatching handles both.
	errRideHasOffer = errors.New("ride already has a pending offer")
	errDriverTaken  = errors.New("driver already offered this ride or has another pending offer")
)

const (
	// offerTTL is how long a driver has to answer before the offer moves on.
	offerTTL = 15 * time.Second
	// searchRadiusMeters and candidateLimit bound one search for drivers around the pickup.
	searchRadiusMeters = 3000
	candidateLimit     = 20
	// maxLat matches the location service's limit (Redis geo indexing).
	maxLat = 85.05112878
)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type Location struct {
	Lat float64
	Lng float64
}

type StartInput struct {
	RideID string
	// Pointer because proto3 reports an unset location as nil, while 0,0 is a real coordinate.
	Pickup *Location
}

func (in StartInput) Validate() error {
	if !uuidPattern.MatchString(in.RideID) {
		return fmt.Errorf("%w: ride_id must be a UUID", ErrInvalidInput)
	}
	l := in.Pickup
	if l == nil {
		return fmt.Errorf("%w: pickup is required", ErrInvalidInput)
	}
	if !(l.Lat >= -maxLat && l.Lat <= maxLat && l.Lng >= -180 && l.Lng <= 180) {
		return fmt.Errorf("%w: lat must be within ±%v and lng within ±180", ErrInvalidInput, maxLat)
	}
	return nil
}

type Offer struct {
	ID        string
	RideID    string
	DriverID  string
	ExpiresAt time.Time
}
