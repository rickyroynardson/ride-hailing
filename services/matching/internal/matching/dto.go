package matching

import (
	"errors"
	"fmt"
	"regexp"
	"time"
)

var (
	ErrInvalidInput     = errors.New("invalid input")
	ErrNotDriver        = errors.New("only approved drivers can answer offers")
	ErrOfferUnavailable = errors.New("offer is no longer available")
	// ErrAssignPending means ride's answer is unknown (it timed out or was unavailable). The offer stays accepted
	// and Tick keeps confirming it, so the driver can retry or simply wait.
	ErrAssignPending = errors.New("could not confirm the assignment yet; retry")
	// errRideHasOffer and errDriverTaken come from the offers unique indexes; StartMatching handles both.
	errRideHasOffer = errors.New("ride already has a pending offer")
	errDriverTaken  = errors.New("driver already offered this ride or has another pending offer")
)

const (
	// offerTTL is how long a driver has to answer before the offer moves on.
	offerTTL = 15 * time.Second
	// matchDeadline is how long matching keeps trying drivers before giving up as no_driver.
	matchDeadline = 2 * time.Minute
	// tickBatch caps how many waiting requests one Tick re-offers.
	tickBatch = 100
	// assignAttempts and assignBackoff bound AcceptOffer's retries when ride's answer is unknown.
	assignAttempts = 3
	assignBackoff  = 200 * time.Millisecond
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

// DriverOffer is a pending offer as the driver sees it.
type DriverOffer struct {
	ID        string
	RideID    string
	Pickup    Location
	ExpiresAt time.Time
}
