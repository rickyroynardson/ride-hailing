package ride

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidInput = errors.New("invalid input")
	ErrActiveRide   = errors.New("rider already has an active ride")
	ErrTooManyIDs   = errors.New("too many driver ids")
	// ErrRideUnavailable and ErrDriverBusy map to different status codes so matching can tell them apart:
	// the first ends matching for the ride, the second only rules out this driver.
	ErrRideUnavailable = errors.New("ride is no longer waiting for a driver")
	ErrDriverBusy      = errors.New("driver already has an active ride")
	// ErrRideNotFound also covers another rider's ride, so a rider can't probe which ride IDs exist.
	ErrRideNotFound = errors.New("ride not found")
	// ErrPaymentOverdue blocks new rides until the rider settles a declined charge.
	ErrPaymentOverdue = errors.New("rider has an unpaid ride")
)

type Location struct {
	Lat float64
	Lng float64
}

// Ride is a ride as its rider and driver see it.
type Ride struct {
	ID           string
	Status       string
	CancelReason string
	DriverID     string
	Pickup       Location
	Dropoff      Location
	// DriverLocation is nil unless the ride has a driver on the way or on the trip, and they are online.
	DriverLocation *Location
	RequestedAt    time.Time
	AcceptedAt     *time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
	// FareAmount and FareCurrency are nil on rides from before pricing.
	FareAmount   *int64
	FareCurrency *string
}

// hasActiveDriver reports whether a driver is heading to the pickup or on the trip.
func (r Ride) hasActiveDriver() bool {
	return r.Status == "accepted" || r.Status == "in_progress"
}

// finished reports whether the ride reached a state it never leaves.
func (r Ride) finished() bool {
	return r.Status == "completed" || r.Status == "cancelled"
}

type CreateRideInput struct {
	RiderID string
	// Pointers because proto3 reports an unset location as nil, while 0,0 is a real coordinate.
	Pickup  *Location
	Dropoff *Location
}

func (in CreateRideInput) Validate() error {
	return validateTrip(in.Pickup, in.Dropoff)
}

func validateTrip(pickup, dropoff *Location) error {
	if pickup == nil || dropoff == nil {
		return fmt.Errorf("%w: pickup and dropoff are required", ErrInvalidInput)
	}
	for _, l := range []Location{*pickup, *dropoff} {
		if l.Lat < -90 || l.Lat > 90 || l.Lng < -180 || l.Lng > 180 {
			return fmt.Errorf("%w: lat must be within ±90 and lng within ±180", ErrInvalidInput)
		}
	}
	if *pickup == *dropoff {
		return fmt.Errorf("%w: pickup and dropoff must differ", ErrInvalidInput)
	}

	return nil
}
