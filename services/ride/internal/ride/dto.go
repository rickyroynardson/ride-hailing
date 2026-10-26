package ride

import (
	"errors"
	"fmt"
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
)

type Location struct {
	Lat float64
	Lng float64
}

type CreateRideInput struct {
	RiderID string
	// Pointers because proto3 reports an unset location as nil, while 0,0 is a real coordinate.
	Pickup  *Location
	Dropoff *Location
}

func (in CreateRideInput) Validate() error {
	if in.Pickup == nil || in.Dropoff == nil {
		return fmt.Errorf("%w: pickup and dropoff are required", ErrInvalidInput)
	}
	for _, l := range []Location{*in.Pickup, *in.Dropoff} {
		if l.Lat < -90 || l.Lat > 90 || l.Lng < -180 || l.Lng > 180 {
			return fmt.Errorf("%w: lat must be within ±90 and lng within ±180", ErrInvalidInput)
		}
	}
	if *in.Pickup == *in.Dropoff {
		return fmt.Errorf("%w: pickup and dropoff must differ", ErrInvalidInput)
	}

	return nil
}
