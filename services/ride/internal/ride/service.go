package ride

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"time"

	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
)

const (
	// driverLocationEvery is how often a watched ride's driver position is refreshed.
	driverLocationEvery = 2 * time.Second
	// locationTimeout is kept short because a ride change waits behind this call before it is sent.
	locationTimeout = time.Second
)

type RideService struct {
	repository *RideRepository
	hub        *Hub
	location   locationv1.LocationServiceClient
}

func NewService(repo *RideRepository, hub *Hub, location locationv1.LocationServiceClient) *RideService {
	return &RideService{repository: repo, hub: hub, location: location}
}

// WatchRide calls send with the ride's current state, then again whenever it changes, until the ride finishes or ctx
// ends. Only the ride's rider or driver may watch; anyone else gets ErrRideNotFound.
// ponytail: each stream polls location; have location publish positions (Redis pub/sub) if watchers multiply
func (s *RideService) WatchRide(ctx context.Context, rideID, userID string, send func(Ride) error) error {
	if !uuidPattern.MatchString(rideID) {
		return fmt.Errorf("%w: ride_id must be a UUID", ErrInvalidInput)
	}
	// A stream can run for an hour, polling all the way: tracing that would hang hundreds of spans off one RPC. Its
	// own span still records that it ran; what it reads does not, the same as background loops.
	ctx = telemetry.Untraced(ctx)

	// Subscribe before the first read, so a change right after that read still wakes this stream.
	changed, unsubscribe := s.hub.Subscribe(rideID)
	defer unsubscribe()
	ticker := time.NewTicker(driverLocationEvery)
	defer ticker.Stop()

	var current, sent Ride
	reload := true
	for {
		if reload {
			r, err := s.repository.GetRide(ctx, rideID, userID)
			if err != nil {
				return err
			}
			r.DriverLocation = current.DriverLocation
			current = r
		}
		current.DriverLocation = s.driverLocation(ctx, current)

		if !reflect.DeepEqual(current, sent) {
			if err := send(current); err != nil {
				return err
			}
			sent = current
		}
		if current.finished() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
			reload = true
		case <-ticker.C:
			reload = false
		}
	}
}

// driverLocation returns where the ride's driver is now. If location can't say, the last known position stands:
// a stale car on the map beats ending the rider's stream.
func (s *RideService) driverLocation(ctx context.Context, r Ride) *Location {
	if !r.hasActiveDriver() {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, locationTimeout)
	defer cancel()

	res, err := s.location.GetDriverLocation(ctx, &locationv1.GetDriverLocationRequest{DriverId: r.DriverID})
	if err != nil {
		return r.DriverLocation
	}
	if res.GetLocation() == nil {
		return nil
	}
	return &Location{Lat: res.GetLocation().GetLat(), Lng: res.GetLocation().GetLng()}
}

func (s *RideService) CreateRide(ctx context.Context, in CreateRideInput) (string, error) {
	if err := in.Validate(); err != nil {
		return "", err
	}

	return s.repository.CreateRide(ctx, in)
}

// maxFilterIDs bounds one FilterAvailableDrivers call; matching asks about a few dozen nearby drivers at a time.
const maxFilterIDs = 100

// FilterAvailableDrivers returns the drivers not on an active ride, keeping the caller's order (nearest first).
func (s *RideService) FilterAvailableDrivers(ctx context.Context, driverIDs []string) ([]string, error) {
	if len(driverIDs) > maxFilterIDs {
		return nil, ErrTooManyIDs
	}
	ids := make([]string, 0, len(driverIDs))
	for _, id := range driverIDs {
		// Not a UUID can't be a driver on a ride, and would make the uuid[] cast fail the whole query.
		if uuidPattern.MatchString(id) {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return []string{}, nil
	}

	busy, err := s.repository.BusyDrivers(ctx, ids)
	if err != nil {
		return nil, err
	}

	available := make([]string, 0, len(ids))
	for _, id := range ids {
		if !busy[id] {
			available = append(available, id)
		}
	}
	return available, nil
}

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func (s *RideService) AssignDriver(ctx context.Context, rideID, driverID string) error {
	if !uuidPattern.MatchString(rideID) || !uuidPattern.MatchString(driverID) {
		return fmt.Errorf("%w: ride_id and driver_id must be UUIDs", ErrInvalidInput)
	}
	return s.repository.AssignDriver(ctx, rideID, driverID)
}

func (s *RideService) MarkNoDriver(ctx context.Context, rideID string) error {
	if !uuidPattern.MatchString(rideID) {
		return fmt.Errorf("%w: ride_id must be a UUID", ErrInvalidInput)
	}
	return s.repository.MarkNoDriver(ctx, rideID)
}

func (s *RideService) CancelRide(ctx context.Context, rideID, riderID string) error {
	if !uuidPattern.MatchString(rideID) {
		return fmt.Errorf("%w: ride_id must be a UUID", ErrInvalidInput)
	}
	return s.repository.CancelRide(ctx, rideID, riderID)
}

// StartRide and CompleteRide only check that the caller is the ride's driver, not that the token still carries the
// driver claim: a driver suspended mid-trip must still be able to finish it.
func (s *RideService) StartRide(ctx context.Context, rideID, driverID string) error {
	if !uuidPattern.MatchString(rideID) {
		return fmt.Errorf("%w: ride_id must be a UUID", ErrInvalidInput)
	}
	return s.repository.StartTrip(ctx, rideID, driverID)
}

func (s *RideService) CompleteRide(ctx context.Context, rideID, driverID string) error {
	if !uuidPattern.MatchString(rideID) {
		return fmt.Errorf("%w: ride_id must be a UUID", ErrInvalidInput)
	}
	return s.repository.CompleteTrip(ctx, rideID, driverID)
}
