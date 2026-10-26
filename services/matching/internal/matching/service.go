package matching

import (
	"context"
	"errors"
	"fmt"
	"time"

	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
)

// callTimeout bounds each call to another service, so a slow dependency can't hold a matching request open.
const callTimeout = 3 * time.Second

type MatchingService struct {
	repository *MatchingRepository
	location   locationv1.LocationServiceClient
	ride       ridev1.RideServiceClient
}

func NewService(repo *MatchingRepository, location locationv1.LocationServiceClient, ride ridev1.RideServiceClient) *MatchingService {
	return &MatchingService{repository: repo, location: location, ride: ride}
}

// StartMatching offers the ride to the nearest available driver and returns the offer, or nil if no driver is
// available yet. It is idempotent: while the ride has a pending offer, calling it again returns that offer, so
// the ride service can safely retry after a timeout.
func (s *MatchingService) StartMatching(ctx context.Context, in StartInput) (*Offer, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}
	if o, err := s.repository.PendingOffer(ctx, in.RideID); err != nil || o != nil {
		return o, err
	}

	candidates, err := s.candidates(ctx, in)
	if err != nil {
		return nil, err
	}

	for _, driverID := range candidates {
		o, err := s.repository.CreateOffer(ctx, in.RideID, driverID, time.Now().Add(offerTTL))
		switch {
		case errors.Is(err, errDriverTaken):
			// Someone else offered this driver a ride between our checks and the insert; try the next one.
			continue
		case errors.Is(err, errRideHasOffer):
			// A concurrent StartMatching for this ride won; return its offer.
			return s.repository.PendingOffer(ctx, in.RideID)
		case err != nil:
			return nil, err
		}
		return &o, nil
	}
	// We created no offer, but a concurrent call may have: its pending offer also excluded its driver from our
	// candidates. Return that offer rather than "no driver available".
	return s.repository.PendingOffer(ctx, in.RideID)
}

// candidates returns drivers who could take the ride, nearest first: online near the pickup (location),
// not already offered it or holding another offer (our offers), and not on a ride (ride).
func (s *MatchingService) candidates(ctx context.Context, in StartInput) ([]string, error) {
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	nearby, err := s.location.ListNearbyDrivers(callCtx, &locationv1.ListNearbyDriversRequest{
		Location:     &locationv1.Location{Lat: in.Pickup.Lat, Lng: in.Pickup.Lng},
		RadiusMeters: searchRadiusMeters,
		Limit:        candidateLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("list nearby drivers: %w", err)
	}
	ids := make([]string, len(nearby.GetDrivers()))
	for i, d := range nearby.GetDrivers() {
		ids[i] = d.GetDriverId()
	}
	if len(ids) == 0 {
		return nil, nil
	}

	excluded, err := s.repository.ExcludedDrivers(ctx, in.RideID, ids)
	if err != nil {
		return nil, err
	}
	open := make([]string, 0, len(ids))
	for _, id := range ids {
		if !excluded[id] {
			open = append(open, id)
		}
	}
	if len(open) == 0 {
		return nil, nil
	}

	available, err := s.ride.FilterAvailableDrivers(callCtx, &ridev1.FilterAvailableDriversRequest{DriverIds: open})
	if err != nil {
		return nil, fmt.Errorf("filter available drivers: %w", err)
	}
	return available.GetDriverIds(), nil
}
