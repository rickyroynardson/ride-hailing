package location

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type LocationService struct {
	repository *LocationRepository
}

func NewService(repo *LocationRepository) *LocationService {
	return &LocationService{repository: repo}
}

// updatesTotal counts accepted readings by outcome, so a rise in ignored ones (bad GPS, broken app clocks) is
// visible. Rejected and rate-limited calls show up in the gRPC metrics by status code instead.
var updatesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "location_updates_total",
	Help: "Accepted driver location readings, by outcome.",
}, []string{"outcome"})

func (s *LocationService) UpdateDriverLocation(ctx context.Context, in UpdateLocationInput) (Outcome, error) {
	now := time.Now()
	if err := in.Validate(now); err != nil {
		return "", err
	}

	outcome := in.skipReason(now)
	if outcome == "" {
		stored, err := s.repository.SetDriverLocation(ctx, in.DriverID, *in.Location, in.RecordedAt)
		if err != nil {
			return "", err
		}
		outcome = OutcomeIgnoredOutOfOrder
		if stored {
			outcome = OutcomeStored
		}
	}

	updatesTotal.WithLabelValues(string(outcome)).Inc()
	return outcome, nil
}

func (s *LocationService) GoOffline(ctx context.Context, driverID string) error {
	return s.repository.SetDriverOffline(ctx, driverID, time.Now())
}

func (s *LocationService) ListNearbyDrivers(ctx context.Context, in ListNearbyInput) ([]NearbyDriver, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}

	return s.repository.FindNearby(ctx, *in.Location, in.RadiusMeters, in.Limit, time.Now().Add(-staleAfter))
}

// GetDriverLocation returns the driver's current position, or nil if they are offline or stale.
func (s *LocationService) GetDriverLocation(ctx context.Context, driverID string) (*Location, error) {
	if driverID == "" {
		return nil, fmt.Errorf("%w: driver_id is required", ErrInvalidInput)
	}
	return s.repository.DriverLocation(ctx, driverID, time.Now().Add(-staleAfter))
}

// PruneStale removes drivers who went stale without calling GoOffline, plus expired offline markers.
func (s *LocationService) PruneStale(ctx context.Context) (int, error) {
	return s.repository.PruneStale(ctx, time.Now().Add(-staleAfter))
}
