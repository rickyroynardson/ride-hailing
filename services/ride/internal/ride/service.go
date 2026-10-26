package ride

import (
	"context"
	"regexp"
)

type RideService struct {
	repository *RideRepository
}

func NewService(repo *RideRepository) *RideService {
	return &RideService{repository: repo}
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
