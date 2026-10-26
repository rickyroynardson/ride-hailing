package location

import (
	"context"
	"time"
)

type LocationService struct {
	repository *LocationRepository
}

func NewService(repo *LocationRepository) *LocationService {
	return &LocationService{repository: repo}
}

func (s *LocationService) UpdateDriverLocation(ctx context.Context, in UpdateLocationInput) error {
	now := time.Now()
	if err := in.Validate(now); err != nil {
		return err
	}
	if !in.Usable(now) {
		return nil
	}

	return s.repository.SetDriverLocation(ctx, in.DriverID, *in.Location, in.RecordedAt)
}

func (s *LocationService) GoOffline(ctx context.Context, driverID string) error {
	return s.repository.SetDriverOffline(ctx, driverID, time.Now())
}
