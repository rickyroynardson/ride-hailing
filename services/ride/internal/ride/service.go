package ride

import "context"

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
