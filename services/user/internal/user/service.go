package user

import "context"

type UserService struct {
	repository *UserRepository
}

func NewService(repo *UserRepository) *UserService {
	return &UserService{repository: repo}
}

func (s *UserService) GetMe(ctx context.Context, userID string) (User, *DriverProfile, error) {
	u, err := s.repository.GetUserByID(ctx, userID)
	if err != nil {
		return User{}, nil, err
	}

	driver, err := s.repository.GetDriverProfile(ctx, userID)
	if err != nil {
		return User{}, nil, err
	}

	return u, driver, nil
}

// RegisterDriver creates a pending driver profile; approval is manual for now.
func (s *UserService) RegisterDriver(ctx context.Context, userID string, in RegisterDriverInput) (DriverProfile, error) {
	if err := in.Validate(); err != nil {
		return DriverProfile{}, err
	}

	return s.repository.CreateDriverProfile(ctx, userID, in)
}
