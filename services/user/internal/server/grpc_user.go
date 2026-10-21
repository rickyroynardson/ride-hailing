package server

import (
	"context"

	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/user"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type UserServer struct {
	userv1.UnimplementedUserServiceServer
	service *user.UserService
}

func NewUserServer(service *user.UserService) *UserServer {
	return &UserServer{service: service}
}

func (s *UserServer) GetMe(ctx context.Context, _ *userv1.GetMeRequest) (*userv1.GetMeResponse, error) {
	u, driver, err := s.service.GetMe(ctx, userIDFromContext(ctx))
	if err != nil {
		return nil, toStatus(err)
	}

	res := &userv1.GetMeResponse{User: toUserProto(u)}
	if driver != nil {
		res.DriverProfile = toDriverProfileProto(*driver)
	}
	return res, nil
}

func (s *UserServer) RegisterDriver(ctx context.Context, req *userv1.RegisterDriverRequest) (*userv1.RegisterDriverResponse, error) {
	driver, err := s.service.RegisterDriver(ctx, userIDFromContext(ctx), user.RegisterDriverInput{
		LicenseNumber: req.GetLicenseNumber(),
		VehiclePlate:  req.GetVehiclePlate(),
		VehicleModel:  req.GetVehicleModel(),
	})
	if err != nil {
		return nil, toStatus(err)
	}

	return &userv1.RegisterDriverResponse{DriverProfile: toDriverProfileProto(driver)}, nil
}

func toUserProto(u user.User) *userv1.User {
	return &userv1.User{
		Id:        u.ID,
		Email:     u.Email,
		Name:      u.Name,
		Phone:     u.Phone,
		CreatedAt: timestamppb.New(u.CreatedAt),
	}
}

func toDriverProfileProto(d user.DriverProfile) *userv1.DriverProfile {
	return &userv1.DriverProfile{
		LicenseNumber: d.LicenseNumber,
		VehiclePlate:  d.VehiclePlate,
		VehicleModel:  d.VehicleModel,
		Status:        d.Status,
		CreatedAt:     timestamppb.New(d.CreatedAt),
	}
}
