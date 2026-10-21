package server

import (
	"context"

	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/auth"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type UserServer struct {
	userv1.UnimplementedUserServiceServer
	service *auth.AuthService
}

func NewUserServer(service *auth.AuthService) *UserServer {
	return &UserServer{service: service}
}

func (s *UserServer) GetMe(ctx context.Context, _ *userv1.GetMeRequest) (*userv1.GetMeResponse, error) {
	user, err := s.service.GetUser(ctx, userIDFromContext(ctx))
	if err != nil {
		return nil, toStatus(err)
	}

	return &userv1.GetMeResponse{
		User: &userv1.User{
			Id:        user.ID,
			Email:     user.Email,
			Name:      user.Name,
			Phone:     user.Phone,
			CreatedAt: timestamppb.New(user.CreatedAt),
		},
	}, nil
}
