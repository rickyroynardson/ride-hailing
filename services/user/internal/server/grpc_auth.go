package server

import (
	"context"
	"errors"

	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type AuthServer struct {
	userv1.UnimplementedAuthServiceServer
	service *auth.AuthService
}

func NewAuthServer(service *auth.AuthService) *AuthServer {
	return &AuthServer{service: service}
}

func (s *AuthServer) Register(ctx context.Context, req *userv1.RegisterRequest) (*userv1.RegisterResponse, error) {
	user, tokens, err := s.service.Register(ctx, auth.RegisterInput{
		Email:    req.GetEmail(),
		Password: req.GetPassword(),
		Name:     req.GetName(),
		Phone:    req.GetPhone(),
	})
	if err != nil {
		return nil, toStatus(err)
	}

	return &userv1.RegisterResponse{
		User: &userv1.User{
			Id:        user.ID,
			Email:     user.Email,
			Name:      user.Name,
			Phone:     user.Phone,
			CreatedAt: timestamppb.New(user.CreatedAt),
		},
		Tokens: &userv1.Token{
			AccessToken:  tokens.Access,
			RefreshToken: tokens.Refresh,
		},
	}, nil
}

func (s *AuthServer) Login(ctx context.Context, req *userv1.LoginRequest) (*userv1.LoginResponse, error) {
	user, tokens, err := s.service.Login(ctx, auth.LoginInput{
		Email:    req.GetEmail(),
		Password: req.GetPassword(),
	})
	if err != nil {
		return nil, toStatus(err)
	}

	return &userv1.LoginResponse{
		User: &userv1.User{
			Id:        user.ID,
			Email:     user.Email,
			Name:      user.Name,
			Phone:     user.Phone,
			CreatedAt: timestamppb.New(user.CreatedAt),
		},
		Tokens: &userv1.Token{
			AccessToken:  tokens.Access,
			RefreshToken: tokens.Refresh,
		},
	}, nil
}

func (s *AuthServer) Refresh(ctx context.Context, req *userv1.RefreshRequest) (*userv1.RefreshResponse, error) {
	user, tokens, err := s.service.Refresh(ctx, req.GetRefreshToken())
	if err != nil {
		return nil, toStatus(err)
	}

	return &userv1.RefreshResponse{
		User: &userv1.User{
			Id:        user.ID,
			Email:     user.Email,
			Name:      user.Name,
			Phone:     user.Phone,
			CreatedAt: timestamppb.New(user.CreatedAt),
		},
		Tokens: &userv1.Token{
			AccessToken:  tokens.Access,
			RefreshToken: tokens.Refresh,
		},
	}, nil
}

func (s *AuthServer) Logout(ctx context.Context, req *userv1.LogoutRequest) (*userv1.LogoutResponse, error) {
	if err := s.service.Logout(ctx, req.GetRefreshToken()); err != nil {
		return nil, toStatus(err)
	}

	return &userv1.LogoutResponse{}, nil
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, auth.ErrInvalidInput):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, auth.ErrUserExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, auth.ErrInvalidCreds), errors.Is(err, auth.ErrInvalidToken), errors.Is(err, auth.ErrInvalidAuth):
		return status.Error(codes.Unauthenticated, err.Error())
	case errors.Is(err, auth.ErrUserNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
