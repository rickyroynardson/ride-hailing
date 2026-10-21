package server

import (
	"context"

	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/auth"
)

type AuthServer struct {
	userv1.UnimplementedAuthServiceServer
	service *auth.AuthService
}

func NewAuthServer(service *auth.AuthService) *AuthServer {
	return &AuthServer{service: service}
}

func (s *AuthServer) Register(ctx context.Context, req *userv1.RegisterRequest) (*userv1.RegisterResponse, error) {
	u, tokens, err := s.service.Register(ctx, auth.RegisterInput{
		Email:    req.GetEmail(),
		Password: req.GetPassword(),
		Name:     req.GetName(),
		Phone:    req.GetPhone(),
	})
	if err != nil {
		return nil, err
	}

	return &userv1.RegisterResponse{
		User: toUserProto(u),
		Tokens: &userv1.Token{
			AccessToken:  tokens.Access,
			RefreshToken: tokens.Refresh,
		},
	}, nil
}

func (s *AuthServer) Login(ctx context.Context, req *userv1.LoginRequest) (*userv1.LoginResponse, error) {
	u, tokens, err := s.service.Login(ctx, auth.LoginInput{
		Email:    req.GetEmail(),
		Password: req.GetPassword(),
	})
	if err != nil {
		return nil, err
	}

	return &userv1.LoginResponse{
		User: toUserProto(u),
		Tokens: &userv1.Token{
			AccessToken:  tokens.Access,
			RefreshToken: tokens.Refresh,
		},
	}, nil
}

func (s *AuthServer) Refresh(ctx context.Context, req *userv1.RefreshRequest) (*userv1.RefreshResponse, error) {
	u, tokens, err := s.service.Refresh(ctx, req.GetRefreshToken())
	if err != nil {
		return nil, err
	}

	return &userv1.RefreshResponse{
		User: toUserProto(u),
		Tokens: &userv1.Token{
			AccessToken:  tokens.Access,
			RefreshToken: tokens.Refresh,
		},
	}, nil
}

func (s *AuthServer) Logout(ctx context.Context, req *userv1.LogoutRequest) (*userv1.LogoutResponse, error) {
	if err := s.service.Logout(ctx, req.GetRefreshToken()); err != nil {
		return nil, err
	}

	return &userv1.LogoutResponse{}, nil
}
