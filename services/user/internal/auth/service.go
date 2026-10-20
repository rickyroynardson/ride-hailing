package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

const (
	accessTokenTTL  = 15 * time.Minute
	refreshTokenTTL = 30 * 24 * time.Hour
)

type AuthService struct {
	repository *AuthRepository
	jwtSecret  string
}

func NewService(repo *AuthRepository, jwtSecret string) *AuthService {
	return &AuthService{repository: repo, jwtSecret: jwtSecret}
}

func (s *AuthService) Register(ctx context.Context, in RegisterInput) (User, Tokens, error) {
	if err := in.Validate(); err != nil {
		return User{}, Tokens{}, err
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), bcrypt.DefaultCost)
	if err != nil {
		return User{}, Tokens{}, err
	}
	in.Password = string(hash)

	user, err := s.repository.CreateUser(ctx, in)
	if err != nil {
		return User{}, Tokens{}, err
	}

	tokens, err := s.issueTokens(ctx, user.ID)
	if err != nil {
		return User{}, Tokens{}, err
	}

	return user, tokens, nil
}

func (s *AuthService) issueTokens(ctx context.Context, userID string) (Tokens, error) {
	now := time.Now()

	access, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   userID,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(accessTokenTTL)),
	}).SignedString(s.jwtSecret)
	if err != nil {
		return Tokens{}, err
	}

	b := make([]byte, 32)
	rand.Read(b)
	refresh := base64.RawURLEncoding.EncodeToString(b)

	sum := sha256.Sum256([]byte(refresh))
	if err := s.repository.CreateRefreshToken(ctx, userID, hex.EncodeToString(sum[:]), now.Add(refreshTokenTTL)); err != nil {
		return Tokens{}, err
	}

	return Tokens{Access: access, Refresh: refresh}, nil
}
