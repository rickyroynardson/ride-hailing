package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
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

// dummyHash keeps unknown-email logins as slow as wrong-password ones, so timing doesn't leak which emails exist.
var dummyHash, _ = bcrypt.GenerateFromPassword([]byte("dummy-password"), bcrypt.DefaultCost)

func (s *AuthService) Login(ctx context.Context, in LoginInput) (User, Tokens, error) {
	if err := in.Validate(); err != nil {
		return User{}, Tokens{}, err
	}

	user, hash, err := s.repository.GetUserByEmail(ctx, in.Email)
	if errors.Is(err, ErrInvalidCreds) {
		bcrypt.CompareHashAndPassword(dummyHash, []byte(in.Password))
		return User{}, Tokens{}, err
	}
	if err != nil {
		return User{}, Tokens{}, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(in.Password)); err != nil {
		return User{}, Tokens{}, ErrInvalidCreds
	}

	tokens, err := s.issueTokens(ctx, user.ID)
	if err != nil {
		return User{}, Tokens{}, err
	}

	return user, tokens, nil
}

func (s *AuthService) Refresh(ctx context.Context, refreshToken string) (User, Tokens, error) {
	if refreshToken == "" {
		return User{}, Tokens{}, ErrInvalidToken
	}

	user, err := s.repository.RevokeRefreshToken(ctx, hashToken(refreshToken))
	if err != nil {
		return User{}, Tokens{}, err
	}

	tokens, err := s.issueTokens(ctx, user.ID)
	if err != nil {
		return User{}, Tokens{}, err
	}

	return user, tokens, nil
}

func (s *AuthService) Logout(ctx context.Context, refreshToken string) error {
	if refreshToken == "" {
		return nil
	}

	_, err := s.repository.RevokeRefreshToken(ctx, hashToken(refreshToken))
	if errors.Is(err, ErrInvalidToken) {
		return nil
	}
	return err
}

func (s *AuthService) VerifyAccessToken(token string) (string, error) {
	var claims jwt.RegisteredClaims

	_, err := jwt.ParseWithClaims(token, &claims, func(*jwt.Token) (any, error) {
		return []byte(s.jwtSecret), nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}), jwt.WithExpirationRequired())
	if err != nil || claims.Subject == "" {
		return "", ErrInvalidAuth
	}

	return claims.Subject, nil
}

func (s *AuthService) GetUser(ctx context.Context, id string) (User, error) {
	return s.repository.GetUserByID(ctx, id)
}

func (s *AuthService) issueTokens(ctx context.Context, userID string) (Tokens, error) {
	now := time.Now()

	access, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   userID,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(accessTokenTTL)),
	}).SignedString([]byte(s.jwtSecret))
	if err != nil {
		return Tokens{}, err
	}

	b := make([]byte, 32)
	rand.Read(b)
	refresh := base64.RawURLEncoding.EncodeToString(b)

	if err := s.repository.CreateRefreshToken(ctx, userID, hashToken(refresh), now.Add(refreshTokenTTL)); err != nil {
		return Tokens{}, err
	}

	return Tokens{Access: access, Refresh: refresh}, nil
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
