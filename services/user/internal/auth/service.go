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
	"github.com/rickyroynardson/ride-hailing/services/user/internal/user"
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

func (s *AuthService) Register(ctx context.Context, in RegisterInput) (user.User, Tokens, error) {
	if err := in.Validate(); err != nil {
		return user.User{}, Tokens{}, err
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), bcrypt.DefaultCost)
	if err != nil {
		return user.User{}, Tokens{}, err
	}
	in.Password = string(hash)

	u, err := s.repository.CreateUser(ctx, in)
	if err != nil {
		return user.User{}, Tokens{}, err
	}

	tokens, err := s.issueTokens(ctx, u.ID)
	if err != nil {
		return user.User{}, Tokens{}, err
	}

	return u, tokens, nil
}

// dummyHash keeps unknown-email logins as slow as wrong-password ones, so timing doesn't leak which emails exist.
var dummyHash, _ = bcrypt.GenerateFromPassword([]byte("dummy-password"), bcrypt.DefaultCost)

func (s *AuthService) Login(ctx context.Context, in LoginInput) (user.User, Tokens, error) {
	if err := in.Validate(); err != nil {
		return user.User{}, Tokens{}, err
	}

	u, hash, err := s.repository.GetUserByEmail(ctx, in.Email)
	if errors.Is(err, ErrInvalidCreds) {
		bcrypt.CompareHashAndPassword(dummyHash, []byte(in.Password))
		return user.User{}, Tokens{}, err
	}
	if err != nil {
		return user.User{}, Tokens{}, err
	}

	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(in.Password)); err != nil {
		return user.User{}, Tokens{}, ErrInvalidCreds
	}

	tokens, err := s.issueTokens(ctx, u.ID)
	if err != nil {
		return user.User{}, Tokens{}, err
	}

	return u, tokens, nil
}

func (s *AuthService) Refresh(ctx context.Context, refreshToken string) (user.User, Tokens, error) {
	if refreshToken == "" {
		return user.User{}, Tokens{}, ErrInvalidToken
	}

	u, err := s.repository.RevokeRefreshToken(ctx, hashToken(refreshToken))
	if err != nil {
		return user.User{}, Tokens{}, err
	}

	tokens, err := s.issueTokens(ctx, u.ID)
	if err != nil {
		return user.User{}, Tokens{}, err
	}

	return u, tokens, nil
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

// accessClaims marks approved drivers so other services can authorize driver actions without calling this one.
// A suspension takes effect once the current access token expires.
type accessClaims struct {
	Driver bool `json:"driver,omitempty"`
	jwt.RegisteredClaims
}

func (s *AuthService) VerifyAccessToken(token string) (string, error) {
	var claims accessClaims

	_, err := jwt.ParseWithClaims(token, &claims, func(*jwt.Token) (any, error) {
		return []byte(s.jwtSecret), nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}), jwt.WithExpirationRequired())
	if err != nil || claims.Subject == "" {
		return "", ErrInvalidAuth
	}

	return claims.Subject, nil
}

func (s *AuthService) issueTokens(ctx context.Context, userID string) (Tokens, error) {
	now := time.Now()

	driver, err := s.repository.IsApprovedDriver(ctx, userID)
	if err != nil {
		return Tokens{}, err
	}

	access, err := s.signAccessToken(userID, driver, now)
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

func (s *AuthService) signAccessToken(userID string, driver bool, now time.Time) (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims{
		Driver: driver,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(accessTokenTTL)),
		},
	}).SignedString([]byte(s.jwtSecret))
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
