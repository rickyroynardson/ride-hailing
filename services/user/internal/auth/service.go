package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"time"

	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/user"
	"golang.org/x/crypto/bcrypt"
)

const (
	accessTokenTTL  = 15 * time.Minute
	refreshTokenTTL = 30 * 24 * time.Hour
)

type AuthService struct {
	repository *AuthRepository
	signUser   jwtauth.UserSigner
}

func NewService(repo *AuthRepository, jwtSecret string) *AuthService {
	// Legacy constructor for isolated HS256 test fixtures; production uses NewServiceWithSigner.
	return NewServiceWithSigner(repo, func(id string, driver bool, now time.Time, ttl time.Duration) (string, error) {
		return jwtauth.SignUser(jwtSecret, id, driver, now, ttl)
	})
}

func NewServiceWithSigner(repo *AuthRepository, signer jwtauth.UserSigner) *AuthService {
	return &AuthService{repository: repo, signUser: signer}
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

func (s *AuthService) issueTokens(ctx context.Context, userID string) (Tokens, error) {
	now := time.Now()

	driver, err := s.repository.IsApprovedDriver(ctx, userID)
	if err != nil {
		return Tokens{}, err
	}

	access, err := s.signUser(userID, driver, now, accessTokenTTL)
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
