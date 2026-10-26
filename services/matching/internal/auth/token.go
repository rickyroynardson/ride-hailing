package auth

import (
	"errors"

	"github.com/golang-jwt/jwt/v5"
)

var ErrInvalidAuth = errors.New("invalid or missing access token")

// Caller is the authenticated user behind a request, as stated by the access token the user service signed.
type Caller struct {
	UserID string
	Driver bool
}

// claims mirrors the user service's access token claims.
// copied from services/user; move to a shared pkg when a third service verifies tokens
type claims struct {
	Driver bool `json:"driver,omitempty"`
	jwt.RegisteredClaims
}

type Verifier struct {
	secret []byte
}

func NewVerifier(secret string) *Verifier {
	return &Verifier{secret: []byte(secret)}
}

func (v *Verifier) Verify(token string) (Caller, error) {
	var c claims

	_, err := jwt.ParseWithClaims(token, &c, func(*jwt.Token) (any, error) {
		return v.secret, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}), jwt.WithExpirationRequired())
	if err != nil || c.Subject == "" {
		return Caller{}, ErrInvalidAuth
	}

	return Caller{UserID: c.Subject, Driver: c.Driver}, nil
}
