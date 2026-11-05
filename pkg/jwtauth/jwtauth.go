// Package jwtauth separates user tokens from centrally issued service tokens and verifies RPC permissions.
package jwtauth

import (
	"context"
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrInvalidAuth = errors.New("invalid or missing access token")
	ErrWrongCaller = errors.New("this method is not available to this caller")
)

// Caller is who is behind a request: a user (rider or driver), as stated by the access token the user service
// signed, or another service, as stated by its service token. Exactly one of UserID and Service is set.
type Caller struct {
	UserID  string
	Driver  bool
	Service string
	Scope   string
}

// claims is the token payload. Driver is set for approved drivers; Service only on service tokens.
// Services roll out one at a time, so changes must stay backward compatible: add claims before relying on them,
// never rename or repurpose one.
type claims struct {
	Kind    string `json:"kind,omitempty"`
	Scope   string `json:"scope,omitempty"`
	Driver  bool   `json:"driver,omitempty"`
	Service string `json:"svc,omitempty"`
	jwt.RegisteredClaims
}

type Verifier struct {
	secret     []byte
	asymmetric *tokenVerifier
}

// NewVerifier is the legacy HS256 verifier for old test fixtures. Production uses VerifierFromEnv;
// asymmetric verifiers never fall back to this shared-secret contract.
func NewVerifier(secret string) *Verifier {
	return &Verifier{secret: []byte(secret)}
}

func (v *Verifier) Verify(token string) (Caller, error) {
	if v.asymmetric != nil {
		return v.asymmetric.verify(token)
	}
	var c claims

	_, err := jwt.ParseWithClaims(token, &c, func(*jwt.Token) (any, error) {
		return v.secret, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}), jwt.WithExpirationRequired())
	if err != nil {
		return Caller{}, ErrInvalidAuth
	}
	if c.Service != "" {
		return Caller{Service: c.Service}, nil
	}
	if c.Subject == "" {
		return Caller{}, ErrInvalidAuth
	}

	return Caller{UserID: c.Subject, Driver: c.Driver}, nil
}

// SignUser is the legacy HS256 signer retained for test fixtures. Production uses UserSignerFromEnv.
func SignUser(secret, userID string, driver bool, now time.Time, ttl time.Duration) (string, error) {
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims{
		Driver: driver,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	}).SignedString([]byte(secret))
}

// serviceTokenTTL is short because a fresh token is signed for every call.
const serviceTokenTTL = time.Minute

// ServiceCredentials is the legacy self-signed HS256 credential for test fixtures only.
// Production uses CredentialsFromEnv and cannot mint service tokens locally.
type ServiceCredentials struct {
	name   string
	secret []byte
}

func NewServiceCredentials(name, secret string) ServiceCredentials {
	return ServiceCredentials{name: name, secret: []byte(secret)}
}

func (s ServiceCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	now := time.Now()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims{
		Service: s.name,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "service:" + s.name,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(serviceTokenTTL)),
		},
	}).SignedString(s.secret)
	if err != nil {
		return nil, err
	}
	return map[string]string{"authorization": "Bearer " + tok}, nil
}

// RequireTransportSecurity is false while services talk over plaintext inside the compose network.
func (ServiceCredentials) RequireTransportSecurity() bool { return false }
