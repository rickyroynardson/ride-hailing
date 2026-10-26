// Package jwtauth is the access-token contract between services: the user service signs user tokens, services sign
// short-lived service tokens for each other, and every service verifies both.
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
}

// claims is the token payload. Driver is set for approved drivers; Service only on service tokens.
// Services roll out one at a time, so changes must stay backward compatible: add claims before relying on them,
// never rename or repurpose one.
type claims struct {
	Driver  bool   `json:"driver,omitempty"`
	Service string `json:"svc,omitempty"`
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

// SignUser signs a user access token; the user service is the only caller.
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

// ServiceCredentials signs a service token for each outgoing gRPC call. Use with grpc.WithPerRPCCredentials.
// note: all services share JWT_SECRET (HS256), so any service could also sign user tokens; move to
// asymmetric keys (RS256/EdDSA, only the user service holding the private key) or mTLS when that matters
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
