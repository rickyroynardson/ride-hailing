package auth

import (
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestVerifyAccessToken(t *testing.T) {
	s := NewService(nil, "secret")
	sign := func(method jwt.SigningMethod, key any, claims jwt.RegisteredClaims) string {
		tok, err := jwt.NewWithClaims(method, claims).SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	future := jwt.NewNumericDate(time.Now().Add(time.Minute))
	past := jwt.NewNumericDate(time.Now().Add(-time.Minute))

	valid := sign(jwt.SigningMethodHS256, []byte("secret"), jwt.RegisteredClaims{Subject: "u1", ExpiresAt: future})
	if id, err := s.VerifyAccessToken(valid); err != nil || id != "u1" {
		t.Fatalf("valid token: got %q, %v", id, err)
	}

	invalid := map[string]string{
		"expired":      sign(jwt.SigningMethodHS256, []byte("secret"), jwt.RegisteredClaims{Subject: "u1", ExpiresAt: past}),
		"wrong secret": sign(jwt.SigningMethodHS256, []byte("other"), jwt.RegisteredClaims{Subject: "u1", ExpiresAt: future}),
		"no expiry":    sign(jwt.SigningMethodHS256, []byte("secret"), jwt.RegisteredClaims{Subject: "u1"}),
		"no subject":   sign(jwt.SigningMethodHS256, []byte("secret"), jwt.RegisteredClaims{ExpiresAt: future}),
		"alg none":     sign(jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, jwt.RegisteredClaims{Subject: "u1", ExpiresAt: future}),
		"garbage":      "not-a-jwt",
	}
	for name, tok := range invalid {
		if _, err := s.VerifyAccessToken(tok); !errors.Is(err, ErrInvalidAuth) {
			t.Fatalf("%s: expected ErrInvalidAuth, got %v", name, err)
		}
	}
}

func TestSignAccessTokenDriverClaim(t *testing.T) {
	s := NewService(nil, "secret")

	for _, driver := range []bool{true, false} {
		tok, err := s.signAccessToken("u1", driver, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if id, err := s.VerifyAccessToken(tok); err != nil || id != "u1" {
			t.Fatalf("verify: got %q, %v", id, err)
		}

		var claims accessClaims
		if _, _, err := jwt.NewParser().ParseUnverified(tok, &claims); err != nil || claims.Driver != driver {
			t.Fatalf("driver=%v: got claims %+v, err %v", driver, claims, err)
		}
	}
}
