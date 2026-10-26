package jwtauth

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestVerify(t *testing.T) {
	v := NewVerifier("secret")
	sign := func(method jwt.SigningMethod, key any, c claims) string {
		tok, err := jwt.NewWithClaims(method, c).SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	future := jwt.NewNumericDate(time.Now().Add(time.Minute))
	past := jwt.NewNumericDate(time.Now().Add(-time.Minute))
	reg := func(sub string, exp *jwt.NumericDate) jwt.RegisteredClaims {
		return jwt.RegisteredClaims{Subject: sub, ExpiresAt: exp}
	}

	for _, driver := range []bool{true, false} {
		tok := sign(jwt.SigningMethodHS256, []byte("secret"), claims{Driver: driver, RegisteredClaims: reg("u1", future)})
		if c, err := v.Verify(tok); err != nil || c != (Caller{UserID: "u1", Driver: driver}) {
			t.Fatalf("driver=%v: got %+v, %v", driver, c, err)
		}
	}

	// A service token never yields a user ID, whatever its subject says.
	svc := sign(jwt.SigningMethodHS256, []byte("secret"), claims{Service: "matching", Driver: true, RegisteredClaims: reg("u1", future)})
	if c, err := v.Verify(svc); err != nil || c != (Caller{Service: "matching"}) {
		t.Fatalf("service token: got %+v, %v", c, err)
	}

	invalid := map[string]string{
		"expired":         sign(jwt.SigningMethodHS256, []byte("secret"), claims{RegisteredClaims: reg("u1", past)}),
		"wrong secret":    sign(jwt.SigningMethodHS256, []byte("other"), claims{RegisteredClaims: reg("u1", future)}),
		"no expiry":       sign(jwt.SigningMethodHS256, []byte("secret"), claims{RegisteredClaims: reg("u1", nil)}),
		"no subject":      sign(jwt.SigningMethodHS256, []byte("secret"), claims{RegisteredClaims: reg("", future)}),
		"alg none":        sign(jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, claims{RegisteredClaims: reg("u1", future)}),
		"expired service": sign(jwt.SigningMethodHS256, []byte("secret"), claims{Service: "matching", RegisteredClaims: reg("", past)}),
		"garbage":         "not-a-jwt",
	}
	for name, tok := range invalid {
		if _, err := v.Verify(tok); !errors.Is(err, ErrInvalidAuth) {
			t.Fatalf("%s: expected ErrInvalidAuth, got %v", name, err)
		}
	}
}

func TestServiceCredentials(t *testing.T) {
	md, err := NewServiceCredentials("matching", "secret").GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	tok, ok := strings.CutPrefix(md["authorization"], "Bearer ")
	if !ok {
		t.Fatalf("metadata: %v", md)
	}
	if c, err := NewVerifier("secret").Verify(tok); err != nil || c != (Caller{Service: "matching"}) {
		t.Fatalf("got %+v, %v", c, err)
	}
	if _, err := NewVerifier("other").Verify(tok); !errors.Is(err, ErrInvalidAuth) {
		t.Fatalf("other secret: expected ErrInvalidAuth, got %v", err)
	}
}

func TestSignUser(t *testing.T) {
	for _, driver := range []bool{true, false} {
		tok, err := SignUser("secret", "u1", driver, time.Now(), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if c, err := NewVerifier("secret").Verify(tok); err != nil || c != (Caller{UserID: "u1", Driver: driver}) {
			t.Fatalf("driver=%v: got %+v, %v", driver, c, err)
		}
	}
	expired, _ := SignUser("secret", "u1", false, time.Now().Add(-2*time.Minute), time.Minute)
	if _, err := NewVerifier("secret").Verify(expired); !errors.Is(err, ErrInvalidAuth) {
		t.Fatalf("expired user token: expected ErrInvalidAuth, got %v", err)
	}
}
