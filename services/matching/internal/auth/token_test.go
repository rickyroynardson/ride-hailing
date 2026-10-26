package auth

import (
	"errors"
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

	invalid := map[string]string{
		"expired":      sign(jwt.SigningMethodHS256, []byte("secret"), claims{RegisteredClaims: reg("u1", past)}),
		"wrong secret": sign(jwt.SigningMethodHS256, []byte("other"), claims{RegisteredClaims: reg("u1", future)}),
		"no expiry":    sign(jwt.SigningMethodHS256, []byte("secret"), claims{RegisteredClaims: reg("u1", nil)}),
		"no subject":   sign(jwt.SigningMethodHS256, []byte("secret"), claims{RegisteredClaims: reg("", future)}),
		"alg none":     sign(jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, claims{RegisteredClaims: reg("u1", future)}),
		"garbage":      "not-a-jwt",
	}
	for name, tok := range invalid {
		if _, err := v.Verify(tok); !errors.Is(err, ErrInvalidAuth) {
			t.Fatalf("%s: expected ErrInvalidAuth, got %v", name, err)
		}
	}
}
