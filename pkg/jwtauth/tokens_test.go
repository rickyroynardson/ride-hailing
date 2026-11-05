package jwtauth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func testKey(t *testing.T, id string) (SigningKey, string) {
	t.Helper()
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return SigningKey{ID: id, PrivateKey: base64.StdEncoding.EncodeToString(private)}, base64.StdEncoding.EncodeToString(pub)
}

func serviceClaims(client, audience string) claims {
	now := time.Now()
	return claims{Kind: "service", Service: client, Scope: "/payment.v1.PaymentService/ChargeRide", RegisteredClaims: jwt.RegisteredClaims{
		Issuer: serviceIssuer, Subject: "service:" + client, Audience: jwt.ClaimStrings{audience},
		IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(serviceTokenTTL)),
	}}
}

func TestAsymmetricTokenValidation(t *testing.T) {
	userKey, userPub := testKey(t, "user")
	serviceKey, servicePub := testKey(t, "service")
	bundle := TrustBundle{User: map[string]string{"user": userPub}, Service: map[string]string{"service": servicePub}}
	v, err := NewTokenVerifier("payment", bundle)
	if err != nil {
		t.Fatal(err)
	}
	userSigner, _ := NewUserSigner(userKey)
	user, err := userSigner("u1", true, time.Now(), 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if caller, err := v.Verify(user); err != nil || caller != (Caller{UserID: "u1", Driver: true}) {
		t.Fatalf("user: %+v %v", caller, err)
	}
	private, _ := serviceKey.private()
	sign := func(c claims) string {
		token, err := signToken(serviceKey.ID, private, c)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	valid := sign(serviceClaims("ride", "payment"))
	caller, err := v.Verify(valid)
	if err != nil || caller.Service != "ride" {
		t.Fatalf("service: %+v %v", caller, err)
	}
	if err := v.Authorize(caller, "/payment.v1.PaymentService/ChargeRide"); err != nil {
		t.Fatal(err)
	}
	if err := v.Authorize(caller, "/payment.v1.PaymentService/RefundCharge"); !errors.Is(err, ErrWrongCaller) {
		t.Fatal("ride granted refunds")
	}
	caller.Service = "location"
	if err := v.Authorize(caller, "/payment.v1.PaymentService/ChargeRide"); !errors.Is(err, ErrWrongCaller) {
		t.Fatal("location granted charges")
	}
	for name, mutate := range map[string]func(*claims){
		"wrong audience":     func(c *claims) { c.Audience = jwt.ClaimStrings{"ride"} },
		"missing audience":   func(c *claims) { c.Audience = nil },
		"multiple audiences": func(c *claims) { c.Audience = jwt.ClaimStrings{"payment", "ride"} },
		"wrong issuer":       func(c *claims) { c.Issuer = userIssuer },
		"expired": func(c *claims) {
			c.IssuedAt = jwt.NewNumericDate(time.Now().Add(-2 * time.Minute))
			c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute))
		},
		"no expiry":             func(c *claims) { c.ExpiresAt = nil },
		"no issued at":          func(c *claims) { c.IssuedAt = nil },
		"future issued at":      func(c *claims) { c.IssuedAt = jwt.NewNumericDate(time.Now().Add(time.Minute)) },
		"excessive lifetime":    func(c *claims) { c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(time.Hour)) },
		"subject impersonation": func(c *claims) { c.Subject = "service:matching" },
		"missing service":       func(c *claims) { c.Service = "" },
		"missing scope":         func(c *claims) { c.Scope = "" },
		"driver service":        func(c *claims) { c.Driver = true },
		"unknown kind":          func(c *claims) { c.Kind = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			c := serviceClaims("ride", "payment")
			mutate(&c)
			if _, err := v.Verify(sign(c)); !errors.Is(err, ErrInvalidAuth) {
				t.Fatalf("accepted invalid token: %v", err)
			}
		})
	}
	// Signing privilege cannot cross identity kinds, even if a malicious signer supplies the correct issuer claims.
	userPrivate, _ := userKey.private()
	forgedService, _ := signToken(userKey.ID, userPrivate, serviceClaims("ride", "payment"))
	if _, err := v.Verify(forgedService); !errors.Is(err, ErrInvalidAuth) {
		t.Fatal("user key minted service token")
	}
	userClaims := claims{Kind: "user", RegisteredClaims: jwt.RegisteredClaims{Issuer: userIssuer, Subject: "u1", Audience: jwt.ClaimStrings{userAudience}, IssuedAt: jwt.NewNumericDate(time.Now()), ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute))}}
	forgedUser := sign(userClaims)
	if _, err := v.Verify(forgedUser); !errors.Is(err, ErrInvalidAuth) {
		t.Fatal("service key minted user token")
	}
	legacy, _ := SignUser("old-shared-secret", "u1", false, time.Now(), time.Minute)
	if _, err := v.Verify(legacy); !errors.Is(err, ErrInvalidAuth) {
		t.Fatal("production verifier accepted legacy HS256")
	}
	forgedWithKnownID, _ := signToken(serviceKey.ID, userPrivate, serviceClaims("ride", "payment"))
	if _, err := v.Verify(forgedWithKnownID); !errors.Is(err, ErrInvalidAuth) {
		t.Fatal("accepted wrong signing key under known kid")
	}
	unsigned, _ := jwt.NewWithClaims(jwt.SigningMethodNone, serviceClaims("ride", "payment")).SignedString(jwt.UnsafeAllowNoneSignatureType)
	if _, err := v.Verify(unsigned); !errors.Is(err, ErrInvalidAuth) {
		t.Fatal("accepted unsigned token")
	}
}

func TestTrustBundleRotation(t *testing.T) {
	_, userPub := testKey(t, "user")
	oldKey, oldPub := testKey(t, "old")
	newKey, newPub := testKey(t, "new")
	bundle := TrustBundle{User: map[string]string{"user": userPub}, Service: map[string]string{"old": oldPub, "new": newPub}}
	v, err := NewTokenVerifier("payment", bundle)
	if err != nil {
		t.Fatal(err)
	}
	oldPrivate, _ := oldKey.private()
	newPrivate, _ := newKey.private()
	oldToken, _ := signToken("old", oldPrivate, serviceClaims("ride", "payment"))
	newToken, _ := signToken("new", newPrivate, serviceClaims("ride", "payment"))
	for _, raw := range []string{oldToken, newToken} {
		if _, err := v.Verify(raw); err != nil {
			t.Fatal(err)
		}
	}
	delete(bundle.Service, "old")
	// Verifiers copy the ring: changing the config doesn't mutate a running verifier.
	if _, err := v.Verify(oldToken); err != nil {
		t.Fatal(err)
	}
	rotated, _ := NewTokenVerifier("payment", bundle)
	if _, err := rotated.Verify(oldToken); !errors.Is(err, ErrInvalidAuth) {
		t.Fatal("retired key accepted")
	}
	if _, err := rotated.Verify(newToken); err != nil {
		t.Fatal(err)
	}
	unknown, _ := signToken("unknown", newPrivate, serviceClaims("ride", "payment"))
	if _, err := rotated.Verify(unknown); !errors.Is(err, ErrInvalidAuth) {
		t.Fatal("unknown kid accepted")
	}
}

func TestAuthConfigurationFailsClosed(t *testing.T) {
	t.Setenv("AUTH_TRUST_BUNDLE_FILE", "")
	t.Setenv("USER_SIGNING_KEY_FILE", "")
	t.Setenv("SERVICE_CLIENT_FILE", "")
	t.Setenv("JWT_SECRET", "legacy-is-not-a-fallback")
	if _, err := VerifierFromEnv("payment"); err == nil {
		t.Fatal("missing trust accepted")
	}
	if _, err := UserSignerFromEnv(); err == nil {
		t.Fatal("missing signer accepted")
	}
	if _, err := CredentialsFromEnv("payment"); err == nil {
		t.Fatal("missing client accepted")
	}
	if _, err := NewTokenVerifier("payment", TrustBundle{}); err == nil {
		t.Fatal("empty trust accepted")
	}
	if _, err := NewUserSigner(SigningKey{ID: "bad", PrivateKey: "bad"}); err == nil {
		t.Fatal("invalid private key accepted")
	}
	_, public := testKey(t, "shared")
	if _, err := NewTokenVerifier("payment", TrustBundle{User: map[string]string{"user": public}, Service: map[string]string{"service": public}}); err == nil {
		t.Fatal("same key trusted for users and services")
	}
}

func TestAuthFilesFromEnv(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, value any) string {
		path := filepath.Join(dir, name)
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	userKey, userPub := testKey(t, "user")
	_, servicePub := testKey(t, "service")
	t.Setenv("AUTH_TRUST_BUNDLE_FILE", write("trust.json", TrustBundle{User: map[string]string{"user": userPub}, Service: map[string]string{"service": servicePub}}))
	t.Setenv("USER_SIGNING_KEY_FILE", write("user.json", userKey))
	t.Setenv("SERVICE_CLIENT_FILE", write("client.json", ClientIdentity{ID: "ride", Secret: "a-random-secret-at-least-32-characters-long"}))
	t.Setenv("SERVICE_TOKEN_URL", "http://identity/token")
	t.Setenv("AUTH_ALLOW_INSECURE", "true")
	signer, err := UserSignerFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	v, err := VerifierFromEnv("payment")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := signer("u1", false, time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Verify(raw); err != nil {
		t.Fatal(err)
	}
	if _, err := CredentialsFromEnv("payment"); err == nil {
		t.Fatal("production env loader allowed plaintext even with legacy insecure flag")
	}
	t.Setenv("SERVICE_TOKEN_URL", "https://identity/token")
	t.Setenv("TLS_DIR", "")
	if _, err := CredentialsFromEnv("payment"); err == nil {
		t.Fatal("missing workload TLS files accepted")
	}
}
