package jwtauth

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	userIssuer    = "ride-hailing/user"
	serviceIssuer = "ride-hailing/identity"
	userAudience  = "ride-hailing/users"
)

// SigningKey files are secrets. Only user holds its user key; only identity holds the service key.
type SigningKey struct {
	ID         string `json:"id"`
	PrivateKey string `json:"private_key"` // base64 Ed25519 private key
}

// TrustBundle contains public keys only. Keep old and new IDs during rotation, then restart verifiers to reload.
// Separate rings prevent a user signing key from issuing service tokens, or the service issuer from minting users.
type TrustBundle struct {
	User    map[string]string `json:"user"`
	Service map[string]string `json:"service"`
}

type UserSigner func(userID string, driver bool, now time.Time, ttl time.Duration) (string, error)

func readJSON(path string, out any) error {
	if path == "" {
		return fmt.Errorf("auth configuration file path is required")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

func (k SigningKey) private() (ed25519.PrivateKey, error) {
	b, err := base64.StdEncoding.DecodeString(k.PrivateKey)
	if err != nil || len(b) != ed25519.PrivateKeySize || k.ID == "" {
		return nil, fmt.Errorf("invalid Ed25519 signing key")
	}
	// Reject inconsistent seed/public halves instead of producing unverifiable signatures.
	if !slices.Equal(b, ed25519.NewKeyFromSeed(b[:ed25519.SeedSize])) {
		return nil, fmt.Errorf("inconsistent Ed25519 signing key")
	}
	return ed25519.PrivateKey(b), nil
}

func UserSignerFromEnv() (UserSigner, error) {
	var key SigningKey
	if err := readJSON(os.Getenv("USER_SIGNING_KEY_FILE"), &key); err != nil {
		return nil, err
	}
	return NewUserSigner(key)
}

func NewUserSigner(key SigningKey) (UserSigner, error) {
	private, err := key.private()
	if err != nil {
		return nil, err
	}
	return func(userID string, driver bool, now time.Time, ttl time.Duration) (string, error) {
		if userID == "" || ttl <= 0 || ttl > 15*time.Minute {
			return "", fmt.Errorf("invalid user token subject or lifetime")
		}
		return signToken(key.ID, private, claims{Kind: "user", Driver: driver, RegisteredClaims: jwt.RegisteredClaims{
			Issuer: userIssuer, Subject: userID, Audience: jwt.ClaimStrings{userAudience},
			IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		}})
	}, nil
}

func signToken(id string, key ed25519.PrivateKey, c claims) (string, error) {
	t := jwt.NewWithClaims(jwt.SigningMethodEdDSA, c)
	t.Header["kid"] = id
	return t.SignedString(key)
}

type tokenVerifier struct {
	audience      string
	user, service map[string]ed25519.PublicKey
}

func VerifierFromEnv(audience string) (*Verifier, error) {
	var bundle TrustBundle
	if err := readJSON(os.Getenv("AUTH_TRUST_BUNDLE_FILE"), &bundle); err != nil {
		return nil, err
	}
	return NewTokenVerifier(audience, bundle)
}

func NewTokenVerifier(audience string, bundle TrustBundle) (*Verifier, error) {
	if audience == "" {
		return nil, fmt.Errorf("service audience is required")
	}
	decode := func(keys map[string]string) (map[string]ed25519.PublicKey, error) {
		if len(keys) == 0 {
			return nil, fmt.Errorf("empty trust key ring")
		}
		out := make(map[string]ed25519.PublicKey, len(keys))
		for id, encoded := range keys {
			b, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil || len(b) != ed25519.PublicKeySize || id == "" {
				return nil, fmt.Errorf("invalid public key %q", id)
			}
			out[id] = ed25519.PublicKey(b)
		}
		return out, nil
	}
	u, err := decode(bundle.User)
	if err != nil {
		return nil, err
	}
	s, err := decode(bundle.Service)
	if err != nil {
		return nil, err
	}
	for _, userKey := range u {
		for _, serviceKey := range s {
			if slices.Equal(userKey, serviceKey) {
				return nil, fmt.Errorf("user and service signing keys must be distinct")
			}
		}
	}
	return &Verifier{asymmetric: &tokenVerifier{audience: audience, user: u, service: s}}, nil
}

func (v *tokenVerifier) verify(raw string) (Caller, error) {
	var c claims
	_, err := jwt.ParseWithClaims(raw, &c, func(t *jwt.Token) (any, error) {
		id, _ := t.Header["kid"].(string)
		keys := v.user
		if c.Kind == "service" {
			keys = v.service
		}
		key, ok := keys[id]
		if !ok {
			return nil, ErrInvalidAuth
		}
		return key, nil
	}, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithExpirationRequired(), jwt.WithIssuedAt())
	if err != nil || c.IssuedAt == nil || c.ExpiresAt == nil || c.Subject == "" || len(c.Audience) != 1 || !c.ExpiresAt.After(c.IssuedAt.Time) {
		return Caller{}, ErrInvalidAuth
	}
	switch c.Kind {
	case "user":
		if c.Issuer != userIssuer || c.Audience[0] != userAudience || c.Service != "" || c.Scope != "" || c.ExpiresAt.Sub(c.IssuedAt.Time) > 15*time.Minute {
			return Caller{}, ErrInvalidAuth
		}
		return Caller{UserID: c.Subject, Driver: c.Driver}, nil
	case "service":
		if c.Issuer != serviceIssuer || c.Audience[0] != v.audience || c.Service == "" || c.Subject != "service:"+c.Service || c.Driver || c.Scope == "" || c.ExpiresAt.Sub(c.IssuedAt.Time) > serviceTokenTTL {
			return Caller{}, ErrInvalidAuth
		}
		return Caller{Service: c.Service, Scope: c.Scope}, nil
	default:
		return Caller{}, ErrInvalidAuth
	}
}

// Authorize is checked for both unary and streaming RPCs, in addition to UsersOnly/ServicesOnly rules.
// The issuer's grant and the backend's local allowlist must both permit the service and exact full method.
func (v *Verifier) Authorize(c Caller, method string) error {
	if c.Service == "" || v.asymmetric == nil {
		return nil
	}
	if !slices.Contains(strings.Fields(c.Scope), method) || !slices.Contains(ServicePermissions(c.Service, v.asymmetric.audience), method) {
		return ErrWrongCaller
	}
	return nil
}

// ServicePermissions is the explicit local policy, shared with the small issuer. New RPCs/services get no grant
// by default. Devconsole is an intentionally privileged development tool, never a production workload.
func ServicePermissions(client, audience string) []string {
	switch client + ":" + audience {
	case "ride:matching", "devconsole:matching":
		return []string{"/matching.v1.MatchingService/StartMatching", "/matching.v1.MatchingService/CancelMatching"}
	case "ride:payment":
		return []string{"/payment.v1.PaymentService/ChargeRide"}
	case "devconsole:payment":
		return []string{"/payment.v1.PaymentService/RefundCharge"}
	case "ride:location":
		return []string{"/location.v1.LocationService/GetDriverLocation"}
	case "matching:location":
		return []string{"/location.v1.LocationService/ListNearbyDrivers"}
	case "matching:ride":
		return []string{"/ride.v1.RideService/FilterAvailableDrivers", "/ride.v1.RideService/AssignDriver", "/ride.v1.RideService/MarkNoDriver"}
	case "payment:ride":
		return []string{"/ride.v1.RideService/RecordPayment"}
	}
	return nil
}
