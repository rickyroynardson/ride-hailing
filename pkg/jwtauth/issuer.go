package jwtauth

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rickyroynardson/ride-hailing/pkg/workloadtls"
)

// IssuerConfig contains SHA-256 digests of high-entropy provisioned client secrets, never user passwords.
// Each workload receives only its own secret. Identity alone receives this complete registry and its signing key.
type IssuerConfig struct {
	Clients map[string]string `json:"clients"`
}

func SecretDigest(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

type Issuer struct {
	key     ed25519.PrivateKey
	id      string
	clients map[string]string
	active  chan struct{}
}

func NewIssuer(key SigningKey, config IssuerConfig) (*Issuer, error) {
	private, err := key.private()
	if err != nil {
		return nil, err
	}
	if len(config.Clients) == 0 {
		return nil, fmt.Errorf("issuer client registry is empty")
	}
	clients := make(map[string]string, len(config.Clients))
	for id, digest := range config.Clients {
		b, err := hex.DecodeString(digest)
		if id == "" || err != nil || len(b) != sha256.Size {
			return nil, fmt.Errorf("invalid issuer client entry")
		}
		clients[id] = strings.ToLower(digest)
	}
	return &Issuer{key: private, id: key.ID, clients: clients, active: make(chan struct{}, 16)}, nil
}

// ServeHTTP implements a deliberately small OAuth-style client_credentials exchange, not a complete OAuth server.
// Grants come from server policy, not request claims/scopes. The deployed identity listener requires workload mTLS;
// the bare handler's plaintext path exists only for isolated loopback tests.
func (s *Issuer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Content-Type", "application/json")
	fail := func(code int, message string) {
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(map[string]string{"error": message})
	}
	if r.URL.Path != "/token" {
		fail(http.StatusNotFound, "not_found")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		fail(http.StatusMethodNotAllowed, "invalid_request")
		return
	}
	select {
	case s.active <- struct{}{}:
		defer func() { <-s.active }()
	default:
		fail(http.StatusTooManyRequests, "temporarily_unavailable")
		return
	}
	id, secret, ok := r.BasicAuth()
	if r.TLS != nil {
		if len(r.TLS.VerifiedChains) == 0 || len(r.TLS.PeerCertificates) == 0 {
			fail(http.StatusUnauthorized, "invalid_client")
			return
		}
		name, err := workloadtls.CertificateIdentity(r.TLS.PeerCertificates[0])
		if err != nil || name != id {
			fail(http.StatusUnauthorized, "invalid_client")
			return
		}
	}
	expected, exists := s.clients[id]
	if !exists {
		expected = strings.Repeat("0", 64)
	}
	if valid := subtle.ConstantTimeCompare([]byte(SecretDigest(secret)), []byte(expected)); !ok || !exists || valid != 1 {
		w.Header().Set("WWW-Authenticate", `Basic realm="identity"`)
		fail(http.StatusUnauthorized, "invalid_client")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := r.ParseForm(); err != nil || r.PostForm.Get("grant_type") != "client_credentials" || r.PostForm.Get("scope") != "" {
		fail(http.StatusBadRequest, "invalid_request")
		return
	}
	audience := r.PostForm.Get("audience")
	permissions := ServicePermissions(id, audience)
	if len(permissions) == 0 {
		fail(http.StatusForbidden, "invalid_target")
		return
	}
	now := time.Now()
	token, err := signToken(s.id, s.key, claims{Kind: "service", Service: id, Scope: strings.Join(permissions, " "), RegisteredClaims: jwt.RegisteredClaims{
		Issuer: serviceIssuer, Subject: "service:" + id, Audience: jwt.ClaimStrings{audience},
		IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(serviceTokenTTL)),
	}})
	if err != nil {
		fail(http.StatusInternalServerError, "server_error")
		return
	}
	json.NewEncoder(w).Encode(tokenResponse{AccessToken: token, TokenType: "Bearer", ExpiresIn: int(serviceTokenTTL / time.Second)})
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}
