package jwtauth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/rickyroynardson/ride-hailing/pkg/workloadtls"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ClientIdentity struct {
	ID     string `json:"id"`
	Secret string `json:"secret"`
}

// TokenCredentials obtains tokens from identity and caches them per audience. It holds only a client secret,
// not a signing key, and cannot choose another identity. Concurrent refreshes are coalesced and cancelable.
type TokenCredentials struct {
	identity           ClientIdentity
	endpoint, audience string
	insecure           bool
	client             *http.Client
	verifier           *Verifier
	refresh            chan struct{}
	mu                 sync.Mutex
	token              string
	expires            time.Time
}

func CredentialsFromEnv(audience string) (*TokenCredentials, error) {
	var identity ClientIdentity
	if err := readJSON(os.Getenv("SERVICE_CLIENT_FILE"), &identity); err != nil {
		return nil, err
	}
	verifier, err := VerifierFromEnv(audience)
	if err != nil {
		return nil, err
	}
	credentials, err := NewTokenCredentials(os.Getenv("SERVICE_TOKEN_URL"), identity, audience, verifier, false)
	if err != nil {
		return nil, err
	}
	config, err := workloadtls.ClientConfigFromEnv("identity")
	if err != nil {
		return nil, err
	}
	credentials.client.Transport = &http.Transport{TLSClientConfig: config}
	return credentials, nil
}

func NewTokenCredentials(endpoint string, identity ClientIdentity, audience string, verifier *Verifier, allowInsecure bool) (*TokenCredentials, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || u.RawQuery != "" || (u.Scheme != "https" && !(allowInsecure && u.Scheme == "http")) {
		return nil, fmt.Errorf("service token URL requires HTTPS (HTTP needs explicit local-development opt-in)")
	}
	if identity.ID == "" || len(identity.Secret) < 32 || audience == "" || verifier == nil || verifier.asymmetric == nil || verifier.asymmetric.audience != audience {
		return nil, fmt.Errorf("invalid service client identity, audience or token verifier")
	}
	return &TokenCredentials{identity: identity, endpoint: endpoint, audience: audience, verifier: verifier, insecure: allowInsecure,
		refresh: make(chan struct{}, 1), client: &http.Client{Timeout: 2 * time.Second,
			// Never forward a client secret across redirects, even to another path on the same host.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}}, nil
}

func (s *TokenCredentials) cached() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Until(s.expires) > 10*time.Second {
		return s.token
	}
	return ""
}

func (s *TokenCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if token := s.cached(); token != "" {
		return map[string]string{"authorization": "Bearer " + token}, nil
	}
	select {
	case s.refresh <- struct{}{}:
		defer func() { <-s.refresh }()
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if token := s.cached(); token != "" {
		return map[string]string{"authorization": "Bearer " + token}, nil
	}
	form := url.Values{"grant_type": {"client_credentials"}, "audience": {s.audience}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, fmt.Errorf("create service token request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(s.identity.ID, s.identity.Secret)
	res, err := s.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Error(codes.Unavailable, "service token exchange unavailable")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, status.Errorf(codes.Unavailable, "service token exchange rejected: HTTP %d", res.StatusCode)
	}
	var response tokenResponse
	if err := json.NewDecoder(io.LimitReader(res.Body, 16<<10)).Decode(&response); err != nil || response.TokenType != "Bearer" || response.ExpiresIn <= 0 || response.ExpiresIn > int(serviceTokenTTL/time.Second) {
		return nil, status.Error(codes.Unavailable, "invalid service token response")
	}
	caller, err := s.verifier.Verify(response.AccessToken)
	if err != nil || caller.Service != s.identity.ID {
		return nil, status.Error(codes.Unavailable, "invalid issued service identity")
	}
	// Signature and audience were checked above; use the actual JWT expiry rather than trusting expires_in/cache time.
	var c claims
	if _, _, err := jwt.NewParser().ParseUnverified(response.AccessToken, &c); err != nil || c.ExpiresAt == nil {
		return nil, status.Error(codes.Unavailable, "invalid issued expiry")
	}
	s.mu.Lock()
	s.token, s.expires = response.AccessToken, c.ExpiresAt.Time
	s.mu.Unlock()
	return map[string]string{"authorization": "Bearer " + response.AccessToken}, nil
}

// CredentialsFromEnv always requires TLS. Only explicit low-level loopback test fixtures can opt out.
func (s *TokenCredentials) RequireTransportSecurity() bool { return !s.insecure }
