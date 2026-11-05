package jwtauth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func issuerFixture(t *testing.T) (*Issuer, *Verifier, ClientIdentity) {
	t.Helper()
	serviceKey, servicePub := testKey(t, "service")
	_, userPub := testKey(t, "user")
	identity := ClientIdentity{ID: "ride", Secret: strings.Repeat("r", 43)}
	issuer, err := NewIssuer(serviceKey, IssuerConfig{Clients: map[string]string{
		"ride": SecretDigest(identity.Secret), "location": SecretDigest(strings.Repeat("l", 43)),
	}})
	if err != nil {
		t.Fatal(err)
	}
	v, err := NewTokenVerifier("payment", TrustBundle{User: map[string]string{"user": userPub}, Service: map[string]string{"service": servicePub}})
	if err != nil {
		t.Fatal(err)
	}
	return issuer, v, identity
}

func TestIssuerGrantsAndImpersonation(t *testing.T) {
	issuer, verifier, identity := issuerFixture(t)
	for _, test := range []struct {
		name, id, secret, audience, scope, grant string
		code                                     int
	}{
		{"valid", "ride", identity.Secret, "payment", "", "client_credentials", 200},
		{"wrong secret", "ride", strings.Repeat("x", 43), "payment", "", "client_credentials", 401},
		{"impersonate ride", "ride", strings.Repeat("l", 43), "payment", "", "client_credentials", 401},
		{"location has no payment grant", "location", strings.Repeat("l", 43), "payment", "", "client_credentials", 403},
		{"ungranted audience", "ride", identity.Secret, "user", "", "client_credentials", 403},
		{"requested elevated scope", "ride", identity.Secret, "payment", "/payment.v1.PaymentService/RefundCharge", "client_credentials", 400},
		{"wrong grant", "ride", identity.Secret, "payment", "", "password", 400},
	} {
		t.Run(test.name, func(t *testing.T) {
			form := url.Values{"grant_type": {test.grant}, "audience": {test.audience}, "scope": {test.scope}, "client_id": {"matching"}}
			req := httptest.NewRequest(http.MethodPost, "/token", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.SetBasicAuth(test.id, test.secret)
			w := httptest.NewRecorder()
			issuer.ServeHTTP(w, req)
			if w.Code != test.code {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("token exchange may be cached")
			}
			if w.Code == 200 {
				var response tokenResponse
				if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				caller, err := verifier.Verify(response.AccessToken)
				if err != nil || caller.Service != "ride" {
					t.Fatalf("request chose another identity: %+v %v", caller, err)
				}
				if err := verifier.Authorize(caller, "/payment.v1.PaymentService/RefundCharge"); err == nil {
					t.Fatal("ride got refund privilege")
				}
			}
		})
	}
}

func TestTokenCredentialsCacheRefreshAndOutage(t *testing.T) {
	issuer, verifier, identity := issuerFixture(t)
	var exchanges atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { exchanges.Add(1); issuer.ServeHTTP(w, r) }))
	defer server.Close()
	credentials, err := NewTokenCredentials(server.URL+"/token", identity, "payment", verifier, true)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			md, err := credentials.GetRequestMetadata(context.Background())
			if err != nil || !strings.HasPrefix(md["authorization"], "Bearer ") {
				t.Errorf("metadata=%v error=%v", md, err)
			}
		})
	}
	wg.Wait()
	if exchanges.Load() != 1 {
		t.Fatalf("concurrent refreshes=%d", exchanges.Load())
	}
	credentials.mu.Lock()
	credentials.expires = time.Now()
	credentials.mu.Unlock()
	if _, err := credentials.GetRequestMetadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	if exchanges.Load() != 2 {
		t.Fatalf("didn't refresh expiring token: %d", exchanges.Load())
	}
	server.Close()
	if _, err := credentials.GetRequestMetadata(context.Background()); err != nil {
		t.Fatal("issuer outage broke a cached token", err)
	}
	credentials.mu.Lock()
	credentials.expires = time.Now().Add(-time.Second)
	credentials.mu.Unlock()
	if _, err := credentials.GetRequestMetadata(context.Background()); err == nil {
		t.Fatal("minted locally during issuer outage")
	}
	// A canceled waiter doesn't block on another caller's refresh.
	credentials.refresh <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := credentials.GetRequestMetadata(ctx); status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
	<-credentials.refresh
}

func TestTokenCredentialsTransportAndRedirects(t *testing.T) {
	_, verifier, identity := issuerFixture(t)
	if _, err := NewTokenCredentials("http://identity/token", identity, "payment", verifier, false); err == nil {
		t.Fatal("plaintext exchange accepted without opt-in")
	}
	secure, err := NewTokenCredentials("https://identity/token", identity, "payment", verifier, false)
	if err != nil || !secure.RequireTransportSecurity() {
		t.Fatalf("TLS not required: %v", err)
	}
	var leaked atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	credentials, err := NewTokenCredentials(redirect.URL, identity, "payment", verifier, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := credentials.GetRequestMetadata(context.Background()); err == nil {
		t.Fatal("redirect accepted")
	}
	if leaked.Load() != 0 {
		t.Fatal("client secret forwarded to redirect target")
	}
}

func TestTokenRefreshCancellation(t *testing.T) {
	_, verifier, identity := issuerFixture(t)
	started := make(chan struct{})
	finished := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
		close(finished)
	}))
	defer server.Close()
	creds, err := NewTokenCredentials(server.URL, identity, "payment", verifier, true)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := creds.GetRequestMetadata(ctx); done <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("exchange did not start")
	}
	// This second caller is waiting for the same refresh, not starting another HTTP request.
	waitCtx, stopWait := context.WithCancel(context.Background())
	waitDone := make(chan error, 1)
	go func() { _, err := creds.GetRequestMetadata(waitCtx); waitDone <- err }()
	stopWait()
	select {
	case err := <-waitDone:
		if status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("refresh waiter ignored cancellation")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled exchange succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("exchange ignored cancellation")
	}
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("HTTP cancellation didn't reach issuer")
	}
	if len(creds.refresh) != 0 || creds.cached() != "" {
		t.Fatal("canceled refresh leaked a slot or cached a token")
	}
}
