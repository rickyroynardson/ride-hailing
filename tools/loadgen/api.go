package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// api calls the gateway the way an app does: HTTP/JSON, bearer token, newline-delimited JSON streams.
type api struct {
	base   string
	hc     *http.Client
	stream *http.Client
	rec    *recorder
}

func newAPI(base string, rec *recorder) *api {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns, t.MaxIdleConnsPerHost = 2000, 2000
	return &api{
		base:   strings.TrimRight(base, "/"),
		hc:     &http.Client{Transport: t, Timeout: 15 * time.Second},
		stream: &http.Client{Transport: t}, // no timeout: a watch lasts as long as the ride
		rec:    rec,
	}
}

// apiError is a non-2xx answer, in grpc-gateway's error shape.
type apiError struct {
	Status  int
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *apiError) Error() string {
	return fmt.Sprintf("HTTP %d (code %d): %s", e.Status, e.Code, e.Message)
}

func statusOf(err error) int {
	if e, ok := err.(*apiError); ok {
		return e.Status
	}
	return 0
}

var unmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}

// call makes one request and records its latency and status under route, the gateway's route pattern, so a run's
// numbers line up with the gateway's own metrics.
func (a *api) call(ctx context.Context, method, route, path, token string, in, out proto.Message) error {
	var body io.Reader
	if method != http.MethodGet && method != http.MethodDelete {
		b := []byte("{}")
		if in != nil {
			var err error
			if b, err = protojson.Marshal(in); err != nil {
				return err
			}
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	start := time.Now()
	res, err := a.hc.Do(req)
	stage := a.rec.current()
	if err != nil {
		a.rec.count(stage, "status "+route+" network")
		return err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	a.rec.observe(stage, "http "+route, time.Since(start))
	a.rec.count(stage, fmt.Sprintf("status %s %d", route, res.StatusCode))
	if err != nil {
		return err
	}
	if res.StatusCode/100 != 2 {
		e := &apiError{Status: res.StatusCode}
		json.Unmarshal(b, e)
		return e
	}
	if out != nil {
		return unmarshal.Unmarshal(b, out)
	}
	return nil
}

// watch reads a stream until it ends, the server closes it, or ctx ends, calling onMsg with each message.
func (a *api) watch(ctx context.Context, path, token string, newMsg func() proto.Message, onMsg func(proto.Message)) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.base+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := a.stream.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		e := &apiError{Status: res.StatusCode}
		b, _ := io.ReadAll(res.Body)
		json.Unmarshal(b, e)
		return e
	}

	lines := bufio.NewScanner(res.Body)
	lines.Buffer(make([]byte, 64<<10), 1<<20)
	for lines.Scan() {
		var line struct {
			Result json.RawMessage `json:"result"`
			Error  *apiError       `json:"error"`
		}
		if err := json.Unmarshal(lines.Bytes(), &line); err != nil {
			return err
		}
		if line.Error != nil {
			return line.Error
		}
		m := newMsg()
		if err := unmarshal.Unmarshal(line.Result, m); err != nil {
			return err
		}
		onMsg(m)
	}
	if err := lines.Err(); err != nil && ctx.Err() == nil {
		return err
	}
	return nil
}

// session is one signed-in user. It refreshes the access token before it expires; refresh tokens are single use,
// hence the lock.
type session struct {
	api *api
	ID  string

	mu       sync.Mutex
	access   string
	refresh  string
	issuedAt time.Time
}

// tokenRefreshAfter stays well inside the user service's 15-minute access token lifetime.
const tokenRefreshAfter = 12 * time.Minute

func (s *session) set(t *userv1.Token) {
	s.access, s.refresh, s.issuedAt = t.GetAccessToken(), t.GetRefreshToken(), time.Now()
}

func (s *session) token(ctx context.Context) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.issuedAt) > tokenRefreshAfter {
		s.refreshLocked(ctx)
	}
	return s.access
}

// refreshNow gets a new access token at once, e.g. to pick up the driver claim after approval.
func (s *session) refreshNow(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refreshLocked(ctx)
}

func (s *session) refreshLocked(ctx context.Context) error {
	var res userv1.RefreshResponse
	if err := s.api.call(ctx, http.MethodPost, "/v1/auth/refresh", "/v1/auth/refresh", "",
		&userv1.RefreshRequest{RefreshToken: s.refresh}, &res); err != nil {
		return err
	}
	s.set(res.GetTokens())
	return nil
}
