// Package lifecycle separates process liveness, local readiness, admission and bounded draining.
package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	DrainDelay  = 500 * time.Millisecond
	GracePeriod = 5 * time.Second
)

var ErrDrainDeadline = errors.New("shutdown grace period expired; remaining calls were canceled")

type State struct {
	mu              sync.Mutex
	ready, draining bool
	check           func(context.Context) error
	workers         []func(context.Context)
	wg              sync.WaitGroup
	workerCtx       context.Context
	stopWorkers     context.CancelFunc
}

func New(check func(context.Context) error) *State {
	ctx, cancel := context.WithCancel(context.Background())
	return &State{check: check, workerCtx: ctx, stopWorkers: cancel}
}

// Go registers a worker before Serve. Shutdown cancels and joins workers before callers close shared resources.
func (s *State) Go(fn func(context.Context)) { s.workers = append(s.workers, fn) }

func (s *State) available() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.ready && !s.draining }
func (s *State) MarkReady() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.draining {
		s.ready = true
	}
}
func (s *State) BeginDrain() {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	s.stopWorkers()
}

func (s *State) UnaryOption() grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if !s.available() {
			return nil, status.Error(codes.Unavailable, "server is not accepting new work")
		}
		return next(ctx, req)
	})
}
func (s *State) StreamOption() grpc.ServerOption {
	return grpc.ChainStreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
		if !s.available() {
			return status.Error(codes.Unavailable, "server is not accepting new work")
		}
		return next(srv, stream)
	})
}

// HTTP gates only business traffic. Wrap it with Health when health routes share the same listener.
func (s *State) HTTP(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.available() {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]any{"code": codes.Unavailable, "message": "server is not accepting new work"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Health is independent of authentication and business admission. Readiness checks only direct local storage,
// never another service's readiness, to avoid cascading an outage through the whole dependency graph.
func (s *State) Health(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/livez" && r.URL.Path != "/readyz" {
			if next == nil {
				http.NotFound(w, r)
			} else {
				next.ServeHTTP(w, r)
			}
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/readyz" {
			ready := s.available()
			if ready && s.check != nil {
				ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
				ready = s.check(ctx) == nil
				cancel()
			}
			if !ready || !s.available() {
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok\n"))
	})
}

type Config struct {
	GRPC         *grpc.Server
	GRPCAddress  string
	GRPCListener net.Listener   // Optional pre-bound listener, primarily for deterministic tests.
	HTTP         []*http.Server // Business HTTP listeners, optionally with TLSConfig.
	Admin        *http.Server   // Kept alive for probes until business draining completes.
	DrainDelay   time.Duration
	GracePeriod  time.Duration
	OnReady      func()
	OnDrain      func()
}

func (s *State) Run(config Config) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return s.Serve(ctx, config)
}

func (s *State) Serve(ctx context.Context, config Config) error {
	if config.DrainDelay <= 0 {
		config.DrainDelay = DrainDelay
	}
	if config.GracePeriod <= 0 {
		config.GracePeriod = GracePeriod
	}
	var listeners []net.Listener
	bind := func(address string) (net.Listener, error) {
		listener, err := net.Listen("tcp", address)
		if err == nil {
			listeners = append(listeners, listener)
		}
		return listener, err
	}
	defer func() {
		for _, listener := range listeners {
			listener.Close()
		}
		s.stopWorkers()
	}()
	errorsCh := make(chan error, len(config.HTTP)+2)
	var grpcListener net.Listener
	var err error
	if config.GRPC != nil {
		if config.GRPCListener != nil {
			grpcListener = config.GRPCListener
			listeners = append(listeners, grpcListener)
		} else {
			grpcListener, err = bind(config.GRPCAddress)
		}
		if err != nil {
			return err
		}
	}
	servers := append([]*http.Server{}, config.HTTP...)
	if config.Admin != nil {
		servers = append(servers, config.Admin)
	}
	httpListeners := make([]net.Listener, 0, len(servers))
	for _, server := range servers {
		listener, err := bind(server.Addr)
		if err != nil {
			return err
		}
		httpListeners = append(httpListeners, listener)
		server.Addr = listener.Addr().String()
	}
	// Every listener is bound before any readiness response can advertise this instance.
	if config.GRPC != nil {
		go func() {
			if err := config.GRPC.Serve(grpcListener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				errorsCh <- err
			}
		}()
	}
	for i, server := range servers {
		listener := httpListeners[i]
		go func() {
			var err error
			if server.TLSConfig != nil {
				err = server.ServeTLS(listener, "", "")
			} else {
				err = server.Serve(listener)
			}
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				errorsCh <- err
			}
		}()
	}
	for _, worker := range s.workers {
		s.wg.Add(1)
		go func() { defer s.wg.Done(); worker(s.workerCtx) }()
	}
	s.MarkReady()
	if config.OnReady != nil {
		config.OnReady()
	}
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errorsCh:
	}
	s.BeginDrain()
	if config.OnDrain != nil {
		config.OnDrain()
	}
	shutdown, cancel := context.WithTimeout(context.Background(), config.GracePeriod)
	defer cancel()
	timer := time.NewTimer(config.DrainDelay)
	select {
	case <-timer.C:
	case <-shutdown.Done():
	}
	timer.Stop()
	// HTTP, gRPC and background workers share one budget; streams cannot postpone shutdown indefinitely.
	var drains sync.WaitGroup
	if config.GRPC != nil {
		drains.Add(1)
		go func() { defer drains.Done(); config.GRPC.GracefulStop() }()
	}
	for _, server := range config.HTTP {
		drains.Add(1)
		go func() {
			defer drains.Done()
			if server.Shutdown(shutdown) != nil {
				server.Close()
			}
		}()
	}
	drains.Add(1)
	go func() { defer drains.Done(); s.wg.Wait() }()
	done := make(chan struct{})
	go func() { drains.Wait(); close(done) }()
	var drainErr error
	select {
	case <-done:
	case <-shutdown.Done():
		drainErr = ErrDrainDeadline
		if config.GRPC != nil {
			config.GRPC.Stop()
		}
		for _, server := range config.HTTP {
			server.Close()
		}
	}
	if config.Admin != nil {
		if config.Admin.Shutdown(shutdown) != nil {
			config.Admin.Close()
		}
	}
	if serveErr != nil {
		return errors.Join(serveErr, drainErr)
	}
	return drainErr
}

// Flush limits telemetry cleanup independently of network availability. Docker's stop grace is the final process
// bound for code that ignores cancellation; arbitrary Go handlers cannot safely be killed individually.
func Flush(shutdown func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = shutdown(ctx)
}
