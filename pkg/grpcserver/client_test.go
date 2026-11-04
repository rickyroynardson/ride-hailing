package grpcserver

import (
	"context"
	"errors"
	"net"
	"net/url"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
)

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for resolver")
		var zero T
		return zero
	}
}

type resolverRecorder struct {
	resolver.ClientConn
	states chan resolver.State
	errors chan error
}

func (r *resolverRecorder) UpdateState(s resolver.State) error { r.states <- s; return nil }
func (r *resolverRecorder) ReportError(err error)              { r.errors <- err }

func TestServiceDNSRefresh(t *testing.T) {
	answers := make(chan []string, 8)
	ticks := make(chan time.Time)
	stopped := make(chan struct{})
	b := &serviceDNSBuilder{
		lookup: func(ctx context.Context, host string) ([]string, error) {
			if host != "ride" {
				return nil, errors.New("unexpected host")
			}
			select {
			case ips := <-answers:
				if ips == nil {
					return nil, errors.New("temporary DNS failure")
				}
				return ips, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		},
		ticks: func() (<-chan time.Time, func()) { return ticks, func() { close(stopped) } },
	}
	cc := &resolverRecorder{states: make(chan resolver.State, 8), errors: make(chan error, 8)}
	u, _ := url.Parse("servicedns:///ride:5000")
	answers <- nil // Initial failure must recover on a later tick, without a connection hint.
	r, err := b.Build(resolver.Target{URL: *u}, cc, resolver.BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	receive(t, cc.errors)
	answers <- []string{"127.0.0.2", "127.0.0.1", "::ffff:127.0.0.1"}
	ticks <- time.Now()
	state := receive(t, cc.states)
	want := []resolver.Address{{Addr: "127.0.0.1:5000"}, {Addr: "127.0.0.2:5000"}}
	if !reflect.DeepEqual(state.Addresses, want) {
		t.Fatalf("addresses: %v", state.Addresses)
	}
	answers <- []string{"127.0.0.1", "127.0.0.2"} // Reordered/duplicate answers don't republish.
	ticks <- time.Now()
	answers <- nil
	ticks <- time.Now()
	receive(t, cc.errors)
	select {
	case s := <-cc.states:
		t.Fatalf("unexpected state after unchanged/failed DNS: %v", s)
	default:
	}
	answers <- []string{"127.0.0.3"}
	ticks <- time.Now()
	if got := receive(t, cc.states).Addresses[0].Addr; got != "127.0.0.3:5000" {
		t.Fatal(got)
	}
	r.Close()
	receive(t, stopped)
}

func TestServiceDNSCloseCancelsLookup(t *testing.T) {
	started := make(chan struct{})
	stopped := make(chan struct{})
	b := &serviceDNSBuilder{
		lookup: func(ctx context.Context, _ string) ([]string, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		},
		ticks: func() (<-chan time.Time, func()) { return make(chan time.Time), func() { close(stopped) } },
	}
	u, _ := url.Parse("servicedns:///ride:5000")
	r, err := b.Build(resolver.Target{URL: *u}, &resolverRecorder{}, resolver.BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	receive(t, started)
	done := make(chan struct{})
	go func() { r.Close(); close(done) }()
	receive(t, done)
	receive(t, stopped)
}

func TestServiceEndpoint(t *testing.T) {
	for _, address := range []string{"ride:5000", "dns:///ride:5000", "127.0.0.1:5000", "[::1]:5000"} {
		if _, err := serviceEndpoint(address); err != nil {
			t.Errorf("%s: %v", address, err)
		}
	}
	for _, address := range []string{"ride", "ride:0", "ride:65536", "ride:http", ":5000", "dns://other/ride:5000", "unix:///socket", "passthrough:///ride:5000", "dns:///ride:5000?x=1"} {
		if _, err := NewClient(address); err == nil {
			t.Errorf("accepted %q", address)
		}
	}
}

func localServer(t *testing.T, register func(*grpc.Server)) (string, func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	register(s)
	go s.Serve(l)
	t.Cleanup(s.Stop)
	return l.Addr().String(), s.Stop
}

// Synthetic DNS IPs are routed to tiny loopback servers. The old address stays reachable and speaks gRPC,
// but no longer implements the intended service: no transport failure prompts normal gRPC DNS discovery.
func TestClientRecoversFromHealthyWrongService(t *testing.T) {
	healthy, _ := localServer(t, func(s *grpc.Server) { healthpb.RegisterHealthServer(s, health.NewServer()) })
	original, stopOriginal := localServer(t, func(s *grpc.Server) { healthpb.RegisterHealthServer(s, health.NewServer()) })
	wrong, _ := localServer(t, func(*grpc.Server) {})
	var mu sync.Mutex
	oldDestination := original
	ip := "192.0.2.1"
	ticks := make(chan time.Time)
	b := &serviceDNSBuilder{
		lookup: func(context.Context, string) ([]string, error) {
			mu.Lock()
			defer mu.Unlock()
			return []string{ip}, nil
		},
		ticks: func() (<-chan time.Time, func()) { return ticks, func() {} },
	}
	var intercepted atomic.Int32
	conn, err := newServiceClient("ride:5000", b,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			intercepted.Add(1)
			return invoke(ctx, method, req, reply, cc, opts...)
		}),
		grpc.WithContextDialer(func(ctx context.Context, address string) (net.Conn, error) {
			mu.Lock()
			destination := healthy
			if address == "192.0.2.1:5000" {
				destination = oldDestination
			}
			mu.Unlock()
			return (&net.Dialer{}).DialContext(ctx, "tcp", destination)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := healthpb.NewHealthClient(conn)
	check := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := client.Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
		return err
	}
	if err := check(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	oldDestination = wrong
	mu.Unlock()
	// Force the original transport to reconnect, simulating recreation and reuse of its address.
	stopOriginal()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := check()
		if status.Code(err) == codes.Unimplemented {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("want healthy wrong service, got %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	ip = "192.0.2.2"
	mu.Unlock()
	ticks <- time.Now()
	// Synchronize with discovery using a bounded, low-volume poll; this isn't a load test.
	deadline = time.Now().Add(5 * time.Second)
	for {
		if err := check(); err == nil {
			break
		} else if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if intercepted.Load() < 3 {
		t.Fatal("caller interceptor was not preserved")
	}
}

type unavailableHealth struct {
	healthpb.UnimplementedHealthServer
	calls atomic.Int32
}

func (s *unavailableHealth) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	s.calls.Add(1)
	return nil, status.Error(codes.Unavailable, "handler received request")
}

func TestClientLiteralIPDoesNotReplayRPC(t *testing.T) {
	s := &unavailableHealth{}
	address, _ := localServer(t, func(gs *grpc.Server) { healthpb.RegisterHealthServer(gs, s) })
	b := &serviceDNSBuilder{
		lookup: func(context.Context, string) ([]string, error) { t.Error("literal IP used DNS"); return nil, nil },
		ticks:  func() (<-chan time.Time, func()) { t.Error("literal IP started ticker"); return nil, func() {} },
	}
	conn, err := newServiceClient(address, b, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if status.Code(err) != codes.Unavailable || s.calls.Load() != 1 {
		t.Fatalf("error=%v calls=%d", err, s.calls.Load())
	}
}
