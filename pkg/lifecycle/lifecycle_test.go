package lifecycle

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func await[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case value := <-c:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("test synchronization timed out")
		var zero T
		return zero
	}
}
func code(t *testing.T, h http.Handler, path string) int {
	t.Helper()
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, nil))
	return r.Code
}

func TestLivenessReadinessAndAdmission(t *testing.T) {
	var dependency atomic.Bool
	s := New(func(context.Context) error {
		if !dependency.Load() {
			return errors.New("storage unavailable")
		}
		return nil
	})
	h := s.Health(s.HTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })))
	if code(t, h, "/livez") != 200 || code(t, h, "/readyz") != 503 || code(t, h, "/business") != 503 {
		t.Fatal("starting state admitted work")
	}
	s.MarkReady()
	if code(t, h, "/readyz") != 503 || code(t, h, "/livez") != 200 {
		t.Fatal("dependency failure affected liveness or was ignored")
	}
	dependency.Store(true)
	if code(t, h, "/readyz") != 200 || code(t, h, "/business") != 204 {
		t.Fatal("ready instance not available")
	}
	s.BeginDrain()
	s.MarkReady()
	if code(t, h, "/readyz") != 503 || code(t, h, "/livez") != 200 || code(t, h, "/business") != 503 {
		t.Fatal("draining state was reversible or admitted work")
	}
}

type heldHealth struct {
	healthpb.UnimplementedHealthServer
	entered, release chan struct{}
	calls            atomic.Int32
}

func (s *heldHealth) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	s.calls.Add(1)
	s.entered <- struct{}{}
	select {
	case <-s.release:
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}
func (s *heldHealth) Watch(_ *healthpb.HealthCheckRequest, stream grpc.ServerStreamingServer[healthpb.HealthCheckResponse]) error {
	if err := stream.Send(&healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func startGRPC(t *testing.T, s *State, server *heldHealth, grace time.Duration) (healthpb.HealthClient, context.CancelFunc, <-chan error, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(s.UnaryOption(), s.StreamOption())
	healthpb.RegisterHealthServer(gs, server)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ready, draining := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.Serve(ctx, Config{GRPC: gs, GRPCListener: listener, DrainDelay: 100 * time.Millisecond, GracePeriod: grace, OnReady: func() { close(ready) }, OnDrain: func() { close(draining) }})
	}()
	await(t, ready)
	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return healthpb.NewHealthClient(conn), cancel, done, draining
}

func TestUnaryDrainingFinishesAcceptedWorkAndJoinsWorkers(t *testing.T) {
	s := New(nil)
	workerStopped := make(chan struct{})
	s.Go(func(ctx context.Context) { <-ctx.Done(); close(workerStopped) })
	server := &heldHealth{entered: make(chan struct{}, 2), release: make(chan struct{})}
	client, cancel, done, draining := startGRPC(t, s, server, time.Second)
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	call := make(chan error, 1)
	go func() { _, err := client.Check(ctx, &healthpb.HealthCheckRequest{}); call <- err }()
	await(t, server.entered)
	cancel()
	await(t, draining)
	if code(t, s.Health(nil), "/readyz") != 503 || code(t, s.Health(nil), "/livez") != 200 {
		t.Fatal("probe state wrong while draining")
	}
	_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if status.Code(err) != codes.Unavailable || server.calls.Load() != 1 {
		t.Fatalf("new work reached handler: %v, calls=%d", err, server.calls.Load())
	}
	close(server.release)
	if err := await(t, call); err != nil {
		t.Fatalf("accepted call was interrupted: %v", err)
	}
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
	await(t, workerStopped)
}

func TestLongStreamHasBoundedShutdown(t *testing.T) {
	s := New(nil)
	client, cancel, done, draining := startGRPC(t, s, &heldHealth{}, 180*time.Millisecond)
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	cancel()
	await(t, draining)
	if err := await(t, done); !errors.Is(err, ErrDrainDeadline) {
		t.Fatalf("stream wasn't forced within budget: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("stream survived forced shutdown")
	}
}

func TestHTTPStreamingShutdownIsBounded(t *testing.T) {
	s := New(nil)
	ready := make(chan struct{})
	requestCanceled := make(chan struct{})
	server := &http.Server{Addr: "127.0.0.1:0", Handler: s.HTTP(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("initial\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(requestCanceled)
	}))}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.Serve(ctx, Config{HTTP: []*http.Server{server}, DrainDelay: 10 * time.Millisecond, GracePeriod: 100 * time.Millisecond, OnReady: func() { close(ready) }})
	}()
	await(t, ready)
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + server.Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	cancel()
	if err := await(t, done); !errors.Is(err, ErrDrainDeadline) {
		t.Fatal(err)
	}
	await(t, requestCanceled)
	if _, err := io.ReadAll(response.Body); err == nil {
		t.Fatal("HTTP stream ended as a false clean completion")
	}
}

func TestReadinessDoesNotRaceBackToReadyDuringDrain(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	s := New(func(ctx context.Context) error {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 500*time.Millisecond {
			t.Error("probe dependency wasn't bounded")
		}
		close(entered)
		<-release
		return nil
	})
	s.MarkReady()
	result := make(chan int, 1)
	go func() { result <- code(t, s.Health(nil), "/readyz") }()
	await(t, entered)
	s.BeginDrain()
	close(release)
	if got := await(t, result); got != 503 {
		t.Fatalf("in-flight probe advertised readiness after drain: %d", got)
	}
}

func TestBindFailureNeverAdvertisesReadinessOrStartsWorkers(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(nil)
	var started atomic.Bool
	s.Go(func(context.Context) { started.Store(true) })
	err = s.Serve(context.Background(), Config{GRPC: grpc.NewServer(), GRPCListener: grpcListener, Admin: &http.Server{Addr: occupied.Addr().String()}})
	if err == nil || s.available() || started.Load() {
		t.Fatalf("bad startup state: err=%v ready=%v worker=%v", err, s.available(), started.Load())
	}
	if _, err := grpcListener.Accept(); err == nil {
		t.Fatal("previously bound listener wasn't closed after startup failed")
	}
}

func TestNonCooperativeWorkerCannotExtendDrainBudget(t *testing.T) {
	s := New(nil)
	release, workerDone, ready := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s.Go(func(context.Context) { <-release; close(workerDone) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.Serve(ctx, Config{Admin: &http.Server{Addr: "127.0.0.1:0", Handler: s.Health(nil)}, DrainDelay: time.Millisecond, GracePeriod: 80 * time.Millisecond, OnReady: func() { close(ready) }})
	}()
	await(t, ready)
	cancel()
	err := await(t, done)
	close(release)
	await(t, workerDone)
	if !errors.Is(err, ErrDrainDeadline) {
		t.Fatalf("worker did not hit the shared deadline: %v", err)
	}
}
