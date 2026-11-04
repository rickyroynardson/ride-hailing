package gateway_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rickyroynardson/ride-hailing/pkg/grpcserver"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"github.com/rickyroynardson/ride-hailing/services/gateway/internal/gateway"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// publicRPCs is every RPC reachable from the internet. Adding a google.api.http option to a proto makes this test fail
// until the RPC is added here too: exposing a method should be a decision someone reviews, never a side effect.
var publicRPCs = []string{
	"location.v1.LocationService/GoOffline",
	"location.v1.LocationService/UpdateDriverLocation",
	"matching.v1.MatchingService/AcceptOffer",
	"matching.v1.MatchingService/DeclineOffer",
	"matching.v1.MatchingService/GetOffer",
	"matching.v1.MatchingService/WatchOffers",
	"payment.v1.PaymentService/GetCharge",
	"payment.v1.PaymentService/GetEarnings",
	"payment.v1.PaymentService/RetryCharge",
	"payment.v1.PaymentService/SetPaymentMethod",
	"ride.v1.RideService/CancelRide",
	"ride.v1.RideService/CompleteRide",
	"ride.v1.RideService/CreateRide",
	"ride.v1.RideService/EstimateFare",
	"ride.v1.RideService/StartRide",
	"ride.v1.RideService/WatchRide",
	"user.v1.AuthService/Login",
	"user.v1.AuthService/Logout",
	"user.v1.AuthService/Refresh",
	"user.v1.AuthService/Register",
	"user.v1.UserService/GetMe",
	"user.v1.UserService/RegisterDriver",
}

func TestOnlyPublicRPCsHaveRoutes(t *testing.T) {
	var routed []string
	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		if !strings.HasSuffix(string(fd.Package()), ".v1") || strings.HasPrefix(string(fd.Package()), "google.") || strings.HasPrefix(string(fd.Package()), "grpc.") {
			return true
		}
		for i := range fd.Services().Len() {
			svc := fd.Services().Get(i)
			for j := range svc.Methods().Len() {
				m := svc.Methods().Get(j)
				if rule, ok := proto.GetExtension(m.Options(), annotations.E_Http).(*annotations.HttpRule); ok && rule != nil {
					routed = append(routed, string(svc.FullName())+"/"+string(m.Name()))
				}
			}
		}
		return true
	})
	slices.Sort(routed)
	if fmt.Sprint(routed) != fmt.Sprint(publicRPCs) {
		t.Fatalf("RPCs with a public route:\n%s\nwant:\n%s", strings.Join(routed, "\n"), strings.Join(publicRPCs, "\n"))
	}
}

func TestGateway(t *testing.T) {
	be := startBackends(t)
	core, logs := observer.New(zap.InfoLevel)
	api := httptest.NewServer(gateway.New(zap.New(core), be.conns, deadLimiter(), gateway.Options{
		CORSOrigins: []string{"https://app.example"},
		Limits:      gateway.Limits{Auth: 1000, User: 1000, Anon: 1000},
	}))
	t.Cleanup(api.Close)

	// A unary call: JSON in, the token forwarded, JSON out.
	res, body := do(t, api, "POST", "/v1/rides", `{"pickup":{"lat":-6.2,"lng":106.8},"dropoff":{"lat":-6.3,"lng":106.9}}`,
		"Authorization", "Bearer rider-token",
		// Neither form may reach a service: it would let a client set metadata a service might trust.
		"Grpc-Metadata-X-Caller-Service", "ride", "X-Caller-Service", "ride")
	if res.StatusCode != 200 || !strings.Contains(body, `"rideId":"ride-1"`) || !strings.Contains(body, `"amount":"12000"`) {
		t.Fatalf("create ride: %d %s", res.StatusCode, body)
	}
	md := be.ride.lastMD()
	if got := md.Get("authorization"); fmt.Sprint(got) != "[Bearer rider-token]" {
		t.Fatalf("authorization forwarded as %v", got)
	}
	for key := range md {
		if strings.Contains(key, "caller") || strings.HasPrefix(key, "grpcgateway-") {
			t.Fatalf("client header reached the service as metadata %q", key)
		}
	}
	if be.ride.lastCreate().GetPickup().GetLat() != -6.2 {
		t.Fatalf("request body: %+v", be.ride.lastCreate())
	}

	// gRPC errors become HTTP statuses, in grpc-gateway's JSON error shape.
	res, body = do(t, api, "POST", "/v1/rides/ride-404:cancel", "", "Authorization", "Bearer rider-token")
	if res.StatusCode != 404 || !strings.Contains(body, `"code":5`) {
		t.Fatalf("cancel unknown ride: %d %s", res.StatusCode, body)
	}
	// Requests are logged and measured by route, not by path, so each ride doesn't become its own series.
	var routes []string
	for _, e := range logs.FilterMessage("request").All() {
		routes = append(routes, fmt.Sprint(e.ContextMap()["route"], " ", e.ContextMap()["status"]))
	}
	if want := "[/v1/rides 200 /v1/rides/{ride_id=*}:cancel 404]"; fmt.Sprint(routes) != want {
		t.Fatalf("logged routes %v, want %s", routes, want)
	}

	// Service-to-service RPCs have no route, by any spelling.
	for _, path := range []string{"/ride.v1.RideService/AssignDriver", "/v1/rides/ride-1:assign", "/v1/payment.v1.PaymentService/ChargeRide"} {
		if res, _ := do(t, api, "POST", path, `{}`, "Authorization", "Bearer rider-token"); res.StatusCode != 404 {
			t.Fatalf("POST %s: %d, want 404", path, res.StatusCode)
		}
	}
	if be.ride.assigns() != 0 {
		t.Fatal("an internal RPC was reached through the gateway")
	}

	// A stream arrives message by message as the service sends, not all at the end, and closing it reaches the
	// service.
	req, _ := http.NewRequest("GET", api.URL+"/v1/rides/ride-1:watch", nil)
	req.Header.Set("Authorization", "Bearer rider-token")
	stream, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	lines := bufio.NewScanner(stream.Body)
	readLine := func(what string) string {
		t.Helper()
		got := make(chan string, 1)
		go func() {
			if lines.Scan() {
				got <- lines.Text()
			}
			close(got)
		}()
		select {
		case l, ok := <-got:
			if !ok {
				t.Fatalf("%s: stream ended (%v)", what, lines.Err())
			}
			return l
		case <-time.After(3 * time.Second):
			t.Fatalf("%s: nothing within 3s; the stream isn't being flushed", what)
		}
		return ""
	}
	if l := readLine("first update"); !strings.Contains(l, `"status":"RIDE_STATUS_REQUESTED"`) {
		t.Fatalf("first update: %s", l)
	}
	be.ride.release <- struct{}{} // the service sends the second update only now
	if l := readLine("second update"); !strings.Contains(l, `"status":"RIDE_STATUS_ACCEPTED"`) {
		t.Fatalf("second update: %s", l)
	}
	stream.Body.Close()
	select {
	case <-be.ride.watchEnded:
	case <-time.After(3 * time.Second):
		t.Fatal("closing the HTTP stream didn't end the gRPC stream")
	}

	// Heartbeats aren't traced: the location service gets them unsampled, while other calls carry a sampled trace.
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(noop.NewTracerProvider()) })
	traced := httptest.NewServer(gateway.New(zap.NewNop(), be.conns, deadLimiter(), gateway.Options{Limits: gateway.Limits{Auth: 1000, User: 1000, Anon: 1000}}))
	t.Cleanup(traced.Close)
	do(t, traced, "PUT", "/v1/driver/location", `{"location":{"lat":1,"lng":1}}`, "Authorization", "Bearer d",
		"Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if tp := be.location.lastTraceparent(); !strings.HasSuffix(tp, "-00") {
		t.Fatalf("heartbeat reached location with traceparent %q, want unsampled", tp)
	}
	do(t, traced, "POST", "/v1/fares:estimate", `{}`, "Authorization", "Bearer r")
	if tp := be.ride.lastTraceparent(); !strings.HasSuffix(tp, "-01") {
		t.Fatalf("estimate reached ride with traceparent %q, want sampled", tp)
	}

	// Bodies are bounded.
	res, _ = do(t, api, "POST", "/v1/auth/login", `{"email":"`+strings.Repeat("a", 100<<10)+`"}`)
	if res.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("huge body: %d, want 413", res.StatusCode)
	}

	// CORS: the listed origin gets a preflight answer; any other origin gets nothing, so browsers block it.
	res, _ = do(t, api, "OPTIONS", "/v1/rides", "", "Origin", "https://app.example", "Access-Control-Request-Method", "POST")
	if res.StatusCode != 204 || res.Header.Get("Access-Control-Allow-Origin") != "https://app.example" ||
		!strings.Contains(res.Header.Get("Access-Control-Allow-Headers"), "Authorization") {
		t.Fatalf("preflight: %d %v", res.StatusCode, res.Header)
	}
	res, _ = do(t, api, "OPTIONS", "/v1/rides", "", "Origin", "https://evil.example", "Access-Control-Request-Method", "POST")
	if res.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("an unlisted origin was allowed")
	}

	// Every request above went through with the rate limiter's Redis unreachable: the limiter fails open.
}

// TestRateLimits runs against a real Redis. Skipped with -short or when Docker is unavailable.
func TestRateLimits(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()
	rc, err := tcredis.Run(ctx, "redis:8-alpine")
	testcontainers.CleanupContainer(t, rc)
	if err != nil {
		t.Fatal(err)
	}
	url, err := rc.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	t.Cleanup(func() { rdb.Close() })

	be := startBackends(t)
	api := httptest.NewServer(gateway.New(zap.NewNop(), be.conns, gateway.NewLimiter(rdb, zap.NewNop()), gateway.Options{
		Limits: gateway.Limits{Auth: 3, User: 5, Anon: 2},
	}))
	t.Cleanup(api.Close)

	// Logins are counted per client IP, tightly.
	for i := range 3 {
		if res, body := do(t, api, "POST", "/v1/auth/login", `{"email":"a@b.c","password":"x"}`); res.StatusCode != 200 {
			t.Fatalf("login %d: %d %s", i+1, res.StatusCode, body)
		}
	}
	res, body := do(t, api, "POST", "/v1/auth/login", `{"email":"a@b.c","password":"x"}`)
	if res.StatusCode != http.StatusTooManyRequests || res.Header.Get("Retry-After") == "" || !strings.Contains(body, `"code":8`) {
		t.Fatalf("4th login: %d %v %s", res.StatusCode, res.Header, body)
	}
	var e struct{ Message string }
	if json.Unmarshal([]byte(body), &e) != nil || e.Message == "" {
		t.Fatalf("429 body isn't grpc-gateway's error shape: %s", body)
	}

	// Signed-in users are counted per token, so one user can't use up another's allowance.
	estimate := `{"pickup":{"lat":-6.2,"lng":106.8},"dropoff":{"lat":-6.3,"lng":106.9}}`
	for range 5 {
		do(t, api, "POST", "/v1/fares:estimate", estimate, "Authorization", "Bearer token-a")
	}
	if res, _ := do(t, api, "POST", "/v1/fares:estimate", estimate, "Authorization", "Bearer token-a"); res.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("6th request for a: %d, want 429", res.StatusCode)
	}
	if res, _ := do(t, api, "POST", "/v1/fares:estimate", estimate, "Authorization", "Bearer token-b"); res.StatusCode != 200 {
		t.Fatalf("b limited by a's requests: %d", res.StatusCode)
	}

	// Requests without a token are counted per IP, apart from logins.
	for range 2 {
		do(t, api, "GET", "/v1/me", "")
	}
	if res, _ := do(t, api, "GET", "/v1/me", ""); res.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("3rd anonymous request: %d, want 429", res.StatusCode)
	}

	// Tokens never reach Redis as they are.
	keys, _ := rdb.Keys(ctx, "*").Result()
	for _, k := range keys {
		if strings.Contains(k, "token-a") {
			t.Fatalf("raw token stored in key %q", k)
		}
	}
}

func do(t *testing.T, api *httptest.Server, method, path, body string, headers ...string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequest(method, api.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return res, string(b)
}

// deadLimiter points at a Redis that isn't there.
func deadLimiter() *gateway.Limiter {
	return gateway.NewLimiter(redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1}), zap.NewNop())
}

type backends struct {
	conns    gateway.Backends
	ride     *fakeRide
	location *fakeLocation
}

// startBackends serves fakes of every service on one in-memory gRPC server.
func startBackends(t *testing.T) backends {
	t.Helper()
	ride := &fakeRide{release: make(chan struct{}, 1), watchEnded: make(chan struct{}, 1)}
	location := &fakeLocation{}
	gs := grpc.NewServer()
	userv1.RegisterAuthServiceServer(gs, fakeAuth{})
	userv1.RegisterUserServiceServer(gs, userv1.UnimplementedUserServiceServer{})
	ridev1.RegisterRideServiceServer(gs, ride)
	locationv1.RegisterLocationServiceServer(gs, location)
	matchingv1.RegisterMatchingServiceServer(gs, matchingv1.UnimplementedMatchingServiceServer{})
	paymentv1.RegisterPaymentServiceServer(gs, paymentv1.UnimplementedPaymentServiceServer{})
	lis := bufconn.Listen(1 << 20)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpcserver.ClientTracing(), // as cmd/main.go dials, so the trace context reaches the fakes
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return backends{conns: gateway.Backends{User: conn, Ride: conn, Location: conn, Matching: conn, Payment: conn}, ride: ride, location: location}
}

type fakeAuth struct {
	userv1.UnimplementedAuthServiceServer
}

func (fakeAuth) Login(context.Context, *userv1.LoginRequest) (*userv1.LoginResponse, error) {
	return &userv1.LoginResponse{Tokens: &userv1.Token{AccessToken: "a", RefreshToken: "r"}}, nil
}

type fakeRide struct {
	ridev1.UnimplementedRideServiceServer
	mu         sync.Mutex
	tp         string
	md         metadata.MD
	create     *ridev1.CreateRideRequest
	assigned   int
	release    chan struct{}
	watchEnded chan struct{}
}

// traceparentOf is the trace context a call arrived with.
func traceparentOf(ctx context.Context) string {
	md, _ := metadata.FromIncomingContext(ctx)
	if v := md.Get("traceparent"); len(v) > 0 {
		return v[0]
	}
	return ""
}

type fakeLocation struct {
	locationv1.UnimplementedLocationServiceServer
	mu sync.Mutex
	tp string
}

func (f *fakeLocation) UpdateDriverLocation(ctx context.Context, _ *locationv1.UpdateDriverLocationRequest) (*locationv1.UpdateDriverLocationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tp = traceparentOf(ctx)
	return &locationv1.UpdateDriverLocationResponse{}, nil
}

func (f *fakeLocation) lastTraceparent() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tp
}

func (f *fakeRide) lastTraceparent() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tp
}

func (f *fakeRide) EstimateFare(ctx context.Context, _ *ridev1.EstimateFareRequest) (*ridev1.EstimateFareResponse, error) {
	f.mu.Lock()
	f.tp = traceparentOf(ctx)
	f.mu.Unlock()
	return &ridev1.EstimateFareResponse{Fare: &ridev1.Money{Amount: 12_000, Currency: "IDR"}}, nil
}

func (f *fakeRide) CreateRide(ctx context.Context, in *ridev1.CreateRideRequest) (*ridev1.CreateRideResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.mu.Lock()
	f.md, f.create = md, in
	f.mu.Unlock()
	return &ridev1.CreateRideResponse{RideId: "ride-1", Fare: &ridev1.Money{Amount: 12_000, Currency: "IDR"}}, nil
}

func (f *fakeRide) CancelRide(context.Context, *ridev1.CancelRideRequest) (*ridev1.CancelRideResponse, error) {
	return nil, status.Error(codes.NotFound, "ride not found")
}

func (f *fakeRide) AssignDriver(context.Context, *ridev1.AssignDriverRequest) (*ridev1.AssignDriverResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assigned++
	return &ridev1.AssignDriverResponse{}, nil
}

func (f *fakeRide) WatchRide(in *ridev1.WatchRideRequest, stream grpc.ServerStreamingServer[ridev1.WatchRideResponse]) error {
	defer func() { f.watchEnded <- struct{}{} }()
	if err := stream.Send(&ridev1.WatchRideResponse{Ride: &ridev1.Ride{RideId: in.GetRideId(), Status: ridev1.RideStatus_RIDE_STATUS_REQUESTED}}); err != nil {
		return err
	}
	select {
	case <-f.release:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	if err := stream.Send(&ridev1.WatchRideResponse{Ride: &ridev1.Ride{RideId: in.GetRideId(), Status: ridev1.RideStatus_RIDE_STATUS_ACCEPTED}}); err != nil {
		return err
	}
	<-stream.Context().Done() // a watch never ends by itself
	return stream.Context().Err()
}

func (f *fakeRide) lastMD() metadata.MD {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.md
}

func (f *fakeRide) lastCreate() *ridev1.CreateRideRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.create
}

func (f *fakeRide) assigns() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.assigned
}
