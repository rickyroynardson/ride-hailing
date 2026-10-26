package server_test

import (
	"context"
	"database/sql"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/auth"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/matching"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/server"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const testSecret = "test-secret"

// fakeLocation returns a fixed list of nearby drivers, nearest first.
type fakeLocation struct {
	locationv1.LocationServiceClient
	mu      sync.Mutex
	drivers []string
}

func (f *fakeLocation) ListNearbyDrivers(context.Context, *locationv1.ListNearbyDriversRequest, ...grpc.CallOption) (*locationv1.ListNearbyDriversResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	res := &locationv1.ListNearbyDriversResponse{}
	for _, id := range f.drivers {
		res.Drivers = append(res.Drivers, &locationv1.NearbyDriver{DriverId: id})
	}
	return res, nil
}

func (f *fakeLocation) set(drivers ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.drivers = drivers
}

// fakeRide drops the drivers marked busy, like the ride service does for drivers on an active ride.
type fakeRide struct {
	ridev1.RideServiceClient
	busy map[string]bool
}

func (f *fakeRide) FilterAvailableDrivers(_ context.Context, in *ridev1.FilterAvailableDriversRequest, _ ...grpc.CallOption) (*ridev1.FilterAvailableDriversResponse, error) {
	res := &ridev1.FilterAvailableDriversResponse{}
	for _, id := range in.GetDriverIds() {
		if !f.busy[id] {
			res.DriverIds = append(res.DriverIds, id)
		}
	}
	return res, nil
}

// TestIntegration runs the real gRPC server, interceptors and SQL against a throwaway Postgres container, with
// location and ride replaced by fakes. Skipped with -short or when Docker is unavailable.
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx, "postgres:18-alpine", tcpostgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, pg)
	if err != nil {
		t.Fatal(err)
	}
	url := pg.MustConnectionString(ctx, "sslmode=disable")

	migrate(t, url)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	const (
		rideA    = "aaaaaaaa-0000-0000-0000-000000000001"
		rideB    = "aaaaaaaa-0000-0000-0000-000000000002"
		rideC    = "aaaaaaaa-0000-0000-0000-000000000003"
		onRide   = "dddddddd-0000-0000-0000-000000000001"
		declined = "dddddddd-0000-0000-0000-000000000002"
		busyElse = "dddddddd-0000-0000-0000-000000000003"
		free     = "dddddddd-0000-0000-0000-000000000004"
		further  = "dddddddd-0000-0000-0000-000000000005"
	)
	location := &fakeLocation{}
	client := startServer(t, pool, location, &fakeRide{busy: map[string]bool{onRide: true}})
	service := withServiceToken(t, ctx)
	pickup := &matchingv1.Location{Lat: -6.2, Lng: 106.8}

	_, err = client.StartMatching(withUserToken(t, ctx, "11111111-1111-1111-1111-111111111111"), &matchingv1.StartMatchingRequest{RideId: rideA, Pickup: pickup})
	wantCode(t, "user token", err, codes.PermissionDenied)
	_, err = client.StartMatching(service, &matchingv1.StartMatchingRequest{RideId: "ride-a", Pickup: pickup})
	wantCode(t, "bad ride id", err, codes.InvalidArgument)
	_, err = client.StartMatching(service, &matchingv1.StartMatchingRequest{RideId: rideA})
	wantCode(t, "missing pickup", err, codes.InvalidArgument)

	// No drivers nearby: no offer yet, and no error.
	res, err := client.StartMatching(service, &matchingv1.StartMatchingRequest{RideId: rideA, Pickup: pickup})
	if err != nil || res.GetOfferId() != "" {
		t.Fatalf("no drivers: got %+v, %v", res, err)
	}

	// Skips the driver on a ride, the one who declined this ride, and the one holding another ride's offer.
	for _, o := range []struct{ ride, driver, status string }{{rideA, declined, "declined"}, {rideB, busyElse, "pending"}} {
		if _, err := pool.Exec(ctx, `INSERT INTO offers(ride_id, driver_id, status, expires_at) VALUES ($1, $2, $3, now() + interval '15 seconds')`, o.ride, o.driver, o.status); err != nil {
			t.Fatal(err)
		}
	}
	location.set(onRide, declined, busyElse, free, further)
	before := time.Now()
	res, err = client.StartMatching(service, &matchingv1.StartMatchingRequest{RideId: rideA, Pickup: pickup})
	if err != nil || res.GetDriverId() != free || res.GetOfferId() == "" {
		t.Fatalf("first offer: got %+v, %v; want driver %s", res, err, free)
	}
	var expiresAt time.Time
	if err := pool.QueryRow(ctx, `SELECT expires_at FROM offers WHERE id = $1 AND status = 'pending'`, res.GetOfferId()).Scan(&expiresAt); err != nil {
		t.Fatal(err)
	}
	if d := expiresAt.Sub(before); d < 14*time.Second || d > 16*time.Second {
		t.Fatalf("offer expires in %v, want about 15s", d)
	}

	// Calling again while the offer is pending returns the same offer (ride may retry after a timeout).
	again, err := client.StartMatching(service, &matchingv1.StartMatchingRequest{RideId: rideA, Pickup: pickup})
	if err != nil || again.GetOfferId() != res.GetOfferId() {
		t.Fatalf("retry: got %+v, %v; want offer %s", again, err, res.GetOfferId())
	}

	// Concurrent calls for a new ride create exactly one pending offer and all return it.
	location.set(further)
	var wg sync.WaitGroup
	offers := make([]string, 10)
	for i := range offers {
		wg.Go(func() {
			r, err := client.StartMatching(service, &matchingv1.StartMatchingRequest{RideId: rideC, Pickup: pickup})
			if err == nil {
				offers[i] = r.GetOfferId()
			}
		})
	}
	wg.Wait()
	var pending int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM offers WHERE ride_id = $1 AND status = 'pending'`, rideC).Scan(&pending); err != nil {
		t.Fatal(err)
	}
	for i, id := range offers {
		if id == "" || id != offers[0] {
			t.Fatalf("concurrent call %d returned offer %q, want %q from all", i, id, offers[0])
		}
	}
	if pending != 1 {
		t.Fatalf("concurrent start: %d pending offers, want 1", pending)
	}
}

func migrate(t *testing.T, url string) {
	t.Helper()
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	provider, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS("../../migrations"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func startServer(t *testing.T, pool *pgxpool.Pool, location locationv1.LocationServiceClient, ride ridev1.RideServiceClient) matchingv1.MatchingServiceClient {
	t.Helper()
	gs := server.NewGRPCServer(zap.NewNop(), auth.NewVerifier(testSecret), matching.NewService(matching.NewRepository(pool), location, ride))
	lis := bufconn.Listen(1 << 20)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return matchingv1.NewMatchingServiceClient(conn)
}

// withServiceToken attaches a service token the way ServiceCredentials does for the ride service's calls.
func withServiceToken(t *testing.T, ctx context.Context) context.Context {
	t.Helper()
	md, err := auth.NewServiceCredentials("ride", testSecret).GetRequestMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", md["authorization"])
}

// withUserToken signs an access token the way the user service does.
func withUserToken(t *testing.T, ctx context.Context, userID string) context.Context {
	t.Helper()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.RegisteredClaims{
		Subject:   userID,
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	}).SignedString([]byte(testSecret))
	if err != nil {
		t.Fatal(err)
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
}

func wantCode(t *testing.T, name string, err error, code codes.Code) {
	t.Helper()
	if got := status.Code(err); got != code {
		t.Fatalf("%s: got %v (%v), want %v", name, got, err, code)
	}
}
