package server_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/postgres"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/database"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/ride"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/server"
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

// TestIntegration runs the real gRPC server, interceptors and SQL against a throwaway Postgres container.
// Skipped with -short or when Docker is unavailable.
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

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	// The startup check refuses an unmigrated database and accepts a migrated one.
	if err := postgres.CheckSchema(ctx, pool, database.SchemaVersion); !errors.Is(err, postgres.ErrNotMigrated) {
		t.Fatalf("schema check before migrating: got %v, want ErrNotMigrated", err)
	}
	migrate(t, url)
	if err := postgres.CheckSchema(ctx, pool, database.SchemaVersion); err != nil {
		t.Fatalf("schema check after migrating: %v", err)
	}

	client := startServer(t, pool)

	const riderID = "11111111-1111-1111-1111-111111111111"
	rider := withToken(t, ctx, riderID)
	pickup, dropoff := &ridev1.Location{Lat: -6.2, Lng: 106.8}, &ridev1.Location{Lat: -6.3, Lng: 106.9}

	_, err = client.CreateRide(ctx, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	wantCode(t, "no token", err, codes.Unauthenticated)

	_, err = client.CreateRide(rider, &ridev1.CreateRideRequest{Pickup: pickup})
	wantCode(t, "missing dropoff", err, codes.InvalidArgument)

	_, err = client.CreateRide(rider, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: &ridev1.Location{Lat: 91}})
	wantCode(t, "lat out of range", err, codes.InvalidArgument)

	// The rider comes from the token, and the ride starts as requested.
	res, err := client.CreateRide(rider, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatal(err)
	}
	var gotRider, gotStatus string
	if err := pool.QueryRow(ctx, `SELECT rider_id, status FROM rides WHERE id = $1`, res.GetRideId()).Scan(&gotRider, &gotStatus); err != nil {
		t.Fatal(err)
	}
	if gotRider != riderID || gotStatus != "requested" {
		t.Fatalf("stored ride: rider %q status %q", gotRider, gotStatus)
	}

	_, err = client.CreateRide(rider, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	wantCode(t, "second active ride", err, codes.AlreadyExists)

	// Once the ride is finished, the rider can request again.
	if _, err := pool.Exec(ctx, `UPDATE rides SET status = 'completed' WHERE id = $1`, res.GetRideId()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CreateRide(rider, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff}); err != nil {
		t.Fatalf("request after completed ride: %v", err)
	}

	// Concurrent requests from one rider (double tap, retries): exactly one may win.
	other := withToken(t, ctx, "22222222-2222-2222-2222-222222222222")
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if _, err := client.CreateRide(other, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff}); err == nil {
				wins.Add(1)
			}
		})
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("concurrent create: %d wins, want 1", wins.Load())
	}

	// FilterAvailableDrivers drops drivers on an accepted or in-progress ride, keeps everyone else in order.
	const (
		onAccepted   = "aaaaaaaa-0000-0000-0000-000000000001"
		onInProgress = "aaaaaaaa-0000-0000-0000-000000000002"
		finished     = "aaaaaaaa-0000-0000-0000-000000000003"
		noRide       = "aaaaaaaa-0000-0000-0000-000000000004"
	)
	for i, r := range []struct{ driver, status string }{{onAccepted, "accepted"}, {onInProgress, "in_progress"}, {finished, "completed"}} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO rides(rider_id, driver_id, status, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng) VALUES ($1, $2, $3, 0, 0, 1, 1)`,
			fmt.Sprintf("bbbbbbbb-0000-0000-0000-00000000000%d", i), r.driver, r.status,
		); err != nil {
			t.Fatal(err)
		}
	}
	md, err := jwtauth.NewServiceCredentials("matching", testSecret).GetRequestMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	service := metadata.AppendToOutgoingContext(ctx, "authorization", md["authorization"])

	filter := &ridev1.FilterAvailableDriversRequest{DriverIds: []string{noRide, onAccepted, finished, "not-a-uuid", onInProgress}}
	avail, err := client.FilterAvailableDrivers(service, filter)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(avail.GetDriverIds()); got != fmt.Sprint([]string{noRide, finished}) {
		t.Fatalf("available drivers: got %s", got)
	}

	_, err = client.FilterAvailableDrivers(rider, filter)
	wantCode(t, "filter with user token", err, codes.PermissionDenied)

	_, err = client.FilterAvailableDrivers(service, &ridev1.FilterAvailableDriversRequest{DriverIds: make([]string, 101)})
	wantCode(t, "too many ids", err, codes.InvalidArgument)

	// AssignDriver: requested → accepted, idempotent for the same driver, refused otherwise.
	newRide := func(rider string) string {
		t.Helper()
		var id string
		if err := pool.QueryRow(ctx,
			`INSERT INTO rides(rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng) VALUES ($1, 0, 0, 1, 1) RETURNING id`, rider,
		).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	const assignee, otherDriver = "cccccccc-0000-0000-0000-000000000001", "cccccccc-0000-0000-0000-000000000002"
	assigned := newRide("bbbbbbbb-0000-0000-0000-000000000011")
	assign := func(ride, driver string) error {
		_, err := client.AssignDriver(service, &ridev1.AssignDriverRequest{RideId: ride, DriverId: driver})
		return err
	}

	_, err = client.AssignDriver(rider, &ridev1.AssignDriverRequest{RideId: assigned, DriverId: assignee})
	wantCode(t, "assign with user token", err, codes.PermissionDenied)
	wantCode(t, "assign bad ids", assign("ride-1", assignee), codes.InvalidArgument)
	for i := range 2 {
		if err := assign(assigned, assignee); err != nil {
			t.Fatalf("assign attempt %d: %v", i+1, err)
		}
	}
	var st, drv string
	pool.QueryRow(ctx, `SELECT status, driver_id FROM rides WHERE id = $1`, assigned).Scan(&st, &drv)
	if st != "accepted" || drv != assignee {
		t.Fatalf("assigned ride: status %q driver %q", st, drv)
	}
	wantCode(t, "assign taken ride to another driver", assign(assigned, otherDriver), codes.FailedPrecondition)
	wantCode(t, "assign unknown ride", assign("cccccccc-0000-0000-0000-00000000ffff", otherDriver), codes.FailedPrecondition)
	wantCode(t, "assign busy driver", assign(newRide("bbbbbbbb-0000-0000-0000-000000000012"), assignee), codes.AlreadyExists)

	cancelled := newRide("bbbbbbbb-0000-0000-0000-000000000013")
	pool.Exec(ctx, `UPDATE rides SET status = 'cancelled' WHERE id = $1`, cancelled)
	wantCode(t, "assign cancelled ride", assign(cancelled, otherDriver), codes.FailedPrecondition)
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

func startServer(t *testing.T, pool *pgxpool.Pool) ridev1.RideServiceClient {
	t.Helper()
	gs := server.NewGRPCServer(zap.NewNop(), jwtauth.NewVerifier(testSecret), ride.NewService(ride.NewRepository(pool)))
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

	return ridev1.NewRideServiceClient(conn)
}

// withToken signs an access token the way the user service does.
func withToken(t *testing.T, ctx context.Context, userID string) context.Context {
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
