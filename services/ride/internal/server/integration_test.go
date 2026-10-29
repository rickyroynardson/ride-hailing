package server_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
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
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/database"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/outbox"
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
	pool.Exec(ctx, `UPDATE rides SET status = 'cancelled', cancel_reason = 'rider' WHERE id = $1`, cancelled)
	wantCode(t, "assign cancelled ride", assign(cancelled, otherDriver), codes.FailedPrecondition)

	// Outbox: CreateRide saves a "start matching" message with the ride, and the relay delivers it.
	relayMatching := &fakeMatching{}
	relay := outbox.NewRelay(pool, relayMatching)
	runRelay := func() error {
		t.Helper()
		return relay.Run(ctx)
	}
	pool.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE sent_at IS NULL`) // earlier steps' rides are not this step's concern
	outboxRider := withToken(t, ctx, "dddddddd-0000-0000-0000-000000000001")
	created, err := client.CreateRide(outboxRider, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatal(err)
	}
	type outboxRow struct {
		kind, rideID string
		attempts     int
		sent         bool
		lastError    string
	}
	rowFor := func(rideID string) outboxRow {
		t.Helper()
		var r outboxRow
		if err := pool.QueryRow(ctx,
			`SELECT kind, payload->>'ride_id', attempts, sent_at IS NOT NULL, COALESCE(last_error, '') FROM outbox WHERE payload->>'ride_id' = $1`, rideID,
		).Scan(&r.kind, &r.rideID, &r.attempts, &r.sent, &r.lastError); err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := rowFor(created.GetRideId()); r.kind != "start_matching" || r.sent || r.attempts != 0 {
		t.Fatalf("outbox row after CreateRide: %+v", r)
	}

	// The ride and its message are saved together: a refused CreateRide leaves no message behind.
	var before, after int
	pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&before)
	_, err = client.CreateRide(outboxRider, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	wantCode(t, "second active ride", err, codes.AlreadyExists)
	pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&after)
	if after != before {
		t.Fatalf("refused CreateRide left %d outbox rows", after-before)
	}

	// Matching down: the message stays unsent, with the attempt and error recorded.
	relayMatching.fail(status.Error(codes.Unavailable, "matching down"))
	if err := runRelay(); err == nil {
		t.Fatal("relay with matching down: want an error")
	}
	if r := rowFor(created.GetRideId()); r.sent || r.attempts != 1 || !strings.Contains(r.lastError, "matching down") {
		t.Fatalf("outbox row with matching down: %+v", r)
	}

	// Matching back: delivered once, with the ride's pickup, and not sent again.
	relayMatching.fail(nil)
	if err := runRelay(); err != nil {
		t.Fatal(err)
	}
	if err := runRelay(); err != nil {
		t.Fatal(err)
	}
	if r := rowFor(created.GetRideId()); !r.sent || r.attempts != 2 || r.lastError != "" {
		t.Fatalf("outbox row after delivery: %+v", r)
	}
	got := relayMatching.calls()
	if len(got) != 1 || got[0].GetRideId() != created.GetRideId() || got[0].GetPickup().GetLat() != pickup.GetLat() {
		t.Fatalf("StartMatching calls: %v", got)
	}

	// MarkNoDriver: requested → cancelled (no_driver), idempotent, never touches a ride that has a driver.
	markNoDriver := func(ride string) error {
		_, err := client.MarkNoDriver(service, &ridev1.MarkNoDriverRequest{RideId: ride})
		return err
	}
	stateOf := func(ride string) (string, string) {
		t.Helper()
		var st, reason string
		if err := pool.QueryRow(ctx, `SELECT status, COALESCE(cancel_reason, '') FROM rides WHERE id = $1`, ride).Scan(&st, &reason); err != nil {
			t.Fatal(err)
		}
		return st, reason
	}
	_, err = client.MarkNoDriver(rider, &ridev1.MarkNoDriverRequest{RideId: created.GetRideId()})
	wantCode(t, "mark no driver with user token", err, codes.PermissionDenied)
	for i := range 2 {
		if err := markNoDriver(created.GetRideId()); err != nil {
			t.Fatalf("mark no driver attempt %d: %v", i+1, err)
		}
	}
	if st, reason := stateOf(created.GetRideId()); st != "cancelled" || reason != "no_driver" {
		t.Fatalf("after MarkNoDriver: %s/%s", st, reason)
	}
	// The rider can request again: the no-driver ride is no longer active.
	if _, err := client.CreateRide(outboxRider, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff}); err != nil {
		t.Fatalf("request after no driver: %v", err)
	}
	if err := markNoDriver(cancelled); err != nil {
		t.Fatalf("mark no driver on rider-cancelled ride: %v", err)
	}
	if st, reason := stateOf(cancelled); st != "cancelled" || reason != "rider" {
		t.Fatalf("rider-cancelled ride changed to %s/%s", st, reason)
	}
	wantCode(t, "mark no driver on accepted ride", markNoDriver(assigned), codes.FailedPrecondition)
	wantCode(t, "mark no driver on unknown ride", markNoDriver("cccccccc-0000-0000-0000-00000000fffe"), codes.FailedPrecondition)

	// The schema refuses a cancelled ride without a reason, and a reason on a ride that isn't cancelled.
	if _, err := pool.Exec(ctx, `UPDATE rides SET cancel_reason = NULL WHERE id = $1`, cancelled); err == nil {
		t.Fatal("cancelled ride without a reason was accepted")
	}
	if _, err := pool.Exec(ctx, `UPDATE rides SET cancel_reason = 'rider' WHERE id = $1`, assigned); err == nil {
		t.Fatal("accepted ride with a cancel reason was accepted")
	}

	// CancelRide: only the rider's own ride, before the trip starts; the cancel reaches matching after the start.
	cancelRider := "dddddddd-0000-0000-0000-000000000002"
	cancelCtx := withToken(t, ctx, cancelRider)
	toCancel, err := client.CreateRide(cancelCtx, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatal(err)
	}
	cancelRide := func(who context.Context, ride string) error {
		_, err := client.CancelRide(who, &ridev1.CancelRideRequest{RideId: ride})
		return err
	}
	wantCode(t, "cancel someone else's ride", cancelRide(outboxRider, toCancel.GetRideId()), codes.NotFound)
	wantCode(t, "cancel bad id", cancelRide(cancelCtx, "ride-1"), codes.InvalidArgument)
	_, err = client.CancelRide(service, &ridev1.CancelRideRequest{RideId: toCancel.GetRideId()})
	wantCode(t, "cancel with service token", err, codes.PermissionDenied)

	// Matching is down when the ride is created, so its start is still unsent when the rider cancels.
	relayMatching.fail(status.Error(codes.Unavailable, "matching down"))
	runRelay()
	for i := range 2 {
		if err := cancelRide(cancelCtx, toCancel.GetRideId()); err != nil {
			t.Fatalf("cancel attempt %d: %v", i+1, err)
		}
	}
	if st, reason := stateOf(toCancel.GetRideId()); st != "cancelled" || reason != "rider" {
		t.Fatalf("after CancelRide: %s/%s", st, reason)
	}
	var cancelMsgs int
	pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE kind = 'cancel_matching' AND payload->>'ride_id' = $1`, toCancel.GetRideId()).Scan(&cancelMsgs)
	if cancelMsgs != 1 {
		t.Fatalf("cancel messages queued: %d, want 1 (repeat cancel must not add another)", cancelMsgs)
	}
	// Matching is back but still refuses this start: the cancel must wait behind it, not be delivered first.
	relayMatching.fail(nil)
	relayMatching.failStart(status.Error(codes.Unavailable, "start refused"))
	runRelay()
	for _, m := range relayMatching.sent() {
		if m == "cancel:"+toCancel.GetRideId() {
			t.Fatal("cancel delivered while the ride's start was still unsent")
		}
	}
	relayMatching.failStart(nil)
	for range 3 {
		if err := runRelay(); err != nil {
			t.Fatal(err)
		}
	}
	var forRide []string
	for _, m := range relayMatching.sent() {
		if strings.HasSuffix(m, toCancel.GetRideId()) {
			forRide = append(forRide, m)
		}
	}
	if want := []string{"start:" + toCancel.GetRideId(), "cancel:" + toCancel.GetRideId()}; fmt.Sprint(forRide) != fmt.Sprint(want) {
		t.Fatalf("delivery order for the ride: %v, want %v", forRide, want)
	}

	// A trip that has started can't be cancelled by the rider; an accepted one can.
	started, _ := client.CreateRide(withToken(t, ctx, "dddddddd-0000-0000-0000-000000000003"), &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	pool.Exec(ctx, `UPDATE rides SET status = 'in_progress', driver_id = 'cccccccc-0000-0000-0000-000000000009' WHERE id = $1`, started.GetRideId())
	wantCode(t, "cancel started trip", cancelRide(withToken(t, ctx, "dddddddd-0000-0000-0000-000000000003"), started.GetRideId()), codes.FailedPrecondition)
	accepted, _ := client.CreateRide(withToken(t, ctx, "dddddddd-0000-0000-0000-000000000004"), &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	pool.Exec(ctx, `UPDATE rides SET status = 'accepted', driver_id = 'cccccccc-0000-0000-0000-00000000000a' WHERE id = $1`, accepted.GetRideId())
	if err := cancelRide(withToken(t, ctx, "dddddddd-0000-0000-0000-000000000004"), accepted.GetRideId()); err != nil {
		t.Fatalf("cancel accepted ride: %v", err)
	}

	// The trip: AssignDriver stamps accepted_at; the ride's driver starts and completes it, each step idempotent.
	tripRider, tripDriver := "dddddddd-0000-0000-0000-000000000005", "cccccccc-0000-0000-0000-00000000000b"
	trip, err := client.CreateRide(withToken(t, ctx, tripRider), &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatal(err)
	}
	if err := assign(trip.GetRideId(), tripDriver); err != nil {
		t.Fatal(err)
	}
	driverCtx := withToken(t, ctx, tripDriver)
	startRide := func(who context.Context, ride string) error {
		_, err := client.StartRide(who, &ridev1.StartRideRequest{RideId: ride})
		return err
	}
	completeRide := func(who context.Context, ride string) error {
		_, err := client.CompleteRide(who, &ridev1.CompleteRideRequest{RideId: ride})
		return err
	}
	stamps := func() (accepted, started, completed bool) {
		t.Helper()
		pool.QueryRow(ctx, `SELECT accepted_at IS NOT NULL, started_at IS NOT NULL, completed_at IS NOT NULL FROM rides WHERE id = $1`,
			trip.GetRideId()).Scan(&accepted, &started, &completed)
		return
	}
	if a, s, c := stamps(); !a || s || c {
		t.Fatalf("after assign: accepted %v started %v completed %v", a, s, c)
	}

	wantCode(t, "start someone else's trip", startRide(withToken(t, ctx, otherDriver), trip.GetRideId()), codes.NotFound)
	wantCode(t, "rider starts the trip", startRide(withToken(t, ctx, tripRider), trip.GetRideId()), codes.NotFound)
	wantCode(t, "complete before starting", completeRide(driverCtx, trip.GetRideId()), codes.FailedPrecondition)
	for i := range 2 {
		if err := startRide(driverCtx, trip.GetRideId()); err != nil {
			t.Fatalf("start attempt %d: %v", i+1, err)
		}
	}
	if st, _ := stateOf(trip.GetRideId()); st != "in_progress" {
		t.Fatalf("after start: %s", st)
	}
	wantCode(t, "rider cancels started trip", cancelRide(withToken(t, ctx, tripRider), trip.GetRideId()), codes.FailedPrecondition)

	// While on the trip the driver isn't available; once it's completed, matching may offer them rides again.
	availableNow := func() bool {
		t.Helper()
		res, err := client.FilterAvailableDrivers(service, &ridev1.FilterAvailableDriversRequest{DriverIds: []string{tripDriver}})
		if err != nil {
			t.Fatal(err)
		}
		return len(res.GetDriverIds()) == 1
	}
	if availableNow() {
		t.Fatal("driver available during the trip")
	}
	for i := range 2 {
		if err := completeRide(driverCtx, trip.GetRideId()); err != nil {
			t.Fatalf("complete attempt %d: %v", i+1, err)
		}
	}
	if err := startRide(driverCtx, trip.GetRideId()); err != nil {
		t.Fatalf("late start retry after completion: %v", err)
	}
	if st, _ := stateOf(trip.GetRideId()); st != "completed" {
		t.Fatalf("after complete: %s", st)
	}
	if a, s, c := stamps(); !a || !s || !c {
		t.Fatalf("after trip: accepted %v started %v completed %v", a, s, c)
	}
	if !availableNow() {
		t.Fatal("driver still busy after completing the trip")
	}

	// A cancelled ride can't be started.
	wantCode(t, "start cancelled ride", startRide(withToken(t, ctx, "cccccccc-0000-0000-0000-00000000000a"), accepted.GetRideId()), codes.FailedPrecondition)
}

// fakeMatching records StartMatching and CancelMatching calls, in order, and fails them while err is set.
type fakeMatching struct {
	matchingv1.MatchingServiceClient
	mu       sync.Mutex
	err      error
	startErr error // fails only StartMatching, to test that a ride's cancel never overtakes its start
	reqs     []*matchingv1.StartMatchingRequest
	order    []string
}

func (f *fakeMatching) failStart(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.startErr = err
}

func (f *fakeMatching) CancelMatching(_ context.Context, in *matchingv1.CancelMatchingRequest, _ ...grpc.CallOption) (*matchingv1.CancelMatchingResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.order = append(f.order, "cancel:"+in.GetRideId())
	return &matchingv1.CancelMatchingResponse{}, nil
}

func (f *fakeMatching) sent() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.order...)
}

func (f *fakeMatching) StartMatching(_ context.Context, in *matchingv1.StartMatchingRequest, _ ...grpc.CallOption) (*matchingv1.StartMatchingResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	if f.startErr != nil {
		return nil, f.startErr
	}
	f.reqs = append(f.reqs, in)
	f.order = append(f.order, "start:"+in.GetRideId())
	return &matchingv1.StartMatchingResponse{}, nil
}

func (f *fakeMatching) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeMatching) calls() []*matchingv1.StartMatchingRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*matchingv1.StartMatchingRequest(nil), f.reqs...)
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
