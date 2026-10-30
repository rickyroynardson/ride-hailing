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
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/postgres"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/matching/internal/database"
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

// fakeRide drops the drivers marked busy, like the ride service does for drivers on an active ride, and answers
// AssignDriver with whatever assignErr returns (nil: assigned).
type fakeRide struct {
	ridev1.RideServiceClient
	busy map[string]bool

	mu          sync.Mutex
	assignErr   func(ride, driver string) error
	assigned    []string
	noDriverErr error
	noDriver    []string
}

func (f *fakeRide) MarkNoDriver(_ context.Context, in *ridev1.MarkNoDriverRequest, _ ...grpc.CallOption) (*ridev1.MarkNoDriverResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.noDriverErr != nil {
		return nil, f.noDriverErr
	}
	f.noDriver = append(f.noDriver, in.GetRideId())
	return &ridev1.MarkNoDriverResponse{}, nil
}

func (f *fakeRide) setNoDriverErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.noDriverErr = err
}

func (f *fakeRide) noDriverCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.noDriver...)
}

func (f *fakeRide) AssignDriver(_ context.Context, in *ridev1.AssignDriverRequest, _ ...grpc.CallOption) (*ridev1.AssignDriverResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.assignErr != nil {
		if err := f.assignErr(in.GetRideId(), in.GetDriverId()); err != nil {
			return nil, err
		}
	}
	f.assigned = append(f.assigned, in.GetRideId()+"/"+in.GetDriverId())
	return &ridev1.AssignDriverResponse{}, nil
}

func (f *fakeRide) setAssign(fn func(ride, driver string) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.assignErr = fn
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
	ride := &fakeRide{busy: map[string]bool{onRide: true}}
	client := startServer(t, pool, location, ride)
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

	// A traced StartMatching stores the incoming trace on the request, for later attempts to join.
	if _, err := telemetry.Setup(ctx, "matching-test"); err != nil {
		t.Fatal(err)
	}
	const incomingTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	tracedRide := "aaaaaaaa-0000-0000-0000-0000000000aa"
	location.set()
	tracedCtx := metadata.AppendToOutgoingContext(service, "traceparent", "00-"+incomingTrace+"-00f067aa0ba902b7-01")
	if _, err := client.StartMatching(tracedCtx, &matchingv1.StartMatchingRequest{RideId: tracedRide, Pickup: pickup}); err != nil {
		t.Fatal(err)
	}
	var storedTrace string
	pool.QueryRow(ctx, `SELECT COALESCE(trace_parent, '') FROM match_requests WHERE ride_id = $1`, tracedRide).Scan(&storedTrace)
	if !strings.Contains(storedTrace, incomingTrace) {
		t.Fatalf("match request trace_parent %q, want it to carry trace %s", storedTrace, incomingTrace)
	}
	pool.Exec(ctx, `UPDATE match_requests SET status = 'cancelled' WHERE ride_id = $1`, tracedRide) // keep it out of later ticks

	// Tick: the loop that keeps matching moving. Deadlines are moved into the past with SQL instead of waiting.
	const (
		rideD  = "aaaaaaaa-0000-0000-0000-000000000004"
		first  = "dddddddd-0000-0000-0000-000000000011"
		second = "dddddddd-0000-0000-0000-000000000012"
	)
	// Same fakes as the server, so ticks see the same location and ride behaviour as RPCs.
	svc := matching.NewService(matching.NewRepository(pool), location, ride)
	tick := func() {
		t.Helper()
		if err := svc.Tick(ctx); err != nil {
			t.Fatalf("tick: %v", err)
		}
	}
	// While ride is unreachable, Tick reports the accepts it couldn't confirm (main logs them as warnings).
	tickWhileRideDown := func() {
		t.Helper()
		if err := svc.Tick(ctx); !errors.Is(err, matching.ErrAssignPending) {
			t.Fatalf("tick with ride down: got %v, want ErrAssignPending", err)
		}
	}
	type offerState struct{ driver, status string }
	offersFor := func(ride string) []offerState {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT driver_id, status FROM offers WHERE ride_id = $1 ORDER BY created_at`, ride)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []offerState
		for rows.Next() {
			var o offerState
			if err := rows.Scan(&o.driver, &o.status); err != nil {
				t.Fatal(err)
			}
			out = append(out, o)
		}
		return out
	}
	wantOffers := func(name string, want ...offerState) {
		t.Helper()
		if got := offersFor(rideD); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s: offers %v, want %v", name, got, want)
		}
	}
	requestStatus := func() string {
		t.Helper()
		var st string
		if err := pool.QueryRow(ctx, `SELECT status FROM match_requests WHERE ride_id = $1`, rideD).Scan(&st); err != nil {
			t.Fatal(err)
		}
		return st
	}

	// No driver at first: the request waits; once one comes online, the next tick offers it.
	location.set()
	if res, err := client.StartMatching(service, &matchingv1.StartMatchingRequest{RideId: rideD, Pickup: pickup}); err != nil || res.GetOfferId() != "" {
		t.Fatalf("start with no drivers: %+v, %v", res, err)
	}
	if st := requestStatus(); st != "matching" {
		t.Fatalf("request status %q, want matching", st)
	}
	location.set(first, second)
	tick()
	wantOffers("driver came online", offerState{first, "pending"})

	// The offer runs out: it expires and the next nearest driver gets the ride, never the first one again.
	pool.Exec(ctx, `UPDATE offers SET expires_at = now() - interval '1 second' WHERE ride_id = $1 AND status = 'pending'`, rideD)
	tick()
	wantOffers("first offer expired", offerState{first, "expired"}, offerState{second, "pending"})

	// Past the deadline with an offer still pending: wait for that offer instead of giving up under it.
	pool.Exec(ctx, `UPDATE match_requests SET deadline_at = now() - interval '1 second' WHERE ride_id = $1`, rideD)
	tick()
	if st := requestStatus(); st != "matching" {
		t.Fatalf("gave up with an offer pending: status %q", st)
	}

	// Once that offer expires too, everyone nearby has been asked and the deadline has passed: no_driver.
	// Ride is unreachable at first, so the news waits until ride is back, then is sent exactly once.
	pool.Exec(ctx, `UPDATE offers SET expires_at = now() - interval '1 second' WHERE ride_id = $1 AND status = 'pending'`, rideD)
	// Meanwhile another ride waits for a driver; ride being unreachable mustn't stop it being offered.
	const rideW, driverW = "aaaaaaaa-0000-0000-0000-000000000009", "dddddddd-0000-0000-0000-000000000099"
	location.set()
	if _, err := client.StartMatching(service, &matchingv1.StartMatchingRequest{RideId: rideW, Pickup: pickup}); err != nil {
		t.Fatal(err)
	}
	location.set(driverW)
	ride.setNoDriverErr(status.Error(codes.Unavailable, "ride down"))
	if err := svc.Tick(ctx); err == nil || !strings.Contains(err.Error(), "ride down") {
		t.Fatalf("tick telling an unreachable ride: got %v, want the ride error", err)
	}
	var offeredW string
	pool.QueryRow(ctx, `SELECT driver_id FROM offers WHERE ride_id = $1 AND status = 'pending'`, rideW).Scan(&offeredW)
	if offeredW != driverW {
		t.Fatalf("waiting ride not offered while ride was down: got %q, want %s", offeredW, driverW)
	}
	if st := requestStatus(); st != "no_driver" {
		t.Fatalf("after deadline: status %q, want no_driver", st)
	}
	wantOffers("given up", offerState{first, "expired"}, offerState{second, "expired"})
	notified := func() bool {
		t.Helper()
		var at *time.Time
		pool.QueryRow(ctx, `SELECT ride_notified_at FROM match_requests WHERE ride_id = $1`, rideD).Scan(&at)
		return at != nil
	}
	if notified() || len(ride.noDriverCalls()) != 0 {
		t.Fatal("no_driver marked notified while ride was down")
	}
	ride.setNoDriverErr(nil)
	tick()
	tick()
	if !notified() || fmt.Sprint(ride.noDriverCalls()) != fmt.Sprint([]string{rideD}) {
		t.Fatalf("after ride recovered: notified %v, MarkNoDriver calls %v; want one call for %s", notified(), ride.noDriverCalls(), rideD)
	}

	// A finished request stays finished: no more ticks, and a late StartMatching retry, offer anything.
	location.set("dddddddd-0000-0000-0000-000000000013")
	tick()
	if res, err := client.StartMatching(service, &matchingv1.StartMatchingRequest{RideId: rideD, Pickup: pickup}); err != nil || res.GetOfferId() != "" {
		t.Fatalf("start after no_driver: %+v, %v", res, err)
	}
	wantOffers("after no_driver", offerState{first, "expired"}, offerState{second, "expired"})

	// Driver side: GetOffer, DeclineOffer, AcceptOffer. Each ride gets its own drivers so steps don't interfere.
	start := func(ride string, drivers ...string) {
		t.Helper()
		location.set(drivers...)
		if _, err := client.StartMatching(service, &matchingv1.StartMatchingRequest{RideId: ride, Pickup: pickup}); err != nil {
			t.Fatalf("start %s: %v", ride, err)
		}
	}
	offerOf := func(driver string) *matchingv1.Offer {
		t.Helper()
		res, err := client.GetOffer(withDriverToken(t, ctx, driver), &matchingv1.GetOfferRequest{})
		if err != nil {
			t.Fatalf("get offer for %s: %v", driver, err)
		}
		return res.GetOffer()
	}
	accept := func(driver, offer string) (string, error) {
		res, err := client.AcceptOffer(withDriverToken(t, ctx, driver), &matchingv1.AcceptOfferRequest{OfferId: offer})
		return res.GetRideId(), err
	}
	decline := func(driver, offer string) error {
		_, err := client.DeclineOffer(withDriverToken(t, ctx, driver), &matchingv1.DeclineOfferRequest{OfferId: offer})
		return err
	}
	statusOf := func(table, idCol, id string) string {
		t.Helper()
		var st string
		if err := pool.QueryRow(ctx, fmt.Sprintf(`SELECT status FROM %s WHERE %s = $1`, table, idCol), id).Scan(&st); err != nil {
			t.Fatal(err)
		}
		return st
	}
	driver := func(n int) string { return fmt.Sprintf("eeeeeeee-0000-0000-0000-%012d", n) }
	rideN := func(n int) string { return fmt.Sprintf("bbbbbbbb-0000-0000-0000-%012d", n) }

	// GetOffer: only the offered driver sees it, with the pickup; riders can't call it.
	rideE := rideN(1)
	start(rideE, driver(1), driver(2))
	o := offerOf(driver(1))
	if o.GetRideId() != rideE || o.GetPickup().GetLat() != pickup.GetLat() || o.GetExpiresAt() == nil {
		t.Fatalf("offer for driver 1: %+v", o)
	}
	if offerOf(driver(2)) != nil {
		t.Fatal("driver 2 sees an offer before being offered")
	}
	_, err = client.GetOffer(withUserToken(t, ctx, driver(1)), &matchingv1.GetOfferRequest{})
	wantCode(t, "get offer without driver claim", err, codes.PermissionDenied)

	// Decline: someone else's offer is refused; the right driver's decline offers the next driver at once.
	wantCode(t, "decline someone else's offer", decline(driver(2), o.GetOfferId()), codes.FailedPrecondition)
	if err := decline(driver(1), o.GetOfferId()); err != nil {
		t.Fatal(err)
	}
	if err := decline(driver(1), o.GetOfferId()); err != nil {
		t.Fatalf("repeat decline: %v", err)
	}
	o2 := offerOf(driver(2))
	if o2.GetRideId() != rideE {
		t.Fatalf("after decline, driver 2 has offer %+v; want ride %s without waiting for a tick", o2, rideE)
	}
	wantCode(t, "accept a declined offer", func() error { _, err := accept(driver(1), o.GetOfferId()); return err }(), codes.FailedPrecondition)

	// Ride unreachable: the accept can't be confirmed. The driver keeps the ride (nobody else is offered it),
	// and the next tick confirms once ride is back.
	ride.setAssign(func(string, string) error { return status.Error(codes.Unavailable, "ride down") })
	_, err = accept(driver(2), o2.GetOfferId())
	wantCode(t, "accept while ride is down", err, codes.Unavailable)
	location.set(driver(3))
	tickWhileRideDown()
	if st := statusOf("offers", "id", o2.GetOfferId()); st != "accepted" {
		t.Fatalf("offer while ride down: %q, want accepted", st)
	}
	if offerOf(driver(3)) != nil {
		t.Fatal("ride was re-offered while an accept awaited confirmation")
	}
	// Ride retrying StartMatching meanwhile must not re-offer either (it skips the loop's query).
	if res, err := client.StartMatching(service, &matchingv1.StartMatchingRequest{RideId: rideE, Pickup: pickup}); err != nil || res.GetOfferId() != "" {
		t.Fatalf("StartMatching retry during unconfirmed accept: %+v, %v", res, err)
	}
	if offerOf(driver(3)) != nil {
		t.Fatal("StartMatching retry re-offered a ride a driver accepted")
	}
	// Nor may the deadline pass and give the ride up under the driver who accepted it.
	pool.Exec(ctx, `UPDATE match_requests SET deadline_at = now() - interval '1 second' WHERE ride_id = $1`, rideE)
	tickWhileRideDown()
	if st := statusOf("match_requests", "ride_id", rideE); st != "matching" {
		t.Fatalf("request given up during unconfirmed accept: %q", st)
	}
	ride.setAssign(nil)
	tick()
	if st := statusOf("match_requests", "ride_id", rideE); st != "matched" {
		t.Fatalf("after ride recovered: request %q, want matched", st)
	}
	// The driver app retrying its accept still succeeds.
	if got, err := accept(driver(2), o2.GetOfferId()); err != nil || got != rideE {
		t.Fatalf("repeat accept: %q, %v", got, err)
	}

	// Expired offers can't be accepted.
	rideF := rideN(2)
	start(rideF, driver(4))
	o = offerOf(driver(4))
	pool.Exec(ctx, `UPDATE offers SET expires_at = now() - interval '1 second' WHERE id = $1`, o.GetOfferId())
	_, err = accept(driver(4), o.GetOfferId())
	wantCode(t, "accept expired offer", err, codes.FailedPrecondition)

	// Ride refuses because the ride is gone (cancelled or taken): matching for it ends.
	rideG := rideN(3)
	start(rideG, driver(5))
	o = offerOf(driver(5))
	ride.setAssign(func(string, string) error { return status.Error(codes.FailedPrecondition, "ride cancelled") })
	_, err = accept(driver(5), o.GetOfferId())
	wantCode(t, "accept cancelled ride", err, codes.FailedPrecondition)
	if st := statusOf("offers", "id", o.GetOfferId()); st != "cancelled" {
		t.Fatalf("offer for cancelled ride: %q", st)
	}
	if st := statusOf("match_requests", "ride_id", rideG); st != "cancelled" {
		t.Fatalf("request for cancelled ride: %q", st)
	}

	// Ride refuses because this driver is busy: only this driver is ruled out, the next one gets the ride.
	rideH := rideN(4)
	start(rideH, driver(6), driver(7))
	o = offerOf(driver(6))
	ride.setAssign(func(_, d string) error {
		if d == driver(6) {
			return status.Error(codes.AlreadyExists, "driver busy")
		}
		return nil
	})
	_, err = accept(driver(6), o.GetOfferId())
	wantCode(t, "accept while busy", err, codes.FailedPrecondition)
	tick()
	if next := offerOf(driver(7)); next.GetRideId() != rideH {
		t.Fatalf("after busy driver, driver 7 has %+v; want ride %s", next, rideH)
	}
	if st := statusOf("match_requests", "ride_id", rideH); st != "matching" {
		t.Fatalf("request after busy driver: %q, want matching", st)
	}

	// CancelMatching: the rider cancelled. The pending offer is withdrawn and nobody else is offered the ride.
	ride.setAssign(nil)
	cancelMatching := func(ride string) error {
		_, err := client.CancelMatching(service, &matchingv1.CancelMatchingRequest{RideId: ride})
		return err
	}
	rideK := rideN(5)
	start(rideK, driver(8), driver(9))
	if offerOf(driver(8)).GetRideId() != rideK {
		t.Fatal("setup: driver 8 not offered ride K")
	}
	_, err = client.CancelMatching(withDriverToken(t, ctx, driver(8)), &matchingv1.CancelMatchingRequest{RideId: rideK})
	wantCode(t, "cancel matching with user token", err, codes.PermissionDenied)
	for i := range 2 {
		if err := cancelMatching(rideK); err != nil {
			t.Fatalf("cancel matching attempt %d: %v", i+1, err)
		}
	}
	if st := statusOf("match_requests", "ride_id", rideK); st != "cancelled" {
		t.Fatalf("request after cancel: %q", st)
	}
	if offerOf(driver(8)) != nil {
		t.Fatal("driver still sees the cancelled ride's offer")
	}
	tick()
	if offerOf(driver(9)) != nil {
		t.Fatal("cancelled ride offered to the next driver")
	}
	if err := cancelMatching("bbbbbbbb-0000-0000-0000-00000000ffff"); err != nil {
		t.Fatalf("cancel matching for an unknown ride: %v", err)
	}
	wantCode(t, "cancel matching bad id", cancelMatching("ride-1"), codes.InvalidArgument)

	// Cancelled while a driver's accept awaited ride's confirmation: the accept is withdrawn, not confirmed later.
	rideL := rideN(6)
	start(rideL, driver(10))
	o = offerOf(driver(10))
	ride.setAssign(func(string, string) error { return status.Error(codes.Unavailable, "ride down") })
	_, err = accept(driver(10), o.GetOfferId())
	wantCode(t, "accept while ride down", err, codes.Unavailable)
	if err := cancelMatching(rideL); err != nil {
		t.Fatal(err)
	}
	if st := statusOf("offers", "id", o.GetOfferId()); st != "cancelled" {
		t.Fatalf("unconfirmed accept after cancel: %q, want cancelled", st)
	}
	ride.setAssign(nil)
	tick()
	if st := statusOf("match_requests", "ride_id", rideL); st != "cancelled" {
		t.Fatalf("cancelled request reconfirmed: %q", st)
	}

	// A confirmed accept stays as history when the rider cancels the ride later.
	matchedOffer := o2.GetOfferId() // ride E, matched earlier
	if err := cancelMatching(rideE); err != nil {
		t.Fatal(err)
	}
	if st := statusOf("offers", "id", matchedOffer); st != "accepted" {
		t.Fatalf("confirmed accept rewritten to %q", st)
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
	gs := server.NewGRPCServer(zap.NewNop(), jwtauth.NewVerifier(testSecret), matching.NewService(matching.NewRepository(pool), location, ride))
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
	md, err := jwtauth.NewServiceCredentials("ride", testSecret).GetRequestMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", md["authorization"])
}

// withDriverToken signs an access token with the driver claim, as the user service does for approved drivers.
func withDriverToken(t *testing.T, ctx context.Context, userID string) context.Context {
	t.Helper()
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": userID, "driver": true, "exp": time.Now().Add(time.Minute).Unix(),
	}).SignedString([]byte(testSecret))
	if err != nil {
		t.Fatal(err)
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
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
