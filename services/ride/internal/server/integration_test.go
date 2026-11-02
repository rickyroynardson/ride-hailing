package server_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
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
	"github.com/rickyroynardson/ride-hailing/pkg/loop"
	"github.com/rickyroynardson/ride-hailing/pkg/postgres"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
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
	reflectionv1 "google.golang.org/grpc/reflection/grpc_reflection_v1"
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

	location := &fakeLocation{}
	client, conn := startServer(t, pool, location)

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
	relayPayment := &fakePayment{}
	relay := outbox.NewRelay(pool, zap.NewNop(), relayMatching, relayPayment)
	// runRelay ends every backoff first; the delivery tests below aren't about waiting.
	runRelay := func() error {
		t.Helper()
		if _, err := pool.Exec(ctx, `UPDATE outbox SET next_attempt_at = now() WHERE sent_at IS NULL`); err != nil {
			t.Fatal(err)
		}
		return relay.Run(ctx)
	}
	pool.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE sent_at IS NULL`) // earlier steps' rides are not this step's concern

	// The listener wakes the relay when a message commits. Notify by hand until a wake-up arrives, so we know it is
	// LISTENing before the real test.
	listenCtx, stopListen := context.WithCancel(ctx)
	defer stopListen()
	go relay.Listen(listenCtx)
	waitWake := func(within time.Duration) bool {
		select {
		case <-relay.Wake():
			return true
		case <-time.After(within):
			return false
		}
	}
	for listening := false; !listening; {
		pool.Exec(ctx, `SELECT pg_notify('outbox', '')`)
		listening = waitWake(100 * time.Millisecond)
	}
	waitWake(100 * time.Millisecond) // drain a late wake-up from the handshake

	outboxRider := withToken(t, ctx, "dddddddd-0000-0000-0000-000000000001")
	created, err := client.CreateRide(outboxRider, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatal(err)
	}
	if !waitWake(2 * time.Second) {
		t.Fatal("CreateRide committed a message but the relay was not woken")
	}
	stopListen()
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

	// A traced request stores its trace context with the message, so the relay's send continues that trace.
	// (Tracing export is off in tests; the propagator alone carries the incoming traceparent through.)
	if _, err := telemetry.Setup(ctx, "ride-test"); err != nil {
		t.Fatal(err)
	}
	const incomingTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	tracedCtx := metadata.AppendToOutgoingContext(withToken(t, ctx, "dddddddd-0000-0000-0000-0000000000aa"),
		"traceparent", "00-"+incomingTrace+"-00f067aa0ba902b7-01")
	traced, err := client.CreateRide(tracedCtx, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatal(err)
	}
	var storedTrace string
	pool.QueryRow(ctx, `SELECT COALESCE(trace_parent, '') FROM outbox WHERE payload->>'ride_id' = $1`, traced.GetRideId()).Scan(&storedTrace)
	if !strings.Contains(storedTrace, incomingTrace) {
		t.Fatalf("outbox trace_parent %q, want it to carry trace %s", storedTrace, incomingTrace)
	}
	pool.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE payload->>'ride_id' = $1`, traced.GetRideId()) // not this step's delivery concern

	// The ride and its message are saved together: a refused CreateRide leaves no message behind.
	var before, after int
	pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&before)
	_, err = client.CreateRide(outboxRider, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	wantCode(t, "second active ride", err, codes.AlreadyExists)
	pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&after)
	if after != before {
		t.Fatalf("refused CreateRide left %d outbox rows", after-before)
	}

	// Matching down: the message stays unsent, with the attempt and error recorded. The relay itself isn't failing,
	// or its loop would back off and delay every other message too.
	relayMatching.fail(status.Error(codes.Unavailable, "matching down"))
	if err := runRelay(); err != nil {
		t.Fatalf("relay with matching down: %v", err)
	}
	if r := rowFor(created.GetRideId()); r.sent || r.attempts != 1 || !strings.Contains(r.lastError, "matching down") {
		t.Fatalf("outbox row with matching down: %+v", r)
	}

	// The failed message backs off: the next round doesn't try it again straight away.
	if err := relay.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if r := rowFor(created.GetRideId()); r.attempts != 1 {
		t.Fatalf("retried during its backoff: %+v", r)
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

	// A message that can never succeed is set aside as dead, and holds back only its own ride's later messages.
	const poisonRide = "eeeeeeee-0000-0000-0000-000000000001"
	pool.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE sent_at IS NULL`)
	pool.Exec(ctx, `INSERT INTO outbox(kind, payload) VALUES ('start_matching', '{"ride_id": "`+poisonRide+`", "pickup": "not a location"}')`)
	pool.Exec(ctx, `INSERT INTO outbox(kind, payload) VALUES ('cancel_matching', '{"ride_id": "`+poisonRide+`"}')`)
	healthy, err := client.CreateRide(withToken(t, ctx, "dddddddd-0000-0000-0000-000000000105"), &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := runRelay(); err != nil {
			t.Fatal(err)
		}
	}
	var dead int
	var deadError string
	pool.QueryRow(ctx, `SELECT count(*), COALESCE(max(last_error), '') FROM outbox WHERE dead_at IS NOT NULL AND sent_at IS NULL`).Scan(&dead, &deadError)
	if dead != 1 || !strings.Contains(deadError, "malformed") {
		t.Fatalf("dead messages: %d (%s), want the one malformed start", dead, deadError)
	}
	for _, m := range relayMatching.sent() {
		if m == "cancel:"+poisonRide {
			t.Fatal("a ride's cancel overtook its dead start")
		}
	}
	if r := rowFor(healthy.GetRideId()); !r.sent {
		t.Fatalf("another ride held up by a dead message: %+v", r)
	}
	pool.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE payload->>'ride_id' = $1`, poisonRide)

	// Receivers refusing a message outright is permanent too.
	relayMatching.failStart(status.Error(codes.InvalidArgument, "refused"))
	refused, err := client.CreateRide(withToken(t, ctx, "dddddddd-0000-0000-0000-000000000106"), &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatal(err)
	}
	runRelay()
	relayMatching.failStart(nil)
	var refusedDead bool
	pool.QueryRow(ctx, `SELECT dead_at IS NOT NULL FROM outbox WHERE payload->>'ride_id' = $1`, refused.GetRideId()).Scan(&refusedDead)
	if !refusedDead {
		t.Fatal("InvalidArgument from a receiver was not treated as permanent")
	}

	// EstimateFare and CreateRide agree, and the fare is fixed on the ride and shown in it.
	_, err = client.EstimateFare(rider, &ridev1.EstimateFareRequest{Pickup: pickup})
	wantCode(t, "estimate without dropoff", err, codes.InvalidArgument)
	estimate, err := client.EstimateFare(rider, &ridev1.EstimateFareRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatal(err)
	}
	const fareRider, fareDriver = "dddddddd-0000-0000-0000-000000000107", "cccccccc-0000-0000-0000-000000000020"
	fareCtx, fareDriverCtx := withToken(t, ctx, fareRider), withToken(t, ctx, fareDriver)
	priced, err := client.CreateRide(fareCtx, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatal(err)
	}
	if estimate.GetFare().GetAmount() <= 0 || priced.GetFare().GetAmount() != estimate.GetFare().GetAmount() || priced.GetFare().GetCurrency() != "IDR" {
		t.Fatalf("estimate %v, booked %v", estimate.GetFare(), priced.GetFare())
	}

	// Completing the trip queues exactly one charge for the fixed fare, with the ride, however often it is retried.
	if err := assign(priced.GetRideId(), fareDriver); err != nil {
		t.Fatal(err)
	}
	if _, err := client.StartRide(fareDriverCtx, &ridev1.StartRideRequest{RideId: priced.GetRideId()}); err != nil {
		t.Fatal(err)
	}
	chargesFor := func(ride string) (n int, kind string, amount int64) {
		t.Helper()
		pool.QueryRow(ctx,
			`SELECT count(*), COALESCE(max(payload->>'charge_kind'), ''), COALESCE(max((payload->>'amount')::bigint), 0)
			FROM outbox WHERE kind = 'charge_ride' AND payload->>'ride_id' = $1`, ride).Scan(&n, &kind, &amount)
		return n, kind, amount
	}
	paymentStatus := func(ride string) string {
		t.Helper()
		var st string
		pool.QueryRow(ctx, `SELECT COALESCE(payment_status, '') FROM rides WHERE id = $1`, ride).Scan(&st)
		return st
	}
	if n, _, _ := chargesFor(priced.GetRideId()); n != 0 {
		t.Fatalf("charged before the trip finished: %d", n)
	}
	for range 2 {
		if _, err := client.CompleteRide(fareDriverCtx, &ridev1.CompleteRideRequest{RideId: priced.GetRideId()}); err != nil {
			t.Fatal(err)
		}
	}
	if n, kind, amount := chargesFor(priced.GetRideId()); n != 1 || kind != "fare" || amount != priced.GetFare().GetAmount() {
		t.Fatalf("charge after completing: %d %s %d", n, kind, amount)
	}
	if got := paymentStatus(priced.GetRideId()); got != "pending" {
		t.Fatalf("payment status after completing: %q", got)
	}
	for range 2 { // the ride's start first, then its charge
		if err := runRelay(); err != nil {
			t.Fatal(err)
		}
	}
	if got := relayPayment.calls(); len(got) != 1 || got[0].GetRideId() != priced.GetRideId() || got[0].GetRiderId() != fareRider ||
		got[0].GetDriverId() != fareDriver || got[0].GetKind() != paymentv1.ChargeKind_CHARGE_KIND_FARE || got[0].GetAmount().GetAmount() != priced.GetFare().GetAmount() {
		t.Fatalf("ChargeRide calls: %v", got)
	}

	// A ride from before pricing has no fare, and finishes without a charge.
	unpriced := newRide("dddddddd-0000-0000-0000-000000000108")
	pool.Exec(ctx, `UPDATE rides SET status = 'in_progress', driver_id = $2 WHERE id = $1`, unpriced, "cccccccc-0000-0000-0000-000000000021")
	if _, err := client.CompleteRide(withToken(t, ctx, "cccccccc-0000-0000-0000-000000000021"), &ridev1.CompleteRideRequest{RideId: unpriced}); err != nil {
		t.Fatal(err)
	}
	if n, _, _ := chargesFor(unpriced); n != 0 {
		t.Fatalf("charged a ride without a fare: %d", n)
	}

	// Payment says the charge failed: the rider can't book until it says the ride is paid. A delayed "failed" after
	// that can't bring the block back.
	record := func(ride string, outcome ridev1.PaymentOutcome) error {
		_, err := client.RecordPayment(service, &ridev1.RecordPaymentRequest{RideId: ride, Outcome: outcome})
		return err
	}
	const paid, unpaid = ridev1.PaymentOutcome_PAYMENT_OUTCOME_PAID, ridev1.PaymentOutcome_PAYMENT_OUTCOME_FAILED
	_, err = client.RecordPayment(fareCtx, &ridev1.RecordPaymentRequest{RideId: priced.GetRideId(), Outcome: paid})
	wantCode(t, "a rider recording their own payment", err, codes.PermissionDenied)
	wantCode(t, "record unknown ride", record("cccccccc-0000-0000-0000-00000000fffd", paid), codes.NotFound)
	wantCode(t, "record no outcome", record(priced.GetRideId(), ridev1.PaymentOutcome_PAYMENT_OUTCOME_UNSPECIFIED), codes.InvalidArgument)

	for range 2 {
		if err := record(priced.GetRideId(), unpaid); err != nil {
			t.Fatal(err)
		}
	}
	_, err = client.CreateRide(fareCtx, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	wantCode(t, "book with an unpaid ride", err, codes.FailedPrecondition)
	for range 2 {
		if err := record(priced.GetRideId(), paid); err != nil {
			t.Fatal(err)
		}
	}
	if err := record(priced.GetRideId(), unpaid); err != nil {
		t.Fatal(err)
	}
	if got := paymentStatus(priced.GetRideId()); got != "paid" {
		t.Fatalf("late failure overwrote paid: %q", got)
	}
	again, err := client.CreateRide(fareCtx, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatalf("book after settling: %v", err)
	}

	// Cancelling before a driver accepts is free; after, the fee is queued with the cancellation.
	if err := cancelRide(fareCtx, again.GetRideId()); err != nil {
		t.Fatal(err)
	}
	if n, _, _ := chargesFor(again.GetRideId()); n != 0 {
		t.Fatalf("charged for a free cancellation: %d", n)
	}
	lateCancel, err := client.CreateRide(fareCtx, &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatal(err)
	}
	if err := assign(lateCancel.GetRideId(), fareDriver); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := cancelRide(fareCtx, lateCancel.GetRideId()); err != nil {
			t.Fatal(err)
		}
	}
	if n, kind, amount := chargesFor(lateCancel.GetRideId()); n != 1 || kind != "cancellation_fee" || amount != ride.CancellationFee {
		t.Fatalf("late cancel charge: %d %s %d", n, kind, amount)
	}

	// Payment being down holds only its own messages: a charge fails and backs off, a new ride still reaches matching.
	relayPayment.fail(status.Error(codes.Unavailable, "payment down"))
	stillMatched, err := client.CreateRide(withToken(t, ctx, "dddddddd-0000-0000-0000-000000000109"), &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatal(err)
	}
	if err := runRelay(); err != nil {
		t.Fatal(err)
	}
	if r := rowFor(stillMatched.GetRideId()); !r.sent {
		t.Fatalf("matching message held up by payment: %+v", r)
	}
	relayPayment.fail(nil)
	for range 3 { // the ride's cancel first, then its fee
		if err := runRelay(); err != nil {
			t.Fatal(err)
		}
	}
	if got := relayPayment.calls(); got[len(got)-1].GetKind() != paymentv1.ChargeKind_CHARGE_KIND_CANCELLATION_FEE ||
		got[len(got)-1].GetAmount().GetAmount() != ride.CancellationFee {
		t.Fatalf("last ChargeRide: %v", got[len(got)-1])
	}

	// A hanging receiver holds no database row: while a send is stuck, its message can be locked by anyone, and a
	// second relay leaves it alone because of the lease.
	release := relayPayment.hold()
	pool.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE sent_at IS NULL`)
	stuck, err := client.CreateRide(withToken(t, ctx, "dddddddd-0000-0000-0000-00000000010a"), &ridev1.CreateRideRequest{Pickup: pickup, Dropoff: dropoff})
	if err != nil {
		t.Fatal(err)
	}
	pool.Exec(ctx, `UPDATE outbox SET sent_at = now() WHERE payload->>'ride_id' = $1`, stuck.GetRideId())
	pool.Exec(ctx, `INSERT INTO outbox(kind, payload) VALUES ('charge_ride', '{"ride_id": "eeeeeeee-0000-0000-0000-000000000002"}')`)
	firstRun := make(chan error, 1)
	go func() { firstRun <- relay.Run(ctx) }()
	<-relayPayment.entered
	lockCtx, cancelLock := context.WithTimeout(ctx, 2*time.Second)
	defer cancelLock()
	if _, err := pool.Exec(lockCtx, `UPDATE outbox SET last_error = 'touched' WHERE payload->>'ride_id' = 'eeeeeeee-0000-0000-0000-000000000002'`); err != nil {
		t.Fatalf("row locked during a send: %v", err)
	}
	callsBefore := len(relayPayment.calls())
	if err := relay.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(relayPayment.calls()); got != callsBefore {
		t.Fatalf("second relay sent a leased message: %d calls, want %d", got, callsBefore)
	}
	release()
	if err := <-firstRun; err != nil {
		t.Fatal(err)
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

	testWatchRide(t, ctx, pool, client, location, service)

	// Stream auth must not lock tools out of reflection, which is itself a stream.
	refl, err := reflectionv1.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := refl.Send(&reflectionv1.ServerReflectionRequest{MessageRequest: &reflectionv1.ServerReflectionRequest_ListServices{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := refl.Recv(); err != nil {
		t.Fatalf("reflection without a token: %v", err)
	}
}

// testWatchRide follows rides through WatchRide. Changes reach the stream through the real trigger and listener;
// only the driver's position is faked.
func testWatchRide(t *testing.T, ctx context.Context, pool *pgxpool.Pool, client ridev1.RideServiceClient, location *fakeLocation, service context.Context) {
	t.Helper()
	const (
		riderID  = "eeeeeeee-0000-0000-0000-000000000001"
		driverID = "eeeeeeee-0000-0000-0000-000000000002"
	)
	rider, driver := withToken(t, ctx, riderID), withToken(t, ctx, driverID)
	// Wait for the listener, so changes below reach streams as notifications and not through its catch-up on connecting.
	listenerPID := func() (pid int) {
		pool.QueryRow(ctx, `SELECT pid FROM pg_stat_activity WHERE query LIKE 'LISTEN "ride_changed"%'`).Scan(&pid)
		return pid
	}
	pid := listenerPID()
	for deadline := time.Now().Add(5 * time.Second); pid == 0 && time.Now().Before(deadline); pid = listenerPID() {
		time.Sleep(10 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("ride change listener never connected")
	}
	newRide := func() string {
		t.Helper()
		res, err := client.CreateRide(rider, &ridev1.CreateRideRequest{Pickup: &ridev1.Location{Lat: 1, Lng: 2}, Dropoff: &ridev1.Location{Lat: 3, Lng: 4}})
		if err != nil {
			t.Fatal(err)
		}
		return res.GetRideId()
	}
	watch := func(who context.Context, rideID string) grpc.ServerStreamingClient[ridev1.WatchRideResponse] {
		t.Helper()
		who, cancel := context.WithTimeout(who, 30*time.Second)
		t.Cleanup(cancel)
		stream, err := client.WatchRide(who, &ridev1.WatchRideRequest{RideId: rideID})
		if err != nil {
			t.Fatal(err)
		}
		return stream
	}
	next := func(stream grpc.ServerStreamingClient[ridev1.WatchRideResponse], want ridev1.RideStatus) *ridev1.Ride {
		t.Helper()
		res, err := stream.Recv()
		if err != nil {
			t.Fatalf("waiting for %v: %v", want, err)
		}
		if got := res.GetRide().GetStatus(); got != want {
			t.Fatalf("status: got %v, want %v", got, want)
		}
		return res.GetRide()
	}
	wantEnd := func(stream grpc.ServerStreamingClient[ridev1.WatchRideResponse]) {
		t.Helper()
		if res, err := stream.Recv(); err != io.EOF {
			t.Fatalf("after the final state: got %v, %v, want the stream to end", res, err)
		}
	}
	wantLocation := func(r *ridev1.Ride, lat float64) {
		t.Helper()
		if r.GetDriverLocation().GetLat() != lat {
			t.Fatalf("driver location: got %v, want lat %v", r.GetDriverLocation(), lat)
		}
	}

	rideID := newRide()
	refused := func(name string, who context.Context, id string, code codes.Code) {
		t.Helper()
		_, err := watch(who, id).Recv()
		wantCode(t, name, err, code)
	}
	refused("watch without token", ctx, rideID, codes.Unauthenticated)
	refused("watch with service token", service, rideID, codes.PermissionDenied)
	refused("watch someone else's ride", withToken(t, ctx, "eeeeeeee-0000-0000-0000-000000000003"), rideID, codes.NotFound)
	refused("watch bad id", rider, "ride-1", codes.InvalidArgument)

	// The first message is the current state, whole.
	riderStream := watch(rider, rideID)
	first := next(riderStream, ridev1.RideStatus_RIDE_STATUS_REQUESTED)
	if first.GetRideId() != rideID || first.GetPickup().GetLat() != 1 || first.GetDropoff().GetLng() != 4 || first.GetRequestedAt() == nil {
		t.Fatalf("first message: %v", first)
	}
	if first.GetDriverId() != "" || first.GetDriverLocation() != nil || first.GetAcceptedAt() != nil {
		t.Fatalf("first message has driver details: %v", first)
	}

	// Assigned: the rider's stream gets the driver and their position, and the driver may now watch too.
	location.set(&locationv1.Location{Lat: 10}, nil)
	if _, err := client.AssignDriver(service, &ridev1.AssignDriverRequest{RideId: rideID, DriverId: driverID}); err != nil {
		t.Fatal(err)
	}
	accepted := next(riderStream, ridev1.RideStatus_RIDE_STATUS_ACCEPTED)
	if accepted.GetDriverId() != driverID || accepted.GetAcceptedAt() == nil {
		t.Fatalf("after assign: %v", accepted)
	}
	wantLocation(accepted, 10)
	driverStream := watch(driver, rideID)
	next(driverStream, ridev1.RideStatus_RIDE_STATUS_ACCEPTED)

	// The driver moves: a message with the same status and the new position.
	location.set(&locationv1.Location{Lat: 11}, nil)
	wantLocation(next(riderStream, ridev1.RideStatus_RIDE_STATUS_ACCEPTED), 11)
	wantLocation(next(driverStream, ridev1.RideStatus_RIDE_STATUS_ACCEPTED), 11)

	// Location down: the stream survives on the last known position.
	location.set(nil, status.Error(codes.Unavailable, "location down"))
	if _, err := client.StartRide(driver, &ridev1.StartRideRequest{RideId: rideID}); err != nil {
		t.Fatal(err)
	}
	started := next(riderStream, ridev1.RideStatus_RIDE_STATUS_IN_PROGRESS)
	if started.GetStartedAt() == nil {
		t.Fatalf("after start: %v", started)
	}
	wantLocation(started, 11)

	// Location says the driver is offline: the position goes away.
	location.set(nil, nil)
	if r := next(riderStream, ridev1.RideStatus_RIDE_STATUS_IN_PROGRESS); r.GetDriverLocation() != nil {
		t.Fatalf("offline driver: %v", r)
	}

	// The final state comes with no position, and ends both streams.
	location.set(&locationv1.Location{Lat: 12}, nil)
	if _, err := client.CompleteRide(driver, &ridev1.CompleteRideRequest{RideId: rideID}); err != nil {
		t.Fatal(err)
	}
	// A position message for the new position may land first, while the trip is still in progress.
	done, err := riderStream.Recv()
	if err == nil && done.GetRide().GetStatus() == ridev1.RideStatus_RIDE_STATUS_IN_PROGRESS {
		done, err = riderStream.Recv()
	}
	if err != nil || done.GetRide().GetStatus() != ridev1.RideStatus_RIDE_STATUS_COMPLETED {
		t.Fatalf("waiting for completed: %v, %v", done, err)
	}
	if r := done.GetRide(); r.GetDriverLocation() != nil || r.GetCompletedAt() == nil {
		t.Fatalf("completed: %v", r)
	}
	wantEnd(riderStream)
	if got := listenerPID(); got != pid {
		t.Fatalf("listener reconnected during the ride: pid %d, was %d", got, pid)
	}

	// Killing the listener's connection loses the notification for a change made before it reconnects. Reconnecting
	// must still tell the stream to look again, and a cancel must reach it with its reason.
	cancelled := newRide()
	stream := watch(rider, cancelled)
	next(stream, ridev1.RideStatus_RIDE_STATUS_REQUESTED)
	tag, err := pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE 'LISTEN "ride_changed"%'`)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatalf("kill listener: %v rows, %v", tag.RowsAffected(), err)
	}
	if _, err := client.CancelRide(rider, &ridev1.CancelRideRequest{RideId: cancelled}); err != nil {
		t.Fatal(err)
	}
	if r := next(stream, ridev1.RideStatus_RIDE_STATUS_CANCELLED); r.GetCancelReason() != ridev1.CancelReason_CANCEL_REASON_RIDER {
		t.Fatalf("cancelled: %v", r)
	}
	wantEnd(stream)
}

// fakeMatching records StartMatching and CancelMatching calls, in order, and fails them while err is set.
type fakePayment struct {
	paymentv1.PaymentServiceClient
	mu      sync.Mutex
	err     error
	reqs    []*paymentv1.ChargeRideRequest
	release chan struct{}
	entered chan struct{}
}

func (f *fakePayment) ChargeRide(ctx context.Context, in *paymentv1.ChargeRideRequest, _ ...grpc.CallOption) (*paymentv1.ChargeRideResponse, error) {
	f.mu.Lock()
	release, entered := f.release, f.entered
	f.mu.Unlock()
	if release != nil {
		entered <- struct{}{}
		<-release
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.reqs = append(f.reqs, in)
	return &paymentv1.ChargeRideResponse{}, nil
}

func (f *fakePayment) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakePayment) calls() []*paymentv1.ChargeRideRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*paymentv1.ChargeRideRequest(nil), f.reqs...)
}

// hold makes ChargeRide hang until the returned function is called, like a receiver that stopped answering.
func (f *fakePayment) hold() func() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.release, f.entered = make(chan struct{}), make(chan struct{}, 8)
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		close(f.release)
		f.release = nil
	}
}

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

// fakeLocation answers GetDriverLocation with what the test last set.
type fakeLocation struct {
	locationv1.LocationServiceClient
	mu  sync.Mutex
	loc *locationv1.Location
	err error
}

func (f *fakeLocation) set(loc *locationv1.Location, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loc, f.err = loc, err
}

func (f *fakeLocation) GetDriverLocation(context.Context, *locationv1.GetDriverLocationRequest, ...grpc.CallOption) (*locationv1.GetDriverLocationResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &locationv1.GetDriverLocationResponse{Location: f.loc}, f.err
}

func startServer(t *testing.T, pool *pgxpool.Pool, location locationv1.LocationServiceClient) (ridev1.RideServiceClient, *grpc.ClientConn) {
	t.Helper()
	hub := ride.NewHub(pool)
	// Reconnects slowly on purpose: the test that kills this connection makes a change during the gap.
	listenCtx, stopListen := context.WithCancel(context.Background())
	t.Cleanup(stopListen)
	go loop.Run(listenCtx, zap.NewNop(), "ride change listener", 200*time.Millisecond, 200*time.Millisecond, hub.Listen)

	gs := server.NewGRPCServer(zap.NewNop(), jwtauth.NewVerifier(testSecret), ride.NewService(ride.NewRepository(pool), hub, location))
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

	return ridev1.NewRideServiceClient(conn), conn
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
