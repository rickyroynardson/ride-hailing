package server_test

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/postgres"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/database"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/payment"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/provider"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/server"
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

const (
	testSecret = "test-secret"
	riderID    = "11111111-1111-1111-1111-111111111111"
	driverID   = "22222222-2222-2222-2222-222222222222"
)

// TestIntegration runs the real gRPC server, interceptors and SQL against a throwaway Postgres container, with fake
// provider and ride. Skipped with -short or when Docker is unavailable.
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

	if err := postgres.CheckSchema(ctx, pool, database.SchemaVersion); !errors.Is(err, postgres.ErrNotMigrated) {
		t.Fatalf("schema check before migrating: got %v, want ErrNotMigrated", err)
	}
	migrate(t, url)
	if err := postgres.CheckSchema(ctx, pool, database.SchemaVersion); err != nil {
		t.Fatalf("schema check after migrating: %v", err)
	}

	fake := provider.NewFake()
	ride := &fakeRide{}
	service := payment.NewService(payment.NewRepository(pool), fake, ride, zap.NewNop())
	client := startServer(t, service)

	rider, driver := withToken(t, ctx, riderID), withToken(t, ctx, driverID)
	stranger := withToken(t, ctx, "33333333-3333-3333-3333-333333333333")
	md, err := jwtauth.NewServiceCredentials("ride", testSecret).GetRequestMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	asRide := metadata.AppendToOutgoingContext(ctx, "authorization", md["authorization"])

	seq := 0
	nextRide := func() string {
		seq++
		return "aaaaaaaa-0000-0000-0000-" + pad(seq)
	}
	chargeReq := func(rideID string, amount int64) *paymentv1.ChargeRideRequest {
		return &paymentv1.ChargeRideRequest{
			RideId: rideID, RiderId: riderID, DriverId: driverID, Kind: paymentv1.ChargeKind_CHARGE_KIND_FARE,
			Amount: &paymentv1.Money{Amount: amount, Currency: "IDR"},
		}
	}
	tick := func() {
		t.Helper()
		if err := service.Tick(ctx); err != nil {
			t.Fatalf("tick: %v", err)
		}
	}
	setCard := func(token string) {
		t.Helper()
		if _, err := client.SetPaymentMethod(rider, &paymentv1.SetPaymentMethodRequest{Token: token}); err != nil {
			t.Fatal(err)
		}
	}
	statusOf := func(rideID string) paymentv1.ChargeStatus {
		t.Helper()
		res, err := client.GetCharge(rider, &paymentv1.GetChargeRequest{RideId: rideID})
		if err != nil {
			t.Fatal(err)
		}
		return res.GetCharge().GetStatus()
	}
	earnings := func() int64 {
		t.Helper()
		res, err := client.GetEarnings(driver, &paymentv1.GetEarningsRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.GetBalances()) == 0 {
			return 0
		}
		return res.GetBalances()[0].GetAmount()
	}

	// Who may call what.
	first := nextRide()
	_, err = client.ChargeRide(ctx, chargeReq(first, 10_000))
	wantCode(t, "charge without token", err, codes.Unauthenticated)
	_, err = client.ChargeRide(rider, chargeReq(first, 10_000))
	wantCode(t, "a rider charging themselves", err, codes.PermissionDenied)
	_, err = client.ChargeRide(asRide, chargeReq("ride-1", 10_000))
	wantCode(t, "bad ride id", err, codes.InvalidArgument)
	_, err = client.ChargeRide(asRide, chargeReq(first, 0))
	wantCode(t, "zero amount", err, codes.InvalidArgument)
	_, err = client.SetPaymentMethod(rider, &paymentv1.SetPaymentMethodRequest{})
	wantCode(t, "empty card", err, codes.InvalidArgument)

	// Happy path. Ride's at-least-once delivery may repeat the message, even with a different amount: the first stands.
	setCard(provider.TokenOK)
	for _, amount := range []int64{10_000, 10_000, 99_999} {
		if _, err := client.ChargeRide(asRide, chargeReq(first, amount)); err != nil {
			t.Fatal(err)
		}
	}
	if got := statusOf(first); got != paymentv1.ChargeStatus_CHARGE_STATUS_PENDING {
		t.Fatalf("before the tick: %v", got)
	}
	tick()
	if got := statusOf(first); got != paymentv1.ChargeStatus_CHARGE_STATUS_CAPTURED {
		t.Fatalf("after the tick: %v", got)
	}
	if fake.Captured() != 1 {
		t.Fatalf("provider captured %d, want 1", fake.Captured())
	}
	if got := earnings(); got != 8_000 {
		t.Fatalf("driver earned %d, want 8000 (80%% of 10000)", got)
	}
	if got := ride.last(first); got != ridev1.PaymentOutcome_PAYMENT_OUTCOME_PAID {
		t.Fatalf("ride told %v", got)
	}
	ride.reset()
	tick()
	if n := ride.count(first); n != 0 {
		t.Fatalf("ride told again after confirming: %d", n)
	}

	// The ride's rider and driver can read the charge; nobody else can tell it exists.
	if _, err := client.GetCharge(driver, &paymentv1.GetChargeRequest{RideId: first}); err != nil {
		t.Fatal(err)
	}
	_, err = client.GetCharge(stranger, &paymentv1.GetChargeRequest{RideId: first})
	wantCode(t, "someone else's charge", err, codes.NotFound)

	// Declined: no money, no ledger, ride hears. The rider's new card is a new attempt at the provider, and pays.
	setCard(provider.TokenDeclined)
	declined := nextRide()
	if _, err := client.ChargeRide(asRide, chargeReq(declined, 20_000)); err != nil {
		t.Fatal(err)
	}
	tick()
	res, err := client.GetCharge(rider, &paymentv1.GetChargeRequest{RideId: declined})
	if err != nil {
		t.Fatal(err)
	}
	if c := res.GetCharge(); c.GetStatus() != paymentv1.ChargeStatus_CHARGE_STATUS_FAILED || c.GetDeclineReason() != "card_declined" {
		t.Fatalf("declined charge: %+v", c)
	}
	if got := ride.last(declined); got != ridev1.PaymentOutcome_PAYMENT_OUTCOME_FAILED {
		t.Fatalf("ride told %v", got)
	}
	if got := earnings(); got != 8_000 {
		t.Fatalf("a decline paid the driver: %d", got)
	}
	_, err = client.RetryCharge(stranger, &paymentv1.RetryChargeRequest{RideId: declined})
	wantCode(t, "retry someone else's charge", err, codes.NotFound)

	setCard(provider.TokenOK)
	for range 2 {
		if _, err := client.RetryCharge(rider, &paymentv1.RetryChargeRequest{RideId: declined}); err != nil {
			t.Fatal(err)
		}
	}
	tick()
	if got := statusOf(declined); got != paymentv1.ChargeStatus_CHARGE_STATUS_CAPTURED {
		t.Fatalf("after retry: %v", got)
	}
	if got := ride.last(declined); got != ridev1.PaymentOutcome_PAYMENT_OUTCOME_PAID {
		t.Fatalf("ride told %v after retry", got)
	}
	if got := earnings(); got != 8_000+16_000 {
		t.Fatalf("earnings after retry: %d", got)
	}
	// Retrying what is already paid changes nothing.
	if _, err := client.RetryCharge(rider, &paymentv1.RetryChargeRequest{RideId: declined}); err != nil {
		t.Fatal(err)
	}
	if got := statusOf(declined); got != paymentv1.ChargeStatus_CHARGE_STATUS_CAPTURED {
		t.Fatalf("retry of a captured charge: %v", got)
	}

	// The provider took the money but its reply was lost. The charge stays pending, nobody hears anything final, it
	// waits out a backoff, and the repeat uses the same key: charged once, posted once.
	setCard(provider.TokenLostResponse)
	lost := nextRide()
	if _, err := client.ChargeRide(asRide, chargeReq(lost, 30_000)); err != nil {
		t.Fatal(err)
	}
	capturedBefore := fake.Captured()
	tick()
	if got := statusOf(lost); got != paymentv1.ChargeStatus_CHARGE_STATUS_PENDING {
		t.Fatalf("after a lost response: %v", got)
	}
	if n := ride.count(lost); n != 0 {
		t.Fatalf("ride told an outcome nobody knows: %d", n)
	}
	var lastError string
	pool.QueryRow(ctx, `SELECT COALESCE(last_error, '') FROM charges WHERE ride_id = $1`, lost).Scan(&lastError)
	if lastError == "" {
		t.Fatal("lost response left no last_error")
	}
	tick() // still backing off
	if got := statusOf(lost); got != paymentv1.ChargeStatus_CHARGE_STATUS_PENDING {
		t.Fatalf("retried before the backoff ended: %v", got)
	}
	makeDue(t, pool)
	tick()
	if got := statusOf(lost); got != paymentv1.ChargeStatus_CHARGE_STATUS_CAPTURED {
		t.Fatalf("after the repeat: %v", got)
	}
	if got := fake.Captured() - capturedBefore; got != 1 {
		t.Fatalf("lost response took money %d times, want 1", got)
	}
	if got := earnings(); got != 24_000+24_000 {
		t.Fatalf("earnings after lost response: %d", got)
	}

	// A worker that dies after the provider took the money leaves the charge claimed. When the lease ends, the repeat
	// gets the same result.
	setCard(provider.TokenOK)
	crashed := nextRide()
	if _, err := client.ChargeRide(asRide, chargeReq(crashed, 40_000)); err != nil {
		t.Fatal(err)
	}
	pool.Exec(ctx, `UPDATE charges SET attempts = 1, next_attempt_at = now() + interval '30 seconds' WHERE ride_id = $1`, crashed)
	tick()
	if got := statusOf(crashed); got != paymentv1.ChargeStatus_CHARGE_STATUS_PENDING {
		t.Fatalf("processed while another worker's lease held: %v", got)
	}
	makeDue(t, pool)
	tick()
	if got := statusOf(crashed); got != paymentv1.ChargeStatus_CHARGE_STATUS_CAPTURED {
		t.Fatalf("after the lease ended: %v", got)
	}

	// Ride down: the money side finishes regardless, and the outcome waits on the row until ride confirms.
	ride.fail(status.Error(codes.Unavailable, "ride down"))
	rideDown := nextRide()
	if _, err := client.ChargeRide(asRide, chargeReq(rideDown, 10_000)); err != nil {
		t.Fatal(err)
	}
	if err := service.Tick(ctx); err == nil {
		t.Fatal("tick with ride down: want an error")
	}
	if got := statusOf(rideDown); got != paymentv1.ChargeStatus_CHARGE_STATUS_CAPTURED {
		t.Fatalf("with ride down: %v", got)
	}
	ride.fail(nil)
	tick()
	if got := ride.last(rideDown); got != ridev1.PaymentOutcome_PAYMENT_OUTCOME_PAID {
		t.Fatalf("ride told %v once back", got)
	}

	// A ride that doesn't exist stops being retried.
	ride.fail(status.Error(codes.NotFound, "no such ride"))
	orphan := nextRide()
	if _, err := client.ChargeRide(asRide, chargeReq(orphan, 10_000)); err != nil {
		t.Fatal(err)
	}
	tick()
	ride.fail(nil)
	ride.reset()
	tick()
	if n := ride.count(orphan); n != 0 {
		t.Fatalf("kept notifying about an unknown ride: %d", n)
	}

	// Replicas ticking at once split the work: every charge is captured and posted exactly once.
	const many = 40
	capturedBefore = fake.Captured()
	earnedBefore := earnings()
	for range many {
		if _, err := client.ChargeRide(asRide, chargeReq(nextRide(), 10_000)); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { service.Tick(ctx) })
	}
	wg.Wait()
	tick()
	if got := fake.Captured() - capturedBefore; got != many {
		t.Fatalf("concurrent ticks captured %d, want %d", got, many)
	}
	if got := earnings() - earnedBefore; got != many*8_000 {
		t.Fatalf("concurrent ticks paid %d, want %d", got, many*8_000)
	}

	// Refunds. Only the service side may ask, and only for a captured charge.
	setCard(provider.TokenOK)
	refundReq := func(rideID string, amount int64, key string) *paymentv1.RefundChargeRequest {
		return &paymentv1.RefundChargeRequest{RideId: rideID, Amount: amount, IdempotencyKey: key, Reason: "support"}
	}
	capture := func(amount int64) string {
		t.Helper()
		rideID := nextRide()
		if _, err := client.ChargeRide(asRide, chargeReq(rideID, amount)); err != nil {
			t.Fatal(err)
		}
		tick()
		return rideID
	}
	chargeOf := func(rideID string) *paymentv1.Charge {
		t.Helper()
		res, err := client.GetCharge(rider, &paymentv1.GetChargeRequest{RideId: rideID})
		if err != nil {
			t.Fatal(err)
		}
		return res.GetCharge()
	}
	refund := func(rideID string, amount int64, key string) *paymentv1.Refund {
		t.Helper()
		res, err := client.RefundCharge(asRide, refundReq(rideID, amount, key))
		if err != nil {
			t.Fatalf("refund %s: %v", key, err)
		}
		return res.GetRefund()
	}
	accountBalance := func(account string) int64 {
		t.Helper()
		var n int64
		if err := pool.QueryRow(ctx, `SELECT COALESCE(sum(amount), 0)::bigint FROM ledger_entries WHERE account = $1`, account).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	full := capture(10_000)
	_, err = client.RefundCharge(ctx, refundReq(full, 1, "no-token"))
	wantCode(t, "refund without token", err, codes.Unauthenticated)
	_, err = client.RefundCharge(rider, refundReq(full, 1, "by-rider"))
	wantCode(t, "a rider refunding themselves", err, codes.PermissionDenied)
	_, err = client.RefundCharge(asRide, refundReq(full, 0, "zero"))
	wantCode(t, "zero refund", err, codes.InvalidArgument)
	_, err = client.RefundCharge(asRide, refundReq(full, 1, ""))
	wantCode(t, "refund without a key", err, codes.InvalidArgument)
	_, err = client.RefundCharge(asRide, refundReq(nextRide(), 1, "no-charge"))
	wantCode(t, "refund of no charge", err, codes.NotFound)
	_, err = client.RefundCharge(asRide, refundReq(full, 10_001, "too-much"))
	wantCode(t, "refund above the charge", err, codes.FailedPrecondition)

	setCard(provider.TokenDeclined)
	unpaid := capture(10_000)
	setCard(provider.TokenOK)
	_, err = client.RefundCharge(asRide, refundReq(unpaid, 1, "declined"))
	wantCode(t, "refund of a declined charge", err, codes.FailedPrecondition)

	// A full refund reverses the capture exactly: the driver and platform each give back their share, and the money
	// reconciles. Repeating the request, before or after it finished, refunds once.
	earnedBefore = earnings()
	platformBefore, clearingBefore := accountBalance("platform_revenue"), accountBalance("provider_clearing")
	refundedBefore := fake.Refunded()
	first1 := refund(full, 10_000, "full-1")
	if again := refund(full, 10_000, "full-1"); again.GetRefundId() != first1.GetRefundId() {
		t.Fatalf("repeat made a new refund: %v vs %v", again, first1)
	}
	if first1.GetStatus() != paymentv1.RefundStatus_REFUND_STATUS_PENDING {
		t.Fatalf("new refund: %v", first1)
	}
	tick()
	if again := refund(full, 10_000, "full-1"); again.GetStatus() != paymentv1.RefundStatus_REFUND_STATUS_SUCCEEDED {
		t.Fatalf("repeat after it finished: %v", again)
	}
	tick()
	if got := fake.Refunded() - refundedBefore; got != 10_000 {
		t.Fatalf("provider refunded %d, want 10000", got)
	}
	if got := earnings() - earnedBefore; got != -8_000 {
		t.Fatalf("driver balance moved %d, want -8000", got)
	}
	if got := accountBalance("platform_revenue") - platformBefore; got != 2_000 {
		t.Fatalf("platform moved %d, want 2000", got)
	}
	if got := accountBalance("provider_clearing") - clearingBefore; got != -10_000 {
		t.Fatalf("clearing moved %d, want -10000", got)
	}
	if c := chargeOf(full); c.GetRefundedAmount() != 10_000 || len(c.GetRefunds()) != 1 || c.GetStatus() != paymentv1.ChargeStatus_CHARGE_STATUS_CAPTURED {
		t.Fatalf("charge after a full refund: %+v", c)
	}
	_, err = client.RefundCharge(asRide, refundReq(full, 1, "full-2"))
	wantCode(t, "refund of a fully refunded charge", err, codes.FailedPrecondition)

	// The same key can't mean a different refund, on this charge or another.
	_, err = client.RefundCharge(asRide, refundReq(full, 5_000, "full-1"))
	wantCode(t, "key reused for another amount", err, codes.FailedPrecondition)
	_, err = client.RefundCharge(asRide, refundReq(capture(10_000), 10_000, "full-1"))
	wantCode(t, "key reused on another charge", err, codes.FailedPrecondition)

	// Partial refunds that don't divide evenly still give back exactly the capture's shares, to the rupiah.
	const uneven = 10_007
	wantDriver := uneven - uneven*20/100
	partial := capture(uneven)
	earnedBefore = earnings()
	for i, part := range []int64{3_333, 3_333, 3_341} {
		refund(partial, part, "part-"+pad(i))
		tick()
	}
	if got := earnings() - earnedBefore; got != int64(-wantDriver) {
		t.Fatalf("partial refunds took %d from the driver, want %d", got, -wantDriver)
	}
	if got := chargeOf(partial).GetRefundedAmount(); got != uneven {
		t.Fatalf("refunded %d of %d", got, uneven)
	}

	// Racing refunds can't together exceed the charge: of ten at 40%, two fit. And ten copies of one request are one.
	raced := capture(10_000)
	refundedBefore = fake.Refunded()
	var mu sync.Mutex
	accepted := 0
	for i := range 10 {
		wg.Go(func() {
			if _, err := client.RefundCharge(asRide, refundReq(raced, 4_000, "race-"+pad(i))); err == nil {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if accepted != 2 {
		t.Fatalf("%d racing refunds accepted, want 2", accepted)
	}
	for range 2 {
		tick()
	}
	raceOne := capture(10_000)
	for range 10 {
		wg.Go(func() { client.RefundCharge(asRide, refundReq(raceOne, 4_000, "same-key")) })
	}
	wg.Wait()
	tick()
	if got := fake.Refunded() - refundedBefore; got != 12_000 {
		t.Fatalf("racing refunds paid %d, want 12000", got)
	}

	// The reply to a refund is lost after the money went back: it stays pending and reserved, and the repeat with the
	// same key pays once.
	setCard(provider.TokenRefundLost)
	lostRefund := capture(10_000)
	refundedBefore = fake.Refunded()
	earnedBefore = earnings()
	lostR := refund(lostRefund, 10_000, "lost-refund")
	tick()
	if c := chargeOf(lostRefund); c.GetRefundedAmount() != 0 || c.GetRefunds()[0].GetStatus() != paymentv1.RefundStatus_REFUND_STATUS_PENDING {
		t.Fatalf("after a lost refund reply: %+v", c)
	}
	_, err = client.RefundCharge(asRide, refundReq(lostRefund, 1, "while-unknown"))
	wantCode(t, "refund while another's outcome is unknown", err, codes.FailedPrecondition)
	if earnings() != earnedBefore {
		t.Fatal("ledger posted before the refund's outcome was known")
	}
	pool.Exec(ctx, `UPDATE refunds SET next_attempt_at = now() WHERE id = $1`, lostR.GetRefundId())
	tick()
	if got := fake.Refunded() - refundedBefore; got != 10_000 {
		t.Fatalf("lost reply refunded %d, want 10000", got)
	}
	if c := chargeOf(lostRefund); c.GetRefundedAmount() != 10_000 {
		t.Fatalf("after the repeat: %+v", c)
	}

	// The provider refuses: nothing moved, nothing posted, and the amount is free for a new refund.
	setCard(provider.TokenRefundRefused)
	refused := capture(10_000)
	earnedBefore = earnings()
	refund(refused, 10_000, "refused-1")
	tick()
	if c := chargeOf(refused); c.GetRefunds()[0].GetStatus() != paymentv1.RefundStatus_REFUND_STATUS_FAILED ||
		c.GetRefunds()[0].GetFailureReason() != "refund_refused" || c.GetRefundedAmount() != 0 {
		t.Fatalf("refused refund: %+v", c)
	}
	if earnings() != earnedBefore {
		t.Fatal("a refused refund moved the ledger")
	}
	refund(refused, 10_000, "refused-2")
	setCard(provider.TokenOK)

	// A driver with nothing else on their account ends at exactly zero after a full refund.
	const newDriver = "44444444-4444-4444-4444-444444444444"
	owed := nextRide()
	req := chargeReq(owed, 10_000)
	req.DriverId = newDriver
	if _, err := client.ChargeRide(asRide, req); err != nil {
		t.Fatal(err)
	}
	tick()
	refund(owed, 10_000, "owed-1")
	tick()
	if got := accountBalance("driver:" + newDriver); got != 0 {
		t.Fatalf("driver account after capture and full refund: %d, want 0", got)
	}

	// Money at the provider equals everything captured minus everything refunded, the reconciliation a finance team runs.
	var captured, refunded int64
	pool.QueryRow(ctx, `SELECT COALESCE(sum(amount), 0)::bigint FROM charges WHERE status = 'captured'`).Scan(&captured)
	pool.QueryRow(ctx, `SELECT COALESCE(sum(amount), 0)::bigint FROM refunds WHERE status = 'succeeded'`).Scan(&refunded)
	if got := accountBalance("provider_clearing"); got != captured-refunded {
		t.Fatalf("clearing %d, want %d captured - %d refunded", got, captured, refunded)
	}

	// The whole ledger balances, posting by posting: the invariant everything above relies on.
	var unbalanced int
	pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM ledger_entries GROUP BY charge_id, refund_id HAVING sum(amount) <> 0) x`).Scan(&unbalanced)
	if unbalanced != 0 {
		t.Fatalf("%d charges don't balance", unbalanced)
	}
	var capturedCharges, posted int
	pool.QueryRow(ctx, `SELECT count(*) FROM charges WHERE status = 'captured'`).Scan(&capturedCharges)
	pool.QueryRow(ctx, `SELECT count(DISTINCT charge_id) FROM ledger_entries`).Scan(&posted)
	if capturedCharges != posted {
		t.Fatalf("%d captured charges but %d posted", capturedCharges, posted)
	}

	// The schema itself refuses to rewrite history or post money from nowhere.
	if _, err := pool.Exec(ctx, `UPDATE ledger_entries SET amount = amount + 1`); err == nil {
		t.Fatal("ledger entry was updated")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM ledger_entries`); err == nil {
		t.Fatal("ledger entry was deleted")
	}
	var chargeID string
	pool.QueryRow(ctx, `SELECT id FROM charges WHERE ride_id = $1`, first).Scan(&chargeID)
	if _, err := pool.Exec(ctx, `INSERT INTO ledger_entries(charge_id, account, amount, currency) VALUES ($1, 'driver:free-money', -100, 'IDR')`, chargeID); err == nil {
		t.Fatal("an unbalanced entry was committed")
	}

	// The refunds migration goes back and forth over real data, the one place it must step around the ledger's rules.
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatalf("down over real refunds: %v", err)
	}
	if err := goose.Up(db, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	if got := accountBalance("provider_clearing"); got != captured {
		t.Fatalf("clearing after dropping refunds: %d, want %d", got, captured)
	}
}

func pad(n int) string {
	s := "000000000000"
	d := []byte(s)
	for i := len(d) - 1; n > 0; i-- {
		d[i] = byte('0' + n%10)
		n /= 10
	}
	return string(d)
}

// makeDue ends every backoff and lease, instead of waiting them out.
func makeDue(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), `UPDATE charges SET next_attempt_at = now() WHERE status = 'pending'`); err != nil {
		t.Fatal(err)
	}
}

type fakeRide struct {
	ridev1.RideServiceClient
	mu    sync.Mutex
	err   error
	calls map[string][]ridev1.PaymentOutcome
}

func (f *fakeRide) RecordPayment(_ context.Context, in *ridev1.RecordPaymentRequest, _ ...grpc.CallOption) (*ridev1.RecordPaymentResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	if f.calls == nil {
		f.calls = map[string][]ridev1.PaymentOutcome{}
	}
	f.calls[in.GetRideId()] = append(f.calls[in.GetRideId()], in.GetOutcome())
	return &ridev1.RecordPaymentResponse{}, nil
}

func (f *fakeRide) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeRide) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

func (f *fakeRide) count(rideID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls[rideID])
}

func (f *fakeRide) last(rideID string) ridev1.PaymentOutcome {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.calls[rideID]
	if len(c) == 0 {
		return ridev1.PaymentOutcome_PAYMENT_OUTCOME_UNSPECIFIED
	}
	return c[len(c)-1]
}

func migrate(t *testing.T, url string) {
	t.Helper()
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	if err := goose.Up(db, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	if err := goose.DownTo(db, "../../migrations", 0); err != nil {
		t.Fatalf("migrations must be reversible: %v", err)
	}
	if err := goose.Up(db, "../../migrations"); err != nil {
		t.Fatal(err)
	}
}

func startServer(t *testing.T, service *payment.PaymentService) paymentv1.PaymentServiceClient {
	t.Helper()
	gs := server.NewGRPCServer(zap.NewNop(), jwtauth.NewVerifier(testSecret), service)
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
	return paymentv1.NewPaymentServiceClient(conn)
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
