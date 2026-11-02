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

	// The whole ledger balances, charge by charge: the invariant everything above relies on.
	var unbalanced int
	pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT charge_id FROM ledger_entries GROUP BY charge_id HAVING sum(amount) <> 0) x`).Scan(&unbalanced)
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
