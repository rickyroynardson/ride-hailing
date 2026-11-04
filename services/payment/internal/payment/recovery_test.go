package payment

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/provider"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Each subtest stops a real worker at a crash boundary or rejects a database checkpoint, then constructs a fresh
// service/provider on the same durable state. This is not a process-kill or load test: no sleeps, no extra service
// containers, and leases expire only when explicitly moved forward in SQL.
func TestFailureRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pg, err := tcpostgres.Run(ctx, "postgres:18-alpine", tcpostgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, pg)
	if err != nil {
		t.Fatal(err)
	}
	dsn := pg.MustConnectionString(ctx, "sslmode=disable")
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	migrations, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS("../../migrations"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrations.Up(ctx); err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	recoveryExec(t, ctx, pool, `CREATE TABLE test_ride_receipts(ride_id UUID PRIMARY KEY, outcome INTEGER NOT NULL)`)
	setup := func(t *testing.T) *recoveryFixture {
		t.Helper()
		recoveryExec(t, ctx, pool, `TRUNCATE ledger_entries, refunds, charge_retry_requests, charges, payment_methods,
			fake_provider_operations, fake_provider_charges, test_ride_receipts`)
		f := &recoveryFixture{ctx: ctx, pool: pool, repo: NewRepository(pool), receiver: &rideReceipts{pool: pool}}
		f.input = ChargeInput{RideID: uuid.NewString(), RiderID: uuid.NewString(), DriverID: uuid.NewString(), Kind: KindFare, Amount: Money{Amount: 10_000, Currency: "IDR"}}
		if err := f.service().ChargeRide(ctx, f.input); err != nil {
			t.Fatal(err)
		}
		return f
	}

	t.Run("worker stops after claim before provider call", func(t *testing.T) {
		f := setup(t)
		claim := f.claimCharge(t)
		if err := f.service().processDue(ctx); err != nil {
			t.Fatal(err)
		}
		f.assertProvider(t, 0, 0)
		f.expireCharge(t, claim.ID)
		if err := f.service().processDue(ctx); err != nil {
			t.Fatal(err)
		}
		f.assertCharge(t, claim.ID, StatusCaptured)
		f.assertLedger(t, claim.ID, 3, 0, 10_000, 8_000)
		f.assertProvider(t, 1, 0)
	})

	t.Run("provider captures but application ledger transaction rolls back", func(t *testing.T) {
		f := setup(t)
		claim := f.claimCharge(t)
		// The charge UPDATE and first ledger inserts run, then the driver's posting fails: the entire application
		// transaction must roll back even though the provider already committed in its separate transaction.
		recoveryExec(t, ctx, pool, fmt.Sprintf(`ALTER TABLE ledger_entries ADD CONSTRAINT test_reject_capture CHECK (account <> 'driver:%s') NOT VALID`, f.input.DriverID))
		drop := func() {
			recoveryExec(t, ctx, pool, `ALTER TABLE ledger_entries DROP CONSTRAINT IF EXISTS test_reject_capture`)
		}
		defer drop()
		err := f.service().process(ctx, claim)
		if err == nil || !strings.Contains(err.Error(), "test_reject_capture") {
			t.Fatalf("capture fault wasn't reached: %v", err)
		}
		f.assertCharge(t, claim.ID, StatusPending)
		f.assertLedger(t, claim.ID, 0, 0, 0, 0)
		f.assertProvider(t, 1, 0)
		if err := f.service().processDue(ctx); err != nil {
			t.Fatal(err)
		}
		f.assertLedger(t, claim.ID, 0, 0, 0, 0) // lease remains held after failed checkpoint
		drop()
		f.expireCharge(t, claim.ID)
		if err := f.service().processDue(ctx); err != nil {
			t.Fatal(err)
		}
		f.assertCharge(t, claim.ID, StatusCaptured)
		f.assertLedger(t, claim.ID, 3, 0, 10_000, 8_000)
		f.assertProvider(t, 1, 0) // replay read the receipt, not another money movement
	})

	for _, fault := range []string{"reply lost after ride commits", "local notification checkpoint fails"} {
		t.Run(fault, func(t *testing.T) {
			f := setup(t)
			claim := f.claimCharge(t)
			if err := f.service().process(ctx, claim); err != nil {
				t.Fatal(err)
			}
			if fault == "reply lost after ride commits" {
				f.receiver.loseReply.Store(true)
			} else {
				recoveryExec(t, ctx, pool, `CREATE FUNCTION test_reject_notification() RETURNS trigger LANGUAGE plpgsql AS $$
					BEGIN RAISE EXCEPTION 'injected notification checkpoint failure'; END $$;
					CREATE TRIGGER test_reject_notification BEFORE UPDATE ON charges FOR EACH ROW
					WHEN (NEW.ride_notified_at IS NOT NULL) EXECUTE FUNCTION test_reject_notification()`)
				defer func() {
					recoveryExec(t, ctx, pool, `DROP TRIGGER IF EXISTS test_reject_notification ON charges; DROP FUNCTION IF EXISTS test_reject_notification()`)
				}()
			}
			if err := f.service().notifyOutcomes(ctx); err == nil {
				t.Fatal("expected notification/checkpoint failure")
			}
			var notified bool
			if err := pool.QueryRow(ctx, `SELECT ride_notified_at IS NOT NULL FROM charges WHERE id = $1`, claim.ID).Scan(&notified); err != nil {
				t.Fatal(err)
			}
			if notified {
				t.Fatal("payment prematurely checkpointed notification")
			}
			var count int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM test_ride_receipts WHERE ride_id = $1 AND outcome = $2`, f.input.RideID, int(ridev1.PaymentOutcome_PAYMENT_OUTCOME_PAID)).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatal("ride did not commit before the simulated failure")
			}
			if fault != "reply lost after ride commits" {
				recoveryExec(t, ctx, pool, `DROP TRIGGER test_reject_notification ON charges; DROP FUNCTION test_reject_notification()`)
			}
			if err := f.service().notifyOutcomes(ctx); err != nil {
				t.Fatal(err)
			}
			if err := f.service().notifyOutcomes(ctx); err != nil {
				t.Fatal(err)
			}
			if f.receiver.calls.Load() != 2 {
				t.Fatalf("want one repeat then stop, got %d calls", f.receiver.calls.Load())
			}
			f.assertCharge(t, claim.ID, StatusCaptured)
			f.assertLedger(t, claim.ID, 3, 0, 10_000, 8_000)
			f.assertProvider(t, 1, 0)
		})
	}

	t.Run("provider refunds but reversal transaction rolls back", func(t *testing.T) {
		f := setup(t)
		capture := f.claimCharge(t)
		if err := f.service().process(ctx, capture); err != nil {
			t.Fatal(err)
		}
		refund := f.newRefund(t)
		claim := f.claimRefund(t)
		recoveryExec(t, ctx, pool, `ALTER TABLE ledger_entries ADD CONSTRAINT test_reject_refund CHECK (refund_id IS NULL) NOT VALID`)
		drop := func() {
			recoveryExec(t, ctx, pool, `ALTER TABLE ledger_entries DROP CONSTRAINT IF EXISTS test_reject_refund`)
		}
		defer drop()
		err := f.service().processRefund(ctx, claim)
		if err == nil || !strings.Contains(err.Error(), "test_reject_refund") {
			t.Fatalf("refund fault wasn't reached: %v", err)
		}
		f.assertRefund(t, refund.ID, RefundPending)
		f.assertProvider(t, 1, 1)
		f.assertLedger(t, capture.ID, 3, 0, 10_000, 8_000)
		drop()
		recoveryExec(t, ctx, pool, `UPDATE refunds SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, refund.ID)
		if err := f.service().processDueRefunds(ctx); err != nil {
			t.Fatal(err)
		}
		f.assertRefund(t, refund.ID, RefundSucceeded)
		f.assertLedger(t, capture.ID, 3, 3, 6_000, 4_800)
		f.assertProvider(t, 1, 1)
		var returned int64
		if err := pool.QueryRow(ctx, `SELECT refunded FROM fake_provider_charges`).Scan(&returned); err != nil {
			t.Fatal(err)
		}
		if returned != 4_000 {
			t.Fatalf("provider returned %d, want 4000", returned)
		}
	})

	t.Run("expired charge worker cannot shorten a replacement lease", func(t *testing.T) {
		f := setup(t)
		old := f.claimCharge(t)
		f.expireCharge(t, old.ID)
		current := f.claimCharge(t)
		deadline := f.chargeDeadline(t, current.ID)
		outage := NewService(f.repo, unavailableProvider{}, f.receiver, zap.NewNop())
		if err := outage.process(ctx, old); err != nil {
			t.Fatal(err)
		}
		if got := f.chargeDeadline(t, current.ID); !got.Equal(deadline) {
			t.Fatal("old worker's error shortened the new worker's lease")
		}
		if err := f.repo.Decline(ctx, old.ID, "late decline", old.ProviderAttempt, old.Attempts); err != nil {
			t.Fatal(err)
		}
		f.assertCharge(t, current.ID, StatusPending)
		// Even a late success can move money at the provider, but mustn't checkpoint the replacement's work.
		if err := f.service().process(ctx, old); err != nil {
			t.Fatal(err)
		}
		f.assertCharge(t, current.ID, StatusPending)
		f.assertLedger(t, current.ID, 0, 0, 0, 0)
		if err := f.service().process(ctx, current); err != nil {
			t.Fatal(err)
		}
		f.assertLedger(t, current.ID, 3, 0, 10_000, 8_000)
		f.assertProvider(t, 1, 0)
	})

	t.Run("expired refund worker cannot shorten a replacement lease", func(t *testing.T) {
		f := setup(t)
		capture := f.claimCharge(t)
		if err := f.service().process(ctx, capture); err != nil {
			t.Fatal(err)
		}
		refund := f.newRefund(t)
		old := f.claimRefund(t)
		recoveryExec(t, ctx, pool, `UPDATE refunds SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, refund.ID)
		current := f.claimRefund(t)
		deadline := f.refundDeadline(t, refund.ID)
		outage := NewService(f.repo, unavailableProvider{}, f.receiver, zap.NewNop())
		if err := outage.processRefund(ctx, old); err != nil {
			t.Fatal(err)
		}
		if got := f.refundDeadline(t, refund.ID); !got.Equal(deadline) {
			t.Fatal("old refund worker's error shortened the new worker's lease")
		}
		if err := f.repo.FailRefund(ctx, old.ID, "late refusal", old.Attempts); err != nil {
			t.Fatal(err)
		}
		f.assertRefund(t, refund.ID, RefundPending)
		if err := f.service().processRefund(ctx, old); err != nil {
			t.Fatal(err)
		}
		f.assertRefund(t, refund.ID, RefundPending)
		f.assertLedger(t, capture.ID, 3, 0, 10_000, 8_000)
		if err := f.service().processRefund(ctx, current); err != nil {
			t.Fatal(err)
		}
		f.assertLedger(t, capture.ID, 3, 3, 6_000, 4_800)
		f.assertProvider(t, 1, 1)
	})
}

type recoveryFixture struct {
	ctx      context.Context
	pool     *pgxpool.Pool
	repo     *PaymentRepository
	receiver *rideReceipts
	input    ChargeInput
}

func (f *recoveryFixture) service() *PaymentService {
	return NewService(NewRepository(f.pool), provider.NewPersistentFake(f.pool), f.receiver, zap.NewNop())
}
func (f *recoveryFixture) claimCharge(t *testing.T) dueCharge {
	t.Helper()
	claims, err := f.repo.ClaimDue(f.ctx, 1, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim charge: %d claims, %v", len(claims), err)
	}
	return claims[0]
}
func (f *recoveryFixture) expireCharge(t *testing.T, id string) {
	recoveryExec(t, f.ctx, f.pool, `UPDATE charges SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, id)
}
func (f *recoveryFixture) newRefund(t *testing.T) Refund {
	t.Helper()
	r, err := f.service().RefundCharge(f.ctx, RefundInput{RideID: f.input.RideID, Amount: 4000, Reason: "failure recovery", IdempotencyKey: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func (f *recoveryFixture) claimRefund(t *testing.T) dueRefund {
	t.Helper()
	claims, err := f.repo.ClaimDueRefunds(f.ctx, 1, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim refund: %d claims, %v", len(claims), err)
	}
	return claims[0]
}
func (f *recoveryFixture) chargeDeadline(t *testing.T, id string) time.Time {
	t.Helper()
	var deadline time.Time
	if err := f.pool.QueryRow(f.ctx, `SELECT next_attempt_at FROM charges WHERE id = $1`, id).Scan(&deadline); err != nil {
		t.Fatal(err)
	}
	return deadline
}
func (f *recoveryFixture) refundDeadline(t *testing.T, id string) time.Time {
	t.Helper()
	var deadline time.Time
	if err := f.pool.QueryRow(f.ctx, `SELECT next_attempt_at FROM refunds WHERE id = $1`, id).Scan(&deadline); err != nil {
		t.Fatal(err)
	}
	return deadline
}
func (f *recoveryFixture) assertCharge(t *testing.T, id, want string) {
	t.Helper()
	var state string
	if err := f.pool.QueryRow(f.ctx, `SELECT status FROM charges WHERE id = $1`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != want {
		t.Fatalf("charge status %s, want %s", state, want)
	}
}
func (f *recoveryFixture) assertRefund(t *testing.T, id, want string) {
	t.Helper()
	var state string
	if err := f.pool.QueryRow(f.ctx, `SELECT status FROM refunds WHERE id = $1`, id).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != want {
		t.Fatalf("refund status %s, want %s", state, want)
	}
}
func (f *recoveryFixture) assertProvider(t *testing.T, charges, refunds int) {
	t.Helper()
	var gotCharges, gotRefunds int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FILTER (WHERE kind = 'charge'), count(*) FILTER (WHERE kind = 'refund') FROM fake_provider_operations`).Scan(&gotCharges, &gotRefunds); err != nil {
		t.Fatal(err)
	}
	if gotCharges != charges || gotRefunds != refunds {
		t.Fatalf("provider operations: %d charges/%d refunds, want %d/%d", gotCharges, gotRefunds, charges, refunds)
	}
}
func (f *recoveryFixture) assertLedger(t *testing.T, chargeID string, captures, reversals int, clearing, earnings int64) {
	t.Helper()
	var gotCapture, gotReversal int
	var gotClearing, gotEarnings int64
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FILTER (WHERE refund_id IS NULL), count(*) FILTER (WHERE refund_id IS NOT NULL),
		COALESCE(sum(amount) FILTER (WHERE account = 'provider_clearing'), 0)::bigint,
		COALESCE(-sum(amount) FILTER (WHERE account = $2), 0)::bigint FROM ledger_entries WHERE charge_id = $1`, chargeID, "driver:"+f.input.DriverID).
		Scan(&gotCapture, &gotReversal, &gotClearing, &gotEarnings); err != nil {
		t.Fatal(err)
	}
	if gotCapture != captures || gotReversal != reversals || gotClearing != clearing || gotEarnings != earnings {
		t.Fatalf("ledger got %d capture/%d refund entries, clearing=%d earnings=%d; want %d/%d %d/%d", gotCapture, gotReversal, gotClearing, gotEarnings, captures, reversals, clearing, earnings)
	}
	var unbalanced int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM (SELECT 1 FROM ledger_entries WHERE charge_id = $1 GROUP BY charge_id, refund_id HAVING sum(amount) <> 0) p`, chargeID).Scan(&unbalanced); err != nil {
		t.Fatal(err)
	}
	if unbalanced != 0 {
		t.Fatal("a posting did not balance")
	}
}
func recoveryExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}

type rideReceipts struct {
	ridev1.RideServiceClient
	pool      *pgxpool.Pool
	calls     atomic.Int32
	loseReply atomic.Bool
}

func (r *rideReceipts) RecordPayment(ctx context.Context, req *ridev1.RecordPaymentRequest, _ ...grpc.CallOption) (*ridev1.RecordPaymentResponse, error) {
	r.calls.Add(1)
	_, err := r.pool.Exec(ctx, `INSERT INTO test_ride_receipts(ride_id, outcome) VALUES ($1, $2)
		ON CONFLICT (ride_id) DO UPDATE SET outcome = EXCLUDED.outcome`, req.GetRideId(), int(req.GetOutcome()))
	if err != nil {
		return nil, err
	}
	if r.loseReply.CompareAndSwap(true, false) {
		return nil, status.Error(codes.Unavailable, "ride committed, reply lost")
	}
	return &ridev1.RecordPaymentResponse{}, nil
}

type unavailableProvider struct{}

func (unavailableProvider) Charge(context.Context, provider.Request) (provider.Result, error) {
	return provider.Result{}, provider.ErrOutcomeUnknown
}
func (unavailableProvider) Refund(context.Context, provider.RefundRequest) (provider.RefundResult, error) {
	return provider.RefundResult{}, provider.ErrOutcomeUnknown
}
