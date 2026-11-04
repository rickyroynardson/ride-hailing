package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// These are deterministic crash-boundary tests, not a load test. Calling claim/send separately stops execution at
// the exact point a process could die; database triggers reject checkpoint writes after the receiver has committed.
// Only leases are moved forward in SQL, so the tests never wait out the 30-second lease or rely on sleeps.
func TestFailureRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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
	mustExec(t, ctx, pool, `CREATE TABLE test_matching_receipts(ride_id UUID PRIMARY KEY, cancelled BOOLEAN NOT NULL DEFAULT false)`)

	setup := func(t *testing.T, withCancel bool) (*Relay, *matchingReceipts, string) {
		t.Helper()
		mustExec(t, ctx, pool, `TRUNCATE outbox, test_matching_receipts`)
		receiver := &matchingReceipts{pool: pool}
		relay := NewRelay(pool, zap.NewNop(), receiver, nil)
		id := uuid.NewString()
		if err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			if err := Add(ctx, tx, StartMatching{RideID: id, Pickup: Location{Lat: 1, Lng: 2}}); err != nil {
				return err
			}
			if withCancel {
				return Add(ctx, tx, CancelMatching{RideID: id})
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return relay, receiver, id
	}
	claimOne := func(t *testing.T, r *Relay) message {
		t.Helper()
		messages, err := r.claim(ctx)
		if err != nil || len(messages) != 1 {
			t.Fatalf("claim: got %d messages, %v", len(messages), err)
		}
		return messages[0]
	}
	expire := func(t *testing.T, m message) {
		mustExec(t, ctx, pool, `UPDATE outbox SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, m.id)
	}

	t.Run("worker stops after claim before sending", func(t *testing.T) {
		a, receiver, _ := setup(t, false)
		m := claimOne(t, a) // discard the worker's state, as a process exit would
		b := NewRelay(pool, zap.NewNop(), receiver, nil)
		if err := b.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if receiver.starts.Load() != 0 {
			t.Fatal("replacement worker ignored the outstanding lease")
		}
		expire(t, m)
		if err := b.Run(ctx); err != nil {
			t.Fatal(err)
		}
		assertOutboxDelivered(t, ctx, pool, m.id)
		if receiver.starts.Load() != 1 {
			t.Fatal("abandoned claim was not delivered exactly once")
		}
	})

	t.Run("receiver commits but acknowledgment write fails", func(t *testing.T) {
		a, receiver, rideID := setup(t, true)
		m := claimOne(t, a)
		mustExec(t, ctx, pool, `CREATE FUNCTION test_reject_ack() RETURNS trigger LANGUAGE plpgsql AS $$
			BEGIN RAISE EXCEPTION 'injected outbox checkpoint failure'; END $$;
			CREATE TRIGGER test_reject_ack BEFORE UPDATE ON outbox FOR EACH ROW
			WHEN (NEW.sent_at IS NOT NULL) EXECUTE FUNCTION test_reject_ack()`)
		drop := func() {
			mustExec(t, ctx, pool, `DROP TRIGGER test_reject_ack ON outbox; DROP FUNCTION test_reject_ack()`)
		}
		// Make cleanup work even if an assertion fails before the explicit drop.
		defer func() {
			mustExec(t, ctx, pool, `DROP TRIGGER IF EXISTS test_reject_ack ON outbox; DROP FUNCTION IF EXISTS test_reject_ack()`)
		}()
		if err := a.deliver(ctx, m); err == nil {
			t.Fatal("fault did not reach the acknowledgment write")
		}
		var sent bool
		if err := pool.QueryRow(ctx, `SELECT sent_at IS NOT NULL FROM outbox WHERE id = $1`, m.id).Scan(&sent); err != nil {
			t.Fatal(err)
		}
		if sent || receiver.starts.Load() != 1 {
			t.Fatal("receiver must have committed while the sender remains unacknowledged")
		}
		drop()
		b := NewRelay(pool, zap.NewNop(), receiver, nil)
		if err := b.Run(ctx); err != nil {
			t.Fatal(err)
		}
		if receiver.cancels.Load() != 0 {
			t.Fatal("cancel overtook its unacknowledged start")
		}
		expire(t, m)
		if err := b.Run(ctx); err != nil {
			t.Fatal(err)
		} // replay start, checkpoint it
		if receiver.starts.Load() != 2 || receiver.cancels.Load() != 0 {
			t.Fatal("expected a replay of start alone")
		}
		if err := b.Run(ctx); err != nil {
			t.Fatal(err)
		} // cancel can now follow
		var count int
		var cancelled bool
		if err := pool.QueryRow(ctx, `SELECT count(*), bool_and(cancelled) FROM test_matching_receipts WHERE ride_id = $1`, rideID).Scan(&count, &cancelled); err != nil {
			t.Fatal(err)
		}
		if count != 1 || !cancelled || receiver.cancels.Load() != 1 {
			t.Fatal("at-least-once delivery changed the receiver's result more than once or lost cancellation")
		}
		assertOutboxDelivered(t, ctx, pool, m.id)
	})

	for _, outcome := range []codes.Code{codes.Unavailable, codes.InvalidArgument, codes.OK} {
		t.Run("late old worker "+outcome.String(), func(t *testing.T) {
			a, receiver, _ := setup(t, false)
			old := claimOne(t, a)
			expire(t, old)
			b := NewRelay(pool, zap.NewNop(), receiver, nil)
			current := claimOne(t, b)
			var deadline time.Time
			if err := pool.QueryRow(ctx, `SELECT next_attempt_at FROM outbox WHERE id = $1`, current.id).Scan(&deadline); err != nil {
				t.Fatal(err)
			}
			a.matching = matchingAnswer{outcome: outcome}
			if err := a.deliver(ctx, old); err != nil {
				t.Fatal(err)
			}
			var untouched bool
			if err := pool.QueryRow(ctx, `SELECT sent_at IS NULL AND dead_at IS NULL AND last_error IS NULL AND next_attempt_at = $2 FROM outbox WHERE id = $1`, current.id, deadline).Scan(&untouched); err != nil {
				t.Fatal(err)
			}
			if !untouched {
				t.Fatalf("expired worker's %s result overwrote the replacement worker's lease", outcome)
			}
			if err := b.deliver(ctx, current); err != nil {
				t.Fatal(err)
			}
			assertOutboxDelivered(t, ctx, pool, current.id)
		})
	}
}

func mustExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}

func assertOutboxDelivered(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id int64) {
	t.Helper()
	var clean bool
	if err := pool.QueryRow(ctx, `SELECT sent_at IS NOT NULL AND dead_at IS NULL AND last_error IS NULL FROM outbox WHERE id = $1`, id).Scan(&clean); err != nil {
		t.Fatal(err)
	}
	if !clean {
		t.Fatal("delivered message has an inconsistent checkpoint")
	}
}

// The receiver has its own durable receipt. A duplicate RPC is observable, but never creates a second request.
type matchingReceipts struct {
	matchingv1.MatchingServiceClient
	pool    *pgxpool.Pool
	starts  atomic.Int32
	cancels atomic.Int32
}

func (r *matchingReceipts) StartMatching(ctx context.Context, req *matchingv1.StartMatchingRequest, _ ...grpc.CallOption) (*matchingv1.StartMatchingResponse, error) {
	r.starts.Add(1)
	_, err := r.pool.Exec(ctx, `INSERT INTO test_matching_receipts(ride_id) VALUES ($1) ON CONFLICT DO NOTHING`, req.GetRideId())
	return &matchingv1.StartMatchingResponse{}, err
}
func (r *matchingReceipts) CancelMatching(ctx context.Context, req *matchingv1.CancelMatchingRequest, _ ...grpc.CallOption) (*matchingv1.CancelMatchingResponse, error) {
	r.cancels.Add(1)
	tag, err := r.pool.Exec(ctx, `UPDATE test_matching_receipts SET cancelled = true WHERE ride_id = $1`, req.GetRideId())
	if err == nil && tag.RowsAffected() != 1 {
		err = errors.New("cancel arrived before start")
	}
	return &matchingv1.CancelMatchingResponse{}, err
}

type matchingAnswer struct {
	matchingv1.MatchingServiceClient
	outcome codes.Code
}

func (r matchingAnswer) StartMatching(context.Context, *matchingv1.StartMatchingRequest, ...grpc.CallOption) (*matchingv1.StartMatchingResponse, error) {
	if r.outcome != codes.OK {
		return nil, status.Error(r.outcome, fmt.Sprintf("old worker result: %s", r.outcome))
	}
	return &matchingv1.StartMatchingResponse{}, nil
}
