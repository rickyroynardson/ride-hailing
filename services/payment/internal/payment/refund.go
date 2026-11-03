package payment

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/provider"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
)

var refundsFinished = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "payment_refunds_finished_total", Help: "Refunds that reached an outcome, by outcome.",
}, []string{"outcome"})

// RefundCharge records a refund and returns; the provider is called from Tick, like a charge. See
// PaymentRepository.CreateRefund for what makes it safe to repeat and to race.
func (s *PaymentService) RefundCharge(ctx context.Context, in RefundInput) (Refund, error) {
	if err := in.Validate(); err != nil {
		return Refund{}, err
	}
	return s.repository.CreateRefund(ctx, in, telemetry.TraceParent(ctx))
}

func (s *PaymentService) processDueRefunds(ctx context.Context) error {
	refunds, err := s.repository.ClaimDueRefunds(ctx, tickBatch, claimLease)
	if err != nil {
		return fmt.Errorf("claim refunds: %w", err)
	}
	errs := make([]error, len(refunds))
	var wg sync.WaitGroup
	for i, r := range refunds {
		wg.Go(func() { errs[i] = s.processRefund(ctx, r) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

// processRefund makes one attempt at a refund, with the same three outcomes as a charge. The key is the refund's ID, so
// a repeat after an unknown outcome can't pay out twice; a refund that failed for certain stays failed, and trying again
// is a new refund with a new key.
func (s *PaymentService) processRefund(ctx context.Context, r dueRefund) (err error) {
	ctx, end := telemetry.StartConsumer(ctx, "refund charge", r.TraceParent,
		attribute.String("refund.id", r.ID), attribute.String("ride.id", r.RideID), attribute.Int("refund.attempt", r.Attempts))
	defer func() { end(err) }()

	callCtx, cancel := context.WithTimeout(ctx, providerTimeout)
	res, err := s.provider.Refund(callCtx, provider.RefundRequest{
		Key:             r.ID,
		ChargeReference: r.ChargeReference,
		Amount:          r.Amount.Amount,
		Currency:        r.Amount.Currency,
	})
	cancel()
	switch {
	case err != nil:
		providerUnknown.Inc()
		s.lg.Warn("provider outcome unknown", zap.String("refund_id", r.ID), zap.Int("attempts", r.Attempts), zap.Error(err))
		return s.repository.PostponeRefund(ctx, r.ID, err.Error(), backoff(r.Attempts))
	case res.FailureReason != "":
		refundsFinished.WithLabelValues(RefundFailed).Inc()
		return s.repository.FailRefund(ctx, r.ID, res.FailureReason)
	default:
		refundsFinished.WithLabelValues(RefundSucceeded).Inc()
		return s.repository.CompleteRefund(ctx, r.ID, res.Reference)
	}
}

// CreateRefund saves a refund, holding the charge's row lock for the whole check. That lock is what keeps two refunds
// racing each other from together exceeding the charge: each sees what the other reserved. Pending refunds count, since
// their money may already have left; failed ones don't. A repeat of the same key returns the first refund, whatever has
// happened since, and the same key on a different refund is refused.
func (r *PaymentRepository) CreateRefund(ctx context.Context, in RefundInput, traceParent string) (Refund, error) {
	var res Refund
	err := pgx.BeginFunc(ctx, r.DB, func(tx pgx.Tx) error {
		var chargeID, status, currency string
		var charged int64
		err := tx.QueryRow(ctx,
			`SELECT id, status, amount, currency FROM charges WHERE ride_id = $1 FOR UPDATE`, in.RideID,
		).Scan(&chargeID, &status, &charged, &currency)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrChargeNotFound
		}
		if err != nil {
			return err
		}

		var firstCharge string
		res.Amount.Currency = currency
		err = tx.QueryRow(ctx,
			`SELECT id, charge_id, amount, status, reason, COALESCE(failure_reason, '') FROM refunds WHERE idempotency_key = $1`,
			in.IdempotencyKey,
		).Scan(&res.ID, &firstCharge, &res.Amount.Amount, &res.Status, &res.Reason, &res.FailureReason)
		if err == nil {
			if firstCharge != chargeID || res.Amount.Amount != in.Amount {
				return ErrKeyReused
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		if status != StatusCaptured {
			return ErrNotRefundable
		}
		var reserved int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(sum(amount), 0)::bigint FROM refunds WHERE charge_id = $1 AND status <> 'failed'`, chargeID,
		).Scan(&reserved); err != nil {
			return err
		}
		if reserved+in.Amount > charged {
			return ErrRefundTooLarge
		}

		res.Amount.Amount, res.Reason, res.Status = in.Amount, in.Reason, RefundPending
		return tx.QueryRow(ctx,
			`INSERT INTO refunds(charge_id, amount, reason, idempotency_key, trace_parent)
			VALUES ($1, $2, $3, $4, NULLIF($5, '')) RETURNING id`,
			chargeID, in.Amount, in.Reason, in.IdempotencyKey, traceParent,
		).Scan(&res.ID)
	})

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		// The same key racing on another charge: this row lock doesn't cover that, the unique index does.
		return Refund{}, ErrKeyReused
	}
	if err != nil {
		return Refund{}, err
	}
	return res, nil
}

// dueRefund is a claimed refund, ready to send to the provider.
type dueRefund struct {
	ID              string
	RideID          string
	Amount          Money
	ChargeReference string
	Attempts        int
	TraceParent     string
}

// ClaimDueRefunds leases pending refunds that are due, the way ClaimDue does charges.
func (r *PaymentRepository) ClaimDueRefunds(ctx context.Context, limit int, lease time.Duration) ([]dueRefund, error) {
	rows, err := r.DB.Query(ctx,
		`WITH due AS (
			SELECT id FROM refunds WHERE status = 'pending' AND next_attempt_at <= now()
			ORDER BY next_attempt_at LIMIT $1 FOR UPDATE SKIP LOCKED
		), claimed AS (
			UPDATE refunds f SET next_attempt_at = now() + make_interval(secs => $2), attempts = f.attempts + 1
			FROM due WHERE f.id = due.id
			RETURNING f.id, f.charge_id, f.amount, f.attempts, f.trace_parent
		)
		SELECT f.id, c.ride_id, f.amount, c.currency, c.provider_ref, f.attempts, COALESCE(f.trace_parent, '')
		FROM claimed f JOIN charges c ON c.id = f.charge_id`,
		limit, lease.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (dueRefund, error) {
		var d dueRefund
		err := row.Scan(&d.ID, &d.RideID, &d.Amount.Amount, &d.Amount.Currency, &d.ChargeReference, &d.Attempts, &d.TraceParent)
		return d, err
	})
}

// CompleteRefund marks a pending refund succeeded and reverses its share of the charge in the ledger, atomically. The
// driver's share comes back too, so their balance can go negative if they were already paid. The charge's row lock
// keeps refunds finishing at once in order, since each one's shares depend on the total before it.
func (r *PaymentRepository) CompleteRefund(ctx context.Context, refundID, providerRef string) error {
	return pgx.BeginFunc(ctx, r.DB, func(tx pgx.Tx) error {
		var chargeID string
		if err := tx.QueryRow(ctx, `SELECT charge_id FROM refunds WHERE id = $1`, refundID).Scan(&chargeID); err != nil {
			return err
		}
		var driverID, currency string
		if err := tx.QueryRow(ctx, `SELECT driver_id, currency FROM charges WHERE id = $1 FOR UPDATE`, chargeID).Scan(&driverID, &currency); err != nil {
			return err
		}
		var refunded int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(sum(amount), 0)::bigint FROM refunds WHERE charge_id = $1 AND status = 'succeeded'`, chargeID,
		).Scan(&refunded); err != nil {
			return err
		}

		var amount int64
		err := tx.QueryRow(ctx,
			`UPDATE refunds SET status = 'succeeded', provider_ref = $2, last_error = NULL, updated_at = now()
			WHERE id = $1 AND status = 'pending' RETURNING amount`,
			refundID, providerRef,
		).Scan(&amount)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}

		platform, driver := refundShares(refunded, amount)
		return post(ctx, tx, chargeID, &refundID, currency, []entry{
			{accountProvider, -amount},
			{accountPlatform, platform},
			{accountDriver + driverID, driver},
		})
	})
}

// FailRefund ends a pending refund as failed: the provider gave a definite no, so no money moved, and the amount it
// reserved is free again.
func (r *PaymentRepository) FailRefund(ctx context.Context, refundID, reason string) error {
	_, err := r.DB.Exec(ctx,
		`UPDATE refunds SET status = 'failed', failure_reason = $2, last_error = NULL, updated_at = now()
		WHERE id = $1 AND status = 'pending'`,
		refundID, reason,
	)
	return err
}

// PostponeRefund leaves a refund pending after an unknown outcome, to try again with the same key after wait.
func (r *PaymentRepository) PostponeRefund(ctx context.Context, refundID, lastError string, wait time.Duration) error {
	_, err := r.DB.Exec(ctx,
		`UPDATE refunds SET next_attempt_at = now() + make_interval(secs => $3), last_error = $2, updated_at = now()
		WHERE id = $1 AND status = 'pending'`,
		refundID, lastError, wait.Seconds(),
	)
	return err
}

// Refunds lists a charge's refunds, oldest first.
func (r *PaymentRepository) Refunds(ctx context.Context, chargeID string) ([]Refund, error) {
	rows, err := r.DB.Query(ctx,
		`SELECT f.id, f.amount, c.currency, f.status, f.reason, COALESCE(f.failure_reason, '')
		FROM refunds f JOIN charges c ON c.id = f.charge_id WHERE f.charge_id = $1 ORDER BY f.created_at`, chargeID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Refund, error) {
		var x Refund
		err := row.Scan(&x.ID, &x.Amount.Amount, &x.Amount.Currency, &x.Status, &x.Reason, &x.FailureReason)
		return x, err
	})
}
