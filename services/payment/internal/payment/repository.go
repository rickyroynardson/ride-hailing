package payment

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Ledger accounts. Money in sits with the provider until paid out; a driver's account holds what they are owed.
const (
	accountProvider = "provider_clearing"
	accountPlatform = "platform_revenue"
	accountDriver   = "driver:"
)

// commissionPercent is the platform's share of every charge.
const commissionPercent = 20

// split divides a charge between platform and driver. Rounding favours the driver, and nothing is lost.
func split(amount int64) (platform, driver int64) {
	platform = amount/100*commissionPercent + amount%100*commissionPercent/100
	return platform, amount - platform
}

// refundShares divides a refund between platform and driver, in proportion to the capture. It works from the running
// total, so rounding can't drift: however many parts a charge is refunded in, the shares add up to exactly what the
// capture took. refunded is what had already been refunded.
func refundShares(refunded, amount int64) (platform, driver int64) {
	before, _ := split(refunded)
	after, _ := split(refunded + amount)
	return after - before, amount - (after - before)
}

type PaymentRepository struct {
	DB *pgxpool.Pool
}

func NewRepository(dbpool *pgxpool.Pool) *PaymentRepository {
	return &PaymentRepository{DB: dbpool}
}

// CreateCharge records one charge per ride. A conflicting duplicate is an error, not a silent success. The INSERT
// waits for any racing insert, then a separate READ COMMITTED statement sees and compares the committed winner.
func (r *PaymentRepository) CreateCharge(ctx context.Context, in ChargeInput, traceParent string) error {
	tag, err := r.DB.Exec(ctx,
		`INSERT INTO charges(ride_id, rider_id, driver_id, kind, amount, currency, trace_parent)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, '')) ON CONFLICT (ride_id) DO NOTHING`,
		in.RideID, in.RiderID, in.DriverID, in.Kind, in.Amount.Amount, in.Amount.Currency, traceParent,
	)
	if err != nil || tag.RowsAffected() == 1 {
		return err
	}
	var same bool
	err = r.DB.QueryRow(ctx, `SELECT rider_id = $2::uuid AND driver_id = $3::uuid AND kind = $4 AND amount = $5 AND currency = $6
		FROM charges WHERE ride_id = $1`, in.RideID, in.RiderID, in.DriverID, in.Kind, in.Amount.Amount, in.Amount.Currency).Scan(&same)
	if err == nil && !same {
		return ErrKeyReused
	}
	return err
}

// dueCharge is a claimed charge, ready to send to the provider.
type dueCharge struct {
	ID              string
	RideID          string
	Amount          Money
	ProviderAttempt int
	Attempts        int
	Token           string
	TraceParent     string
}

// ClaimDue leases pending charges that are due, so concurrent workers split them. A worker that dies leaves the charge
// to come due again when the lease ends; the provider's idempotency key makes the repeat safe.
func (r *PaymentRepository) ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]dueCharge, error) {
	rows, err := r.DB.Query(ctx,
		`WITH due AS (
			SELECT id FROM charges WHERE status = 'pending' AND next_attempt_at <= now()
			ORDER BY next_attempt_at LIMIT $1 FOR UPDATE SKIP LOCKED
		), claimed AS (
			UPDATE charges c SET next_attempt_at = now() + make_interval(secs => $2), attempts = c.attempts + 1,
			provider_token = COALESCE(c.provider_token, (SELECT token FROM payment_methods WHERE rider_id = c.rider_id), '')
			FROM due WHERE c.id = due.id
			RETURNING c.id, c.ride_id, c.amount, c.currency, c.provider_attempt, c.attempts, c.trace_parent, c.provider_token
		)
		SELECT c.id, c.ride_id, c.amount, c.currency, c.provider_attempt, c.attempts, c.provider_token, COALESCE(c.trace_parent, '')
		FROM claimed c`,
		limit, lease.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (dueCharge, error) {
		var c dueCharge
		err := row.Scan(&c.ID, &c.RideID, &c.Amount.Amount, &c.Amount.Currency, &c.ProviderAttempt, &c.Attempts, &c.Token, &c.TraceParent)
		return c, err
	})
}

// Capture marks a pending charge paid and posts it to the ledger, atomically: money the provider took is either in
// the ledger or still pending, which the next attempt repeats. The status guard and the ledger's uniqueness keep a
// second worker from posting it twice. (provider_attempt, attempts) fences the claim: an old worker may still get a
// provider response, but cannot checkpoint a newer claim, even within the same logical provider attempt.
func (r *PaymentRepository) Capture(ctx context.Context, chargeID, providerRef string, attempt, claim int) error {
	return pgx.BeginFunc(ctx, r.DB, func(tx pgx.Tx) error {
		var driverID, currency string
		var amount int64
		err := tx.QueryRow(ctx,
			`UPDATE charges SET status = 'captured', provider_ref = $2, last_error = NULL, updated_at = now()
			WHERE id = $1 AND status = 'pending' AND provider_attempt = $3 AND attempts = $4 RETURNING driver_id, amount, currency`,
			chargeID, providerRef, attempt, claim,
		).Scan(&driverID, &amount, &currency)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}

		platform, driver := split(amount)
		return post(ctx, tx, chargeID, nil, currency, []entry{
			{accountProvider, amount},
			{accountPlatform, -platform},
			{accountDriver + driverID, -driver},
		})
	})
}

type entry struct {
	account string
	amount  int64
}

// post writes one posting to the ledger: the capture of a charge (refundID nil), or one refund of it.
func post(ctx context.Context, tx pgx.Tx, chargeID string, refundID *string, currency string, entries []entry) error {
	for _, e := range entries {
		if e.amount == 0 {
			continue // the platform's share of a tiny amount can round to nothing
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO ledger_entries(charge_id, refund_id, account, amount, currency) VALUES ($1, $2, $3, $4, $5)`,
			chargeID, refundID, e.account, e.amount, currency,
		); err != nil {
			return err
		}
	}
	return nil
}

// Decline ends a pending charge as failed: the provider gave a definite no, so no money moved.
func (r *PaymentRepository) Decline(ctx context.Context, chargeID, reason string, attempt, claim int) error {
	_, err := r.DB.Exec(ctx,
		`UPDATE charges SET status = 'failed', decline_reason = $2, last_error = NULL, updated_at = now()
		WHERE id = $1 AND status = 'pending' AND provider_attempt = $3 AND attempts = $4`,
		chargeID, reason, attempt, claim,
	)
	return err
}

// Postpone leaves a charge pending after an unknown outcome, to try again with the same key after wait.
func (r *PaymentRepository) Postpone(ctx context.Context, chargeID, lastError string, wait time.Duration, attempt, claim int) error {
	_, err := r.DB.Exec(ctx,
		`UPDATE charges SET next_attempt_at = now() + make_interval(secs => $3), last_error = $2, updated_at = now()
		WHERE id = $1 AND status = 'pending' AND provider_attempt = $4 AND attempts = $5`,
		chargeID, lastError, wait.Seconds(), attempt, claim,
	)
	return err
}

// outcome is a finished charge that ride hasn't confirmed hearing about yet.
type outcome struct {
	ID          string
	RideID      string
	Status      string
	TraceParent string
}

// UnnotifiedOutcomes lists finished charges ride hasn't confirmed; the charge row works as payment's outbox.
func (r *PaymentRepository) UnnotifiedOutcomes(ctx context.Context, limit int) ([]outcome, error) {
	rows, err := r.DB.Query(ctx,
		`SELECT id, ride_id, status, COALESCE(trace_parent, '') FROM charges
		WHERE status <> 'pending' AND ride_notified_at IS NULL ORDER BY created_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (outcome, error) {
		var o outcome
		err := row.Scan(&o.ID, &o.RideID, &o.Status, &o.TraceParent)
		return o, err
	})
}

// MarkNotified records that ride has the outcome. The status guard keeps a charge retried meanwhile unnotified, so its
// new outcome still gets sent.
func (r *PaymentRepository) MarkNotified(ctx context.Context, chargeID, status string) error {
	_, err := r.DB.Exec(ctx, `UPDATE charges SET ride_notified_at = now() WHERE id = $1 AND status = $2`, chargeID, status)
	return err
}

// GetCharge returns the ride's charge if userID is its rider or driver. Anyone else gets ErrChargeNotFound.
func (r *PaymentRepository) GetCharge(ctx context.Context, rideID, userID string) (Charge, error) {
	c := Charge{RideID: rideID}
	err := r.DB.QueryRow(ctx,
		`SELECT id, kind, status, amount, currency, COALESCE(decline_reason, '') FROM charges
		WHERE ride_id = $1 AND (rider_id = $2 OR driver_id = $2)`,
		rideID, userID,
	).Scan(&c.ID, &c.Kind, &c.Status, &c.Amount.Amount, &c.Amount.Currency, &c.DeclineReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return Charge{}, ErrChargeNotFound
	}
	return c, err
}

// RetryCharge puts the rider's declined charge back in the queue, as a new attempt at the provider. A charge that
// isn't declined is left as it is.
func (r *PaymentRepository) RetryCharge(ctx context.Context, rideID, riderID, requestID string) error {
	return pgx.BeginFunc(ctx, r.DB, func(tx pgx.Tx) error {
		var id string
		if err := tx.QueryRow(ctx, `SELECT id FROM charges WHERE ride_id = $1 AND rider_id = $2 FOR UPDATE`, rideID, riderID).Scan(&id); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrChargeNotFound
			}
			return err
		}
		tag, err := tx.Exec(ctx, `INSERT INTO charge_retry_requests(charge_id, request_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, id, requestID)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		_, err = tx.Exec(ctx, `UPDATE charges SET status = 'pending', decline_reason = NULL, provider_attempt = provider_attempt + 1,
			provider_token = NULL, attempts = 0, next_attempt_at = now(), ride_notified_at = NULL, updated_at = now()
			WHERE id = $1 AND status = 'failed'`, id)
		return err
	})
}

func (r *PaymentRepository) SetPaymentMethod(ctx context.Context, riderID, token string) error {
	_, err := r.DB.Exec(ctx,
		`INSERT INTO payment_methods(rider_id, token) VALUES ($1, $2)
		ON CONFLICT (rider_id) DO UPDATE SET token = EXCLUDED.token, updated_at = now()`,
		riderID, token,
	)
	return err
}

// Earnings sums the driver's account per currency. Credits are negative, so what they are owed is the sum negated.
func (r *PaymentRepository) Earnings(ctx context.Context, driverID string) ([]Money, error) {
	rows, err := r.DB.Query(ctx,
		`SELECT -sum(amount)::bigint, currency FROM ledger_entries WHERE account = $1 GROUP BY currency ORDER BY currency`,
		accountDriver+driverID)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Money, error) {
		var m Money
		err := row.Scan(&m.Amount, &m.Currency)
		return m, err
	})
}
