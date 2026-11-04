package ride

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickyroynardson/ride-hailing/services/ride/internal/outbox"
)

type RideRepository struct {
	DB *pgxpool.Pool
}

func NewRepository(dbpool *pgxpool.Pool) *RideRepository {
	return &RideRepository{DB: dbpool}
}

// CreateRide inserts the ride and its "start matching" outbox message in one transaction: both are saved or
// neither is, so a ride can never exist without matching eventually hearing about it.
// A transaction-scoped lock serializes bookings for this rider, including different keys. Replay is checked before
// active-ride and payment checks, and returns the stored fare, never a newly computed one. The unique indexes remain
// the database's final guard. An outbox failure rolls back the ride and key together, so retrying is safe.
func (r *RideRepository) CreateRide(ctx context.Context, in CreateRideInput, fare Money) (string, Money, error) {
	var id string
	err := pgx.BeginFunc(ctx, r.DB, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1::uuid::text, 0))`, in.RiderID); err != nil {
			return err
		}
		var pickup, dropoff Location
		err := tx.QueryRow(ctx, `SELECT id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, fare_amount, fare_currency
			FROM rides WHERE rider_id = $1 AND request_id = $2`, in.RiderID, in.RequestID).
			Scan(&id, &pickup.Lat, &pickup.Lng, &dropoff.Lat, &dropoff.Lng, &fare.Amount, &fare.Currency)
		if err == nil {
			if pickup != *in.Pickup || dropoff != *in.Dropoff {
				return ErrRequestReused
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var overdue bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM rides WHERE rider_id = $1 AND payment_status = 'failed')`, in.RiderID).Scan(&overdue); err != nil {
			return err
		}
		if overdue {
			return ErrPaymentOverdue
		}
		if err := tx.QueryRow(ctx,
			`INSERT INTO rides(rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, fare_amount, fare_currency, request_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
			in.RiderID, in.Pickup.Lat, in.Pickup.Lng, in.Dropoff.Lat, in.Dropoff.Lng, fare.Amount, fare.Currency, in.RequestID,
		).Scan(&id); err != nil {
			return err
		}
		return outbox.Add(ctx, tx, outbox.StartMatching{RideID: id, Pickup: outbox.Location{Lat: in.Pickup.Lat, Lng: in.Pickup.Lng}})
	})

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return "", Money{}, ErrActiveRide
	}
	return id, fare, err
}

// GetCurrentRide checks both participant roles, without relying on the token's driver claim: a driver suspended
// mid-trip still needs to recover and complete it. Prefer the driver trip if an account occupies both roles.
func (r *RideRepository) GetCurrentRide(ctx context.Context, userID string) (*Ride, error) {
	x := Ride{}
	err := r.DB.QueryRow(ctx, `SELECT id, status, COALESCE(cancel_reason, ''), COALESCE(driver_id::text, ''),
		pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, created_at, accepted_at, started_at, completed_at,
		fare_amount, fare_currency, COALESCE(request_id::text, '') FROM rides
		WHERE (rider_id = $1 AND status IN ('requested', 'accepted', 'in_progress'))
		OR (driver_id = $1 AND status IN ('accepted', 'in_progress'))
		ORDER BY (driver_id = $1) DESC NULLS LAST, created_at DESC LIMIT 1`, userID).
		Scan(&x.ID, &x.Status, &x.CancelReason, &x.DriverID, &x.Pickup.Lat, &x.Pickup.Lng, &x.Dropoff.Lat, &x.Dropoff.Lng,
			&x.RequestedAt, &x.AcceptedAt, &x.StartedAt, &x.CompletedAt, &x.FareAmount, &x.FareCurrency, &x.RequestID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &x, nil
}

// GetRide returns the ride if userID is its rider or driver. Anyone else gets ErrRideNotFound.
func (r *RideRepository) GetRide(ctx context.Context, rideID, userID string) (Ride, error) {
	x := Ride{ID: rideID}
	err := r.DB.QueryRow(ctx,
		`SELECT status, COALESCE(cancel_reason, ''), COALESCE(driver_id::text, ''),
		pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, created_at, accepted_at, started_at, completed_at,
		fare_amount, fare_currency, COALESCE(request_id::text, '')
		FROM rides WHERE id = $1 AND (rider_id = $2 OR driver_id = $2)`,
		rideID, userID,
	).Scan(&x.Status, &x.CancelReason, &x.DriverID,
		&x.Pickup.Lat, &x.Pickup.Lng, &x.Dropoff.Lat, &x.Dropoff.Lng, &x.RequestedAt, &x.AcceptedAt, &x.StartedAt, &x.CompletedAt,
		&x.FareAmount, &x.FareCurrency, &x.RequestID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Ride{}, ErrRideNotFound
	}
	return x, err
}

// BusyDrivers returns which of driverIDs are on an accepted or in-progress ride. The partial unique index
// rides_one_active_per_driver covers exactly these rows, so the lookup uses it.
func (r *RideRepository) BusyDrivers(ctx context.Context, driverIDs []string) (map[string]bool, error) {
	rows, err := r.DB.Query(ctx,
		`SELECT driver_id FROM rides WHERE driver_id = ANY($1::uuid[]) AND status IN ('accepted', 'in_progress')`,
		driverIDs,
	)
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}

	busy := make(map[string]bool, len(ids))
	for _, id := range ids {
		busy[id] = true
	}
	return busy, nil
}

// AssignDriver moves a requested ride to accepted with this driver. It is idempotent: if the ride is already
// accepted or in progress with the same driver, it succeeds without changing anything, so matching can retry
// after a timeout without knowing whether its first call landed.
func (r *RideRepository) AssignDriver(ctx context.Context, rideID, driverID string) error {
	tag, err := r.DB.Exec(ctx,
		`UPDATE rides SET driver_id = $2, status = 'accepted', accepted_at = now(), updated_at = now() WHERE id = $1 AND status = 'requested'`,
		rideID, driverID,
	)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		// rides_one_active_per_driver: this driver is on another ride.
		return ErrDriverBusy
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	var status, current string
	err = r.DB.QueryRow(ctx, `SELECT status, COALESCE(driver_id::text, '') FROM rides WHERE id = $1`, rideID).Scan(&status, &current)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrRideUnavailable
	}
	if err != nil {
		return err
	}
	if current == driverID && (status == "accepted" || status == "in_progress") {
		return nil
	}
	return ErrRideUnavailable
}

// MarkNoDriver cancels a requested ride because matching found no driver. An already cancelled ride is left as
// it is (for example the rider cancelled first) and counts as success, so matching can retry safely.
func (r *RideRepository) MarkNoDriver(ctx context.Context, rideID string) error {
	tag, err := r.DB.Exec(ctx,
		`UPDATE rides SET status = 'cancelled', cancel_reason = 'no_driver', updated_at = now() WHERE id = $1 AND status = 'requested'`,
		rideID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	var status string
	err = r.DB.QueryRow(ctx, `SELECT status FROM rides WHERE id = $1`, rideID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && status != "cancelled") {
		return ErrRideUnavailable
	}
	return err
}

// CancelRide cancels the rider's ride before the trip starts, and in the same transaction queues a message telling
// matching to stop. If a driver had already accepted, it also queues the cancellation fee. Cancelling an already
// cancelled ride succeeds without queueing a second message.
func (r *RideRepository) CancelRide(ctx context.Context, rideID, riderID string) error {
	var cancelled bool
	err := pgx.BeginFunc(ctx, r.DB, func(tx pgx.Tx) error {
		var driverID string
		var fare *int64
		var currency *string
		err := tx.QueryRow(ctx,
			`UPDATE rides SET status = 'cancelled', cancel_reason = 'rider', updated_at = now()
			WHERE id = $1 AND rider_id = $2 AND status IN ('requested', 'accepted')
			RETURNING COALESCE(driver_id::text, ''), fare_amount, fare_currency`,
			rideID, riderID,
		).Scan(&driverID, &fare, &currency)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		cancelled = true
		if err := outbox.Add(ctx, tx, outbox.CancelMatching{RideID: rideID}); err != nil {
			return err
		}
		if driverID == "" || fare == nil {
			return nil
		}
		return queueCharge(ctx, tx, outbox.ChargeRide{
			RideID: rideID, RiderID: riderID, DriverID: driverID,
			ChargeKind: outbox.ChargeCancellationFee, Amount: CancellationFee, Currency: *currency,
		})
	})
	if err != nil || cancelled {
		return err
	}

	var status string
	err = r.DB.QueryRow(ctx, `SELECT status FROM rides WHERE id = $1 AND rider_id = $2`, rideID, riderID).Scan(&status)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrRideNotFound
	case err != nil:
		return err
	case status == "cancelled":
		return nil
	default:
		return ErrRideUnavailable
	}
}

// queueCharge marks the ride as awaiting payment and queues the charge, in the transaction that finished the ride.
func queueCharge(ctx context.Context, tx pgx.Tx, m outbox.ChargeRide) error {
	if _, err := tx.Exec(ctx, `UPDATE rides SET payment_status = 'pending' WHERE id = $1`, m.RideID); err != nil {
		return err
	}
	return outbox.Add(ctx, tx, m)
}

// RecordPayment stores how the ride's charge ended. It is idempotent, and paid is final, so a delayed "failed" from
// before the rider settled up can't overwrite it.
func (r *RideRepository) RecordPayment(ctx context.Context, rideID, status string) error {
	tag, err := r.DB.Exec(ctx,
		`UPDATE rides SET payment_status = $2, updated_at = now() WHERE id = $1 AND payment_status IS DISTINCT FROM 'paid'`,
		rideID, status,
	)
	if err != nil || tag.RowsAffected() == 1 {
		return err
	}

	var exists bool
	if err := r.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM rides WHERE id = $1)`, rideID).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrRideNotFound
	}
	return nil
}

// tripStep is a transition the ride's driver makes: from one status to the next, stamping when it happened.
// done lists the statuses where the step has already happened, so a retried request succeeds. charge makes the step
// queue the ride's fare.
type tripStep struct {
	from, to string
	stampCol string // a constant below, never input: it is formatted into the SQL
	done     []string
	charge   bool
}

var (
	startTrip    = tripStep{from: "accepted", to: "in_progress", stampCol: "started_at", done: []string{"in_progress", "completed"}}
	completeTrip = tripStep{from: "in_progress", to: "completed", stampCol: "completed_at", done: []string{"completed"}, charge: true}
)

func (r *RideRepository) StartTrip(ctx context.Context, rideID, driverID string) error {
	return r.advanceTrip(ctx, rideID, driverID, startTrip)
}

func (r *RideRepository) CompleteTrip(ctx context.Context, rideID, driverID string) error {
	return r.advanceTrip(ctx, rideID, driverID, completeTrip)
}

// advanceTrip moves the driver's ride one step along the trip, guarded by the status it must be in. Another driver's
// ride reads as not found, like a rider's in CancelRide. A step that charges queues the fare in the same transaction,
// so a completed ride can never go unbilled; a ride from before pricing has no fare and is not charged.
func (r *RideRepository) advanceTrip(ctx context.Context, rideID, driverID string, step tripStep) error {
	var advanced bool
	err := pgx.BeginFunc(ctx, r.DB, func(tx pgx.Tx) error {
		var riderID string
		var fare *int64
		var currency *string
		err := tx.QueryRow(ctx,
			fmt.Sprintf(`UPDATE rides SET status = $3, %s = now(), updated_at = now()
			WHERE id = $1 AND driver_id = $2 AND status = $4
			RETURNING rider_id, fare_amount, fare_currency`, step.stampCol),
			rideID, driverID, step.to, step.from,
		).Scan(&riderID, &fare, &currency)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		advanced = true
		if !step.charge || fare == nil {
			return nil
		}
		return queueCharge(ctx, tx, outbox.ChargeRide{
			RideID: rideID, RiderID: riderID, DriverID: driverID,
			ChargeKind: outbox.ChargeFare, Amount: *fare, Currency: *currency,
		})
	})
	if err != nil || advanced {
		return err
	}

	var status string
	err = r.DB.QueryRow(ctx, `SELECT status FROM rides WHERE id = $1 AND driver_id = $2`, rideID, driverID).Scan(&status)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrRideNotFound
	case err != nil:
		return err
	case slices.Contains(step.done, status):
		return nil
	default:
		return ErrRideUnavailable
	}
}
