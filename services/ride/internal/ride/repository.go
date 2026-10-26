package ride

import (
	"context"
	"errors"

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
// rides_one_active_per_rider makes concurrent requests from one rider fail with ErrActiveRide.
func (r *RideRepository) CreateRide(ctx context.Context, in CreateRideInput) (string, error) {
	var id string
	err := pgx.BeginFunc(ctx, r.DB, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`INSERT INTO rides(rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng) VALUES ($1, $2, $3, $4, $5)
			RETURNING id`,
			in.RiderID, in.Pickup.Lat, in.Pickup.Lng, in.Dropoff.Lat, in.Dropoff.Lng,
		).Scan(&id); err != nil {
			return err
		}
		return outbox.Add(ctx, tx, outbox.StartMatching{RideID: id, Pickup: outbox.Location{Lat: in.Pickup.Lat, Lng: in.Pickup.Lng}})
	})

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return "", ErrActiveRide
	}
	return id, err
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
		`UPDATE rides SET driver_id = $2, status = 'accepted', updated_at = now() WHERE id = $1 AND status = 'requested'`,
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
// matching to stop. Cancelling an already cancelled ride succeeds without queueing a second message.
func (r *RideRepository) CancelRide(ctx context.Context, rideID, riderID string) error {
	var cancelled bool
	err := pgx.BeginFunc(ctx, r.DB, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE rides SET status = 'cancelled', cancel_reason = 'rider', updated_at = now()
			WHERE id = $1 AND rider_id = $2 AND status IN ('requested', 'accepted')`,
			rideID, riderID,
		)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		cancelled = true
		return outbox.Add(ctx, tx, outbox.CancelMatching{RideID: rideID})
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
