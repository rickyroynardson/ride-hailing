package ride

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RideRepository struct {
	DB *pgxpool.Pool
}

func NewRepository(dbpool *pgxpool.Pool) *RideRepository {
	return &RideRepository{DB: dbpool}
}

// CreateRide relies on rides_one_active_per_rider, so concurrent requests from one rider can't both succeed.
func (r *RideRepository) CreateRide(ctx context.Context, in CreateRideInput) (string, error) {
	var id string

	err := r.DB.QueryRow(ctx,
		`INSERT INTO rides(rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng) VALUES ($1, $2, $3, $4, $5)
		RETURNING id`,
		in.RiderID, in.Pickup.Lat, in.Pickup.Lng, in.Dropoff.Lat, in.Dropoff.Lng,
	).Scan(&id)

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
