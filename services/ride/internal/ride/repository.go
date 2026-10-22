package ride

import (
	"context"
	"errors"

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
