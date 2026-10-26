package matching

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MatchingRepository struct {
	DB *pgxpool.Pool
}

func NewRepository(dbpool *pgxpool.Pool) *MatchingRepository {
	return &MatchingRepository{DB: dbpool}
}

// PendingOffer returns the ride's pending offer, or nil if it has none.
func (r *MatchingRepository) PendingOffer(ctx context.Context, rideID string) (*Offer, error) {
	var o Offer
	err := r.DB.QueryRow(ctx,
		`SELECT id, ride_id, driver_id, expires_at FROM offers WHERE ride_id = $1 AND status = 'pending'`,
		rideID,
	).Scan(&o.ID, &o.RideID, &o.DriverID, &o.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}

// ExcludedDrivers returns which of driverIDs can't get this ride: already offered it (whatever they answered),
// or holding a pending offer for another ride.
func (r *MatchingRepository) ExcludedDrivers(ctx context.Context, rideID string, driverIDs []string) (map[string]bool, error) {
	rows, err := r.DB.Query(ctx,
		`SELECT driver_id FROM offers WHERE driver_id = ANY($2::uuid[]) AND (ride_id = $1 OR status = 'pending')`,
		rideID, driverIDs,
	)
	if err != nil {
		return nil, err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}

	excluded := make(map[string]bool, len(ids))
	for _, id := range ids {
		excluded[id] = true
	}
	return excluded, nil
}

// CreateOffer relies on the offers unique indexes, so concurrent StartMatching calls can't create two pending
// offers for one ride, or give one driver two pending offers.
func (r *MatchingRepository) CreateOffer(ctx context.Context, rideID, driverID string, expiresAt time.Time) (Offer, error) {
	o := Offer{RideID: rideID, DriverID: driverID, ExpiresAt: expiresAt}
	err := r.DB.QueryRow(ctx,
		`INSERT INTO offers(ride_id, driver_id, expires_at) VALUES ($1, $2, $3) RETURNING id`,
		rideID, driverID, expiresAt,
	).Scan(&o.ID)

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		if pgErr.ConstraintName == "idx_offers_one_pending_per_ride" {
			return Offer{}, errRideHasOffer
		}
		return Offer{}, errDriverTaken
	}
	return o, err
}
