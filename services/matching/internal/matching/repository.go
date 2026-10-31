package matching

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
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

// CreateRequest records that the ride needs a driver. A retried StartMatching keeps the first request, so its
// deadline isn't pushed back. It returns the request's status.
func (r *MatchingRepository) CreateRequest(ctx context.Context, rideID string, pickup Location, deadline time.Time) (status string, err error) {
	// The request queues follow-up work (re-offers, confirming an accept, giving up) that Tick does later: a
	// PRODUCER span whose stored context makes that work its CONSUMER children in the same trace.
	ctx, end := telemetry.StartProducer(ctx, "match_request publish",
		attribute.String("messaging.system", "match_requests"), attribute.String("messaging.operation.type", "publish"),
		attribute.String("ride.id", rideID))
	defer func() { end(err) }()

	if _, err := r.DB.Exec(ctx,
		`INSERT INTO match_requests(ride_id, pickup_lat, pickup_lng, deadline_at, trace_parent) VALUES ($1, $2, $3, $4, NULLIF($5, ''))
		ON CONFLICT (ride_id) DO NOTHING`,
		rideID, pickup.Lat, pickup.Lng, deadline, telemetry.TraceParent(ctx),
	); err != nil {
		return "", err
	}

	err = r.DB.QueryRow(ctx, `SELECT status FROM match_requests WHERE ride_id = $1`, rideID).Scan(&status)
	return status, err
}

// ExpireOffers marks pending offers past their deadline as expired. One statement, so replicas running the loop
// at the same time can't both act on the same offer.
func (r *MatchingRepository) ExpireOffers(ctx context.Context) (int64, error) {
	tag, err := r.DB.Exec(ctx,
		`UPDATE offers SET status = 'expired', updated_at = now() WHERE status = 'pending' AND expires_at < now()`)
	return tag.RowsAffected(), err
}

// GiveUp ends requests past their deadline as no_driver. A request with a pending offer waits for that offer to be
// answered or expire first, and one with an accepted offer waits for ride to confirm it, so a ride is never given
// up under a driver who has it.
func (r *MatchingRepository) GiveUp(ctx context.Context) ([]string, error) {
	rows, err := r.DB.Query(ctx,
		`UPDATE match_requests m SET status = 'no_driver', updated_at = now()
		WHERE m.status = 'matching' AND m.deadline_at < now()
		AND NOT EXISTS (SELECT 1 FROM offers o WHERE o.ride_id = m.ride_id AND o.status IN ('pending', 'accepted'))
		RETURNING m.ride_id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// WaitingRequests returns requests still matching with no pending or accepted offer: their last offer expired or
// was declined, or no driver was available yet. Oldest first, so long-waiting riders aren't starved.
func (r *MatchingRepository) WaitingRequests(ctx context.Context, limit int) ([]StartInput, error) {
	rows, err := r.DB.Query(ctx,
		`SELECT m.ride_id, m.pickup_lat, m.pickup_lng, COALESCE(m.trace_parent, '') FROM match_requests m
		WHERE m.status = 'matching'
		AND NOT EXISTS (SELECT 1 FROM offers o WHERE o.ride_id = m.ride_id AND o.status IN ('pending', 'accepted'))
		ORDER BY m.created_at LIMIT $1`,
		limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (StartInput, error) {
		in := StartInput{Pickup: &Location{}}
		err := row.Scan(&in.RideID, &in.Pickup.Lat, &in.Pickup.Lng, &in.TraceParent)
		return in, err
	})
}

// OpenOffer returns the ride's pending or accepted offer, or nil if it has neither. A pending offer wins: the
// unique index allows one per ride, while an accepted one only blocks new offers until ride confirms it.
func (r *MatchingRepository) OpenOffer(ctx context.Context, rideID string) (*Offer, string, error) {
	var o Offer
	var status string
	err := r.DB.QueryRow(ctx,
		`SELECT id, ride_id, driver_id, expires_at, status FROM offers
		WHERE ride_id = $1 AND status IN ('pending', 'accepted') ORDER BY status = 'pending' DESC LIMIT 1`,
		rideID,
	).Scan(&o.ID, &o.RideID, &o.DriverID, &o.ExpiresAt, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	return &o, status, nil
}

// DriverPendingOffer returns the driver's unexpired pending offer with its pickup, or nil.
func (r *MatchingRepository) DriverPendingOffer(ctx context.Context, driverID string) (*DriverOffer, error) {
	var o DriverOffer
	err := r.DB.QueryRow(ctx,
		`SELECT o.id, o.ride_id, m.pickup_lat, m.pickup_lng, o.expires_at
		FROM offers o JOIN match_requests m ON m.ride_id = o.ride_id
		WHERE o.driver_id = $1 AND o.status = 'pending' AND o.expires_at > now()`,
		driverID,
	).Scan(&o.ID, &o.RideID, &o.Pickup.Lat, &o.Pickup.Lng, &o.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}

// AnswerOffer moves the driver's pending, unexpired offer to answer ('accepted' or 'declined') and returns its
// ride. Answering again with the same answer succeeds (a retried request); anything else is ErrOfferUnavailable.
func (r *MatchingRepository) AnswerOffer(ctx context.Context, offerID, driverID, answer string) (string, error) {
	var rideID string
	err := r.DB.QueryRow(ctx,
		`UPDATE offers SET status = $3, updated_at = now()
		WHERE id = $1 AND driver_id = $2 AND status = 'pending' AND expires_at > now()
		RETURNING ride_id`,
		offerID, driverID, answer,
	).Scan(&rideID)
	if err == nil || !errors.Is(err, pgx.ErrNoRows) {
		return rideID, err
	}

	var current string
	err = r.DB.QueryRow(ctx, `SELECT status, ride_id FROM offers WHERE id = $1 AND driver_id = $2`, offerID, driverID).Scan(&current, &rideID)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && current != answer) {
		return "", ErrOfferUnavailable
	}
	return rideID, err
}

// CancelAccepted marks an accepted offer cancelled after ride refused the assignment.
func (r *MatchingRepository) CancelAccepted(ctx context.Context, offerID string) error {
	_, err := r.DB.Exec(ctx, `UPDATE offers SET status = 'cancelled', updated_at = now() WHERE id = $1 AND status = 'accepted'`, offerID)
	return err
}

// FinishRequest ends a request still matching with status ('matched' or 'cancelled') and reports whether it did,
// so a confirmation running twice (AcceptOffer and Tick) counts once.
func (r *MatchingRepository) FinishRequest(ctx context.Context, rideID, status string) (bool, error) {
	tag, err := r.DB.Exec(ctx,
		`UPDATE match_requests SET status = $2, updated_at = now() WHERE ride_id = $1 AND status = 'matching'`, rideID, status)
	return tag.RowsAffected() == 1, err
}

// UnconfirmedAccepts returns accepted offers whose request is still matching: ride hasn't confirmed them yet,
// usually because AcceptOffer couldn't reach it.
func (r *MatchingRepository) UnconfirmedAccepts(ctx context.Context, limit int) ([]Offer, error) {
	rows, err := r.DB.Query(ctx,
		`SELECT o.id, o.ride_id, o.driver_id, o.expires_at, COALESCE(m.trace_parent, '') FROM offers o JOIN match_requests m ON m.ride_id = o.ride_id
		WHERE o.status = 'accepted' AND m.status = 'matching' ORDER BY o.updated_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Offer, error) {
		var o Offer
		err := row.Scan(&o.ID, &o.RideID, &o.DriverID, &o.ExpiresAt, &o.TraceParent)
		return o, err
	})
}

// Request returns the ride's match request with its status, or nil if matching never heard of the ride.
func (r *MatchingRepository) Request(ctx context.Context, rideID string) (*StartInput, string, error) {
	in := StartInput{RideID: rideID, Pickup: &Location{}}
	var status string
	err := r.DB.QueryRow(ctx,
		`SELECT pickup_lat, pickup_lng, status, COALESCE(trace_parent, '') FROM match_requests WHERE ride_id = $1`, rideID,
	).Scan(&in.Pickup.Lat, &in.Pickup.Lng, &status, &in.TraceParent)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	return &in, status, nil
}

// UnnotifiedNoDriver returns requests that ended no_driver but ride hasn't confirmed knowing yet.
func (r *MatchingRepository) UnnotifiedNoDriver(ctx context.Context, limit int) ([]NoDriverRequest, error) {
	rows, err := r.DB.Query(ctx,
		`SELECT ride_id, COALESCE(trace_parent, '') FROM match_requests
		WHERE status = 'no_driver' AND ride_notified_at IS NULL ORDER BY updated_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (NoDriverRequest, error) {
		var n NoDriverRequest
		err := row.Scan(&n.RideID, &n.TraceParent)
		return n, err
	})
}

func (r *MatchingRepository) MarkRideNotified(ctx context.Context, rideID string) error {
	_, err := r.DB.Exec(ctx, `UPDATE match_requests SET ride_notified_at = now() WHERE ride_id = $1`, rideID)
	return err
}

// CancelMatching ends a request that is still matching and withdraws its offers: pending ones, and an accepted one
// ride hasn't confirmed (it never will, the ride is cancelled). A request that already finished keeps its offers as
// history. Reports whether the request was still matching.
func (r *MatchingRepository) CancelMatching(ctx context.Context, rideID string) (bool, error) {
	var wasMatching bool
	err := pgx.BeginFunc(ctx, r.DB, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`UPDATE match_requests SET status = 'cancelled', updated_at = now() WHERE ride_id = $1 AND status = 'matching'`, rideID)
		if err != nil {
			return err
		}
		wasMatching = tag.RowsAffected() == 1
		_, err = tx.Exec(ctx,
			`UPDATE offers SET status = 'cancelled', updated_at = now()
			WHERE ride_id = $1 AND (status = 'pending' OR ($2 AND status = 'accepted'))`,
			rideID, wasMatching)
		return err
	})
	return wasMatching, err
}
