package main

import (
	"context"
	"net/http"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

type driverPosition struct {
	ID       string  `json:"id"`
	Lat      float64 `json:"lat"`
	Lng      float64 `json:"lng"`
	LastSeen int64   `json:"lastSeen"`
}

type rideRow struct {
	ID       string `json:"id"`
	RiderID  string `json:"riderId"`
	DriverID string `json:"driverId"`
	Status   string `json:"status"`
	Reason   string `json:"cancelReason"`
	// Trip timeline; nil until the step happens.
	AcceptedAt  *time.Time `json:"acceptedAt"`
	StartedAt   *time.Time `json:"startedAt"`
	CompletedAt *time.Time `json:"completedAt"`
	Pickup      latLng     `json:"pickup"`
	Dropoff     latLng     `json:"dropoff"`
	CreatedAt   time.Time  `json:"createdAt"`
	// Fare is zero on rides from before pricing. PaymentStatus is ride's own copy of the outcome.
	Fare          int64  `json:"fare"`
	Currency      string `json:"currency"`
	PaymentStatus string `json:"paymentStatus"`
}

type chargeRow struct {
	RideID        string `json:"rideId"`
	DriverID      string `json:"driverId"`
	Kind          string `json:"kind"`
	Status        string `json:"status"`
	Amount        int64  `json:"amount"`
	Currency      string `json:"currency"`
	DeclineReason string `json:"declineReason"`
	Attempts      int    `json:"attempts"`
	LastError     string `json:"lastError"`
	// Refunded has gone back to the rider; RefundPending is on its way, or its outcome is unknown.
	Refunded      int64  `json:"refunded"`
	RefundPending int64  `json:"refundPending"`
	RefundFailure string `json:"refundFailure"`
}

type earningRow struct {
	DriverID string `json:"driverId"`
	Amount   int64  `json:"amount"`
}

type matchRequestRow struct {
	RideID     string    `json:"rideId"`
	Status     string    `json:"status"`
	DeadlineAt time.Time `json:"deadlineAt"`
}

type offerRow struct {
	ID        string    `json:"id"`
	RideID    string    `json:"rideId"`
	DriverID  string    `json:"driverId"`
	Status    string    `json:"status"`
	ExpiresAt time.Time `json:"expiresAt"`
	CreatedAt time.Time `json:"createdAt"`
}

// handleState is what the page polls: actors, the call log, and each service's data read straight from its store.
// A store that fails is reported in errors, and the rest still renders.
func (c *console) handleState(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	state := map[string]any{"now": time.Now().UnixMilli(), "jaegerUrl": c.jaegerURL}
	errs := map[string]string{}

	c.mu.Lock()
	actors := slices.Clone(c.actors)
	calls := append([]callEntry{}, c.calls...)
	c.mu.Unlock()
	views := make([]actorView, len(actors))
	for i, a := range actors {
		views[i] = a.view()
	}
	slices.Reverse(calls)
	state["actors"], state["calls"] = views, calls

	if drivers, err := c.drivers(ctx); err != nil {
		errs["redis"] = err.Error()
	} else {
		state["drivers"] = drivers
	}

	rides, err := collect(ctx, c.ridesDB, `SELECT id, rider_id, COALESCE(driver_id::text, ''), status, COALESCE(cancel_reason, ''),
		accepted_at, started_at, completed_at,
		pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, created_at,
		COALESCE(fare_amount, 0), COALESCE(fare_currency, ''), COALESCE(payment_status, '') FROM rides ORDER BY created_at DESC LIMIT 50`,
		func(row pgx.CollectableRow) (rideRow, error) {
			var x rideRow
			err := row.Scan(&x.ID, &x.RiderID, &x.DriverID, &x.Status, &x.Reason, &x.AcceptedAt, &x.StartedAt, &x.CompletedAt, &x.Pickup.Lat, &x.Pickup.Lng, &x.Dropoff.Lat, &x.Dropoff.Lng, &x.CreatedAt, &x.Fare, &x.Currency, &x.PaymentStatus)
			return x, err
		})
	if err != nil {
		errs["rides"] = err.Error()
	}
	state["rides"] = rides

	offers, err := collect(ctx, c.matchingDB, `SELECT id, ride_id, driver_id, status, expires_at, created_at
		FROM offers ORDER BY created_at DESC LIMIT 50`,
		func(row pgx.CollectableRow) (offerRow, error) {
			var x offerRow
			err := row.Scan(&x.ID, &x.RideID, &x.DriverID, &x.Status, &x.ExpiresAt, &x.CreatedAt)
			return x, err
		})
	if err != nil {
		errs["offers"] = err.Error()
	}
	state["offers"] = offers

	requests, err := collect(ctx, c.matchingDB, `SELECT ride_id, status, deadline_at FROM match_requests
		ORDER BY created_at DESC LIMIT 50`,
		func(row pgx.CollectableRow) (matchRequestRow, error) {
			var x matchRequestRow
			err := row.Scan(&x.RideID, &x.Status, &x.DeadlineAt)
			return x, err
		})
	if err != nil {
		errs["match_requests"] = err.Error()
	}
	state["matchRequests"] = requests

	charges, err := collect(ctx, c.paymentsDB, `SELECT c.ride_id, c.driver_id, c.kind, c.status, c.amount, c.currency,
		COALESCE(c.decline_reason, ''), c.attempts, COALESCE(c.last_error, ''),
		COALESCE(sum(f.amount) FILTER (WHERE f.status = 'succeeded'), 0)::bigint,
		COALESCE(sum(f.amount) FILTER (WHERE f.status = 'pending'), 0)::bigint,
		COALESCE(max(f.failure_reason), '')
		FROM charges c LEFT JOIN refunds f ON f.charge_id = c.id
		GROUP BY c.id ORDER BY c.created_at DESC LIMIT 50`,
		func(row pgx.CollectableRow) (chargeRow, error) {
			var x chargeRow
			err := row.Scan(&x.RideID, &x.DriverID, &x.Kind, &x.Status, &x.Amount, &x.Currency, &x.DeclineReason, &x.Attempts, &x.LastError, &x.Refunded, &x.RefundPending, &x.RefundFailure)
			return x, err
		})
	if err != nil {
		errs["charges"] = err.Error()
	}
	state["charges"] = charges

	earnings, err := collect(ctx, c.paymentsDB, `SELECT substr(account, 8), -sum(amount)::bigint FROM ledger_entries
		WHERE account LIKE 'driver:%' GROUP BY account`,
		func(row pgx.CollectableRow) (earningRow, error) {
			var x earningRow
			err := row.Scan(&x.DriverID, &x.Amount)
			return x, err
		})
	if err != nil {
		errs["ledger"] = err.Error()
	}
	state["earnings"] = earnings

	state["errors"] = errs
	writeJSON(w, http.StatusOK, state)
}

func collect[T any](ctx context.Context, db interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, sql string, scan pgx.RowToFunc[T]) ([]T, error) {
	rows, err := db.Query(ctx, sql)
	if err != nil {
		return []T{}, err
	}
	out, err := pgx.CollectRows(rows, scan)
	if out == nil {
		out = []T{}
	}
	return out, err
}

// drivers reads every driver in the location service's geo set with their last-seen time; the page greys out
// those older than the 30s freshness window.
func (c *console) drivers(ctx context.Context) ([]driverPosition, error) {
	ids, err := c.rdb.ZRange(ctx, "drivers:geo", 0, 499).Result()
	if err != nil || len(ids) == 0 {
		return []driverPosition{}, err
	}
	pos, err := c.rdb.GeoPos(ctx, "drivers:geo", ids...).Result()
	if err != nil {
		return nil, err
	}
	seen, err := c.rdb.ZMScore(ctx, "drivers:last_seen", ids...).Result()
	if err != nil {
		return nil, err
	}

	out := make([]driverPosition, 0, len(ids))
	for i, id := range ids {
		if pos[i] != nil {
			out = append(out, driverPosition{ID: id, Lat: pos[i].Latitude, Lng: pos[i].Longitude, LastSeen: int64(seen[i])})
		}
	}
	return out, nil
}
