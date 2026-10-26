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
	ID        string    `json:"id"`
	RiderID   string    `json:"riderId"`
	DriverID  string    `json:"driverId"`
	Status    string    `json:"status"`
	Pickup    latLng    `json:"pickup"`
	Dropoff   latLng    `json:"dropoff"`
	CreatedAt time.Time `json:"createdAt"`
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

	state := map[string]any{"now": time.Now().UnixMilli()}
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

	rides, err := collect(ctx, c.ridesDB, `SELECT id, rider_id, COALESCE(driver_id::text, ''), status,
		pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, created_at FROM rides ORDER BY created_at DESC LIMIT 50`,
		func(row pgx.CollectableRow) (rideRow, error) {
			var x rideRow
			err := row.Scan(&x.ID, &x.RiderID, &x.DriverID, &x.Status, &x.Pickup.Lat, &x.Pickup.Lng, &x.Dropoff.Lat, &x.Dropoff.Lng, &x.CreatedAt)
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
