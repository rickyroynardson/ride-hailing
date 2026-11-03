package ride

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickyroynardson/ride-hailing/pkg/postgres"
)

// changedChannel is the Postgres channel the rides_notify_changed trigger announces status changes on, with the
// ride's ID as payload.
const changedChannel = "ride_changed"

// NewHub tells this process's WatchRide streams when their ride changes.
func NewHub(db *pgxpool.Pool) *postgres.Hub {
	return postgres.NewHub(db, changedChannel)
}
