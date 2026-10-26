package database

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SchemaVersion is the newest migration this code needs. TestSchemaVersionIsLatestMigration fails when a
// migration is added without bumping it, so the startup check can't silently fall behind.
const SchemaVersion = 20261021051004

var ErrNotMigrated = errors.New("database not migrated")

// CheckSchema fails unless migration SchemaVersion is applied, so the service refuses to start against a database
// it can't use instead of failing every request. Migrations stay a manual step; this only checks them.
// ponytail: copied in services/{user,ride,matching}; only SchemaVersion differs
func CheckSchema(ctx context.Context, pool *pgxpool.Pool) error {
	var applied bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM goose_db_version WHERE version_id = $1 AND is_applied)`, SchemaVersion,
	).Scan(&applied)

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
		// goose_db_version doesn't exist: goose has never run against this database.
		return fmt.Errorf("%w: no migrations applied", ErrNotMigrated)
	}
	if err != nil {
		return err
	}
	if !applied {
		return fmt.Errorf("%w: migration %d not applied", ErrNotMigrated, SchemaVersion)
	}
	return nil
}
