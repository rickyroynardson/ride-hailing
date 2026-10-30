package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect opens a pool with the services' shared pool settings.
func Connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	c, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	c.MaxConns = 25
	c.MinConns = 5
	c.MaxConnIdleTime = 15 * time.Minute
	c.MaxConnLifetime = time.Hour
	c.HealthCheckPeriod = time.Minute
	// A span per query, inside the request's trace. With tracing off it records nothing.
	c.ConnConfig.Tracer = newQueryTracer()

	pool, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, err
	}

	return pool, nil
}
