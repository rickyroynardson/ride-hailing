package database

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func ConnectPgx(ctx context.Context, url string) (*pgxpool.Pool, error) {
	c, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}

	c.MaxConns = 25
	c.MinConns = 5
	c.MaxConnIdleTime = 15 * time.Minute
	c.MaxConnLifetime = time.Hour
	c.HealthCheckPeriod = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, err
	}

	return pool, nil
}
