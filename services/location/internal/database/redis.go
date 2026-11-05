package database

import (
	"context"

	"github.com/redis/go-redis/v9"
)

func ConnectRedis(ctx context.Context, url string) (*redis.Client, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, err
	}

	// Readiness and request deadlines must bound network reads, not just connection setup.
	opts.ContextTimeoutEnabled = true
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, err
	}

	return client, nil
}
