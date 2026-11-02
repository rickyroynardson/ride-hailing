package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Listen receives notifications from Postgres LISTEN/NOTIFY on a dedicated connection, until it drops or ctx ends;
// run it with loop.Run to reconnect. onConnect runs once LISTEN is active: notifications sent while disconnected are
// gone, so it is the moment to catch up. A connection that worked and then dropped returns nil, so loop.Run reconnects
// without growing its backoff over days of occasional drops; if Postgres is really down, connecting fails and is logged.
func Listen(ctx context.Context, pool *pgxpool.Pool, channel string, onConnect func(), onNotify func(payload string)) error {
	acquired, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	// Take the connection out of the pool for good: a pooled connection still LISTENing would collect
	// notifications for whoever borrows it next.
	conn := acquired.Hijack()
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		return err
	}
	onConnect()
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return nil
		}
		onNotify(n.Payload)
	}
}
