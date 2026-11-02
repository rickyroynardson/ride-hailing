package ride

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickyroynardson/ride-hailing/pkg/postgres"
)

// changedChannel is the Postgres channel the rides_notify_changed trigger announces status changes on, with the
// ride's ID as payload. A trigger means no update can forget to announce, and Postgres delivers only on commit, to
// every replica, so a stream is told whichever replica made the change.
const changedChannel = "ride_changed"

// Hub tells this process's WatchRide streams when their ride changes. One connection listens for all of them, so
// open streams don't each hold a Postgres connection.
type Hub struct {
	db *pgxpool.Pool

	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}
}

func NewHub(db *pgxpool.Pool) *Hub {
	return &Hub{db: db, subs: map[string]map[chan struct{}]struct{}{}}
}

// Subscribe returns a channel that receives when the ride changes, and a function to stop. A signal says only "look
// again", so several changes in a row fold into one and a slow stream never holds up the others.
func (h *Hub) Subscribe(rideID string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[rideID] == nil {
		h.subs[rideID] = map[chan struct{}]struct{}{}
	}
	h.subs[rideID][ch] = struct{}{}

	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.subs[rideID], ch)
		if len(h.subs[rideID]) == 0 {
			delete(h.subs, rideID)
		}
	}
}

// Listen delivers change notifications to subscribers; run it with loop.Run to reconnect. On connecting it signals
// every stream: unlike the outbox relay, streams have no poll to fall back on for a change announced while it was
// disconnected.
func (h *Hub) Listen(ctx context.Context) error {
	return postgres.Listen(ctx, h.db, changedChannel, h.signalAll, h.signal)
}

func (h *Hub) signal(rideID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs[rideID] {
		select {
		case ch <- struct{}{}:
		default: // a signal is already pending
		}
	}
}

func (h *Hub) signalAll() {
	h.mu.Lock()
	ids := make([]string, 0, len(h.subs))
	for id := range h.subs {
		ids = append(ids, id)
	}
	h.mu.Unlock()
	for _, id := range ids {
		h.signal(id)
	}
}
