package postgres

import (
	"context"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Hub tells this process's streams when what they watch changes. A trigger announces each change on a Postgres
// channel, with the key of what changed as payload. A trigger means no update can forget to announce, and Postgres
// delivers only on commit, to every replica, so a stream is told whichever replica made the change. One connection
// listens for all streams, so open streams don't each hold a Postgres connection.
type Hub struct {
	db      *pgxpool.Pool
	channel string

	mu   sync.Mutex
	subs map[string]map[chan struct{}]struct{}
}

func NewHub(db *pgxpool.Pool, channel string) *Hub {
	return &Hub{db: db, channel: channel, subs: map[string]map[chan struct{}]struct{}{}}
}

// Subscribe returns a channel that receives when key changes, and a function to stop. A signal says only "look
// again", so several changes in a row fold into one and a slow stream never holds up the others.
func (h *Hub) Subscribe(key string) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[key] == nil {
		h.subs[key] = map[chan struct{}]struct{}{}
	}
	h.subs[key][ch] = struct{}{}

	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.subs[key], ch)
		if len(h.subs[key]) == 0 {
			delete(h.subs, key)
		}
	}
}

// Listen delivers change notifications to subscribers; run it with loop.Run to reconnect. On connecting it signals
// every stream: streams have no poll to fall back on for a change announced while it was disconnected.
func (h *Hub) Listen(ctx context.Context) error {
	return Listen(ctx, h.db, h.channel, h.signalAll, h.signal)
}

func (h *Hub) signal(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs[key] {
		select {
		case ch <- struct{}{}:
		default: // a signal is already pending
		}
	}
}

func (h *Hub) signalAll() {
	h.mu.Lock()
	keys := make([]string, 0, len(h.subs))
	for k := range h.subs {
		keys = append(keys, k)
	}
	h.mu.Unlock()
	for _, k := range keys {
		h.signal(k)
	}
}
