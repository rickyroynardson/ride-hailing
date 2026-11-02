// Package outbox delivers ride's messages to other services reliably (the transactional outbox pattern): a message
// is written in the same transaction as the change it announces, and a relay keeps sending it until the receiver
// confirms. Delivery is at least once, so receivers must be idempotent.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rickyroynardson/ride-hailing/pkg/postgres"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	"go.opentelemetry.io/otel/attribute"
)

const (
	// batchSize caps how many messages one relay round claims.
	batchSize = 50
	// sendTimeout bounds each delivery; a claimed message's row lock is held while it is sent.
	sendTimeout = 3 * time.Second
	// notifyChannel is the Postgres LISTEN/NOTIFY channel that announces new messages.
	notifyChannel = "outbox"
)

// Message is something ride must tell another service. Kind names it in the outbox table.
type Message interface {
	Kind() string
}

type Location struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// StartMatching asks matching to find a driver for a new ride.
type StartMatching struct {
	RideID string   `json:"ride_id"`
	Pickup Location `json:"pickup"`
}

func (StartMatching) Kind() string { return "start_matching" }

// CancelMatching tells matching the rider cancelled, so it stops offering the ride.
type CancelMatching struct {
	RideID string `json:"ride_id"`
}

func (CancelMatching) Kind() string { return "cancel_matching" }

// Add writes a message in the caller's transaction, so it is saved exactly when the change it announces is. It
// records a PRODUCER span and stores that span's trace context with the message, so the relay's processing appears
// in the request's trace as an async hop, its CONSUMER child.
func Add(ctx context.Context, tx pgx.Tx, m Message) (err error) {
	ctx, end := telemetry.StartProducer(ctx, "outbox publish "+m.Kind(), messagingAttrs(m.Kind(), "publish")...)
	defer func() { end(err) }()

	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO outbox(kind, payload, trace_parent) VALUES ($1, $2, NULLIF($3, ''))`,
		m.Kind(), payload, telemetry.TraceParent(ctx)); err != nil {
		return err
	}
	// Wake the relays. Postgres delivers a notification only when its transaction commits, so a relay never wakes
	// for a message it can't see yet, or for one that was rolled back.
	_, err = tx.Exec(ctx, `SELECT pg_notify($1, '')`, notifyChannel)
	return err
}

// messagingAttrs are the OpenTelemetry messaging attributes for an outbox message of kind.
func messagingAttrs(kind, operation string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.system", "outbox"),
		attribute.String("messaging.destination.name", kind),
		attribute.String("messaging.operation.type", operation),
	}
}

var (
	sentTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ride_outbox_sent_total", Help: "Outbox messages delivered, by kind.",
	}, []string{"kind"})
	failedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ride_outbox_failed_total", Help: "Outbox delivery attempts that failed, by kind.",
	}, []string{"kind"})
)

// Relay sends unsent messages to the services they are for.
type Relay struct {
	db       *pgxpool.Pool
	matching matchingv1.MatchingServiceClient
	wake     chan struct{}
}

func NewRelay(db *pgxpool.Pool, matching matchingv1.MatchingServiceClient) *Relay {
	// Buffer one wake-up: a message added while a round runs gets its own round next, and further wake-ups in the
	// meantime fold into it.
	return &Relay{db: db, matching: matching, wake: make(chan struct{}, 1)}
}

// Wake receives whenever a message may be waiting; run the relay with loop.RunWake on it.
func (r *Relay) Wake() <-chan struct{} { return r.wake }

// Listen turns new-message notifications into wake-ups, so a message is sent within milliseconds instead of at the
// relay's next poll; run it with loop.Run to reconnect. It also wakes the relay on connecting, for anything announced
// while it was disconnected.
func (r *Relay) Listen(ctx context.Context) error {
	wake := func() {
		select {
		case r.wake <- struct{}{}:
		default: // a wake-up is already pending
		}
	}
	return postgres.Listen(ctx, r.db, notifyChannel, wake, func(string) { wake() })
}

// Run delivers one batch of unsent messages, oldest first, and returns the joined delivery errors. It claims rows
// with FOR UPDATE SKIP LOCKED, so relays on several ride replicas split the work instead of sending the same
// message at the same time. A failed message stays unsent, with its attempt count and error, and is retried on the
// next round; one failure doesn't hold up the others.
// ponytail: retries failed messages forever; move them to a dead-letter state after N attempts if a permanent
// error (e.g. a malformed payload) ever shows up
func (r *Relay) Run(ctx context.Context) error {
	var errs []error
	err := pgx.BeginFunc(ctx, r.db, func(tx pgx.Tx) error {
		// A ride's messages go in order: one waits while an older message for the same ride is unsent, so
		// matching can never get a ride's cancel before its start. Other rides' messages aren't held up.
		rows, err := tx.Query(ctx,
			`SELECT o.id, o.kind, o.payload, COALESCE(o.trace_parent, '') FROM outbox o
			WHERE o.sent_at IS NULL
			AND NOT EXISTS (
				SELECT 1 FROM outbox p
				WHERE p.sent_at IS NULL AND p.id < o.id AND p.payload->>'ride_id' = o.payload->>'ride_id'
			)
			ORDER BY o.id LIMIT $1 FOR UPDATE OF o SKIP LOCKED`, batchSize)
		if err != nil {
			return err
		}
		type msg struct {
			id          int64
			kind        string
			payload     []byte
			traceParent string
		}
		msgs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (msg, error) {
			var m msg
			err := row.Scan(&m.id, &m.kind, &m.payload, &m.traceParent)
			return m, err
		})
		if err != nil {
			return err
		}

		for _, m := range msgs {
			// Processing continues the trace of the request that queued the message, as the CONSUMER child of its
			// PRODUCER span; polling for messages isn't traced at all (the relay runs under telemetry.Untraced).
			msgCtx, end := telemetry.StartConsumer(ctx, "outbox process "+m.kind, m.traceParent,
				append(messagingAttrs(m.kind, "process"),
					attribute.Int64("messaging.message.id", m.id), attribute.String("ride.id", rideIDOf(m.payload)))...)
			if err := r.send(msgCtx, m.kind, m.payload); err != nil {
				failedTotal.WithLabelValues(m.kind).Inc()
				errs = append(errs, fmt.Errorf("outbox %d (%s): %w", m.id, m.kind, err))
				_, uerr := tx.Exec(msgCtx, `UPDATE outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1`, m.id, err.Error())
				end(err)
				if uerr != nil {
					return uerr
				}
				continue
			}
			sentTotal.WithLabelValues(m.kind).Inc()
			_, uerr := tx.Exec(msgCtx, `UPDATE outbox SET attempts = attempts + 1, sent_at = now(), last_error = NULL WHERE id = $1`, m.id)
			end(uerr)
			if uerr != nil {
				return uerr
			}
		}
		return nil
	})
	return errors.Join(append(errs, err)...)
}

// rideIDOf reads the ride a message is about, for its trace; every message kind carries ride_id.
func rideIDOf(payload []byte) string {
	var p struct {
		RideID string `json:"ride_id"`
	}
	json.Unmarshal(payload, &p)
	return p.RideID
}

func (r *Relay) send(ctx context.Context, kind string, payload []byte) error {
	ctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()

	switch kind {
	case StartMatching{}.Kind():
		var m StartMatching
		if err := json.Unmarshal(payload, &m); err != nil {
			return err
		}
		_, err := r.matching.StartMatching(ctx, &matchingv1.StartMatchingRequest{
			RideId: m.RideID,
			Pickup: &matchingv1.Location{Lat: m.Pickup.Lat, Lng: m.Pickup.Lng},
		})
		return err
	case CancelMatching{}.Kind():
		var m CancelMatching
		if err := json.Unmarshal(payload, &m); err != nil {
			return err
		}
		_, err := r.matching.CancelMatching(ctx, &matchingv1.CancelMatchingRequest{RideId: m.RideID})
		return err
	default:
		return fmt.Errorf("unknown outbox kind %q", kind)
	}
}
