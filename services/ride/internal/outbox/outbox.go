// Package outbox delivers ride's messages to other services reliably (the transactional outbox pattern): a message
// is written in the same transaction as the change it announces, and a relay keeps sending it until the receiver
// confirms. Delivery is at least once, so receivers must be idempotent.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rickyroynardson/ride-hailing/pkg/postgres"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// batchSize caps how many messages one relay round claims.
	batchSize = 50
	// sendTimeout bounds each delivery.
	sendTimeout = 3 * time.Second
	// claimLease is how long a claimed message is left alone. If its relay dies mid-send, another picks it up after
	// this. It must outlast a round, which sends in parallel and so takes about one sendTimeout.
	claimLease = 30 * time.Second
	// maxBackoff caps the wait between attempts at a failing message.
	maxBackoff = 5 * time.Minute
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

// Charge kinds, as ChargeRide.ChargeKind.
const (
	ChargeFare            = "fare"
	ChargeCancellationFee = "cancellation_fee"
)

// ChargeRide tells payment the rider owes for a finished ride.
type ChargeRide struct {
	RideID     string `json:"ride_id"`
	RiderID    string `json:"rider_id"`
	DriverID   string `json:"driver_id"`
	ChargeKind string `json:"charge_kind"`
	Amount     int64  `json:"amount"`
	Currency   string `json:"currency"`
}

func (ChargeRide) Kind() string { return "charge_ride" }

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
	// deliveryLag is the time from the change committing to the receiver confirming its message: what the outbox costs.
	deliveryLag = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "ride_outbox_delivery_lag_seconds", Help: "Time from a message being queued to its delivery, by kind.",
		Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300},
	}, []string{"kind"})
	deadTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ride_outbox_dead_total", Help: "Outbox messages given up on after a permanent error, by kind.",
	}, []string{"kind"})
)

// Relay sends unsent messages to the services they are for.
type Relay struct {
	db       *pgxpool.Pool
	lg       *zap.Logger
	matching matchingv1.MatchingServiceClient
	payment  paymentv1.PaymentServiceClient
	wake     chan struct{}
}

func NewRelay(db *pgxpool.Pool, lg *zap.Logger, matching matchingv1.MatchingServiceClient, payment paymentv1.PaymentServiceClient) *Relay {
	// Buffer one wake-up: a message added while a round runs gets its own round next, and further wake-ups in the
	// meantime fold into it.
	return &Relay{db: db, lg: lg, matching: matching, payment: payment, wake: make(chan struct{}, 1)}
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

type message struct {
	id          int64
	kind        string
	payload     []byte
	traceParent string
	attempts    int
	createdAt   time.Time
}

// Run delivers one batch of due messages, oldest first. It returns only database errors: a failed delivery backs off
// that message alone, so one unreachable receiver doesn't slow the relay down for the others.
func (r *Relay) Run(ctx context.Context) error {
	msgs, err := r.claim(ctx)
	if err != nil {
		return err
	}

	// A batch holds at most one message per ride (see claim), so sending in parallel keeps each ride's order, and a
	// receiver that hangs costs one sendTimeout rather than one per message.
	errs := make([]error, len(msgs))
	var wg sync.WaitGroup
	for i, m := range msgs {
		wg.Go(func() { errs[i] = r.deliver(ctx, m) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

// claim leases due messages to this relay in a short transaction of its own, so no row lock or connection is held
// while they are sent. FOR UPDATE SKIP LOCKED makes relays on several ride replicas split the work, and a claimed
// message stays unsent, so it keeps holding back its ride's later messages: matching can never get a ride's cancel
// before its start. A dead message holds them back too, until someone looks at it. Other rides aren't held up.
func (r *Relay) claim(ctx context.Context) ([]message, error) {
	rows, err := r.db.Query(ctx,
		`WITH due AS (
			SELECT o.id FROM outbox o
			WHERE o.sent_at IS NULL AND o.dead_at IS NULL AND o.next_attempt_at <= now()
			AND NOT EXISTS (
				SELECT 1 FROM outbox p
				WHERE p.sent_at IS NULL AND p.id < o.id AND p.payload->>'ride_id' = o.payload->>'ride_id'
			)
			ORDER BY o.id LIMIT $1 FOR UPDATE OF o SKIP LOCKED
		)
		UPDATE outbox SET next_attempt_at = now() + make_interval(secs => $2)
		FROM due WHERE outbox.id = due.id
		RETURNING outbox.id, outbox.kind, outbox.payload, COALESCE(outbox.trace_parent, ''), outbox.attempts, outbox.created_at`,
		batchSize, claimLease.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (message, error) {
		var m message
		err := row.Scan(&m.id, &m.kind, &m.payload, &m.traceParent, &m.attempts, &m.createdAt)
		return m, err
	})
}

// deliver sends one claimed message and records how it went. If the record is lost, the lease runs out and the
// message goes again; receivers are idempotent. Processing continues the trace of the request that queued the
// message, as the CONSUMER child of its PRODUCER span; polling for messages isn't traced at all (the relay runs under
// telemetry.Untraced).
func (r *Relay) deliver(ctx context.Context, m message) error {
	msgCtx, end := telemetry.StartConsumer(ctx, "outbox process "+m.kind, m.traceParent,
		append(messagingAttrs(m.kind, "process"),
			attribute.Int64("messaging.message.id", m.id), attribute.String("ride.id", rideIDOf(m.payload)))...)

	err := r.send(msgCtx, m.kind, m.payload)
	if err == nil {
		sentTotal.WithLabelValues(m.kind).Inc()
		deliveryLag.WithLabelValues(m.kind).Observe(time.Since(m.createdAt).Seconds())
		_, uerr := r.db.Exec(msgCtx, `UPDATE outbox SET attempts = attempts + 1, sent_at = now(), last_error = NULL WHERE id = $1`, m.id)
		end(uerr)
		return uerr
	}

	defer end(err)
	failedTotal.WithLabelValues(m.kind).Inc()
	if permanent(err) {
		deadTotal.WithLabelValues(m.kind).Inc()
		r.lg.Error("outbox message dead", zap.Int64("id", m.id), zap.String("kind", m.kind), zap.Error(err))
		_, uerr := r.db.Exec(msgCtx, `UPDATE outbox SET attempts = attempts + 1, dead_at = now(), last_error = $2 WHERE id = $1`, m.id, err.Error())
		return uerr
	}
	r.lg.Warn("outbox delivery failed", zap.Int64("id", m.id), zap.String("kind", m.kind), zap.Int("attempts", m.attempts+1), zap.Error(err))
	_, uerr := r.db.Exec(msgCtx,
		`UPDATE outbox SET attempts = attempts + 1, next_attempt_at = now() + make_interval(secs => $3), last_error = $2 WHERE id = $1`,
		m.id, err.Error(), backoff(m.attempts+1).Seconds())
	return uerr
}

// backoff doubles from one second per failed attempt, up to maxBackoff.
func backoff(attempts int) time.Duration {
	if attempts > 16 { // past this the shift below is well beyond maxBackoff
		return maxBackoff
	}
	return min(time.Second<<attempts, maxBackoff)
}

// errMalformed is a payload no number of retries will fix.
var errMalformed = errors.New("malformed payload")

// permanent reports whether retrying can never help. Anything else is retried: an outage or a misconfiguration ends,
// and an unknown kind may be one a newer replica queued during a rollout.
func permanent(err error) bool {
	return errors.Is(err, errMalformed) || status.Code(err) == codes.InvalidArgument
}

// rideIDOf reads the ride a message is about, for its trace; every message kind carries ride_id.
func rideIDOf(payload []byte) string {
	var p struct {
		RideID string `json:"ride_id"`
	}
	json.Unmarshal(payload, &p)
	return p.RideID
}

func decode[T any](payload []byte) (T, error) {
	var m T
	if err := json.Unmarshal(payload, &m); err != nil {
		return m, fmt.Errorf("%w: %v", errMalformed, err)
	}
	return m, nil
}

var toChargeKindProto = map[string]paymentv1.ChargeKind{
	ChargeFare:            paymentv1.ChargeKind_CHARGE_KIND_FARE,
	ChargeCancellationFee: paymentv1.ChargeKind_CHARGE_KIND_CANCELLATION_FEE,
}

func (r *Relay) send(ctx context.Context, kind string, payload []byte) error {
	ctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()

	switch kind {
	case StartMatching{}.Kind():
		m, err := decode[StartMatching](payload)
		if err != nil {
			return err
		}
		_, err = r.matching.StartMatching(ctx, &matchingv1.StartMatchingRequest{
			RideId: m.RideID,
			Pickup: &matchingv1.Location{Lat: m.Pickup.Lat, Lng: m.Pickup.Lng},
		})
		return err
	case CancelMatching{}.Kind():
		m, err := decode[CancelMatching](payload)
		if err != nil {
			return err
		}
		_, err = r.matching.CancelMatching(ctx, &matchingv1.CancelMatchingRequest{RideId: m.RideID})
		return err
	case ChargeRide{}.Kind():
		m, err := decode[ChargeRide](payload)
		if err != nil {
			return err
		}
		_, err = r.payment.ChargeRide(ctx, &paymentv1.ChargeRideRequest{
			RideId:   m.RideID,
			RiderId:  m.RiderID,
			DriverId: m.DriverID,
			Kind:     toChargeKindProto[m.ChargeKind],
			Amount:   &paymentv1.Money{Amount: m.Amount, Currency: m.Currency},
		})
		return err
	default:
		return fmt.Errorf("unknown outbox kind %q", kind)
	}
}
