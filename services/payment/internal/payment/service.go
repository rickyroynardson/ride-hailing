package payment

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"github.com/rickyroynardson/ride-hailing/services/payment/internal/provider"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// tickBatch caps how many charges one tick handles.
	tickBatch = 50
	// providerTimeout bounds each provider call. Past it the outcome is unknown, not failed.
	providerTimeout = 5 * time.Second
	// claimLease is how long a claimed charge is left alone; it must outlast providerTimeout.
	claimLease = 30 * time.Second
	// maxBackoff caps the wait between attempts at a charge whose outcome stays unknown.
	maxBackoff = 5 * time.Minute
	// callTimeout bounds each call to ride.
	callTimeout = 3 * time.Second
)

var chargesFinished = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "payment_charges_finished_total", Help: "Charges that reached an outcome, by outcome.",
}, []string{"outcome"})

var providerUnknown = promauto.NewCounter(prometheus.CounterOpts{
	Name: "payment_provider_unknown_total", Help: "Provider calls whose outcome was unknown, so will be repeated.",
})

type PaymentService struct {
	repository *PaymentRepository
	provider   provider.Provider
	ride       ridev1.RideServiceClient
	lg         *zap.Logger
}

func NewService(repo *PaymentRepository, p provider.Provider, ride ridev1.RideServiceClient, lg *zap.Logger) *PaymentService {
	return &PaymentService{repository: repo, provider: p, ride: ride, lg: lg}
}

// ChargeRide records that a ride must be paid for. The provider is called from Tick, not here: ride's relay only needs
// to know the charge is safely saved, however slow the provider is. Ride delivers at least once, so it is idempotent.
func (s *PaymentService) ChargeRide(ctx context.Context, in ChargeInput) error {
	if err := in.Validate(); err != nil {
		return err
	}
	return s.repository.CreateCharge(ctx, in, telemetry.TraceParent(ctx))
}

// Tick moves charges and refunds forward; it runs every second, on every replica. It sends what is due to the provider,
// then tells ride how finished charges ended.
func (s *PaymentService) Tick(ctx context.Context) error {
	return errors.Join(s.processDue(ctx), s.processDueRefunds(ctx), s.notifyOutcomes(ctx))
}

func (s *PaymentService) processDue(ctx context.Context) error {
	charges, err := s.repository.ClaimDue(ctx, tickBatch, claimLease)
	if err != nil {
		return fmt.Errorf("claim charges: %w", err)
	}
	errs := make([]error, len(charges))
	var wg sync.WaitGroup
	for i, c := range charges {
		wg.Go(func() { errs[i] = s.process(ctx, c) })
	}
	wg.Wait()
	return errors.Join(errs...)
}

// process makes one attempt at a charge. There are three outcomes, and telling them apart is the point: the money
// moved, it definitely didn't, or nobody knows. Only the last repeats, with the same key, so it can't charge twice.
func (s *PaymentService) process(ctx context.Context, c dueCharge) (err error) {
	ctx, end := telemetry.StartConsumer(ctx, "charge ride", c.TraceParent,
		attribute.String("charge.id", c.ID), attribute.String("ride.id", c.RideID), attribute.Int("charge.attempt", c.Attempts))
	defer func() { end(err) }()

	callCtx, cancel := context.WithTimeout(ctx, providerTimeout)
	res, err := s.provider.Charge(callCtx, provider.Request{
		Key:      c.ID + ":" + strconv.Itoa(c.ProviderAttempt),
		Token:    c.Token,
		Amount:   c.Amount.Amount,
		Currency: c.Amount.Currency,
	})
	cancel()
	switch {
	case err != nil:
		// An unknown outcome is routine, not a failure of the tick: failing would slow every other charge down.
		providerUnknown.Inc()
		s.lg.Warn("provider outcome unknown", zap.String("charge_id", c.ID), zap.Int("attempts", c.Attempts), zap.Error(err))
		return s.repository.Postpone(ctx, c.ID, err.Error(), backoff(c.Attempts))
	case res.DeclineReason != "":
		chargesFinished.WithLabelValues(StatusFailed).Inc()
		return s.repository.Decline(ctx, c.ID, res.DeclineReason)
	default:
		chargesFinished.WithLabelValues(StatusCaptured).Inc()
		return s.repository.Capture(ctx, c.ID, res.Reference)
	}
}

// backoff doubles from one second per attempt, up to maxBackoff.
func backoff(attempts int) time.Duration {
	if attempts > 16 { // past this the shift below is well beyond maxBackoff
		return maxBackoff
	}
	return min(time.Second<<attempts, maxBackoff)
}

// notifyOutcomes tells ride how charges ended, until it confirms. RecordPayment is idempotent, so an outcome ride
// handled but whose confirmation got lost is simply sent again.
func (s *PaymentService) notifyOutcomes(ctx context.Context) error {
	outcomes, err := s.repository.UnnotifiedOutcomes(ctx, tickBatch)
	if err != nil {
		return fmt.Errorf("list outcomes: %w", err)
	}
	var errs []error
	for _, o := range outcomes {
		errs = append(errs, s.notifyRide(ctx, o))
	}
	return errors.Join(errs...)
}

func (s *PaymentService) notifyRide(ctx context.Context, o outcome) (err error) {
	ctx, end := telemetry.StartConsumer(ctx, "notify payment outcome", o.TraceParent,
		attribute.String("charge.id", o.ID), attribute.String("ride.id", o.RideID), attribute.String("charge.status", o.Status))
	defer func() { end(err) }()

	result := ridev1.PaymentOutcome_PAYMENT_OUTCOME_FAILED
	if o.Status == StatusCaptured {
		result = ridev1.PaymentOutcome_PAYMENT_OUTCOME_PAID
	}
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	_, callErr := s.ride.RecordPayment(callCtx, &ridev1.RecordPaymentRequest{RideId: o.RideID, Outcome: result})
	cancel()
	if status.Code(callErr) == codes.NotFound {
		// Ride has no such ride, and retrying won't change that.
		s.lg.Warn("charge for an unknown ride", zap.String("charge_id", o.ID), zap.String("ride_id", o.RideID))
		callErr = nil
	}
	if callErr != nil {
		return fmt.Errorf("notify ride %s: %w", o.RideID, callErr)
	}
	return s.repository.MarkNotified(ctx, o.ID, o.Status)
}

func (s *PaymentService) GetCharge(ctx context.Context, rideID, userID string) (Charge, error) {
	if !uuidPattern.MatchString(rideID) {
		return Charge{}, fmt.Errorf("%w: ride_id must be a UUID", ErrInvalidInput)
	}
	c, err := s.repository.GetCharge(ctx, rideID, userID)
	if err != nil {
		return Charge{}, err
	}
	c.Refunds, err = s.repository.Refunds(ctx, c.ID)
	if err != nil {
		return Charge{}, err
	}
	for _, r := range c.Refunds {
		if r.Status == RefundSucceeded {
			c.RefundedAmount += r.Amount.Amount
		}
	}
	return c, nil
}

func (s *PaymentService) RetryCharge(ctx context.Context, rideID, riderID string) error {
	if !uuidPattern.MatchString(rideID) {
		return fmt.Errorf("%w: ride_id must be a UUID", ErrInvalidInput)
	}
	return s.repository.RetryCharge(ctx, rideID, riderID)
}

func (s *PaymentService) SetPaymentMethod(ctx context.Context, riderID, token string) error {
	if !validToken(token) {
		return fmt.Errorf("%w: token is required", ErrInvalidInput)
	}
	return s.repository.SetPaymentMethod(ctx, riderID, token)
}

func (s *PaymentService) Earnings(ctx context.Context, driverID string) ([]Money, error) {
	return s.repository.Earnings(ctx, driverID)
}
