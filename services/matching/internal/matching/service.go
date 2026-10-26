package matching

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// callTimeout bounds each call to another service, so a slow dependency can't hold a matching request open.
const callTimeout = 3 * time.Second

type MatchingService struct {
	repository *MatchingRepository
	location   locationv1.LocationServiceClient
	ride       ridev1.RideServiceClient
}

func NewService(repo *MatchingRepository, location locationv1.LocationServiceClient, ride ridev1.RideServiceClient) *MatchingService {
	return &MatchingService{repository: repo, location: location, ride: ride}
}

// StartMatching records the ride as needing a driver and offers it to the nearest available one. It returns the
// offer, or nil if no driver is available yet; Tick keeps trying until the request's deadline. It is idempotent:
// a retry keeps the first request and returns its pending offer, so the ride service can retry after a timeout.
func (s *MatchingService) StartMatching(ctx context.Context, in StartInput) (*Offer, error) {
	if err := in.Validate(); err != nil {
		return nil, err
	}
	reqStatus, err := s.repository.CreateRequest(ctx, in.RideID, *in.Pickup, time.Now().Add(matchDeadline))
	if err != nil {
		return nil, err
	}
	if reqStatus != "matching" {
		// Already matched, given up or cancelled: nothing more to offer.
		return s.repository.PendingOffer(ctx, in.RideID)
	}
	return s.offerNext(ctx, in)
}

// Tick moves matching forward; it runs every second. It expires unanswered offers, gives up on requests past
// their deadline, and offers every request still waiting to its nearest available driver.
// ponytail: retries every waiting request each second, calling location and ride each time; add a per-request
// next_attempt_at backoff when many rides wait at once
func (s *MatchingService) Tick(ctx context.Context) error {
	expired, err := s.repository.ExpireOffers(ctx)
	if err != nil {
		return fmt.Errorf("expire offers: %w", err)
	}
	offersExpired.Add(float64(expired))

	// Accepts ride hasn't confirmed: ask again until it answers. This is what makes accepting safe when ride is
	// briefly unreachable.
	unconfirmed, err := s.repository.UnconfirmedAccepts(ctx, tickBatch)
	if err != nil {
		return fmt.Errorf("list unconfirmed accepts: %w", err)
	}
	var errs []error
	for _, o := range unconfirmed {
		if err := s.confirm(ctx, o); err != nil && !errors.Is(err, ErrOfferUnavailable) {
			errs = append(errs, fmt.Errorf("confirm offer %s: %w", o.ID, err))
		}
	}

	gaveUp, err := s.repository.GiveUp(ctx)
	if err != nil {
		return fmt.Errorf("give up requests: %w", err)
	}
	requestsFinished.WithLabelValues("no_driver").Add(float64(len(gaveUp)))

	// Tell ride about every request that ended without a driver, until it confirms. The request row works as
	// matching's outbox: ride_notified_at stays empty, and this keeps retrying, until ride has the news.
	// Its errors are reported but don't stop the rest of the tick: ride being down mustn't stall offering.
	if err := s.notifyNoDriver(ctx); err != nil {
		errs = append(errs, fmt.Errorf("notify no driver: %w", err))
	}

	waiting, err := s.repository.WaitingRequests(ctx, tickBatch)
	if err != nil {
		return fmt.Errorf("list waiting requests: %w", err)
	}
	for _, in := range waiting {
		if _, err := s.offerNext(ctx, in); err != nil {
			errs = append(errs, fmt.Errorf("ride %s: %w", in.RideID, err))
		}
	}
	return errors.Join(errs...)
}

// offerNext offers the ride to its nearest available driver, unless it already has a pending offer, which it
// returns. Concurrent calls for one ride (a StartMatching retry racing Tick, or two replicas) create at most one
// pending offer, thanks to the offers unique indexes.
func (s *MatchingService) offerNext(ctx context.Context, in StartInput) (*Offer, error) {
	if o, err := s.repository.PendingOffer(ctx, in.RideID); err != nil || o != nil {
		return o, err
	}
	// A driver already accepted; until ride confirms or refuses, the ride belongs to them.
	if accepted, err := s.repository.HasAcceptedOffer(ctx, in.RideID); err != nil || accepted {
		return nil, err
	}

	candidates, err := s.candidates(ctx, in)
	if err != nil {
		return nil, err
	}

	for _, driverID := range candidates {
		o, err := s.repository.CreateOffer(ctx, in.RideID, driverID, time.Now().Add(offerTTL))
		switch {
		case errors.Is(err, errDriverTaken):
			// Someone else offered this driver a ride between our checks and the insert; try the next one.
			continue
		case errors.Is(err, errRideHasOffer):
			// A concurrent call for this ride won; return its offer.
			return s.repository.PendingOffer(ctx, in.RideID)
		case err != nil:
			return nil, err
		}
		offersCreated.Inc()
		return &o, nil
	}
	// We created no offer, but a concurrent call may have: its pending offer also excluded its driver from our
	// candidates. Return that offer rather than "no driver available".
	return s.repository.PendingOffer(ctx, in.RideID)
}

// CancelMatching stops matching a ride the rider cancelled. Ride delivers it through its outbox, at least once, so
// it is idempotent and a no-op for rides matching never heard of or already finished.
func (s *MatchingService) CancelMatching(ctx context.Context, rideID string) error {
	if !uuidPattern.MatchString(rideID) {
		return fmt.Errorf("%w: ride_id must be a UUID", ErrInvalidInput)
	}
	wasMatching, err := s.repository.CancelMatching(ctx, rideID)
	if err == nil && wasMatching {
		requestsFinished.WithLabelValues("cancelled").Inc()
	}
	return err
}

// notifyNoDriver asks ride to cancel rides whose request ended no_driver. MarkNoDriver is idempotent, so a request
// that ride handled but whose confirmation got lost is simply sent again.
func (s *MatchingService) notifyNoDriver(ctx context.Context) error {
	rides, err := s.repository.UnnotifiedNoDriver(ctx, tickBatch)
	if err != nil {
		return err
	}
	var errs []error
	for _, rideID := range rides {
		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		_, err := s.ride.MarkNoDriver(callCtx, &ridev1.MarkNoDriverRequest{RideId: rideID})
		cancel()
		switch status.Code(err) {
		case codes.OK:
		case codes.FailedPrecondition:
			// Ride says the ride has a driver, so it isn't ours to cancel. Retrying won't change that: stop, but
			// report it, since matching and ride disagree about this ride.
			errs = append(errs, fmt.Errorf("ride %s refused no_driver: %w", rideID, err))
		default:
			errs = append(errs, fmt.Errorf("ride %s: %w", rideID, err))
			continue
		}
		if err := s.repository.MarkRideNotified(ctx, rideID); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// GetOffer returns the driver's pending offer, or nil. Driver apps poll it.
func (s *MatchingService) GetOffer(ctx context.Context, driverID string) (*DriverOffer, error) {
	return s.repository.DriverPendingOffer(ctx, driverID)
}

// AcceptOffer is a saga across two databases: the offer here, the ride in the ride service.
//  1. Claim the offer (guarded update: still this driver's, pending, unexpired).
//  2. Ask ride to assign the driver. Its AssignDriver is idempotent, so unknown outcomes are retried safely.
//  3. Ride's definite answer decides: matched, or the offer is cancelled.
//
// If ride never answers, the offer stays accepted and Tick keeps confirming it, so the step can't be lost.
func (s *MatchingService) AcceptOffer(ctx context.Context, offerID, driverID string) (string, error) {
	if !uuidPattern.MatchString(offerID) {
		return "", fmt.Errorf("%w: offer_id must be a UUID", ErrInvalidInput)
	}
	rideID, err := s.repository.AnswerOffer(ctx, offerID, driverID, "accepted")
	if err != nil {
		return "", err
	}
	err = s.confirm(ctx, Offer{ID: offerID, RideID: rideID, DriverID: driverID})
	if errors.Is(err, ErrAssignPending) {
		// The wrapped detail (ride's internal error) is for Tick's logs, not for the driver.
		return "", ErrAssignPending
	}
	return rideID, err
}

// DeclineOffer records the answer and offers the ride to the next driver right away instead of on the next tick.
func (s *MatchingService) DeclineOffer(ctx context.Context, offerID, driverID string) error {
	if !uuidPattern.MatchString(offerID) {
		return fmt.Errorf("%w: offer_id must be a UUID", ErrInvalidInput)
	}
	rideID, err := s.repository.AnswerOffer(ctx, offerID, driverID, "declined")
	if err != nil {
		return err
	}
	// Best effort: if this fails, Tick re-offers within a second. The driver's decline already succeeded.
	if in, err := s.repository.MatchingRequest(ctx, rideID); err == nil && in != nil {
		s.offerNext(ctx, *in)
	}
	return nil
}

// confirm asks ride to assign an accepted offer's driver and settles the offer on ride's definite answer.
// It is safe to run more than once for the same offer (AcceptOffer and Tick may both run it).
func (s *MatchingService) confirm(ctx context.Context, o Offer) error {
	err := s.assign(ctx, o.RideID, o.DriverID)
	switch status.Code(err) {
	case codes.OK:
		if done, err := s.repository.FinishRequest(ctx, o.RideID, "matched"); err != nil {
			return err
		} else if done {
			requestsFinished.WithLabelValues("matched").Inc()
		}
		return nil
	case codes.FailedPrecondition:
		// The ride was cancelled or taken: matching for it is over.
		if err := s.repository.CancelAccepted(ctx, o.ID); err != nil {
			return err
		}
		if done, err := s.repository.FinishRequest(ctx, o.RideID, "cancelled"); err != nil {
			return err
		} else if done {
			requestsFinished.WithLabelValues("ride_unavailable").Inc()
		}
		return ErrOfferUnavailable
	case codes.AlreadyExists:
		// This driver is on another ride; the request stays matching and Tick offers the next driver.
		if err := s.repository.CancelAccepted(ctx, o.ID); err != nil {
			return err
		}
		return ErrOfferUnavailable
	default:
		return fmt.Errorf("%w: %v", ErrAssignPending, err)
	}
}

// assign calls ride's AssignDriver, retrying while the outcome is unknown (unavailable or timed out).
func (s *MatchingService) assign(ctx context.Context, rideID, driverID string) error {
	req := &ridev1.AssignDriverRequest{RideId: rideID, DriverId: driverID}
	for attempt := 1; ; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		_, err := s.ride.AssignDriver(callCtx, req)
		cancel()
		code := status.Code(err)
		if (code != codes.Unavailable && code != codes.DeadlineExceeded) || attempt == assignAttempts {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(assignBackoff * time.Duration(attempt)):
		}
	}
}

var (
	offersCreated = promauto.NewCounter(prometheus.CounterOpts{
		Name: "matching_offers_created_total", Help: "Offers sent to drivers.",
	})
	offersExpired = promauto.NewCounter(prometheus.CounterOpts{
		Name: "matching_offers_expired_total", Help: "Offers that ran out of time without an answer.",
	})
	requestsFinished = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "matching_requests_finished_total", Help: "Match requests that ended, by outcome.",
	}, []string{"outcome"})
)

// candidates returns drivers who could take the ride, nearest first: online near the pickup (location),
// not already offered it or holding another offer (our offers), and not on a ride (ride).
func (s *MatchingService) candidates(ctx context.Context, in StartInput) ([]string, error) {
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	nearby, err := s.location.ListNearbyDrivers(callCtx, &locationv1.ListNearbyDriversRequest{
		Location:     &locationv1.Location{Lat: in.Pickup.Lat, Lng: in.Pickup.Lng},
		RadiusMeters: searchRadiusMeters,
		Limit:        candidateLimit,
	})
	if err != nil {
		return nil, fmt.Errorf("list nearby drivers: %w", err)
	}
	ids := make([]string, len(nearby.GetDrivers()))
	for i, d := range nearby.GetDrivers() {
		ids[i] = d.GetDriverId()
	}
	if len(ids) == 0 {
		return nil, nil
	}

	excluded, err := s.repository.ExcludedDrivers(ctx, in.RideID, ids)
	if err != nil {
		return nil, err
	}
	open := make([]string, 0, len(ids))
	for _, id := range ids {
		if !excluded[id] {
			open = append(open, id)
		}
	}
	if len(open) == 0 {
		return nil, nil
	}

	available, err := s.ride.FilterAvailableDrivers(callCtx, &ridev1.FilterAvailableDriversRequest{DriverIds: open})
	if err != nil {
		return nil, fmt.Errorf("filter available drivers: %w", err)
	}
	return available.GetDriverIds(), nil
}
