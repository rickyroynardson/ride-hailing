package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync/atomic"
	"time"

	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// heartbeatEvery keeps drivers inside location's 30s freshness window, well above its 1s rate limit.
	heartbeatEvery = 4 * time.Second
	// rideTimeout gives up on a ride well after matching would (it gives up at 2 minutes).
	rideTimeout = 4 * time.Minute
	// chargeTimeout gives up waiting for payment to capture.
	chargeTimeout = time.Minute
	// chargePollEvery stays far under the gateway's per-user limit.
	chargePollEvery = 500 * time.Millisecond
)

type world struct {
	api   *api
	rec   *recorder
	rides *rides
	// center and spread bound where drivers wait and riders are picked up, in degrees.
	center   *locationv1.Location
	spread   float64
	acceptIn time.Duration
	pickupIn time.Duration
	tripFor  time.Duration
}

func (w *world) randomPoint() *locationv1.Location {
	return &locationv1.Location{
		Lat: w.center.GetLat() + (rand.Float64()*2-1)*w.spread,
		Lng: w.center.GetLng() + (rand.Float64()*2-1)*w.spread,
	}
}

// --- drivers ---

type driver struct {
	w    *world
	s    *session
	pos  *locationv1.Location
	busy atomic.Bool
}

// run keeps the driver online and answers every offer, until ctx ends.
func (d *driver) run(ctx context.Context) {
	go d.heartbeat(ctx)
	seen := map[string]bool{}
	for ctx.Err() == nil {
		err := d.w.api.watch(ctx, "/v1/driver/offer:watch", d.s.token(ctx),
			func() proto.Message { return &matchingv1.WatchOffersResponse{} },
			func(m proto.Message) {
				o := m.(*matchingv1.WatchOffersResponse).GetOffer()
				if o == nil || seen[o.GetOfferId()] {
					return
				}
				seen[o.GetOfferId()] = true
				d.w.rides.offered(o.GetRideId(), time.Now())
				if d.busy.CompareAndSwap(false, true) {
					go d.take(ctx, o)
				} else {
					// Mid-trip, or the offer arrived as the last trip ended: say no at once, as the app would, rather
					// than leave the rider waiting out the offer's 15 seconds.
					go d.decline(ctx, o)
				}
			})
		if ctx.Err() != nil {
			return
		}
		d.w.rec.count(d.w.rec.current(), fmt.Sprintf("fail offer stream %d", statusOf(err)))
		sleep(ctx, time.Second) // the stream dropped (e.g. a matching restart): reconnect, as an app would
	}
}

func (d *driver) heartbeat(ctx context.Context) {
	t := time.NewTicker(heartbeatEvery)
	defer t.Stop()
	for {
		req := &locationv1.UpdateDriverLocationRequest{Location: d.pos, AccuracyMeters: 5, RecordedAt: timestamppb.Now()}
		var res locationv1.UpdateDriverLocationResponse
		if err := d.w.api.call(ctx, http.MethodPut, "/v1/driver/location", "/v1/driver/location", d.s.token(ctx), req, &res); err != nil && ctx.Err() == nil {
			d.w.rec.count(d.w.rec.current(), fmt.Sprintf("fail heartbeat %d", statusOf(err)))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (d *driver) decline(ctx context.Context, o *matchingv1.Offer) {
	d.w.rec.count(d.w.rec.current(), "declined busy")
	err := d.w.api.call(ctx, http.MethodPost, "/v1/offers/{offer_id=*}:decline", "/v1/offers/"+o.GetOfferId()+":decline", d.s.token(ctx), nil, nil)
	if err != nil && statusOf(err) != http.StatusBadRequest && ctx.Err() == nil {
		d.w.rec.count(d.w.rec.current(), fmt.Sprintf("fail decline %d", statusOf(err)))
	}
}

// take accepts the offer, then drives the trip.
func (d *driver) take(ctx context.Context, o *matchingv1.Offer) {
	defer d.busy.Store(false)
	sleep(ctx, d.w.acceptIn)

	var accepted matchingv1.AcceptOfferResponse
	err := retry(ctx, func() error {
		return d.w.api.call(ctx, http.MethodPost, "/v1/offers/{offer_id=*}:accept", "/v1/offers/"+o.GetOfferId()+":accept", d.s.token(ctx), nil, &accepted)
	})
	if err != nil {
		if statusOf(err) != http.StatusBadRequest { // 400: the offer expired or was withdrawn, which happens
			d.w.rec.count(d.w.rec.current(), fmt.Sprintf("fail accept %d", statusOf(err)))
		}
		return
	}
	for _, step := range []struct {
		name string
		wait time.Duration
	}{{"start", d.w.pickupIn}, {"complete", d.w.tripFor}} {
		if !sleep(ctx, step.wait) {
			return
		}
		path := "/v1/rides/" + o.GetRideId() + ":" + step.name
		if err := retry(ctx, func() error {
			return d.w.api.call(ctx, http.MethodPost, "/v1/rides/{ride_id=*}:"+step.name, path, d.s.token(ctx), nil, nil)
		}); err != nil {
			d.w.rec.count(d.w.rec.current(), fmt.Sprintf("fail %s %d", step.name, statusOf(err)))
			return
		}
	}
}

// --- riders ---

type rider struct {
	w *world
	s *session
}

// run books rides one after another until stop is set or ctx ends, finishing the ride in progress when stop is set.
func (r *rider) run(ctx context.Context, stop *atomic.Bool) {
	for ctx.Err() == nil && !stop.Load() {
		if !r.ride(ctx) {
			sleep(ctx, time.Second)
		}
		sleep(ctx, time.Duration(rand.IntN(1000))*time.Millisecond) // spread riders out
	}
}

// ride books one ride, follows it to the end, then waits for its charge. It reports whether the ride completed.
func (r *rider) ride(ctx context.Context) bool {
	rec, stage := r.w.rec, r.w.rec.current()
	pickup := r.w.randomPoint()
	dropoff := &ridev1.Location{Lat: pickup.GetLat() + 0.015, Lng: pickup.GetLng() + 0.01}

	booked := time.Now()
	var created ridev1.CreateRideResponse
	if err := r.w.api.call(ctx, http.MethodPost, "/v1/rides", "/v1/rides", r.s.token(ctx),
		&ridev1.CreateRideRequest{Pickup: &ridev1.Location{Lat: pickup.GetLat(), Lng: pickup.GetLng()}, Dropoff: dropoff}, &created); err != nil {
		if ctx.Err() == nil {
			rec.count(stage, fmt.Sprintf("fail book %d", statusOf(err)))
		}
		return false
	}
	rec.count(stage, "booked")
	r.w.rides.booked(created.GetRideId(), booked, stage)

	// Follow the ride on its stream, as the rider app does; reconnect if it drops before the ride ends.
	ctx, cancel := context.WithTimeout(ctx, rideTimeout)
	defer cancel()
	var final ridev1.RideStatus
	var completedAt time.Time
	acceptedSeen := false
	for final == ridev1.RideStatus_RIDE_STATUS_UNSPECIFIED && ctx.Err() == nil {
		err := r.w.api.watch(ctx, "/v1/rides/"+created.GetRideId()+":watch", r.s.token(ctx),
			func() proto.Message { return &ridev1.WatchRideResponse{} },
			func(m proto.Message) {
				ride := m.(*ridev1.WatchRideResponse).GetRide()
				switch ride.GetStatus() {
				case ridev1.RideStatus_RIDE_STATUS_ACCEPTED, ridev1.RideStatus_RIDE_STATUS_IN_PROGRESS:
					if !acceptedSeen {
						acceptedSeen = true
						rec.observe(stage, "to accepted", time.Since(booked))
					}
				case ridev1.RideStatus_RIDE_STATUS_COMPLETED, ridev1.RideStatus_RIDE_STATUS_CANCELLED:
					final, completedAt = ride.GetStatus(), time.Now()
					if ride.GetCancelReason() == ridev1.CancelReason_CANCEL_REASON_NO_DRIVER {
						rec.count(stage, "no driver")
					}
				}
			})
		if err != nil && ctx.Err() == nil {
			rec.count(stage, fmt.Sprintf("fail ride stream %d", statusOf(err)))
			sleep(ctx, time.Second)
		}
	}
	if final != ridev1.RideStatus_RIDE_STATUS_COMPLETED {
		if final == ridev1.RideStatus_RIDE_STATUS_UNSPECIFIED {
			rec.count(stage, "fail ride timed out")
		}
		return false
	}
	rec.count(stage, "completed")

	// The charge appears once ride's outbox delivers it (404 until then), and captures once payment's worker runs.
	for time.Since(completedAt) < chargeTimeout {
		var res paymentv1.GetChargeResponse
		err := r.w.api.call(ctx, http.MethodGet, "/v1/rides/{ride_id=*}/charge", "/v1/rides/"+created.GetRideId()+"/charge", r.s.token(ctx), nil, &res)
		switch {
		case err == nil && res.GetCharge().GetStatus() == paymentv1.ChargeStatus_CHARGE_STATUS_CAPTURED:
			rec.observe(stage, "to charged", time.Since(completedAt))
			return true
		case err == nil && res.GetCharge().GetStatus() == paymentv1.ChargeStatus_CHARGE_STATUS_FAILED:
			rec.count(stage, "fail charge declined")
			return true
		case err != nil && statusOf(err) != http.StatusNotFound:
			if ctx.Err() != nil {
				return true
			}
			rec.count(stage, fmt.Sprintf("fail charge poll %d", statusOf(err)))
		}
		if !sleep(ctx, chargePollEvery) {
			return true
		}
	}
	rec.count(stage, "fail charge timed out")
	return true
}

// retry repeats an idempotent call while the outcome is unknown (503, 504, network), as the API docs allow.
func retry(ctx context.Context, fn func() error) error {
	var err error
	for attempt := range 5 {
		if err = fn(); err == nil {
			return nil
		}
		if s := statusOf(err); s != 0 && s != http.StatusServiceUnavailable && s != http.StatusGatewayTimeout {
			return err
		}
		if !sleep(ctx, time.Duration(200<<attempt)*time.Millisecond) {
			return err
		}
	}
	return err
}

// sleep waits d, or until ctx ends; it reports whether the full wait happened.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
