package main

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"time"

	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"google.golang.org/protobuf/proto"
)

// handleGetOffer is an explicit HTTP snapshot for debugging. Normal offer cards use WatchOffers instead.
func (c *console) handleGetOffer(w http.ResponseWriter, r *http.Request) {
	a, err := c.actor(r.PathValue("id"))
	if err != nil || a.Kind != "driver" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "not a driver"})
		return
	}
	a.mu.Lock()
	tok, err := c.token(a)
	a.mu.Unlock()
	if err != nil {
		writeErr(w, err)
		return
	}
	res, err := c.call(a.Name, "matching.v1.MatchingService/GetOffer", tok, false, &matchingv1.GetOfferRequest{})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, marshal(res))
}

// handleAnswerOffer accepts or declines an offer as the driver.
func (c *console) handleAnswerOffer(w http.ResponseWriter, r *http.Request) {
	a, err := c.actor(r.PathValue("id"))
	if err != nil || a.Kind != "driver" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "not a driver"})
		return
	}
	offerID, answer := r.PathValue("offer"), r.PathValue("answer")
	if answer != "accept" && answer != "decline" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "answer must be accept or decline"})
		return
	}

	a.mu.Lock()
	tok, err := c.token(a)
	a.mu.Unlock()
	if err != nil {
		writeErr(w, err)
		return
	}
	var res proto.Message
	if answer == "accept" {
		req := &matchingv1.AcceptOfferRequest{OfferId: offerID}
		res, err = c.call(a.Name, "matching.v1.MatchingService/AcceptOffer", tok, false, req)
	} else {
		req := &matchingv1.DeclineOfferRequest{OfferId: offerID}
		res, err = c.call(a.Name, "matching.v1.MatchingService/DeclineOffer", tok, false, req)
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	if answer == "accept" {
		rideID := res.(*matchingv1.AcceptOfferResponse).GetRideId()
		a.mu.Lock()
		if a.stopWatch != nil {
			a.stopWatch()
		}
		ctx, cancel := context.WithCancel(context.Background())
		a.activeRideID, a.stopWatch, a.live = rideID, cancel, nil
		a.mu.Unlock()
		go c.watchRide(ctx, a, rideID)
	}
	writeJSON(w, http.StatusOK, marshal(res))
}

// handleCancelRide cancels the ride as its rider.
func (c *console) handleCancelRide(w http.ResponseWriter, r *http.Request) {
	a, err := c.actor(r.PathValue("id"))
	if err != nil || a.Kind != "rider" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "not a rider"})
		return
	}
	a.mu.Lock()
	tok, err := c.token(a)
	a.mu.Unlock()
	if err != nil {
		writeErr(w, err)
		return
	}
	req := &ridev1.CancelRideRequest{RideId: r.PathValue("ride")}
	if _, err := c.call(a.Name, "ride.v1.RideService/CancelRide", tok, false, req); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleRefund calls RefundCharge with a service token, standing in for a support tool. The page sends a fresh key per
// click, or a repeated one to show that a retry refunds once.
func (c *console) handleRefund(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Amount int64  `json:"amount"`
		Key    string `json:"key"`
		Reason string `json:"reason"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be {amount, key, reason}"})
		return
	}
	req := &paymentv1.RefundChargeRequest{RideId: r.PathValue("id"), Amount: body.Amount, IdempotencyKey: body.Key, Reason: body.Reason}
	res, err := c.call(systemActor, "payment.v1.PaymentService/RefundCharge", "", false, req)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, marshal(res))
}

// handleEstimateFare prices the rider's drafted trip, the way a rider app does before they book.
func (c *console) handleEstimateFare(w http.ResponseWriter, r *http.Request) {
	a, err := c.actor(r.PathValue("id"))
	if err != nil || a.Kind != "rider" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "not a rider"})
		return
	}
	var body struct {
		Pickup  latLng `json:"pickup"`
		Dropoff latLng `json:"dropoff"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be {pickup, dropoff}"})
		return
	}
	a.mu.Lock()
	tok, err := c.token(a)
	a.mu.Unlock()
	if err != nil {
		writeErr(w, err)
		return
	}
	req := &ridev1.EstimateFareRequest{
		Pickup:  &ridev1.Location{Lat: body.Pickup.Lat, Lng: body.Pickup.Lng},
		Dropoff: &ridev1.Location{Lat: body.Dropoff.Lat, Lng: body.Dropoff.Lng},
	}
	res, err := c.call(a.Name, "ride.v1.RideService/EstimateFare", tok, false, req)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, marshal(res))
}

// handleGetCharge calls GetCharge as the rider, the way a rider app checks how payment went.
func (c *console) handleGetCharge(w http.ResponseWriter, r *http.Request) {
	a, err := c.actor(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	a.mu.Lock()
	tok, err := c.token(a)
	a.mu.Unlock()
	if err != nil {
		writeErr(w, err)
		return
	}
	req := &paymentv1.GetChargeRequest{RideId: r.PathValue("ride")}
	res, err := c.call(a.Name, "payment.v1.PaymentService/GetCharge", tok, false, req)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, marshal(res))
}

// handleGetEarnings calls GetEarnings as the driver.
func (c *console) handleGetEarnings(w http.ResponseWriter, r *http.Request) {
	a, err := c.actor(r.PathValue("id"))
	if err != nil || a.Kind != "driver" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "not a driver"})
		return
	}
	a.mu.Lock()
	tok, err := c.token(a)
	a.mu.Unlock()
	if err != nil {
		writeErr(w, err)
		return
	}
	req := &paymentv1.GetEarningsRequest{}
	res, err := c.call(a.Name, "payment.v1.PaymentService/GetEarnings", tok, false, req)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, marshal(res))
}

// handleSetCard sets the rider's card to one of the fake provider's tokens.
func (c *console) handleSetCard(w http.ResponseWriter, r *http.Request) {
	a, err := c.actor(r.PathValue("id"))
	if err != nil || a.Kind != "rider" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "not a rider"})
		return
	}
	var body struct {
		Token string `json:"token"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be {token}"})
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	tok, err := c.token(a)
	if err != nil {
		writeErr(w, err)
		return
	}
	req := &paymentv1.SetPaymentMethodRequest{Token: body.Token}
	if _, err := c.call(a.Name, "payment.v1.PaymentService/SetPaymentMethod", tok, false, req); err != nil {
		writeErr(w, err)
		return
	}
	a.card = body.Token
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleRetryCharge tries the ride's declined charge again as its rider.
func (c *console) handleRetryCharge(w http.ResponseWriter, r *http.Request) {
	a, err := c.actor(r.PathValue("id"))
	if err != nil || a.Kind != "rider" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "not a rider"})
		return
	}
	a.mu.Lock()
	tok, err := c.token(a)
	a.mu.Unlock()
	if err != nil {
		writeErr(w, err)
		return
	}
	var body struct {
		RequestID string `json:"requestId"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be {requestId}"})
		return
	}
	req := &paymentv1.RetryChargeRequest{RideId: r.PathValue("ride"), RequestId: body.RequestID}
	if _, err := c.call(a.Name, "payment.v1.PaymentService/RetryCharge", tok, false, req); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleTripStep starts or completes the trip as the ride's driver.
func (c *console) handleTripStep(w http.ResponseWriter, r *http.Request) {
	a, err := c.actor(r.PathValue("id"))
	if err != nil || a.Kind != "driver" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "not a driver"})
		return
	}
	a.mu.Lock()
	tok, err := c.token(a)
	a.mu.Unlock()
	if err != nil {
		writeErr(w, err)
		return
	}

	rideID := r.PathValue("ride")
	switch r.PathValue("step") {
	case "start":
		req := &ridev1.StartRideRequest{RideId: rideID}
		_, err = c.call(a.Name, "ride.v1.RideService/StartRide", tok, false, req)
	case "complete":
		req := &ridev1.CompleteRideRequest{RideId: rideID}
		_, err = c.call(a.Name, "ride.v1.RideService/CompleteRide", tok, false, req)
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "step must be start or complete"})
		return
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleRemoveActor stops a driver's heartbeat, takes them offline, and forgets the session. Their user and
// rides stay in the databases.
func (c *console) handleRemoveActor(w http.ResponseWriter, r *http.Request) {
	a, err := c.actor(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	a.mu.Lock()
	if a.stopWatch != nil {
		a.stopWatch()
	}
	if a.stopOffers != nil {
		a.stopOffers()
	}
	if a.Kind == "driver" && a.online {
		close(a.stopBeat)
		a.online = false
		if tok, err := c.token(a); err == nil {
			c.call(a.Name, "location.v1.LocationService/GoOffline", tok, false, &locationv1.GoOfflineRequest{})
		}
	}
	a.mu.Unlock()

	c.mu.Lock()
	c.actors = slices.DeleteFunc(c.actors, func(x *actor) bool { return x == a })
	delete(c.byID, a.ID)
	c.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (c *console) handleClearCalls(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	c.calls = nil
	c.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleReset wipes local dev data so testing starts clean: every ride (and ride's outbox), offer, match request,
// charge, refund, ledger entry and driver location, plus the users the console created (their refresh tokens and driver profiles go with them by
// cascade). Users you registered yourself are kept.
func (c *console) handleReset(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	actors := c.actors
	c.actors, c.byID, c.calls, c.seq = nil, map[string]*actor{}, nil, 0
	c.mu.Unlock()
	for _, a := range actors {
		a.mu.Lock()
		if a.stopWatch != nil {
			a.stopWatch()
		}
		if a.stopOffers != nil {
			a.stopOffers()
		}
		if a.online {
			close(a.stopBeat)
			a.online = false
		}
		a.mu.Unlock()
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	removed := map[string]int64{}
	steps := []struct {
		name string
		run  func() (int64, error)
	}{
		{"offers", exec(ctx, c.matchingDB.Exec, `DELETE FROM offers`)},
		{"matchRequests", exec(ctx, c.matchingDB.Exec, `DELETE FROM match_requests`)},
		{"rides", exec(ctx, c.ridesDB.Exec, `DELETE FROM rides`)},
		{"outbox", exec(ctx, c.ridesDB.Exec, `DELETE FROM outbox`)},
		// TRUNCATE, because the ledger refuses deletes; that is the point of it outside a dev reset.
		{"payments", exec(ctx, c.paymentsDB.Exec, `TRUNCATE ledger_entries, refunds, charge_retry_requests, charges, payment_methods, fake_provider_operations, fake_provider_charges`)},
		{"consoleUsers", exec(ctx, c.usersDB.Exec, `DELETE FROM users WHERE email LIKE 'dev-%@devconsole.local'`)},
		{"redisKeys", func() (int64, error) {
			keys := []string{"drivers:geo", "drivers:last_seen"}
			iter := c.rdb.Scan(ctx, 0, "driver:rate:*", 1000).Iterator()
			for iter.Next(ctx) {
				keys = append(keys, iter.Val())
			}
			if err := iter.Err(); err != nil {
				return 0, err
			}
			return c.rdb.Del(ctx, keys...).Result()
		}},
	}
	for _, step := range steps {
		n, err := step.run()
		if err != nil {
			writeErr(w, err)
			return
		}
		removed[step.name] = n
	}
	writeJSON(w, http.StatusOK, removed)
}

func exec[T interface{ RowsAffected() int64 }](ctx context.Context, run func(context.Context, string, ...any) (T, error), sql string) func() (int64, error) {
	return func() (int64, error) {
		tag, err := run(ctx, sql)
		return tag.RowsAffected(), err
	}
}
