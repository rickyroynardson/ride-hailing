package main

import (
	"context"
	"net/http"
	"slices"
	"time"

	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	"google.golang.org/protobuf/proto"
)

// handleGetOffer calls GetOffer as the driver, the way a driver app polls for offers.
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
	res, err := c.call(a.Name, "matching.v1.MatchingService/GetOffer", tok, false, &matchingv1.GetOfferRequest{}, func(ctx context.Context) (proto.Message, error) {
		return c.matching.GetOffer(ctx, &matchingv1.GetOfferRequest{})
	})
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
		res, err = c.call(a.Name, "matching.v1.MatchingService/AcceptOffer", tok, false, req, func(ctx context.Context) (proto.Message, error) {
			return c.matching.AcceptOffer(ctx, req)
		})
	} else {
		req := &matchingv1.DeclineOfferRequest{OfferId: offerID}
		res, err = c.call(a.Name, "matching.v1.MatchingService/DeclineOffer", tok, false, req, func(ctx context.Context) (proto.Message, error) {
			return c.matching.DeclineOffer(ctx, req)
		})
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, marshal(res))
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
	if a.Kind == "driver" && a.online {
		close(a.stopBeat)
		a.online = false
		if tok, err := c.token(a); err == nil {
			c.call(a.Name, "location.v1.LocationService/GoOffline", tok, false, &locationv1.GoOfflineRequest{}, func(ctx context.Context) (proto.Message, error) {
				return c.location.GoOffline(ctx, &locationv1.GoOfflineRequest{})
			})
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

// handleReset wipes local dev data so testing starts clean: every ride, offer, match request and driver location,
// plus the users the console created (their refresh tokens and driver profiles go with them by cascade). Users
// you registered yourself are kept.
func (c *console) handleReset(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	actors := c.actors
	c.actors, c.byID, c.calls, c.seq = nil, map[string]*actor{}, nil, 0
	c.mu.Unlock()
	for _, a := range actors {
		a.mu.Lock()
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
