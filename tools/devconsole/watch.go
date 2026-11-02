package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// liveRide is what a rider's WatchRide stream last said.
type liveRide struct {
	RideID         string  `json:"rideId"`
	Status         string  `json:"status"`
	DriverLocation *latLng `json:"driverLocation,omitempty"`
	Updates        int     `json:"updates"`
}

// watchRide follows the rider's ride the way a rider app does: reconnect with a fresh token when the stream breaks
// (the server ends connections after 15 minutes, or ride restarted), until the ride finishes or ctx is cancelled.
func (c *console) watchRide(ctx context.Context, a *actor, rideID string) {
	for ctx.Err() == nil {
		a.mu.Lock()
		tok, err := c.token(a)
		a.mu.Unlock()
		if err == nil && c.followStream(ctx, a, rideID, tok) {
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
}

// followStream reads one stream and reports whether watching is over, as opposed to worth reconnecting.
func (c *console) followStream(ctx context.Context, a *actor, rideID, tok string) bool {
	req := &ridev1.WatchRideRequest{RideId: rideID}
	stream, err := c.ride.WatchRide(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok), req)
	for err == nil {
		start := time.Now()
		var res *ridev1.WatchRideResponse
		if res, err = stream.Recv(); err == nil {
			c.onRideUpdate(a, res, start)
		}
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	c.record(callEntry{
		At: time.Now(), Actor: a.Name, Method: "ride.v1.RideService/WatchRide", Request: marshal(req), Response: marshal(nil),
		Code: status.Code(err).String(), Error: status.Convert(err).Message(),
	})
	return status.Code(err) == codes.NotFound
}

// onRideUpdate keeps the message for the page and logs it. Messages that only move the driver count as heartbeats, so
// they stay hidden by default like the location updates they mirror.
func (c *console) onRideUpdate(a *actor, res *ridev1.WatchRideResponse, waitingSince time.Time) {
	r := res.GetRide()
	live := liveRide{RideID: r.GetRideId(), Status: strings.ToLower(strings.TrimPrefix(r.GetStatus().String(), "RIDE_STATUS_"))}
	if l := r.GetDriverLocation(); l != nil {
		live.DriverLocation = &latLng{l.GetLat(), l.GetLng()}
	}

	a.mu.Lock()
	sameStatus := a.live != nil && a.live.Status == live.Status
	if a.live != nil {
		live.Updates = a.live.Updates
	}
	live.Updates++
	a.live = &live
	a.mu.Unlock()

	c.record(callEntry{
		At: time.Now(), Actor: a.Name, Method: "ride.v1.RideService/WatchRide", Heartbeat: sameStatus,
		Request: marshal(&ridev1.WatchRideRequest{RideId: r.GetRideId()}), Response: marshal(res), Code: codes.OK.String(),
		DurationMs: float64(time.Since(waitingSince).Microseconds()) / 1000,
	})
	c.tellPages()
}

func (c *console) tellPages() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for ch := range c.pages {
		select {
		case ch <- struct{}{}:
		default: // this page already has a refresh coming
		}
	}
}

// handleEvents tells a page to refresh whenever a stream brings news, over Server-Sent Events.
func (c *console) handleEvents(w http.ResponseWriter, r *http.Request) {
	ch := make(chan struct{}, 1)
	c.mu.Lock()
	c.pages[ch] = struct{}{}
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pages, ch)
		c.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ch:
			fmt.Fprint(w, "data: changed\n\n")
			w.(http.Flusher).Flush()
		}
	}
}
