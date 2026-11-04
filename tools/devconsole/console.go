package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// heartbeatEvery keeps online drivers inside the location service's 30s freshness window, well above its
	// 1s rate limit.
	heartbeatEvery = 4 * time.Second
	// maxCalls is how many recent calls the log keeps.
	maxCalls = 300
	// systemActor labels calls the console makes as a service, e.g. StartMatching before the ride service does it.
	systemActor = "devconsole"
)

type console struct {
	jwtSecret []byte
	// jaegerURL is where the page links each call's trace; the browser opens it, so it is the host-side address.
	jaegerURL string

	gateway *gatewayClient
	// Only development-only controls bypass the gateway.
	matching matchingv1.MatchingServiceClient
	payment  paymentv1.PaymentServiceClient

	usersDB, ridesDB, matchingDB, paymentsDB *pgxpool.Pool
	rdb                                      *redis.Client

	mu     sync.Mutex
	seq    int
	actors []*actor
	byID   map[string]*actor
	calls  []callEntry
	// pages holds a channel per open browser tab, to tell it when a stream brought news (handleEvents).
	pages map[chan struct{}]struct{}
}

// actor is one rider or driver session. mu guards its tokens and position, so the heartbeat and a click on the
// page never refresh the same refresh token twice (refresh tokens are single use).
type actor struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	Name string `json:"name"`

	mu              sync.Mutex
	access          string
	refresh         string
	lat, lng        float64
	online          bool
	lastSent        time.Time
	lastOutcome     string
	stopBeat        chan struct{}
	activeRideID    string
	stopWatch       context.CancelFunc
	live            *liveRide
	stopOffers      context.CancelFunc
	offer           *liveOffer
	offersConnected bool
	// card is the fake provider token last set; empty means the fake's default card.
	card string
}

type actorView struct {
	ID          string   `json:"id"`
	Kind        string   `json:"kind"`
	Name        string   `json:"name"`
	Online      bool     `json:"online"`
	Location    *latLng  `json:"location,omitempty"`
	LastOutcome string   `json:"lastOutcome,omitempty"`
	RideID      string   `json:"rideId,omitempty"`
	TokenExpiry int64    `json:"tokenExpiry"`
	Claims      []string `json:"claims"`
	// Live is the latest WatchRide message for the rider's ride.
	Live            *liveRide  `json:"live,omitempty"`
	Card            string     `json:"card,omitempty"`
	Offer           *liveOffer `json:"offer,omitempty"`
	OffersConnected bool       `json:"offersConnected"`
}

type latLng struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

type callEntry struct {
	At         time.Time       `json:"at"`
	Actor      string          `json:"actor"`
	Method     string          `json:"method"`
	Request    json.RawMessage `json:"request"`
	Response   json.RawMessage `json:"response"`
	Code       string          `json:"code"`
	Error      string          `json:"error,omitempty"`
	DurationMs float64         `json:"durationMs"`
	Heartbeat  bool            `json:"heartbeat"`
	Transport  string          `json:"transport"`
	HTTPMethod string          `json:"httpMethod,omitempty"`
	Path       string          `json:"path,omitempty"`
	HTTPStatus int             `json:"httpStatus,omitempty"`
	// TraceID opens the call's whole journey in Jaeger; empty for untraced heartbeats or with tracing off.
	TraceID string `json:"traceId,omitempty"`
}

// call runs a public action through the gateway, or one of the two explicitly internal development controls, and
// records its transport and result. Public actions never use a service token or fall back to a direct gRPC call.
func (c *console) call(who, method, token string, heartbeat bool, req proto.Message) (proto.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Each call starts the trace of what it causes across the services. Heartbeats stay untraced: a driver's
	// location every 4s would bury everything else in Jaeger (the log hides them by default for the same reason).
	var traceID string
	if heartbeat {
		ctx = telemetry.Untraced(ctx)
	} else {
		var span trace.Span
		// Named after the actor's action, e.g. "rider-1: CreateRide", so a trace's first line reads as what the
		// user did; the HTTP client span below it carries the gateway request.
		ctx, span = otel.Tracer("devconsole").Start(ctx, who+": "+method[strings.LastIndex(method, "/")+1:],
			trace.WithAttributes(attribute.String("devconsole.actor", who)))
		defer span.End()
		if sc := span.SpanContext(); sc.IsValid() {
			traceID = sc.TraceID().String()
		}
	}
	if token != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	}

	start := time.Now()
	var res proto.Message
	var err error
	transport, httpMethod, path, httpStatus := "HTTP", "", "", 0
	switch method {
	case "matching.v1.MatchingService/StartMatching":
		transport = "internal gRPC"
		res, err = c.matching.StartMatching(ctx, req.(*matchingv1.StartMatchingRequest))
	case "payment.v1.PaymentService/RefundCharge":
		transport = "internal gRPC"
		res, err = c.payment.RefundCharge(ctx, req.(*paymentv1.RefundChargeRequest))
	default:
		res, httpMethod, path, httpStatus, err = c.gateway.call(ctx, method, token, req)
	}
	e := callEntry{
		At: start, Actor: who, Method: method, Heartbeat: heartbeat,
		Request: marshal(req), Response: marshal(res),
		Code: status.Code(err).String(), DurationMs: float64(time.Since(start).Microseconds()) / 1000,
		TraceID:   traceID,
		Transport: transport, HTTPMethod: httpMethod, Path: path, HTTPStatus: httpStatus,
	}
	if err != nil {
		e.Error = status.Convert(err).Message()
	}
	c.record(e)
	return res, err
}

func (c *console) record(e callEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, e)
	if len(c.calls) > maxCalls {
		c.calls = c.calls[len(c.calls)-maxCalls:]
	}
}

func marshal(m proto.Message) json.RawMessage {
	if m == nil {
		return json.RawMessage("null")
	}
	b, err := protojson.Marshal(m)
	if err != nil || len(b) == 0 {
		return json.RawMessage("{}")
	}
	return b
}

// token returns a valid access token, refreshing it first when it expires within a minute.
// Caller must hold a.mu.
func (c *console) token(a *actor) (string, error) {
	if time.Until(time.Unix(tokenExpiry(a.access), 0)) > time.Minute {
		return a.access, nil
	}
	res, err := c.call(a.Name, "user.v1.AuthService/Refresh", "", false, &userv1.RefreshRequest{RefreshToken: a.refresh})
	if err != nil {
		return "", err
	}
	t := res.(*userv1.RefreshResponse).GetTokens()
	a.access, a.refresh = t.GetAccessToken(), t.GetRefreshToken()
	return a.access, nil
}

// tokenExpiry and tokenClaims read the JWT payload without verifying it: the console only shows and schedules
// refreshes from them, the services do the verifying.
func tokenExpiry(tok string) int64 {
	var c struct {
		Exp int64 `json:"exp"`
	}
	decodePayload(tok, &c)
	return c.Exp
}

func tokenClaims(tok string) []string {
	var c struct {
		Driver bool `json:"driver"`
	}
	decodePayload(tok, &c)
	if c.Driver {
		return []string{"driver"}
	}
	return []string{}
}

func decodePayload(tok string, v any) {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return
	}
	if b, err := base64.RawURLEncoding.DecodeString(parts[1]); err == nil {
		json.Unmarshal(b, v)
	}
}

// serviceToken signs a service token through pkg/jwtauth, the same way the services sign theirs, for internal RPCs
// the console triggers by hand.
func (c *console) serviceToken() (string, error) {
	md, err := jwtauth.NewServiceCredentials(systemActor, string(c.jwtSecret)).GetRequestMetadata(context.Background())
	if err != nil {
		return "", err
	}
	return strings.TrimPrefix(md["authorization"], "Bearer "), nil
}

func (c *console) actor(id string) (*actor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if a := c.byID[id]; a != nil {
		return a, nil
	}
	return nil, fmt.Errorf("unknown actor %s", id)
}

func (a *actor) view() actorView {
	a.mu.Lock()
	defer a.mu.Unlock()
	v := actorView{
		ID: a.ID, Kind: a.Kind, Name: a.Name, Online: a.online, LastOutcome: a.lastOutcome, RideID: a.activeRideID,
		TokenExpiry: tokenExpiry(a.access), Claims: tokenClaims(a.access),
	}
	if !a.lastSent.IsZero() {
		v.Location = &latLng{a.lat, a.lng}
	}
	v.Live, v.Card = a.live, a.card
	v.Offer, v.OffersConnected = a.offer, a.offersConnected
	return v
}

// handleCreateActor registers a rider, or a driver that is approved and holds a token with the driver claim.
func (c *console) handleCreateActor(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Kind string `json:"kind"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil || (body.Kind != "rider" && body.Kind != "driver") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": `kind must be "rider" or "driver"`})
		return
	}

	c.mu.Lock()
	c.seq++
	n := c.seq
	c.mu.Unlock()
	stamp := time.Now().UnixNano()
	a := &actor{Kind: body.Kind, Name: fmt.Sprintf("%s-%d", body.Kind, n)}

	reg := &userv1.RegisterRequest{
		Email:    fmt.Sprintf("dev-%s-%d-%d@devconsole.local", body.Kind, n, stamp),
		Password: "password123",
		Name:     a.Name,
	}
	res, err := c.call(a.Name, "user.v1.AuthService/Register", "", false, reg)
	if err != nil {
		writeErr(w, err)
		return
	}
	created := res.(*userv1.RegisterResponse)
	a.ID = created.GetUser().GetId()
	a.access, a.refresh = created.GetTokens().GetAccessToken(), created.GetTokens().GetRefreshToken()

	if a.Kind == "driver" {
		if err := c.makeApprovedDriver(a, n, stamp); err != nil {
			writeErr(w, err)
			return
		}
	}

	// Install cancellation before publishing the actor: a concurrent reset must be able to stop its stream.
	if a.Kind == "driver" {
		ctx, cancel := context.WithCancel(context.Background())
		a.stopOffers = cancel
		go c.watchOffers(ctx, a)
	}
	c.mu.Lock()
	c.actors = append(c.actors, a)
	c.byID[a.ID] = a
	c.mu.Unlock()
	writeJSON(w, http.StatusCreated, a.view())
}

// makeApprovedDriver registers the driver profile, approves it by SQL (there is no admin RPC yet), and refreshes
// so the access token carries the driver claim.
func (c *console) makeApprovedDriver(a *actor, n int, stamp int64) error {
	req := &userv1.RegisterDriverRequest{
		LicenseNumber: fmt.Sprintf("DEV%d%d", n, stamp),
		VehiclePlate:  fmt.Sprintf("B%dDEV%d", n, stamp%1_000_000),
		VehicleModel:  "Toyota Avanza",
	}
	if _, err := c.call(a.Name, "user.v1.UserService/RegisterDriver", a.access, false, req); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.usersDB.Exec(ctx, `UPDATE driver_profiles SET status = 'approved' WHERE user_id = $1`, a.ID); err != nil {
		return fmt.Errorf("approve driver: %w", err)
	}

	a.access = "" // force a refresh: the driver claim is set when tokens are issued
	_, err := c.token(a)
	return err
}

// handleLocation moves a driver and reports the position right away; the first move also starts the heartbeat.
func (c *console) handleLocation(w http.ResponseWriter, r *http.Request) {
	a, err := c.actor(r.PathValue("id"))
	if err != nil || a.Kind != "driver" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "not a driver"})
		return
	}
	var p latLng
	if json.NewDecoder(r.Body).Decode(&p) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be {lat, lng}"})
		return
	}

	a.mu.Lock()
	a.lat, a.lng = p.Lat, p.Lng
	startBeat := !a.online
	a.online = true
	if startBeat {
		a.stopBeat = make(chan struct{})
		go c.heartbeat(a, a.stopBeat)
	}
	err = c.sendLocation(a, false)
	a.mu.Unlock()

	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a.view())
}

// heartbeat re-sends an online driver's position, like a phone does, until stop is closed.
func (c *console) heartbeat(a *actor, stop chan struct{}) {
	t := time.NewTicker(heartbeatEvery)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			a.mu.Lock()
			// A manual move just reported; skip this beat rather than hit the 1s rate limit.
			if time.Since(a.lastSent) >= heartbeatEvery/2 {
				c.sendLocation(a, true)
			}
			a.mu.Unlock()
		}
	}
}

// sendLocation reports the driver's position with a fresh device timestamp. Caller must hold a.mu.
func (c *console) sendLocation(a *actor, heartbeat bool) error {
	tok, err := c.token(a)
	if err != nil {
		return err
	}
	req := &locationv1.UpdateDriverLocationRequest{
		Location:       &locationv1.Location{Lat: a.lat, Lng: a.lng},
		AccuracyMeters: 8,
		RecordedAt:     timestamppb.Now(),
	}
	res, err := c.call(a.Name, "location.v1.LocationService/UpdateDriverLocation", tok, heartbeat, req)
	a.lastSent = time.Now()
	if err != nil {
		a.lastOutcome = status.Code(err).String()
		return err
	}
	a.lastOutcome = res.(*locationv1.UpdateDriverLocationResponse).GetOutcome().String()
	return nil
}

func (c *console) handleOffline(w http.ResponseWriter, r *http.Request) {
	a, err := c.actor(r.PathValue("id"))
	if err != nil || a.Kind != "driver" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "not a driver"})
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if a.online {
		close(a.stopBeat)
		a.online = false
	}
	tok, err := c.token(a)
	if err == nil {
		_, err = c.call(a.Name, "location.v1.LocationService/GoOffline", tok, false, &locationv1.GoOfflineRequest{})
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (c *console) handleRequestRide(w http.ResponseWriter, r *http.Request) {
	a, err := c.actor(r.PathValue("id"))
	if err != nil || a.Kind != "rider" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "not a rider"})
		return
	}
	var body struct {
		Pickup    latLng `json:"pickup"`
		Dropoff   latLng `json:"dropoff"`
		RequestID string `json:"requestId"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be {pickup, dropoff}"})
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	tok, err := c.token(a)
	if err != nil {
		writeErr(w, err)
		return
	}
	req := &ridev1.CreateRideRequest{
		RequestId: body.RequestID,
		Pickup:    &ridev1.Location{Lat: body.Pickup.Lat, Lng: body.Pickup.Lng},
		Dropoff:   &ridev1.Location{Lat: body.Dropoff.Lat, Lng: body.Dropoff.Lng},
	}
	res, err := c.call(a.Name, "ride.v1.RideService/CreateRide", tok, false, req)
	if err != nil {
		writeErr(w, err)
		return
	}
	a.activeRideID = res.(*ridev1.CreateRideResponse).GetRideId()
	if a.stopWatch != nil {
		a.stopWatch()
	}
	ctx, stop := context.WithCancel(context.Background())
	a.stopWatch, a.live = stop, nil
	go c.watchRide(ctx, a, a.activeRideID)
	writeJSON(w, http.StatusCreated, map[string]string{"rideId": a.activeRideID})
}

// handleStartMatching is a development-only override that bypasses the gateway with a service token. Normal rides
// start matching automatically through ride's outbox.
func (c *console) handleStartMatching(w http.ResponseWriter, r *http.Request) {
	rideID := r.PathValue("id")
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var p latLng
	if err := c.ridesDB.QueryRow(ctx, `SELECT pickup_lat, pickup_lng FROM rides WHERE id = $1`, rideID).Scan(&p.Lat, &p.Lng); err != nil {
		writeErr(w, fmt.Errorf("read ride pickup: %w", err))
		return
	}
	tok, err := c.serviceToken()
	if err != nil {
		writeErr(w, err)
		return
	}
	req := &matchingv1.StartMatchingRequest{RideId: rideID, Pickup: &matchingv1.Location{Lat: p.Lat, Lng: p.Lng}}
	res, err := c.call(systemActor, "matching.v1.MatchingService/StartMatching", tok, false, req)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, json.RawMessage(marshal(res)))
}
