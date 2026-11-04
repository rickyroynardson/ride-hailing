package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"
	paymentv1 "github.com/rickyroynardson/ride-hailing/proto/gen/payment/v1"
	ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func TestPublicRoutes(t *testing.T) {
	// Every annotated RPC must be usable without a gRPC client or a hand-maintained second route table.
	n := 0
	protoregistry.GlobalFiles.RangeFiles(func(file protoreflect.FileDescriptor) bool {
		for i := range file.Services().Len() {
			svc := file.Services().Get(i)
			for j := range svc.Methods().Len() {
				m := svc.Methods().Get(j)
				typeOf, err := protoregistry.GlobalTypes.FindMessageByName(m.Input().FullName())
				if err != nil {
					t.Fatal(err)
				}
				verb, path, _, err := publicRoute(string(svc.FullName())+"/"+string(m.Name()), typeOf.New().Interface())
				if err == nil {
					n++
					if verb == "" || !strings.HasPrefix(path, "/v1/") {
						t.Fatalf("bad route for %s: %s %s", m.FullName(), verb, path)
					}
				}
			}
		}
		return true
	})
	if n != 23 {
		t.Fatalf("found %d public routes, want 23", n)
	}
	for _, tc := range []struct {
		method string
		req    proto.Message
	}{
		{"ride.v1.RideService/AssignDriver", &ridev1.AssignDriverRequest{}},
		{"payment.v1.PaymentService/RefundCharge", &paymentv1.RefundChargeRequest{}},
	} {
		if _, _, _, err := publicRoute(tc.method, tc.req); err == nil {
			t.Fatalf("internal RPC %s got an HTTP route", tc.method)
		}
	}
	_, path, _, err := publicRoute("ride.v1.RideService/CancelRide", &ridev1.CancelRideRequest{RideId: "a/b?c"})
	if err != nil || path != "/v1/rides/a%2Fb%3Fc:cancel" {
		t.Fatalf("path parameter wasn't escaped: %q %v", path, err)
	}
}

func TestPublicCallUsesHTTPAndRecordsIt(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1/rides" || r.Header.Get("Authorization") != "Bearer user-token" {
			t.Errorf("wrong request: %s %s auth=%s", r.Method, r.URL, r.Header.Get("Authorization"))
		}
		var body struct{ Pickup struct{ Lat float64 } }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Pickup.Lat != -6.2 {
			t.Errorf("wrong JSON body: %+v %v", body, err)
		}
		fmt.Fprint(w, `{"rideId":"r1","fare":{"amount":"12000","currency":"IDR"}}`)
	}))
	defer s.Close()
	c := &console{gateway: newGatewayClient(s.URL)} // no secret or direct service clients
	res, err := c.call("rider", "ride.v1.RideService/CreateRide", "user-token", false,
		&ridev1.CreateRideRequest{Pickup: &ridev1.Location{Lat: -6.2}, Dropoff: &ridev1.Location{Lat: -6.3}})
	if err != nil || res.(*ridev1.CreateRideResponse).GetFare().GetAmount() != 12000 {
		t.Fatalf("response: %v %v", res, err)
	}
	if len(c.calls) != 1 || c.calls[0].Transport != "HTTP" || c.calls[0].HTTPStatus != 200 || c.calls[0].Path != "/v1/rides" {
		t.Fatalf("call log: %+v", c.calls)
	}
}

func TestGatewayErrorsAreNotHidden(t *testing.T) {
	for _, code := range []int{401, 403, 429, 503} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(code)
				fmt.Fprint(w, `{"code":8,"message":"try later","details":[]}`)
			}))
			defer s.Close()
			c := &console{gateway: newGatewayClient(s.URL)}
			_, err := c.call("rider", "ride.v1.RideService/CreateRide", "t", false, &ridev1.CreateRideRequest{})
			if status.Code(err) != codes.ResourceExhausted || c.calls[0].HTTPStatus != code {
				t.Fatalf("lost gateway error: %+v %v", c.calls, err)
			}
			w := httptest.NewRecorder()
			writeErr(w, err)
			if w.Code != code {
				t.Fatalf("UI returned %d, gateway returned %d", w.Code, code)
			}
		})
	}
}

func TestWatchDeliversWithoutWaitingForEOF(t *testing.T) {
	ended := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(ended)
		fmt.Fprintln(w, `{"result":{"offer":{"offerId":"o1","rideId":"r1"}}}`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer s.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := make(chan proto.Message, 1)
	done := make(chan error, 1)
	go func() {
		done <- newGatewayClient(s.URL).watch(ctx, "matching.v1.MatchingService/WatchOffers", "token", &matchingv1.WatchOffersRequest{}, func() {}, func(m proto.Message) bool {
			got <- m
			return true
		})
	}()
	select {
	case m := <-got:
		if m.(*matchingv1.WatchOffersResponse).GetOffer().GetOfferId() != "o1" {
			t.Fatal(m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("did not deliver the first frame while the stream was still open")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watch ignored cancellation")
	}
	select {
	case <-ended:
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP request stayed open")
	}
}

func TestWatchInBandError(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"error":{"code":16,"message":"token expired"}}`)
	}))
	defer s.Close()
	err := newGatewayClient(s.URL).watch(context.Background(), "matching.v1.MatchingService/WatchOffers", "token", &matchingv1.WatchOffersRequest{}, func() {}, func(proto.Message) bool { t.Fatal("error treated as data"); return true })
	if status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
}

func TestOffersReconnectClearAndCancel(t *testing.T) {
	var connections atomic.Int32
	second := make(chan struct{})
	clear := make(chan struct{})
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if connections.Add(1) == 1 {
			fmt.Fprintln(w, `{"result":{"offer":{"offerId":"o1","rideId":"r1","expiresAt":"2030-01-01T00:00:00Z"}}}`)
			return // EOF: the driver must reconnect, not finish watching
		}
		fmt.Fprintln(w, `{"result":{"offer":{"offerId":"o2","rideId":"r2","expiresAt":"2030-01-01T00:00:00Z"}}}`)
		w.(http.Flusher).Flush()
		close(second)
		<-clear
		fmt.Fprintln(w, `{"result":{"offer":null}}`)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer s.Close()
	c := &console{gateway: newGatewayClient(s.URL), pages: map[chan struct{}]struct{}{}}
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, time.Now().Add(time.Hour).Unix())))
	a := &actor{Name: "driver", access: "header." + payload + ".sig"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { c.watchOffers(ctx, a); close(done) }()
	select {
	case <-second:
	case <-time.After(3 * time.Second):
		t.Fatal("did not reconnect after EOF")
	}
	wait := func(check func(actorView) bool) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if check(a.view()) {
				return
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("stream state did not update")
	}
	wait(func(v actorView) bool { return v.OffersConnected && v.Offer != nil && v.Offer.ID == "o2" })
	close(clear)
	wait(func(v actorView) bool { return v.OffersConnected && v.Offer == nil })
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("driver watcher did not stop")
	}
	if a.view().OffersConnected {
		t.Fatal("cancelled watcher still shows live")
	}
}

func TestRefreshUsesGateway(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/auth/refresh" || r.Header.Get("Authorization") != "" {
			t.Errorf("unexpected refresh request: %s %v", r.URL, r.Header)
		}
		var body struct{ RefreshToken string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RefreshToken != "refresh-old" {
			t.Errorf("refresh body: %+v %v", body, err)
		}
		fmt.Fprint(w, `{"tokens":{"accessToken":"access-new","refreshToken":"refresh-new"}}`)
	}))
	defer s.Close()
	c := &console{gateway: newGatewayClient(s.URL)}
	a := &actor{Name: "driver", refresh: "refresh-old"}
	a.mu.Lock()
	tok, err := c.token(a)
	a.mu.Unlock()
	if err != nil || tok != "access-new" || a.refresh != "refresh-new" {
		t.Fatalf("refresh didn't replace the single-use token: %q %q %v", tok, a.refresh, err)
	}
}

func TestRemoveStopsBothWatchers(t *testing.T) {
	rideCtx, stopRide := context.WithCancel(context.Background())
	offersCtx, stopOffers := context.WithCancel(context.Background())
	a := &actor{ID: "driver", Kind: "driver", stopWatch: stopRide, stopOffers: stopOffers}
	c := &console{actors: []*actor{a}, byID: map[string]*actor{a.ID: a}}
	r := httptest.NewRequest("POST", "/api/actors/driver/remove", nil)
	r.SetPathValue("id", a.ID)
	w := httptest.NewRecorder()
	c.handleRemoveActor(w, r)
	if w.Code != http.StatusOK || rideCtx.Err() == nil || offersCtx.Err() == nil || len(c.actors) != 0 {
		t.Fatal("removing the actor left a watcher or session behind")
	}
}
