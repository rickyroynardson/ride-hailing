package server_test

import (
	"context"
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"github.com/rickyroynardson/ride-hailing/pkg/jwtauth"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	"github.com/rickyroynardson/ride-hailing/services/location/internal/database"
	"github.com/rickyroynardson/ride-hailing/services/location/internal/location"
	"github.com/rickyroynardson/ride-hailing/services/location/internal/server"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const testSecret = "test-secret"

// TestIntegration runs the real gRPC server, interceptors and Redis commands against a throwaway Redis container.
// Skipped with -short or when Docker is unavailable.
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	rc, err := tcredis.Run(ctx, "redis:8-alpine")
	testcontainers.CleanupContainer(t, rc)
	if err != nil {
		t.Fatal(err)
	}
	url, err := rc.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rdb, err := database.ConnectRedis(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rdb.Close() })

	client := startServer(t, rdb)

	const driverID = "33333333-3333-3333-3333-333333333333"
	driver := withToken(t, ctx, driverID, true)
	here, there := &locationv1.Location{Lat: -6.2, Lng: 106.8}, &locationv1.Location{Lat: -6.25, Lng: 106.85}
	now := time.Now()
	reading := func(l *locationv1.Location, accuracy float64, at time.Time) *locationv1.UpdateDriverLocationRequest {
		return &locationv1.UpdateDriverLocationRequest{Location: l, AccuracyMeters: accuracy, RecordedAt: timestamppb.New(at)}
	}
	// update clears the rate-limit mark first, so steps can run back to back; the limit has its own check below.
	update := func(name string, req *locationv1.UpdateDriverLocationRequest, want locationv1.UpdateOutcome) {
		t.Helper()
		rdb.Del(ctx, "driver:rate:"+driverID)
		res, err := client.UpdateDriverLocation(driver, req)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if res.GetOutcome() != want {
			t.Fatalf("%s: outcome %v, want %v", name, res.GetOutcome(), want)
		}
	}
	wantAt := func(name string, l *locationv1.Location, recordedAt time.Time) {
		t.Helper()
		pos, err := rdb.GeoPos(ctx, "drivers:geo", driverID).Result()
		if err != nil || pos[0] == nil {
			t.Fatalf("%s: GEOPOS %v %v", name, pos, err)
		}
		// Redis geo stores positions as a 52-bit geohash, accurate to well under a meter.
		if math.Abs(pos[0].Latitude-l.GetLat()) > 1e-5 || math.Abs(pos[0].Longitude-l.GetLng()) > 1e-5 {
			t.Fatalf("%s: GEOPOS got %+v, want %+v", name, pos[0], l)
		}
		if seen, err := rdb.ZScore(ctx, "drivers:last_seen", driverID).Result(); err != nil || int64(seen) != recordedAt.UnixMilli() {
			t.Fatalf("%s: last_seen got %v (%v), want %d", name, seen, err, recordedAt.UnixMilli())
		}
	}

	_, err = client.UpdateDriverLocation(ctx, reading(here, 10, now))
	wantCode(t, "no token", err, codes.Unauthenticated)

	_, err = client.UpdateDriverLocation(withToken(t, ctx, "44444444-4444-4444-4444-444444444444", false), reading(here, 10, now))
	wantCode(t, "rider", err, codes.PermissionDenied)

	for name, req := range map[string]*locationv1.UpdateDriverLocationRequest{
		"missing location":           {AccuracyMeters: 10, RecordedAt: timestamppb.New(now)},
		"missing accuracy":           {Location: here, RecordedAt: timestamppb.New(now)},
		"missing recorded_at":        {Location: here, AccuracyMeters: 10},
		"lat beyond redis geo range": reading(&locationv1.Location{Lat: 86}, 10, now),
		"recorded in the future":     reading(here, 10, now.Add(2*time.Minute)),
	} {
		_, err = client.UpdateDriverLocation(driver, req)
		wantCode(t, name, err, codes.InvalidArgument)
	}

	// A good reading is stored with its device timestamp; the next one moves the driver instead of adding a second entry.
	update("first", reading(here, 10, now.Add(-2*time.Second)), locationv1.UpdateOutcome_UPDATE_OUTCOME_STORED)
	wantAt("first", here, now.Add(-2*time.Second))
	update("second", reading(there, 10, now), locationv1.UpdateOutcome_UPDATE_OUTCOME_STORED)
	wantAt("second", there, now)
	if n, _ := rdb.ZCard(ctx, "drivers:geo").Result(); n != 1 {
		t.Fatalf("drivers:geo has %d members, want 1", n)
	}

	// Accepted but not stored: each leaves the second reading in place.
	update("arrived out of order", reading(here, 10, now.Add(-time.Second)), locationv1.UpdateOutcome_UPDATE_OUTCOME_IGNORED_OUT_OF_ORDER)
	wantAt("arrived out of order", there, now)
	update("inaccurate", reading(here, 51, now.Add(time.Second)), locationv1.UpdateOutcome_UPDATE_OUTCOME_IGNORED_INACCURATE)
	wantAt("inaccurate", there, now)
	update("too old", reading(here, 10, now.Add(-time.Minute)), locationv1.UpdateOutcome_UPDATE_OUTCOME_IGNORED_TOO_OLD)
	wantAt("too old", there, now)

	// A second update within a second is rejected and stores nothing; the limit lifts once the mark expires.
	update("before limit", reading(here, 10, now.Add(time.Second)), locationv1.UpdateOutcome_UPDATE_OUTCOME_STORED)
	_, err = client.UpdateDriverLocation(driver, reading(there, 10, now.Add(2*time.Second)))
	wantCode(t, "rate limited", err, codes.ResourceExhausted)
	wantAt("rate limited", here, now.Add(time.Second))
	if ttl, _ := rdb.PTTL(ctx, "driver:rate:"+driverID).Result(); ttl <= 0 || ttl > time.Second {
		t.Fatalf("rate-limit mark TTL %v, want within (0, 1s]", ttl)
	}

	// GoOffline removes the driver from search but keeps a last-seen marker at the offline time.
	_, err = client.GoOffline(ctx, &locationv1.GoOfflineRequest{})
	wantCode(t, "go offline without token", err, codes.Unauthenticated)
	offlineAt := time.Now()
	for range 2 {
		if _, err := client.GoOffline(driver, &locationv1.GoOfflineRequest{}); err != nil {
			t.Fatalf("go offline: %v", err)
		}
	}
	wantOffline := func(name string) {
		t.Helper()
		if pos, _ := rdb.GeoPos(ctx, "drivers:geo", driverID).Result(); pos[0] != nil {
			t.Fatalf("%s: driver still in drivers:geo at %+v", name, pos[0])
		}
	}
	wantOffline("after go offline")
	if seen, err := rdb.ZScore(ctx, "drivers:last_seen", driverID).Result(); err != nil || int64(seen) < offlineAt.UnixMilli() {
		t.Fatalf("offline marker: got %v (%v), want >= %d", seen, err, offlineAt.UnixMilli())
	}

	// An update that was in flight when the driver went offline must not bring them back.
	update("in flight", reading(there, 10, offlineAt.Add(-500*time.Millisecond)), locationv1.UpdateOutcome_UPDATE_OUTCOME_IGNORED_OUT_OF_ORDER)
	wantOffline("after in-flight update")

	// A reading newer than both the offline time and the earlier readings (stamped up to now+1s) brings them back.
	backAt := now.Add(3 * time.Second)
	update("back online", reading(there, 10, backAt), locationv1.UpdateOutcome_UPDATE_OUTCOME_STORED)
	wantAt("back online", there, backAt)

	// Non-drivers may call it; it removes nothing of anyone else's.
	if _, err := client.GoOffline(withToken(t, ctx, "44444444-4444-4444-4444-444444444444", false), &locationv1.GoOfflineRequest{}); err != nil {
		t.Fatalf("rider go offline: %v", err)
	}
	wantAt("after rider go offline", there, backAt)

	// GetDriverLocation is for services, and answers only for drivers online and fresh.
	_, err = client.GetDriverLocation(driver, &locationv1.GetDriverLocationRequest{DriverId: driverID})
	wantCode(t, "get location as user", err, codes.PermissionDenied)
	md, err := jwtauth.NewServiceCredentials("ride-service", testSecret).GetRequestMetadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	service := metadata.AppendToOutgoingContext(ctx, "authorization", md["authorization"])
	_, err = client.GetDriverLocation(service, &locationv1.GetDriverLocationRequest{})
	wantCode(t, "get location without driver", err, codes.InvalidArgument)
	position := func(id string) *locationv1.Location {
		t.Helper()
		res, err := client.GetDriverLocation(service, &locationv1.GetDriverLocationRequest{DriverId: id})
		if err != nil {
			t.Fatal(err)
		}
		return res.GetLocation()
	}
	if l := position("never-seen"); l != nil {
		t.Fatalf("unknown driver has location %v", l)
	}
	for id, seen := range map[string]time.Time{"fresh-one": time.Now(), "stale-one": time.Now().Add(-time.Hour)} {
		rdb.GeoAdd(ctx, "drivers:geo", &redis.GeoLocation{Name: id, Latitude: 10, Longitude: 20})
		rdb.ZAdd(ctx, "drivers:last_seen", redis.Z{Score: float64(seen.UnixMilli()), Member: id})
		defer rdb.ZRem(ctx, "drivers:geo", id)
		defer rdb.ZRem(ctx, "drivers:last_seen", id)
	}
	if l := position("fresh-one"); l == nil || math.Abs(l.GetLat()-10) > 0.001 || math.Abs(l.GetLng()-20) > 0.001 {
		t.Fatalf("fresh driver: got %v, want 10, 20", l)
	}
	if l := position("stale-one"); l != nil {
		t.Fatalf("stale driver has location %v", l)
	}
	// Going offline leaves a fresh last-seen marker but no position.
	rdb.ZAdd(ctx, "drivers:last_seen", redis.Z{Score: float64(time.Now().UnixMilli()), Member: "offline-one"})
	defer rdb.ZRem(ctx, "drivers:last_seen", "offline-one")
	if l := position("offline-one"); l != nil {
		t.Fatalf("offline driver has location %v", l)
	}

	// ListNearbyDrivers, around a point far from the drivers above. Drivers are written to Redis directly.
	center := &locationv1.Location{Lat: 1.30, Lng: 103.85}
	put := func(id string, metersNorth float64, seen time.Time) {
		t.Helper()
		// 1 degree of latitude is about 111,320 m.
		rdb.GeoAdd(ctx, "drivers:geo", &redis.GeoLocation{Name: id, Latitude: center.GetLat() + metersNorth/111320, Longitude: center.GetLng()})
		rdb.ZAdd(ctx, "drivers:last_seen", redis.Z{Score: float64(seen.UnixMilli()), Member: id})
	}
	fresh, stale := time.Now(), time.Now().Add(-time.Minute)
	put("near-500", 500, fresh)
	put("near-1000", 1000, fresh)
	put("stale-1500", 1500, stale)
	put("far-20000", 20000, fresh)
	rdb.GeoAdd(ctx, "drivers:geo", &redis.GeoLocation{Name: "no-last-seen", Latitude: center.GetLat() + 2000.0/111320, Longitude: center.GetLng()})

	rider := withToken(t, ctx, "55555555-5555-5555-5555-555555555555", false)
	list := func(name string, req *locationv1.ListNearbyDriversRequest) []string {
		t.Helper()
		res, err := client.ListNearbyDrivers(rider, req)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		ids := make([]string, len(res.GetDrivers()))
		for i, d := range res.GetDrivers() {
			ids[i] = d.GetDriverId()
		}
		return ids
	}
	wantIDs := func(name string, got []string, want ...string) {
		t.Helper()
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s: got %v, want %v", name, got, want)
		}
	}
	_, err = client.ListNearbyDrivers(ctx, &locationv1.ListNearbyDriversRequest{Location: center})
	wantCode(t, "list without token", err, codes.Unauthenticated)
	for name, req := range map[string]*locationv1.ListNearbyDriversRequest{
		"list without location": {},
		"radius too big":        {Location: center, RadiusMeters: 10001},
		"limit too big":         {Location: center, Limit: 51},
	} {
		_, err = client.ListNearbyDrivers(rider, req)
		wantCode(t, name, err, codes.InvalidArgument)
	}

	// Nearest first; stale and out-of-radius drivers excluded.
	res, err := client.ListNearbyDrivers(rider, &locationv1.ListNearbyDriversRequest{Location: center, RadiusMeters: 3000})
	if err != nil {
		t.Fatal(err)
	}
	if d := res.GetDrivers(); len(d) != 2 || d[0].GetDriverId() != "near-500" || math.Abs(d[0].GetDistanceMeters()-500) > 5 || d[1].GetDriverId() != "near-1000" {
		t.Fatalf("nearby: got %v", d)
	}

	// Stale drivers found by the search were pruned from both keys.
	for _, id := range []string{"stale-1500", "no-last-seen"} {
		if pos, _ := rdb.GeoPos(ctx, "drivers:geo", id).Result(); pos[0] != nil {
			t.Fatalf("%s still in drivers:geo", id)
		}
		if _, err := rdb.ZScore(ctx, "drivers:last_seen", id).Result(); err != redis.Nil {
			t.Fatalf("%s still in drivers:last_seen", id)
		}
	}

	wantIDs("limit", list("limit", &locationv1.ListNearbyDriversRequest{Location: center, Limit: 1}), "near-500")

	// The limit applies after the staleness filter: 5 stale drivers nearer than every fresh one must not
	// fill the first batch of candidates and hide the fresh ones.
	for i := range 5 {
		put(fmt.Sprintf("stale-close-%d", i), float64(10+i), stale)
	}
	wantIDs("stale drivers closer", list("stale drivers closer", &locationv1.ListNearbyDriversRequest{Location: center, Limit: 2}), "near-500", "near-1000")

	// Fresh drivers are never pruned.
	if pos, _ := rdb.GeoPos(ctx, "drivers:geo", "near-500").Result(); pos[0] == nil {
		t.Fatal("fresh driver near-500 was pruned")
	}

	// The cleanup job removes what searches never reach: stale drivers where nobody searched, and expired
	// offline markers. Fresh drivers and recent offline markers stay.
	put("stale-unsearched", 50000, stale)
	rdb.ZAdd(ctx, "drivers:last_seen", redis.Z{Score: float64(stale.UnixMilli()), Member: "old-offline-marker"})
	rdb.ZAdd(ctx, "drivers:last_seen", redis.Z{Score: float64(fresh.UnixMilli()), Member: "recent-offline-marker"})
	if _, err := location.NewService(location.NewRepository(rdb)).PruneStale(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"stale-unsearched", "old-offline-marker", "stale-close-0", "stale-close-4"} {
		if _, err := rdb.ZScore(ctx, "drivers:last_seen", id).Result(); err != redis.Nil {
			t.Fatalf("cleanup: %s still in drivers:last_seen", id)
		}
		if pos, _ := rdb.GeoPos(ctx, "drivers:geo", id).Result(); pos[0] != nil {
			t.Fatalf("cleanup: %s still in drivers:geo", id)
		}
	}
	for _, id := range []string{"near-500", "near-1000", "far-20000", "recent-offline-marker"} {
		if _, err := rdb.ZScore(ctx, "drivers:last_seen", id).Result(); err != nil {
			t.Fatalf("cleanup removed %s: %v", id, err)
		}
	}
}

func startServer(t *testing.T, rdb *redis.Client) locationv1.LocationServiceClient {
	t.Helper()
	gs := server.NewGRPCServer(zap.NewNop(), jwtauth.NewVerifier(testSecret), location.NewService(location.NewRepository(rdb)))
	lis := bufconn.Listen(1 << 20)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return locationv1.NewLocationServiceClient(conn)
}

// withToken signs an access token the way the user service does.
func withToken(t *testing.T, ctx context.Context, userID string, driver bool) context.Context {
	t.Helper()
	claims := jwt.MapClaims{"sub": userID, "exp": time.Now().Add(time.Minute).Unix()}
	if driver {
		claims["driver"] = true
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(testSecret))
	if err != nil {
		t.Fatal(err)
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+tok)
}

func wantCode(t *testing.T, name string, err error, code codes.Code) {
	t.Helper()
	if got := status.Code(err); got != code {
		t.Fatalf("%s: got %v (%v), want %v", name, got, err, code)
	}
}
