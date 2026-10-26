package server_test

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	"github.com/rickyroynardson/ride-hailing/services/location/internal/auth"
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
	update := func(name string, req *locationv1.UpdateDriverLocationRequest) {
		t.Helper()
		rdb.Del(ctx, "driver:rate:"+driverID)
		if _, err := client.UpdateDriverLocation(driver, req); err != nil {
			t.Fatalf("%s: %v", name, err)
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
	update("first", reading(here, 10, now.Add(-2*time.Second)))
	wantAt("first", here, now.Add(-2*time.Second))
	update("second", reading(there, 10, now))
	wantAt("second", there, now)
	if n, _ := rdb.ZCard(ctx, "drivers:geo").Result(); n != 1 {
		t.Fatalf("drivers:geo has %d members, want 1", n)
	}

	// Accepted but not stored: each leaves the second reading in place.
	update("arrived out of order", reading(here, 10, now.Add(-time.Second)))
	wantAt("arrived out of order", there, now)
	update("inaccurate", reading(here, 51, now.Add(time.Second)))
	wantAt("inaccurate", there, now)
	update("too old", reading(here, 10, now.Add(-time.Minute)))
	wantAt("too old", there, now)

	// A second update within a second is rejected and stores nothing; the limit lifts once the mark expires.
	update("before limit", reading(here, 10, now.Add(time.Second)))
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
	update("in flight", reading(there, 10, offlineAt.Add(-500*time.Millisecond)))
	wantOffline("after in-flight update")

	// A reading newer than both the offline time and the earlier readings (stamped up to now+1s) brings them back.
	backAt := now.Add(3 * time.Second)
	update("back online", reading(there, 10, backAt))
	wantAt("back online", there, backAt)

	// Non-drivers may call it; it removes nothing of anyone else's.
	if _, err := client.GoOffline(withToken(t, ctx, "44444444-4444-4444-4444-444444444444", false), &locationv1.GoOfflineRequest{}); err != nil {
		t.Fatalf("rider go offline: %v", err)
	}
	wantAt("after rider go offline", there, backAt)
}

func startServer(t *testing.T, rdb *redis.Client) locationv1.LocationServiceClient {
	t.Helper()
	gs := server.NewGRPCServer(zap.NewNop(), auth.NewVerifier(testSecret), location.NewService(location.NewRepository(rdb)))
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
