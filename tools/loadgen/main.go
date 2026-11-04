// Command loadgen drives the system through the public gateway the way apps do: drivers stay online and answer offers
// from their stream, riders book, follow their ride's stream and wait for the charge. It ramps the number of riders in
// stages and reports, per stage, how long each step took and what failed, so a run shows what breaks first.
//
// Development only: it approves its drivers straight in the user database, standing in for an admin.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	locationv1 "github.com/rickyroynardson/ride-hailing/proto/gen/location/v1"
	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"golang.org/x/sync/errgroup"
)

func main() {
	gateway := flag.String("gateway", "http://localhost:8000", "gateway base URL")
	usersDB := flag.String("users-db", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable", "user service database, to approve drivers")
	stagesFlag := flag.String("stages", "10,25,50,100", "riders active in each stage, comma separated")
	stageLen := flag.Duration("stage", time.Minute, "how long each stage runs")
	drivers := flag.Int("drivers", 0, "drivers online throughout (default: 1.5 × the largest stage)")
	center := flag.String("center", "-6.2,106.8", "lat,lng of the area drivers and pickups are spread around")
	spread := flag.Float64("spread", 0.02, "how far from the center, in degrees (0.02 ≈ 2 km)")
	tripFor := flag.Duration("trip", 5*time.Second, "time from pickup to dropoff")
	pickupIn := flag.Duration("pickup", 2*time.Second, "time from accepting to pickup")
	acceptIn := flag.Duration("accept", 300*time.Millisecond, "time a driver takes to accept an offer")
	drain := flag.Duration("drain", 2*time.Minute, "after the last stage, how long to wait for rides in progress to finish")
	flag.Parse()

	stages, err := parseStages(*stagesFlag)
	if err != nil {
		log.Fatal(err)
	}
	if *drivers == 0 {
		*drivers = stages[len(stages)-1] * 3 / 2
	}
	lat, lng, err := parseLatLng(*center)
	if err != nil {
		log.Fatal(err)
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	rec := newRecorder()
	w := &world{
		api: newAPI(*gateway, rec), rec: rec, rides: newRides(rec),
		center: &locationv1.Location{Lat: lat, Lng: lng}, spread: *spread,
		acceptIn: *acceptIn, pickupIn: *pickupIn, tripFor: *tripFor,
	}
	db, err := pgxpool.New(ctx, *usersDB)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	runID := strconv.FormatInt(time.Now().Unix()%1_000_000, 10)
	log.Printf("run %s: signing up %d drivers and %d riders", runID, *drivers, stages[len(stages)-1])
	ds, err := signUpDrivers(ctx, w, db, runID, *drivers)
	if err != nil {
		log.Fatalf("drivers: %v (raise the gateway's AUTH_LIMIT_PER_MINUTE for load runs, see the README)", err)
	}
	rs, err := signUp(ctx, w.api, runID, "r", stages[len(stages)-1])
	if err != nil {
		log.Fatalf("riders: %v", err)
	}

	driversCtx, stopDrivers := context.WithCancel(ctx)
	defer stopDrivers()
	for _, s := range ds {
		d := &driver{w: w, s: s, pos: w.randomPoint()}
		go d.run(driversCtx)
	}
	sleep(ctx, 2*heartbeatEvery) // every driver is online and watching before the first booking

	var stop atomic.Bool
	var riders sync.WaitGroup
	started := 0
	for i, n := range stages {
		rec.stage.Store(int32(i))
		log.Printf("stage %d: %d riders for %s", i+1, n, *stageLen)
		for ; started < n; started++ {
			r := &rider{w: w, s: rs[started]}
			riders.Go(func() { r.run(ctx, &stop) })
		}
		if !sleep(ctx, *stageLen) {
			break
		}
	}

	stop.Store(true)
	log.Printf("draining: waiting up to %s for rides in progress", *drain)
	done := make(chan struct{})
	go func() { riders.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(*drain):
		log.Print("drain timed out; rides still in progress are left as they are")
	case <-ctx.Done():
	}
	stopDrivers()
	for _, s := range ds { // take the drivers off the map, so the next run doesn't offer rides to ghosts
		w.api.call(context.Background(), http.MethodDelete, "/v1/driver/location", "/v1/driver/location", s.token(context.Background()), nil, nil)
	}

	fmt.Printf("\nrun %s: %d drivers, stages of %s, trip %s\n\n", runID, *drivers, *stageLen, *tripFor)
	rec.report(os.Stdout, stages, *stageLen)
}

// signUp registers n users, a few at a time: registering hashes a password, which is slow on purpose.
func signUp(ctx context.Context, a *api, runID, kind string, n int) ([]*session, error) {
	out := make([]*session, n)
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i := range n {
		g.Go(func() error {
			var res userv1.RegisterResponse
			err := a.call(ctx, http.MethodPost, "/v1/auth/register", "/v1/auth/register", "", &userv1.RegisterRequest{
				Email:    fmt.Sprintf("load-%s-%s%d@loadgen.local", runID, kind, i),
				Password: "loadgen-password",
				Name:     fmt.Sprintf("load %s%d", kind, i),
				Phone:    fmt.Sprintf("+62%s%s%06d", map[string]string{"r": "1", "d": "2"}[kind], runID, i),
			}, &res)
			if err != nil {
				return err
			}
			s := &session{api: a, ID: res.GetUser().GetId()}
			s.set(res.GetTokens())
			out[i] = s
			return nil
		})
	}
	return out, g.Wait()
}

// signUpDrivers registers drivers, approves them in the user database as an admin would, and refreshes their tokens to
// pick up the driver claim.
func signUpDrivers(ctx context.Context, w *world, db *pgxpool.Pool, runID string, n int) ([]*session, error) {
	ss, err := signUp(ctx, w.api, runID, "d", n)
	if err != nil {
		return nil, err
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i, s := range ss {
		g.Go(func() error {
			return w.api.call(gctx, http.MethodPost, "/v1/me/driver", "/v1/me/driver", s.token(gctx), &userv1.RegisterDriverRequest{
				LicenseNumber: fmt.Sprintf("LG-%s-%d", runID, i), VehiclePlate: fmt.Sprintf("LG %s %d", runID, i), VehicleModel: "Loadgen",
			}, nil)
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	ids := make([]string, len(ss))
	for i, s := range ss {
		ids[i] = s.ID
	}
	if _, err := db.Exec(ctx, `UPDATE driver_profiles SET status = 'approved' WHERE user_id = ANY($1::uuid[])`, ids); err != nil {
		return nil, err
	}
	for _, s := range ss {
		if err := s.refreshNow(ctx); err != nil {
			return nil, err
		}
	}
	return ss, nil
}

func parseStages(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n <= 0 || (len(out) > 0 && n < out[len(out)-1]) {
			return nil, fmt.Errorf("stages must be positive and not decreasing, got %q", s)
		}
		out = append(out, n)
	}
	return out, nil
}

func parseLatLng(s string) (float64, float64, error) {
	lat, lng, ok := strings.Cut(s, ",")
	a, err1 := strconv.ParseFloat(strings.TrimSpace(lat), 64)
	b, err2 := strconv.ParseFloat(strings.TrimSpace(lng), 64)
	if !ok || err1 != nil || err2 != nil {
		return 0, 0, fmt.Errorf("center must be lat,lng, got %q", s)
	}
	return a, b, nil
}
