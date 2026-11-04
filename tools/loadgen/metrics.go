package main

import (
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"
)

// recorder keeps every sample of a run, by stage, for exact percentiles at the end.
type recorder struct {
	stage   atomic.Int32
	mu      sync.Mutex
	samples map[key][]time.Duration
	counts  map[key]int
}

type key struct {
	stage int
	name  string
}

func newRecorder() *recorder {
	return &recorder{samples: map[key][]time.Duration{}, counts: map[key]int{}}
}

func (r *recorder) current() int { return int(r.stage.Load()) }

func (r *recorder) observe(stage int, name string, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	k := key{stage, name}
	r.samples[k] = append(r.samples[k], d)
}

func (r *recorder) count(stage int, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[key{stage, name}]++
}

// rides pairs each ride's booking with its first offer, which arrive in different goroutines and in either order: an
// offer can reach the driver before the rider has read CreateRide's response.
type rides struct {
	rec *recorder
	mu  sync.Mutex
	m   map[string]*rideTimes
}

type rideTimes struct {
	stage            int
	booked, offered  time.Time
	hasBook, hasOffr bool
}

func newRides(rec *recorder) *rides { return &rides{rec: rec, m: map[string]*rideTimes{}} }

func (r *rides) booked(id string, at time.Time, stage int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.get(id)
	t.booked, t.stage, t.hasBook = at, stage, true
	r.done(id, t)
}

func (r *rides) offered(id string, at time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	t := r.get(id)
	if t.hasOffr {
		return // a re-offer after a decline or expiry; the first one is what the rider waited for
	}
	t.offered, t.hasOffr = at, true
	r.done(id, t)
}

func (r *rides) get(id string) *rideTimes {
	if r.m[id] == nil {
		r.m[id] = &rideTimes{}
	}
	return r.m[id]
}

func (r *rides) done(id string, t *rideTimes) {
	if t.hasBook && t.hasOffr {
		r.rec.observe(t.stage, "to offer", t.offered.Sub(t.booked))
		delete(r.m, id)
	}
}

func pct(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	s := slices.Clone(ds)
	slices.Sort(s)
	return s[min(len(s)-1, int(float64(len(s))*p))]
}

func ms(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	if d >= 10*time.Second {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}

// report prints a line per stage, then each route over the whole run.
func (r *recorder) report(w io.Writer, riders []int, stageLen time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "stage\triders\tbooked\tdone\tdone/min\tno driver\tfailed\tbook p95\toffer p50\toffer p95\toffer p99\taccepted p95\tcharged p95\thttp p95\t429\t5xx\t")
	for i, n := range riders {
		var httpAll []time.Duration
		var s429, s5xx int
		for k, v := range r.samples {
			if k.stage == i && strings.HasPrefix(k.name, "http ") {
				httpAll = append(httpAll, v...)
			}
		}
		for k, c := range r.counts {
			if k.stage != i || !strings.HasPrefix(k.name, "status ") {
				continue
			}
			switch code := k.name[strings.LastIndex(k.name, " ")+1:]; {
			case code == "429":
				s429 += c
			case strings.HasPrefix(code, "5"), code == "network":
				s5xx += c
			}
		}
		at := func(name string) []time.Duration { return r.samples[key{i, name}] }
		failed := 0
		for k, c := range r.counts {
			if k.stage == i && (strings.HasPrefix(k.name, "fail ")) {
				failed += c
			}
		}
		done := r.counts[key{i, "completed"}]
		fmt.Fprintf(tw, "%d\t%d\t%d\t%d\t%.0f\t%d\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t\n",
			i+1, n, r.counts[key{i, "booked"}], done, float64(done)/stageLen.Minutes(), r.counts[key{i, "no driver"}], failed,
			ms(pct(at("http /v1/rides"), .95)),
			ms(pct(at("to offer"), .50)), ms(pct(at("to offer"), .95)), ms(pct(at("to offer"), .99)),
			ms(pct(at("to accepted"), .95)), ms(pct(at("to charged"), .95)), ms(pct(httpAll, .95)), s429, s5xx)
	}
	tw.Flush()

	var fails []string
	for k, c := range r.counts {
		if strings.HasPrefix(k.name, "fail ") {
			fails = append(fails, fmt.Sprintf("stage %d %s: %d", k.stage+1, strings.TrimPrefix(k.name, "fail "), c))
		}
	}
	if len(fails) > 0 {
		sort.Strings(fails)
		fmt.Fprintln(w, "\nfailures:")
		for _, f := range fails {
			fmt.Fprintln(w, "  "+f)
		}
	}

	routes := map[string][]time.Duration{}
	statuses := map[string]map[string]int{}
	for k, v := range r.samples {
		if name, ok := strings.CutPrefix(k.name, "http "); ok {
			routes[name] = append(routes[name], v...)
		}
	}
	for k, c := range r.counts {
		if rest, ok := strings.CutPrefix(k.name, "status "); ok {
			i := strings.LastIndex(rest, " ")
			route, code := rest[:i], rest[i+1:]
			if statuses[route] == nil {
				statuses[route] = map[string]int{}
			}
			statuses[route][code] += c
		}
	}
	names := make([]string, 0, len(routes))
	for n := range routes {
		names = append(names, n)
	}
	sort.Strings(names)
	fmt.Fprintln(w)
	tw = tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "route\trequests\tp50\tp95\tp99\tnot 2xx")
	for _, n := range names {
		var bad []string
		for code, c := range statuses[n] {
			if !strings.HasPrefix(code, "2") {
				bad = append(bad, fmt.Sprintf("%s×%d", code, c))
			}
		}
		sort.Strings(bad)
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\t%s\t%s\n", n, len(routes[n]), ms(pct(routes[n], .5)), ms(pct(routes[n], .95)), ms(pct(routes[n], .99)), strings.Join(bad, " "))
	}
	tw.Flush()
}
