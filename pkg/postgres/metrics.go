package postgres

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// RegisterPoolMetrics exposes pool stats read on each scrape. Saturation shows as acquired == max
// with empty_acquire and acquire_seconds climbing: requests are queueing for a connection.
func RegisterPoolMetrics(pool *pgxpool.Pool) {
	gauges := map[string]struct {
		help string
		fn   func(*pgxpool.Stat) float64
	}{
		"acquired_conns": {"Connections currently in use.", func(s *pgxpool.Stat) float64 { return float64(s.AcquiredConns()) }},
		"idle_conns":     {"Connections currently idle.", func(s *pgxpool.Stat) float64 { return float64(s.IdleConns()) }},
		"total_conns":    {"Connections currently open.", func(s *pgxpool.Stat) float64 { return float64(s.TotalConns()) }},
		"max_conns":      {"Maximum pool size.", func(s *pgxpool.Stat) float64 { return float64(s.MaxConns()) }},
	}
	counters := map[string]struct {
		help string
		fn   func(*pgxpool.Stat) float64
	}{
		"acquire_total":          {"Successful connection acquires.", func(s *pgxpool.Stat) float64 { return float64(s.AcquireCount()) }},
		"acquire_seconds_total":  {"Total time spent acquiring connections.", func(s *pgxpool.Stat) float64 { return s.AcquireDuration().Seconds() }},
		"empty_acquire_total":    {"Acquires that had to wait because no idle connection was available.", func(s *pgxpool.Stat) float64 { return float64(s.EmptyAcquireCount()) }},
		"canceled_acquire_total": {"Acquires canceled by context while waiting.", func(s *pgxpool.Stat) float64 { return float64(s.CanceledAcquireCount()) }},
	}

	for name, g := range gauges {
		prometheus.MustRegister(prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{Namespace: "pgxpool", Name: name, Help: g.help},
			func() float64 { return g.fn(pool.Stat()) },
		))
	}
	for name, c := range counters {
		prometheus.MustRegister(prometheus.NewCounterFunc(
			prometheus.CounterOpts{Namespace: "pgxpool", Name: name, Help: c.help},
			func() float64 { return c.fn(pool.Stat()) },
		))
	}
}
