package gateway

import (
	"net/http"
	"strconv"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var requestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "gateway_request_duration_seconds",
	Help:    "Public API requests by route and status. Streams count from open to close.",
	Buckets: prometheus.DefBuckets,
}, []string{"method", "route", "status"})

// observe logs and measures each request by its route pattern, not its path: /v1/rides/{ride_id}:cancel is one
// series, however many rides there are. It runs inside the mux, after routing, which is where the pattern is known.
func observe(lg *zap.Logger) runtime.Middleware {
	return func(next runtime.HandlerFunc) runtime.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request, params map[string]string) {
			start := time.Now()
			rec := &recorder{ResponseWriter: w, status: http.StatusOK}
			next(rec, r, params)

			route := "unknown"
			if p, ok := runtime.HTTPPattern(r.Context()); ok {
				route = p.String()
			}
			d := time.Since(start)
			requestDuration.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Observe(d.Seconds())
			fields := []zap.Field{
				zap.String("method", r.Method), zap.String("route", route), zap.Int("status", rec.status),
				zap.Duration("duration", d),
			}
			if rec.status >= 500 {
				lg.Error("request", fields...)
			} else {
				lg.Info("request", fields...)
			}
		}
	}
}

// recorder notes the status a handler wrote. Unwrap lets http.ResponseController reach the real writer, which is how
// grpc-gateway flushes each message of a stream; without it a stream would arrive all at once, at the end.
type recorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *recorder) WriteHeader(status int) {
	if !r.wroteHeader {
		r.status, r.wroteHeader = status, true
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
