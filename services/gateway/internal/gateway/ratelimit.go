package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	window = time.Minute
	// redisTimeout bounds the check, so a slow Redis adds at most this to a request before failing open.
	redisTimeout = 100 * time.Millisecond
)

// authRoutes are rate limited per client IP, tighter than the rest: they take passwords and refresh tokens.
var authRoutes = map[string]bool{
	"/v1/auth/register": true,
	"/v1/auth/login":    true,
	"/v1/auth/refresh":  true,
}

// Limits are requests allowed per minute, as in config.Config.
type Limits struct {
	Auth, User, Anon int
}

var (
	limited = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_rate_limited_total", Help: "Requests refused by the rate limiter, by bucket.",
	}, []string{"bucket"})
	limiterErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_rate_limiter_errors_total", Help: "Rate limit checks that failed, so let the request through.",
	})
)

// Limiter counts requests in Redis, so every gateway replica shares one count per client.
type Limiter struct {
	rdb *redis.Client
	lg  *zap.Logger
}

func NewLimiter(rdb *redis.Client, lg *zap.Logger) *Limiter {
	return &Limiter{rdb: rdb, lg: lg}
}

// Middleware refuses requests over the client's limit with 429 and Retry-After. A request with a token is counted
// against that token, which nobody else can present, so one user can't use up another's allowance. Hashing it keeps
// tokens out of Redis.
// ponytail: client IP is the connection's address; trust X-Forwarded-For from a known load balancer once there is one
func (l *Limiter) Middleware(limits Limits) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			bucket, id, limit := "anon", clientIP(r), limits.Anon
			if authRoutes[r.URL.Path] {
				bucket, limit = "auth", limits.Auth
			} else if tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok && tok != "" {
				sum := sha256.Sum256([]byte(tok))
				bucket, id, limit = "user", hex.EncodeToString(sum[:16]), limits.User
			}

			allowed, retryAfter := l.allow(r.Context(), bucket+":"+id, limit)
			if !allowed {
				limited.WithLabelValues(bucket).Inc()
				w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds()+0.999)))
				writeError(w, http.StatusTooManyRequests, "too many requests, retry later")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// allow counts the request in the current fixed window. If Redis can't answer it lets the request through: an
// outage of the limiter shouldn't become an outage of the API, and the services still check every token.
func (l *Limiter) allow(ctx context.Context, key string, limit int) (bool, time.Duration) {
	now := time.Now()
	start := now.Truncate(window)
	key = "ratelimit:" + key + ":" + strconv.FormatInt(start.Unix(), 10)

	ctx, cancel := context.WithTimeout(ctx, redisTimeout)
	defer cancel()
	pipe := l.rdb.TxPipeline()
	count := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 2*window)
	if _, err := pipe.Exec(ctx); err != nil {
		limiterErrors.Inc()
		l.lg.Warn("rate limiter unavailable, allowing request", zap.Error(err))
		return true, 0
	}
	return count.Val() <= int64(limit), start.Add(window).Sub(now)
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
