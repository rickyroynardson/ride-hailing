package location

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// geoKey holds each online driver's position.
	geoKey = "drivers:geo"
	// lastSeenKey scores each driver by the unix milliseconds of their latest reading, so stale drivers can be
	// skipped and pruned, and out-of-order readings ignored.
	lastSeenKey = "drivers:last_seen"
)

// rateKeyPrefix + driver ID marks a driver who updated within the last minUpdateGap. Each mark expires on its
// own, so there is nothing to clean up.
const rateKeyPrefix = "driver:rate:"

// minUpdateGap caps each driver at one update per second. Apps send every few seconds, so only a buggy or abusive
// client hits it, and it can't flood Redis.
const minUpdateGap = time.Second

// setLocation runs as one atomic step on the Redis server: no other command can run between its lines.
//
// Why a script instead of go-redis methods: later steps depend on earlier results (store only if not rate
// limited and newer than the stored reading). Sent as separate commands from Go, another update for the same
// driver can run between our read and our write:
//
//	A (older) ZSCORE → 09:59:57
//	                              B (newer) ZSCORE → 09:59:57, GEOADD + ZADD 10:00:03
//	A sees 09:59:57 < 10:00:00, GEOADD + ZADD 10:00:00   ← overwrites B with the older position
//
// The rate-limit check has the same gap: two requests could both see "no mark" and both pass.
// Alternatives considered:
//   - Pipeline / TxPipelined (MULTI): batch commands, but replies arrive only after all run, so no step can
//     depend on another's result.
//   - ZADD GT: guards only the timestamp key; GEOADD has no such condition and would still write the old position.
//   - WATCH a per-driver key: correct, but 3 round trips per update instead of 1, plus a retry or drop path on
//     conflict. Updates are the highest-volume write in the system.
//
// Single commands elsewhere (GEOSEARCH, ZMSCORE, ...) are already atomic and use plain go-redis methods.
//
//	KEYS[1] drivers:geo, KEYS[2] drivers:last_seen, KEYS[3] this driver's rate-limit mark
//	ARGV[1] driver ID, ARGV[2] lat, ARGV[3] lng, ARGV[4] recorded_at in unix ms, ARGV[5] minUpdateGap in ms
//
// Returns -1 rate limited, 0 older than the stored reading (ignored), 1 stored.
var setLocation = redis.NewScript(`
if not redis.call('SET', KEYS[3], '1', 'NX', 'PX', ARGV[5]) then
  return -1
end
local seen = redis.call('ZSCORE', KEYS[2], ARGV[1])
if seen and tonumber(seen) >= tonumber(ARGV[4]) then
  return 0
end
redis.call('GEOADD', KEYS[1], ARGV[3], ARGV[2], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[1])
return 1
`)

type LocationRepository struct {
	RDB *redis.Client
}

func NewRepository(rdb *redis.Client) *LocationRepository {
	return &LocationRepository{RDB: rdb}
}

// SetDriverLocation stores a reading unless a newer one is already stored, which is not an error.
func (r *LocationRepository) SetDriverLocation(ctx context.Context, driverID string, l Location, recordedAt time.Time) error {
	result, err := setLocation.Run(ctx, r.RDB, []string{geoKey, lastSeenKey, rateKeyPrefix + driverID},
		driverID, l.Lat, l.Lng, recordedAt.UnixMilli(), minUpdateGap.Milliseconds(),
	).Int()
	if err != nil {
		return err
	}
	if result == -1 {
		return ErrRateLimited
	}
	return nil
}
