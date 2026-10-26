package location

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
)

const (
	// geoKey holds each online driver's position.
	geoKey = "drivers:geo"
	// lastSeenKey scores each driver by the unix milliseconds of their latest reading, so stale drivers can be
	// skipped and pruned, and out-of-order readings ignored. A driver who went offline keeps an entry here (but
	// not in geoKey) holding the offline time, so readings taken before it are ignored too.
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

// pruneStale removes drivers from both keys only if they are still stale. A driver can send a fresh reading
// between our freshness check and this removal, so the check is repeated here, atomically with the removal.
//
//	KEYS[1] drivers:geo, KEYS[2] drivers:last_seen
//	ARGV[1] cutoff in unix ms, ARGV[2..] driver IDs
//
// Returns how many drivers it removed.
var pruneStale = redis.NewScript(`
local cutoff = tonumber(ARGV[1])
local removed = 0
for i = 2, #ARGV do
  local seen = redis.call('ZSCORE', KEYS[2], ARGV[i])
  if not seen or tonumber(seen) < cutoff then
    redis.call('ZREM', KEYS[1], ARGV[i])
    redis.call('ZREM', KEYS[2], ARGV[i])
    removed = removed + 1
  end
end
return removed
`)

// prunedTotal counts stale drivers and expired offline markers removed, by searches and by the cleanup job.
var prunedTotal = promauto.NewCounter(prometheus.CounterOpts{
	Name: "location_stale_pruned_total",
	Help: "Stale drivers and expired offline markers removed from Redis.",
})

// pruneBatch caps how many drivers one PruneStale round sends to the script, keeping each script call short:
// Redis runs nothing else while a script runs.
const pruneBatch = 1000

// maxNearbyCandidates caps how many entries one search reads from Redis while skipping stale drivers.
const maxNearbyCandidates = 500

type LocationRepository struct {
	RDB *redis.Client
}

func NewRepository(rdb *redis.Client) *LocationRepository {
	return &LocationRepository{RDB: rdb}
}

// SetDriverLocation reports whether the reading was stored; false means a newer one already was.
func (r *LocationRepository) SetDriverLocation(ctx context.Context, driverID string, l Location, recordedAt time.Time) (bool, error) {
	result, err := setLocation.Run(ctx, r.RDB, []string{geoKey, lastSeenKey, rateKeyPrefix + driverID},
		driverID, l.Lat, l.Lng, recordedAt.UnixMilli(), minUpdateGap.Milliseconds(),
	).Int()
	if err != nil {
		return false, err
	}
	if result == -1 {
		return false, ErrRateLimited
	}
	return result == 1, nil
}

// SetDriverOffline removes the driver from geoKey and records the offline time in lastSeenKey, so an update that
// was in flight when they went offline is ignored as older instead of putting them back online.
// ZADD GT keeps the stored time if it is already later, e.g. a reading from a phone whose clock runs fast.
// Two independent commands, so MULTI is enough; see setLocation for when a script is needed instead.
func (r *LocationRepository) SetDriverOffline(ctx context.Context, driverID string, at time.Time) error {
	_, err := r.RDB.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.ZRem(ctx, geoKey, driverID)
		p.ZAddGT(ctx, lastSeenKey, redis.Z{Score: float64(at.UnixMilli()), Member: driverID})
		return nil
	})
	return err
}

// FindNearby returns up to limit drivers seen at or after freshSince, nearest first. The limit is applied after
// dropping stale drivers: if stale ones fill the first batch of candidates, it searches again with a larger batch,
// so fresh drivers slightly further away are not missed. Stale drivers it finds are pruned on the way.
func (r *LocationRepository) FindNearby(ctx context.Context, center Location, radiusMeters float64, limit int, freshSince time.Time) ([]NearbyDriver, error) {
	cutoff := freshSince.UnixMilli()
	count := limit * 2

	for {
		candidates, err := r.RDB.GeoSearchLocation(ctx, geoKey, &redis.GeoSearchLocationQuery{
			GeoSearchQuery: redis.GeoSearchQuery{
				Longitude:  center.Lng,
				Latitude:   center.Lat,
				Radius:     radiusMeters,
				RadiusUnit: "m",
				Sort:       "ASC",
				Count:      count,
			},
			WithCoord: true,
			WithDist:  true,
		}).Result()
		if err != nil || len(candidates) == 0 {
			return nil, err
		}

		ids := make([]string, len(candidates))
		for i, c := range candidates {
			ids[i] = c.Name
		}
		// A driver with no last-seen entry reads as 0, i.e. stale.
		seen, err := r.RDB.ZMScore(ctx, lastSeenKey, ids...).Result()
		if err != nil {
			return nil, err
		}

		drivers := make([]NearbyDriver, 0, limit)
		var stale []any
		for i, c := range candidates {
			if int64(seen[i]) < cutoff {
				stale = append(stale, c.Name)
				continue
			}
			if len(drivers) < limit {
				drivers = append(drivers, NearbyDriver{
					DriverID:       c.Name,
					Location:       Location{Lat: c.Latitude, Lng: c.Longitude},
					DistanceMeters: c.Dist,
				})
			}
		}

		if len(stale) > 0 {
			if _, err := r.prune(ctx, cutoff, stale); err != nil {
				return nil, err
			}
		}

		// Done when enough fresh drivers were found, the radius has no more entries, or the batch hit the cap.
		if len(drivers) == limit || len(candidates) < count || count >= maxNearbyCandidates {
			return drivers, nil
		}
		count = min(count*4, maxNearbyCandidates)
	}
}

// PruneStale removes every driver last seen before cutoff, including expired offline markers, which searches
// never reach: offline drivers are not in geoKey, and stale drivers may sit where nobody searches.
func (r *LocationRepository) PruneStale(ctx context.Context, cutoff time.Time) (int, error) {
	total := 0
	for {
		ids, err := r.RDB.ZRangeArgs(ctx, redis.ZRangeArgs{
			Key:     lastSeenKey,
			Start:   "-inf",
			Stop:    fmt.Sprintf("(%d", cutoff.UnixMilli()),
			ByScore: true,
			Count:   pruneBatch,
		}).Result()
		if err != nil || len(ids) == 0 {
			return total, err
		}

		args := make([]any, len(ids))
		for i, id := range ids {
			args[i] = id
		}
		removed, err := r.prune(ctx, cutoff.UnixMilli(), args)
		total += removed
		// removed == 0 means every listed driver refreshed in between; their new scores are past the cutoff.
		if err != nil || removed == 0 || len(ids) < pruneBatch {
			return total, err
		}
	}
}

func (r *LocationRepository) prune(ctx context.Context, cutoff int64, ids []any) (int, error) {
	removed, err := pruneStale.Run(ctx, r.RDB, []string{geoKey, lastSeenKey}, append([]any{cutoff}, ids...)...).Int()
	prunedTotal.Add(float64(removed))
	return removed, err
}
