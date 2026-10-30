package loop

import (
	"context"
	"time"

	"github.com/rickyroynardson/ride-hailing/pkg/telemetry"
	"go.uber.org/zap"
)

// reminderEvery is how often a loop that keeps failing logs again; in between it stays quiet.
const reminderEvery = time.Minute

// Run calls fn every interval until ctx is done. While fn keeps failing it backs off, doubling the wait up to
// maxWait, and logs only state changes: the first failure, a reminder every reminderEvery, and the recovery.
// A loop failing every second would otherwise log 3,600 identical warnings an hour.
// Iterations aren't traced: most find nothing to do, and one iteration can touch many unrelated rides. fn runs
// under telemetry.Untraced and starts its own trace (telemetry.StartWork) for each real piece of work.
func Run(ctx context.Context, lg *zap.Logger, name string, interval, maxWait time.Duration, fn func(context.Context) error) {
	wait := interval
	var failingSince, lastLog time.Time
	timer := time.NewTimer(wait)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		err := fn(telemetry.Untraced(ctx))
		if ctx.Err() != nil {
			return
		}
		now := time.Now()
		switch {
		case err == nil:
			if !failingSince.IsZero() {
				lg.Info(name+" recovered", zap.Duration("failed_for", now.Sub(failingSince)))
				failingSince = time.Time{}
			}
			wait = interval
		case failingSince.IsZero():
			failingSince, lastLog = now, now
			lg.Warn(name+" failing", zap.Error(err))
			wait = min(wait*2, maxWait)
		default:
			if now.Sub(lastLog) >= reminderEvery {
				lastLog = now
				lg.Warn(name+" still failing", zap.Error(err), zap.Duration("failing_for", now.Sub(failingSince)))
			}
			wait = min(wait*2, maxWait)
		}
		timer.Reset(wait)
	}
}
