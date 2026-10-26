package main

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// loopReminderEvery is how often a loop that keeps failing logs again; in between it stays quiet.
const loopReminderEvery = time.Minute

// runLoop calls fn every interval until ctx is done. While fn keeps failing it backs off, doubling the wait up to
// maxWait, and logs only state changes: the first failure, a reminder every loopReminderEvery, and the recovery.
// A loop failing every second would otherwise log 3,600 identical warnings an hour.
// ponytail: copied in services/{matching,location}/cmd; keep the copies identical
func runLoop(ctx context.Context, lg *zap.Logger, name string, interval, maxWait time.Duration, fn func(context.Context) error) {
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

		err := fn(ctx)
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
			if now.Sub(lastLog) >= loopReminderEvery {
				lastLog = now
				lg.Warn(name+" still failing", zap.Error(err), zap.Duration("failing_for", now.Sub(failingSince)))
			}
			wait = min(wait*2, maxWait)
		}
		timer.Reset(wait)
	}
}
