package loop

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestRun(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Fails 5 times, then succeeds; the loop stops itself after 8 calls.
	var calls atomic.Int32
	done := make(chan struct{})
	go func() {
		Run(ctx, zap.New(core), "test loop", time.Millisecond, 4*time.Millisecond, func(context.Context) error {
			n := calls.Add(1)
			if n == 8 {
				cancel()
			}
			if n <= 5 {
				return errors.New("db down")
			}
			return nil
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not stop")
	}

	var got []string
	for _, e := range logs.All() {
		got = append(got, e.Level.String()+" "+e.Message)
	}
	want := []string{"warn test loop failing", "info test loop recovered"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("logs: got %q, want %q (one warning for 5 failures, one recovery)", got, want)
	}
}

func TestRunWake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The interval is an hour, so every call here comes from a wake-up. The first call fails: the wake-up after it
	// must be ignored (backoff holds), so only two calls happen for three wake-ups.
	wake := make(chan struct{})
	calls := make(chan int, 10)
	var n atomic.Int32
	go RunWake(ctx, zap.NewNop(), "test loop", time.Hour, time.Hour, wake, func(context.Context) error {
		calls <- int(n.Add(1))
		if n.Load() == 1 {
			return errors.New("down")
		}
		return nil
	})

	wake <- struct{}{}
	if got := <-calls; got != 1 {
		t.Fatalf("first wake-up: call %d", got)
	}
	wake <- struct{}{} // ignored while failing
	select {
	case got := <-calls:
		t.Fatalf("wake-up while failing ran call %d", got)
	case <-time.After(50 * time.Millisecond):
	}
}
