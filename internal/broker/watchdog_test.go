package broker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWatchWritesReportsFailingProbe(t *testing.T) {
	fired := make(chan error, 1)
	go WatchWrites(ctx, 5*time.Millisecond, 40*time.Millisecond, func(context.Context) error {
		return errors.New("database is locked")
	}, func(_ time.Time, cause error) { fired <- cause })
	select {
	case cause := <-fired:
		if cause == nil || cause.Error() != "database is locked" {
			t.Fatal(cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watchdog did not fire")
	}
}

func TestWatchWritesReportsProbeThatNeverReturns(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	fired := make(chan struct{})
	go WatchWrites(ctx, 5*time.Millisecond, 40*time.Millisecond, func(context.Context) error {
		calls.Add(1)
		<-release
		return nil
	}, func(time.Time, error) { close(fired) })
	select {
	case <-fired:
	case <-time.After(5 * time.Second):
		t.Fatal("watchdog did not fire")
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("started %d probes beside a blocked one", n)
	}
}

func TestWatchWritesStaysQuietWhileWritesRecover(t *testing.T) {
	var calls atomic.Int32
	fired := make(chan struct{}, 1)
	c, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	WatchWrites(c, 5*time.Millisecond, 60*time.Millisecond, func(context.Context) error {
		// Short failures, shorter than the limit, are not a stall.
		if calls.Add(1)%3 != 0 {
			return errors.New("busy")
		}
		return nil
	}, func(time.Time, error) { fired <- struct{}{} })
	select {
	case <-fired:
		t.Fatal("fired while writes were succeeding")
	default:
	}
}

func TestWriteProbeCommits(t *testing.T) {
	b, _ := setup(t)
	if e := b.WriteProbe(ctx); e != nil {
		t.Fatal(e)
	}
}
