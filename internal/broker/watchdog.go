package broker

import (
	"context"
	"time"
)

// WriteProbe takes the locks a send takes and commits a small durable write, so it
// fails when mail could not be accepted. It never reads the config: a bad config is
// reported elsewhere and restarting the broker would not repair it.
func (b *Broker) WriteProbe(ctx context.Context) error {
	unlock, e := b.lockConfig(ctx)
	if e != nil {
		return e
	}
	defer unlock()
	return b.Store.WriteProbe(ctx)
}

// WatchWrites calls stalled once, then returns, when probe has not succeeded for
// limit. Each probe runs on its own goroutine, because the failure this guards against
// can block a caller forever. A probe still running counts as not having succeeded,
// and no second one starts beside it.
func WatchWrites(ctx context.Context, every, limit time.Duration, probe func(context.Context) error, stalled func(since time.Time, cause error)) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	result := make(chan error, 1)
	running := false
	start := func() {
		running = true
		go func() {
			call, stop := context.WithTimeout(ctx, every)
			defer stop()
			result <- probe(call)
		}()
	}
	last := time.Now()
	var cause error
	start()
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-result:
			running = false
			if e == nil {
				last, cause = time.Now(), nil
			} else {
				cause = e
			}
		case <-ticker.C:
			if time.Since(last) >= limit {
				stalled(last, cause)
				return
			}
			if !running {
				start()
			}
		}
	}
}
