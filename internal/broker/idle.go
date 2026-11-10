package broker

import (
	"context"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/store"
)

const ReceiverWait = 25 * time.Second
const IdleResync = time.Minute
const WriteProbeInterval = time.Minute
const WriteStallLimit = 3 * time.Minute

func (b *Broker) receiverState(ctx context.Context, r Request) Response {
	timer := time.NewTimer(ReceiverWait)
	defer timer.Stop()
	for {
		changed := b.Store.SessionChanges(r.Caller.ID)
		d, e := b.Store.ReadSessions(ctx)
		if e != nil {
			return response(nil, e)
		}
		token := hash(struct {
			Epoch   string
			Session store.Session
		}{b.Epoch, d.Sessions[r.Caller.ID]})
		if !r.Wait || r.IfUpdated != token {
			return response(map[string]any{"data": d, "epoch": b.Epoch, "if_updated": token}, nil)
		}
		select {
		case <-ctx.Done():
			return response(nil, ctx.Err())
		case <-timer.C:
			return response(map[string]any{"data": d, "epoch": b.Epoch, "if_updated": token}, nil)
		case <-changed:
		}
	}
}

// RunTicks schedules due receipts and claims independently of slow cleanup.
func (b *Broker) RunTicks(ctx context.Context) {
	retry := 200 * time.Millisecond
	for ctx.Err() == nil {
		tickErr := b.Tick(ctx)
		// Subscribe after our own commits, then read the state. Incoming changes
		// are either in this snapshot or wake the wait below.
		changed := b.Store.Changes()
		delay := time.Second
		d, e := b.Store.Read(ctx)
		if e == nil && tickErr == nil {
			delay = b.tickDelay(d, retry)
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-changed:
			timer.Stop()
			retry = 200 * time.Millisecond
		case <-b.TickWake:
			timer.Stop()
			retry = 200 * time.Millisecond
		case <-timer.C:
			if retry < 5*time.Second {
				retry *= 2
				if retry > 5*time.Second {
					retry = 5 * time.Second
				}
			}
		}
	}
}

// WakeTicks coalesces delivery wakeups without blocking policy changes.
func (b *Broker) WakeTicks() {
	select {
	case b.TickWake <- struct{}{}:
	default:
	}
}

// WakeDiscovery coalesces sends and herdr events without blocking admission.
func (b *Broker) WakeDiscovery() {
	b.WakeTicks()
	select {
	case b.DiscoveryWake <- struct{}{}:
	default:
	}
}

// RunDiscovery observes immediately, on demand, and once per idle resync window.
// Event bursts are coalesced and limited to one pass per second.
func (b *Broker) RunDiscovery(ctx context.Context, refresh func(context.Context)) {
	for ctx.Err() == nil {
		refresh(ctx)
		cooldown := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			cooldown.Stop()
			return
		case <-cooldown.C:
		}
		timer := time.NewTimer(IdleResync - time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-b.DiscoveryWake:
			timer.Stop()
		case <-timer.C:
		}
	}
}
