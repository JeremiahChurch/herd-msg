package broker

import (
	"context"
	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/store"
	"time"
)

// Refresh updates daemon observations without replacing durable receiver work.
func (b *Broker) Refresh(ctx context.Context, sessions []store.Session) error {
	return b.RefreshHarness(ctx, "codex", sessions)
}

// RefreshHarness reconciles one harness without clearing another driver's faults.
func (b *Broker) RefreshHarness(ctx context.Context, harness string, sessions []store.Session) error {
	for _, s := range sessions {
		if s.Harness != harness {
			return fail(2, false, "discovery returned a different harness")
		}
	}
	b.PauseDriver(harness, "")
	if e := b.Store.Update(ctx, func(d *store.Data) error {
		delete(d.Health, "driver/"+harness)
		if harness == "codex" {
			delete(d.Health, "probe/codex-socket")
		}
		return nil
	}); e != nil {
		return e
	}
	for _, s := range sessions {
		if s.State != "live" && s.State != "detached" {
			continue
		}
		d, _, e := b.Data(ctx)
		if e != nil {
			return e
		}
		old, ok := d.Sessions[s.ID]
		if !ok || old.Generation == "" {
			if e = b.Register(ctx, s); e != nil {
				return e
			}
			continue
		}
		if old.Name == s.Name && old.Pane == s.Pane && old.State == s.State && old.Blocked == s.Blocked && old.Reason == s.Reason && sameHints(old.Hints, s.Hints) && (s.State != "live" || !seenDue(old, b.now())) {
			continue
		}
		e = b.withConfig(ctx, func(c policy.Config) error {
			return b.Store.Update(ctx, func(d *store.Data) error {
				current := d.Sessions[s.ID]
				if current.Generation != old.Generation {
					return nil
				}
				current.Name = s.Name
				current.Pane = s.Pane
				current.State = s.State
				current.Blocked = s.Blocked
				current.Reason = s.Reason
				if s.State == "live" && seenDue(current, b.now()) {
					current.LastSeen = b.now()
				}
				if current.Hints == nil {
					current.Hints = map[string]string{}
				}
				for k, v := range s.Hints {
					current.Hints[k] = v
				}
				d.Sessions[s.ID] = current
				delete(d.Health, "driver/"+harness)
				return nil
			})
		})
		if e != nil {
			return e
		}
	}
	return nil
}
func (b *Broker) DriverNotice(ctx context.Context, harness, detail string) error {
	return b.Store.Update(ctx, func(d *store.Data) error {
		key := "driver/" + harness
		if d.Health[key].Detail != detail {
			d.Notice(key, detail, "check the harness socket and supported protocol version", b.now())
		}
		return nil
	})
}

// RefreshAvailability keeps offline and blocked notices current before a send arrives.
func (b *Broker) RefreshAvailability(ctx context.Context) error {
	ctx = identity.SharedAgents(ctx)
	for _, driver := range b.Drivers {
		if batch, ok := driver.(interface {
			AvailabilityContext(context.Context) context.Context
		}); ok {
			ctx = batch.AvailabilityContext(ctx)
		}
	}
	d, c, e := b.Data(ctx)
	if e != nil {
		return e
	}
	for _, s := range d.Sessions {
		if s.Generation == "" || s.Tombstone {
			continue
		}
		a, e := b.availability(ctx, b.Drivers[s.Harness], s, c)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if e != nil {
			a = Availability{State: "unavailable", Reason: e.Error()}
		}
		if s.State == a.State && s.Blocked == a.Blocked && s.Reason == a.Reason && sameHints(s.Hints, a.Hints) && (a.State != "live" || !seenDue(s, b.now())) {
			continue
		}
		if e = b.Store.Update(ctx, func(d *store.Data) error {
			current := d.Sessions[s.ID]
			if current.Generation != s.Generation {
				return nil
			}
			current.State = a.State
			current.Blocked = a.Blocked
			current.Reason = a.Reason
			if a.State == "live" {
				if seenDue(current, b.now()) {
					current.LastSeen = b.now()
				}
			}
			if current.Hints == nil {
				current.Hints = map[string]string{}
			}
			for k, v := range a.Hints {
				current.Hints[k] = v
			}
			d.Sessions[s.ID] = current
			return nil
		}); e != nil {
			return e
		}
	}
	return nil
}

func seenDue(s store.Session, now time.Time) bool {
	return s.LastSeen.IsZero() || now.Sub(s.LastSeen) >= time.Minute
}
func sameHints(old, next map[string]string) bool {
	for k, v := range next {
		if previous, ok := old[k]; !ok || previous != v {
			return false
		}
	}
	return true
}

// PauseDriver changes only the named harness gate. Queued mail stays durable.
func (b *Broker) PauseDriver(harness, reason string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.paused == nil {
		b.paused = map[string]string{}
	}
	if reason == "" {
		delete(b.paused, harness)
	} else {
		b.paused[harness] = reason
	}
}
