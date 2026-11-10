package broker

import (
	"context"
	"errors"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

var ErrRecipientGone = errors.New("recipient thread is no longer loaded")

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
func sameLookup(before, current *store.Message) bool {
	return current != nil && (current.State == states.Indeterminate || current.State == states.Dead) && current.State == before.State && current.BudgetEpoch == before.BudgetEpoch && len(current.Attempts) == len(before.Attempts)
}
func unresolved(m *store.Message) bool {
	for _, a := range m.Attempts {
		if !a.Positive && a.Outcome != NotSent {
			return true
		}
	}
	return false
}
func lookupEnd(m *store.Message, c policy.Config) time.Time {
	started := m.Created
	for _, a := range m.Attempts {
		if !a.Positive && a.Outcome != NotSent {
			started = maxTime(started, a.Started)
		}
	}
	return started.Add(policy.Seconds(c.LookupWindow))
}
func lookupExpired(d *store.Data, m *store.Message, c policy.Config, now time.Time) string {
	if m.State != states.Indeterminate && m.State != states.Dead {
		return ""
	}
	if !unresolved(m) {
		return ""
	}
	if !now.Before(lookupEnd(m, c)) {
		return "unconfirmed; delivery not proven within lookup window"
	}
	s, ok := d.Sessions[m.Recipient]
	if !ok || s.Tombstone || s.State == "offline" || s.State == "dead" {
		for _, next := range successors(d, m.Recipient) {
			if next.State == "live" && !next.Tombstone {
				return ""
			}
		}
		return "unconfirmed; delivery not proven before recipient went offline"
	}
	return ""
}

// Claims need their short lease timer even between slow maintenance sweeps.
// Read first so the ordinary idle path never opens a write transaction.
func (b *Broker) recoverDue(ctx context.Context) error {
	d, e := b.Store.Read(ctx)
	if e != nil {
		return e
	}
	due := false
	for _, m := range d.Messages {
		if m.State == states.Claimed || m.State == states.Injecting {
			due = true
			break
		}
	}
	if !due {
		return nil
	}
	return b.withConfig(ctx, func(c policy.Config) error {
		return b.Store.Update(ctx, func(d *store.Data) error {
			now := b.now()
			for _, m := range d.Messages {
				if m.State == states.Claimed {
					b.mu.Lock()
					started, exists := b.claimStarted[m.ClaimToken]
					b.mu.Unlock()
					if m.BrokerEpoch != b.Epoch || !now.Before(m.Lease) || !exists || time.Since(started) >= policy.Seconds(c.Claim) {
						if e := d.Move(m, states.Queued, "broker", "claim lease recovered", now); e != nil {
							return e
						}
					}
				}
				if m.State == states.Injecting {
					b.mu.Lock()
					_, active := b.outstanding[m.ActiveAttempt]
					b.mu.Unlock()
					if !active {
						if e := d.Move(m, states.Indeterminate, "broker", "orphaned attempt awaiting_confirmation", now); e != nil {
							return e
						}
					}
				}
			}
			return nil
		})
	})
}

func (b *Broker) tickDelay(d *store.Data, retry time.Duration) time.Duration {
	now := b.now()
	delay := 30 * time.Second
	sooner := func(at time.Time) {
		wait := at.Sub(now)
		if wait < 0 {
			wait = 0
		}
		if wait < delay {
			delay = wait
		}
	}
	b.mu.Lock()
	nextSweep := b.nextSweep
	b.mu.Unlock()
	sooner(nextSweep)
	blocked := map[string]bool{}
	for _, m := range d.Messages {
		if m.State == states.Indeterminate || (m.State == states.Dead && unresolved(m)) || m.State == states.Injecting || m.State == states.Claimed {
			blocked[m.Recipient] = true
		}
	}
	for _, m := range d.Messages {
		switch m.State {
		case states.Indeterminate:
			sooner(m.NextLookup)
			b.gate.Lock()
			c := b.displayConfig
			b.gate.Unlock()
			sooner(lookupEnd(m, c))
		case states.Dead:
			if unresolved(m) {
				sooner(m.NextLookup)
			}
		case states.Claimed, states.Injecting:
			if delay > 200*time.Millisecond {
				delay = 200 * time.Millisecond
			}
		case states.Queued:
			if blocked[m.Recipient] || d.Sessions[m.Recipient].State != "live" {
				continue
			}
			if m.NextAttempt.After(now) {
				sooner(m.NextAttempt)
			} else if retry < delay {
				delay = retry
			}
		}
	}
	return delay
}
