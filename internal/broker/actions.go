package broker

import (
	"context"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func (b *Broker) Release(ctx context.Context, id string, caller Caller) error {
	if e := operator(caller); e != nil {
		return e
	}
	return b.withConfig(ctx, func(c policy.Config) error {
		return b.Store.Update(ctx, func(d *store.Data) error {
			m, e := d.Find(id)
			if e != nil {
				return e
			}
			if m.State != states.Held {
				return errors.New("only held mail can be released")
			}
			if m.Hop > c.HopLimit {
				return fail(2, false, "hop limit exceeded")
			}
			if _, ok := c.Links[m.Origin]; m.Origin != c.Machine && !ok {
				return fail(2, false, "sender machine is not linked")
			}
			if d.Sessions[m.Recipient].Tombstone {
				return fail(2, false, "recipient was pruned")
			}
			m.Released = true
			m.Sequence = d.NextSequence()
			b.hook("before_release_commit")
			return d.Move(m, states.Queued, "operator", "released", b.now())
		})
	})
}
func (b *Broker) Resolve(ctx context.Context, id, action string, confirm bool, updated string, caller Caller) error {
	if e := operator(caller); e != nil {
		return e
	}
	d, c, e := b.Data(ctx)
	if e != nil {
		return e
	}
	m, e := d.Find(id)
	if e != nil {
		return e
	}
	unlock, e := b.lock(ctx, m.Recipient)
	if e != nil {
		return e
	}
	defer unlock()
	if b.busy(m.Recipient) {
		return fail(2, true, "recipient has an outstanding driver call")
	}
	// Refresh after acquiring delivery ownership.
	d, c, e = b.Data(ctx)
	if e != nil {
		return e
	}
	m, e = d.Find(id)
	if e != nil {
		return e
	}
	if updated != "" && updated != m.Updated.UTC().Format(time.RFC3339Nano) {
		return fail(2, true, "message changed; refresh before resolving")
	}
	if action != "drop" && action != "resend" {
		return errors.New("resolve action must be resend or drop")
	}
	if action == "resend" {
		if m.State == states.Injected {
			return nil
		}
		if m.State != states.Dead && m.State != states.Indeterminate {
			return errors.New("only dead or indeterminate mail can be resent")
		}
		if len(d.Sessions[m.Recipient].Outstanding) > 0 {
			return fail(2, true, "receiver has outstanding work")
		}
		for _, a := range m.Attempts {
			if a.Positive {
				return b.Finish(ctx, a, Result{Outcome: Accepted}, nil)
			}
		}
		uncertain := m.State == states.Indeterminate
		for _, a := range m.Attempts {
			if a.Outcome != NotSent && !a.Positive {
				uncertain = true
			}
		}
		if uncertain && !confirm {
			return fail(2, false, "resend may run twice; pass --confirm")
		}
		if uncertain {
			for _, a := range m.Attempts {
				if a.Outcome == NotSent && m.State != states.Indeterminate {
					continue
				}
				found, err := b.lookup(ctx, d, a, c)
				if err != nil {
					return fail(2, true, "fresh lookup failed: "+err.Error())
				}
				if found || a.Positive {
					if e = b.Finish(ctx, a, Result{Outcome: Accepted}, nil); e != nil {
						return e
					}
					return nil
				}
			}
		}
	}
	e = b.withConfig(ctx, func(c policy.Config) error {
		return b.Store.Update(ctx, func(d *store.Data) error {
			current, e := d.Find(id)
			if e != nil {
				return e
			}
			if current.Updated != m.Updated || current.State != m.State {
				return fail(2, true, "message changed; refresh before resolving")
			}
			now := b.now()
			if action == "drop" {
				b.hook("before_resolve_commit")
				return d.Move(current, states.Dropped, "operator", "dropped by operator", now)
			}
			if current.Hop > c.HopLimit {
				return fail(2, false, "hop limit exceeded")
			}
			if c.Killed {
				return fail(2, true, "traffic stopped")
			}
			to := states.Queued
			ttl := c.TTL
			if current.Destination != c.Machine {
				if _, ok := c.Links[current.Destination]; !ok {
					return fail(2, false, "destination machine is not linked")
				}
				to = states.Outbox
				ttl = c.OutboxTTL
			} else {
				s, ok := d.Sessions[current.Recipient]
				if !ok || s.Tombstone || s.Generation == "" {
					return fail(2, false, "recipient registration is not active")
				}
				if len(s.Outstanding) > 0 {
					return fail(2, true, "receiver has outstanding work")
				}
				if c.OptOut[s.ID] {
					return fail(2, false, "recipient opted out")
				}
				if _, ok := c.Links[current.Origin]; current.Origin != c.Machine && !ok {
					return fail(2, false, "sender machine is not linked")
				}
			}
			current.BudgetEpoch++
			current.Failures = 0
			current.Sequence = d.NextSequence()
			current.Expires = now.Add(policy.Seconds(ttl))
			current.NextAttempt = time.Time{}
			current.Released = true
			b.hook("before_resolve_commit")
			return d.Move(current, to, "operator", "explicit resend; duplicate execution possible", now)
		})
	})
	if e == nil {
		b.hook("after_resolve_commit")
	}
	return e
}
func (b *Broker) Prune(ctx context.Context, id string, confirm bool, expect *int, caller Caller) (int, error) {
	if e := operator(caller); e != nil {
		return 0, e
	}
	if !confirm {
		return 0, fail(2, false, "prune requires --confirm")
	}
	unlock, e := b.lock(ctx, id)
	if e != nil {
		return 0, e
	}
	defer unlock()
	if b.busy(id) {
		return 0, fail(2, true, "recipient has an outstanding driver call")
	}
	d, c, e := b.Data(ctx)
	if e != nil {
		return 0, e
	}
	s, ok := d.Sessions[id]
	if !ok {
		return 0, errors.New("session not registered")
	}
	if s.Tombstone {
		if expect != nil && *expect != 0 {
			return 0, fail(2, true, "unsent count changed")
		}
		return 0, nil
	}
	if len(s.Outstanding) > 0 {
		return 0, fail(2, true, "receiver has outstanding work")
	}
	a, e := b.availability(ctx, b.Drivers[s.Harness], s, c)
	if e != nil || (a.State != "offline" && a.State != "dead") {
		return 0, fail(2, true, "prune requires a fresh offline or dead session probe")
	}
	n := 0
	e = b.withConfig(ctx, func(c policy.Config) error {
		return b.Store.Update(ctx, func(d *store.Data) error {
			current := d.Sessions[id]
			if current.Generation != s.Generation {
				return fail(2, true, "registration changed")
			}
			for _, m := range d.Messages {
				if m.Recipient == id && m.Destination == c.Machine && states.Unsent(m.State) {
					n++
				}
			}
			if expect != nil && n != *expect {
				return fail(2, true, "unsent count changed")
			}
			current.Tombstone = true
			current.PrunedAt = b.now()
			current.Generation = ""
			current.State = "offline"
			d.Sessions[id] = current
			for _, m := range d.Messages {
				if m.Recipient == id && m.Destination == c.Machine && states.Unsent(m.State) {
					if e := d.Move(m, states.Dropped, "operator", "session pruned", b.now()); e != nil {
						return e
					}
				}
			}
			b.hook("before_prune_commit")
			return nil
		})
	})
	if e == nil {
		b.hook("after_prune_commit")
	}
	return n, e
}

// AcknowledgeOutbox records positive remote durable commit, including late acknowledgements.
func (b *Broker) AcknowledgeOutbox(ctx context.Context, id string) error {
	return b.withConfig(ctx, func(c policy.Config) error {
		return b.Store.Update(ctx, func(d *store.Data) error {
			m, e := d.Find(id)
			if e != nil {
				return e
			}
			if m.Destination == c.Machine {
				return errors.New("local message cannot receive a transport acknowledgement")
			}
			if states.Terminal(m.State) {
				if m.State == states.Sent {
					return nil
				}
				for _, h := range m.History {
					if h.Reason == "late durable acknowledgement" {
						return nil
					}
				}
				m.History = append(m.History, store.History{From: m.State, To: m.State, Actor: "transport", Reason: "late durable acknowledgement", At: b.now()})
				return nil
			}
			return d.Move(m, states.Sent, "transport", "peer durably committed payload", b.now())
		})
	})
}

// Accept is the receiving mailbox boundary for an authenticated canonical wire payload.
// Authentication and machine reachability belong to the transport caller.
func (b *Broker) Accept(ctx context.Context, wire store.Message) (Receipt, error) {
	return b.accept(ctx, wire, nil)
}
func (b *Broker) accept(ctx context.Context, wire store.Message, auth *PeerRequest) (Receipt, error) {
	var receipt Receipt
	var refusal error
	wireDigest := wireHash(wire)
	e := b.withConfig(ctx, func(c policy.Config) error {
		if auth != nil {
			if _, e := b.authenticate(c, *auth); e != nil {
				return &AuthenticationError{Cause: e}
			}
			if wire.Origin != auth.Machine {
				return fail(2, false, "authenticated origin mismatch")
			}
		}
		return b.Store.Update(ctx, func(d *store.Data) error {
			key := store.Key(wire.Origin, wire.ID)
			now := b.now()
			if dd, ok := d.Dedupe[key]; ok {
				if dd.WireHash != wireDigest {
					return fail(2, false, "message ID reused with a different payload")
				}
				delete(d.Health, "admission")
				receipt = Receipt{ID: wire.ID, Created: dd.Created, State: dd.State, Expired: d.Messages[key] == nil}
				return nil
			}
			refuse := func(reason string, retry bool) error {
				d.Notice("admission", reason, "herd-msg status", now)
				refusal = fail(2, retry, reason)
				return nil
			}
			for _, v := range []string{wire.ID, wire.Origin, wire.SenderID, wire.Recipient, wire.Destination} {
				if e := policy.Identity(v); e != nil {
					return refuse(e.Error(), false)
				}
			}
			if e := policy.SenderLabel(wire.Sender); e != nil {
				return refuse(e.Error(), false)
			}
			if wire.Origin == c.Machine || wire.Destination != c.Machine {
				return refuse("invalid remote routing", false)
			}
			if !utf8.ValidString(wire.Body) || wire.Hop < 0 || len(wire.Body) > 65536 {
				return refuse("invalid hop or body size", false)
			}
			if wire.Created.After(now.Add(policy.Seconds(c.Skew))) {
				return refuse("sender clock is ahead; synchronize clocks", true)
			}
			if wire.Created.IsZero() || wire.Created.Before(now.Add(-policy.Seconds(c.Dedupe-c.Skew))) || wire.Created.After(now.Add(policy.Seconds(c.Skew))) || (!d.ReplayFloor.IsZero() && !wire.Created.After(d.ReplayFloor.Add(policy.Seconds(c.Skew)))) {
				return refuse("created timestamp is outside replay horizon", false)
			}
			s, exists := d.Sessions[wire.Recipient]
			if !exists || s.Machine != c.Machine || s.Tombstone {
				return refuse("recipient is missing or pruned", false)
			}
			wire.Recipient = s.ID
			count, pending := 0, 0
			var active int64
			for _, m := range d.Messages {
				if m.SenderID == wire.SenderID && m.Origin == wire.Origin && m.Recipient == wire.Recipient && !m.Accepted.Before(now.Add(-policy.Seconds(c.PairWindow))) {
					count++
				}
				if !states.Terminal(m.State) {
					active += int64(len(m.Body))
					if m.Recipient == wire.Recipient {
						pending++
					}
				}
			}
			dec := gateFor(c, &wire, s, true, count)
			if dec.Refuse {
				return refuse(dec.Reason, dec.Retryable)
			}
			if pending >= c.PendingLimit || active+int64(len(wire.Body)) > c.ActiveBytes {
				return refuse("pending capacity exceeded", true)
			}
			m := &store.Message{ID: wire.ID, Origin: wire.Origin, Sender: wire.Sender, SenderID: wire.SenderID, Recipient: wire.Recipient, Destination: wire.Destination, Address: wire.Address, ParentID: wire.ParentID, Body: wire.Body, Hop: wire.Hop, Created: wire.Created, Accepted: now, Updated: now, Expires: now.Add(policy.Seconds(c.TTL)), BudgetEpoch: 1, Sequence: d.NextSequence(), State: states.Queued, Error: dec.Reason, Attempts: []store.Attempt{}, History: []store.History{}}
			if dec.Held {
				m.State = states.Held
			}
			m.History = append(m.History, store.History{To: m.State, Actor: "broker", Reason: "remote accepted", At: now})
			delete(d.Health, "admission")
			b.hook("before_admission_commit")
			d.Messages[key] = m
			d.Dedupe[key] = store.Dedupe{ID: m.ID, Origin: m.Origin, WireHash: wireDigest, Created: m.Created, State: m.State}
			receipt = Receipt{ID: m.ID, Created: m.Created, State: m.State, Notice: m.Error}
			return nil
		})
	})
	if e != nil {
		return receipt, e
	}
	if refusal == nil {
		b.hook("after_admission_commit")
	}
	return receipt, refusal
}
