package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func bounded[T any](ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	type answer struct {
		v T
		e error
	}
	ch := make(chan answer, 1)
	go func() { v, e := fn(ctx); ch <- answer{v, e} }()
	select {
	case r := <-ch:
		return r.v, r.e
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
func (b *Broker) availability(ctx context.Context, driver Driver, s store.Session, c policy.Config) (Availability, error) {
	b.mu.Lock()
	paused := b.paused[s.Harness]
	b.mu.Unlock()
	if paused != "" {
		return Availability{State: "unavailable", Reason: paused}, nil
	}
	if driver == nil {
		return Availability{State: "unavailable", Reason: "driver is not installed"}, nil
	}
	call, cancel := context.WithTimeout(ctx, policy.Seconds(c.DriverTimeout))
	defer cancel()
	return bounded(call, func(ctx context.Context) (Availability, error) {
		if e := driver.Probe(ctx); e != nil {
			return Availability{}, e
		}
		return driver.SessionState(ctx, s)
	})
}
func (b *Broker) Dispatch(ctx context.Context, recipient string) error {
	lockctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	unlock, e := b.lock(lockctx, recipient)
	if e != nil {
		return nil
	}
	defer unlock()
	b.mu.Lock()
	stop := b.stopping
	b.mu.Unlock()
	if stop || b.busy(recipient) {
		return nil
	}
	d, c, e := b.Data(ctx)
	if e != nil {
		return e
	}
	s, ok := d.Sessions[recipient]
	if !ok || s.Tombstone || s.Generation == "" {
		return nil
	}
	// Discovery refreshes unavailable recipients and wakes delivery. Retrying a
	// known offline row every few seconds only clones and rewrites the archive.
	if s.State == "offline" || s.State == "detached" || s.State == "dead" {
		return nil
	}
	if len(s.Outstanding) > 0 {
		return b.Store.Update(ctx, func(d *store.Data) error { receiverNotice(d, s, b.now()); return nil })
	}
	for _, m := range d.Messages {
		if m.Recipient == recipient && m.Destination == c.Machine && (m.State == states.Indeterminate || (m.State == states.Dead && unresolved(m)) || m.State == states.Injecting || m.State == states.Claimed) {
			return nil
		}
	}
	// A recipient can have no queued mail after another dispatch wins its lock.
	var next *store.Message
	for _, m := range d.Messages {
		if m.Recipient == recipient && m.Destination == c.Machine && m.State == states.Queued && (next == nil || m.Sequence < next.Sequence || (m.Sequence == next.Sequence && m.ID < next.ID)) {
			next = m
		}
	}
	if next == nil || b.now().Before(next.NextAttempt) {
		return nil
	}
	driver := b.Drivers[s.Harness]
	availability, e := b.availability(ctx, driver, s, c)
	if e != nil {
		availability = Availability{State: "unavailable", Reason: e.Error()}
	}
	observed := s
	observed.State, observed.Blocked = availability.State, availability.Blocked
	decision := gateFor(c, next, observed, false, 0)
	if (decision.Wait || decision.Held) && next.Error == decision.Reason && s.State == availability.State && s.Blocked == availability.Blocked && s.Reason == availability.Reason && sameHints(s.Hints, availability.Hints) && (s.State != "live" || !seenDue(s, b.now())) {
		return nil
	}
	var key, token string
	e = b.withConfig(ctx, func(c policy.Config) error {
		return b.Store.Update(ctx, func(d *store.Data) error {
			now := b.now()
			if e := b.reconcile(d, c, now); e != nil {
				return e
			}
			current := d.Sessions[recipient]
			if current.Generation != s.Generation {
				return nil
			}
			current.State = availability.State
			current.Blocked = availability.Blocked
			current.Reason = availability.Reason
			if current.Hints == nil {
				current.Hints = map[string]string{}
			}
			for k, v := range availability.Hints {
				current.Hints[k] = v
			}
			if availability.State == "live" && (current.LastSeen.IsZero() || now.Sub(current.LastSeen) >= time.Minute) {
				current.LastSeen = now
			}
			d.Sessions[recipient] = current
			s = current
			var head *store.Message
			for _, m := range d.Messages {
				if m.Recipient != recipient || m.Destination != c.Machine {
					continue
				}
				if m.State == states.Claimed || m.State == states.Injecting || m.State == states.Indeterminate || (m.State == states.Dead && unresolved(m)) {
					return nil
				}
				if m.State == states.Queued && (head == nil || m.Sequence < head.Sequence || (m.Sequence == head.Sequence && m.ID < head.ID)) {
					head = m
				}
			}
			if head == nil || now.Before(head.NextAttempt) {
				return nil
			}
			dec := gateFor(c, head, s, false, 0)
			if dec.Refuse || dec.Held || dec.Wait {
				head.Error = dec.Reason
				return nil
			}
			starts := 0
			for _, m := range d.Messages {
				if m.Recipient == recipient {
					for _, a := range m.Attempts {
						if !a.Started.Before(now.Add(-time.Minute)) && !(a.Outcome == NotSent && freeClass(a.Class)) {
							starts++
						}
					}
				}
			}
			if starts >= c.InjectionLimit {
				head.Error = "recipient injection pace exceeded"
				return nil
			}
			token = store.Random()
			key = store.Key(head.Origin, head.ID)
			if e := d.Move(head, states.Claimed, "broker", "claimed", now); e != nil {
				return e
			}
			head.ClaimToken = token
			head.BrokerEpoch = b.Epoch
			head.Lease = now.Add(policy.Seconds(c.Claim))
			head.ClaimGeneration = s.Generation
			b.mu.Lock()
			b.claimStarted[token] = time.Now()
			b.mu.Unlock()
			b.hook("before_claim_commit")
			return nil
		})
	})
	if e != nil || key == "" {
		return e
	}
	b.hook("after_claim_commit")
	aid := store.Random()
	preparation := ""
	if preparer, ok := driver.(interface {
		Prepare(context.Context, store.Session, string) (string, error)
	}); ok {
		call, cancel := context.WithTimeout(ctx, policy.Seconds(c.DriverTimeout))
		preparation, e = bounded(call, func(ctx context.Context) (string, error) { return preparer.Prepare(ctx, s, aid) })
		cancel()
		if e != nil {
			reason := "receipt preparation failed before send: " + e.Error()
			b.mu.Lock()
			delete(b.claimStarted, token)
			b.mu.Unlock()
			return b.Store.Update(context.Background(), func(d *store.Data) error {
				m := d.Messages[key]
				if m == nil || m.State != states.Claimed || m.ClaimToken != token {
					return nil
				}
				m.NextAttempt = b.now().Add(policy.Seconds(c.LookupInterval))
				return d.Move(m, states.Queued, "broker", reason, b.now())
			})
		}
	}
	var attempt store.Attempt
	e = b.withConfig(ctx, func(c policy.Config) error {
		return b.Store.Update(ctx, func(d *store.Data) error {
			now := b.now()
			m := d.Messages[key]
			if m == nil || m.State != states.Claimed || m.ClaimToken != token || m.BrokerEpoch != b.Epoch {
				return nil
			}
			s = d.Sessions[recipient]
			b.mu.Lock()
			start, valid := b.claimStarted[token]
			b.mu.Unlock()
			if !now.Before(m.Lease) || !valid || time.Since(start) >= policy.Seconds(c.Claim) || s.Generation != m.ClaimGeneration || s.Tombstone || s.Generation == "" {
				return d.Move(m, states.Queued, "broker", "claim fence changed", now)
			}
			if e := b.reconcile(d, c, now); e != nil {
				return e
			}
			if m.State != states.Claimed {
				return nil
			}
			dec := gateFor(c, m, s, false, 0)
			if dec.Wait || len(s.Outstanding) > 0 {
				return d.Move(m, states.Queued, "broker", dec.Reason, now)
			}
			for _, a := range m.Attempts {
				if a.Positive {
					return d.Move(m, states.Injected, "broker", "prior acceptance proof", now)
				}
			}
			attempt = store.Attempt{Proof: preparation, ID: aid, MessageID: m.ID, Origin: m.Origin, Session: s, BudgetEpoch: m.BudgetEpoch, Envelope: Envelope(*m, aid), Started: now}
			if e := d.Move(m, states.Injecting, "broker", "attempt committed", now); e != nil {
				return e
			}
			m.ActiveAttempt = aid
			m.Envelope = attempt.Envelope
			m.Attempts = append(m.Attempts, attempt)
			// Publish before the injecting commit so an orphan sweep cannot race the call.
			b.mu.Lock()
			b.outstanding[aid] = recipient
			b.mu.Unlock()
			b.hook("before_injecting_commit")
			return nil
		})
	})
	b.mu.Lock()
	delete(b.claimStarted, token)
	b.mu.Unlock()
	if e != nil {
		if attempt.ID != "" {
			b.clearOutstanding(attempt.ID)
		}
		return e
	}
	if attempt.ID == "" {
		return nil
	}
	b.hook("after_injecting_commit")
	call, cancelCall := context.WithTimeout(ctx, policy.Seconds(c.DriverTimeout))
	defer cancelCall()
	type answer struct {
		r Result
		e error
	}
	ch := make(chan answer, 1)
	go func() { r, e := safeInject(call, driver, attempt); ch <- answer{r, e} }()
	select {
	case result := <-ch:
		b.hook("after_driver_return")
		e = b.finishRetry(attempt, result.r, result.e)
		b.clearOutstanding(attempt.ID)
		b.hook("after_result_commit")
		return e
	case <-call.Done():
		e = b.finishRetry(attempt, Result{Outcome: Unconfirmed}, call.Err())
		_ = b.Store.Update(context.Background(), func(d *store.Data) error {
			d.Notice("outstanding/"+recipient, "driver call did not return; recipient paused", "restart broker after fencing the driver", b.now())
			return nil
		})
		b.hook("after_timeout_commit")
		go func() { result := <-ch; _ = b.finishRetry(attempt, result.r, result.e); b.clearOutstanding(attempt.ID) }()
		return e
	}
}
func safeInject(ctx context.Context, driver Driver, a store.Attempt) (r Result, e error) {
	defer func() {
		if p := recover(); p != nil {
			r = Result{Outcome: Unconfirmed}
			e = fmt.Errorf("driver panic: %v", p)
		}
	}()
	return driver.Inject(ctx, a.Session, a.Envelope, a.ID)
}
func (b *Broker) clearOutstanding(aid string) {
	b.mu.Lock()
	recipient := b.outstanding[aid]
	delete(b.outstanding, aid)
	b.mu.Unlock()
	_ = b.Store.Update(context.Background(), func(d *store.Data) error { delete(d.Health, "outstanding/"+recipient); return nil })
}
func freeClass(c string) bool {
	return c == "transport" || c == "detached" || c == "unsupported" || c == "precondition_race"
}
func (b *Broker) finishRetry(a store.Attempt, r Result, err error) error {
	var e error
	for i := 0; i < 3; i++ {
		e = b.Finish(context.Background(), a, r, err)
		if e == nil {
			return nil
		}
		time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
	}
	return e
}

// Finish records immutable-attempt results, including late proof from older epochs.
func (b *Broker) Finish(ctx context.Context, a store.Attempt, r Result, callErr error) error {
	if callErr != nil || (r.Outcome != Accepted && (r.Outcome != NotSent || r.Proof == "" || r.Class == "")) {
		r = Result{Outcome: Unconfirmed}
	}
	return b.Store.Update(ctx, func(d *store.Data) error {
		m := d.Messages[store.Key(a.Origin, a.MessageID)]
		if m == nil {
			return errors.New("attempt payload missing")
		}
		index := -1
		for i := range m.Attempts {
			if m.Attempts[i].ID == a.ID {
				index = i
				break
			}
		}
		if index < 0 {
			return errors.New("attempt not recorded")
		}
		stored := &m.Attempts[index]
		stored.Finished = b.now()
		if r.Outcome == Accepted {
			stored.Positive = true
			stored.Outcome = Accepted
			stored.Proof = "driver acceptance"
		} else if !stored.Positive {
			stored.Outcome = r.Outcome
			if r.Proof != "" {
				stored.Proof = r.Proof
			}
			stored.Class = r.Class
		}
		b.hook("before_result_commit")
		positive := false
		for _, old := range m.Attempts {
			positive = positive || old.Positive
		}
		if states.Terminal(m.State) {
			return nil
		}
		current := m.State == states.Injecting && m.ActiveAttempt == a.ID
		if positive {
			if m.State == states.Injecting && !current {
				return nil
			}
			return d.Move(m, states.Injected, "broker", "acceptance confirmed", b.now())
		}
		if !current {
			return nil
		}
		if r.Outcome == NotSent {
			if !freeClass(r.Class) {
				m.Failures++
			}
			if m.Failures >= 4 {
				return d.Move(m, states.Dead, "broker", "four proven delivery failures", b.now())
			}
			// Cumulative attempt count bounds backoff even for free failure classes.
			delay := time.Second * time.Duration(1<<min(len(m.Attempts), 8))
			m.NextAttempt = b.now().Add(delay)
			return d.Move(m, states.Queued, "broker", r.Proof, b.now())
		}
		m.LookupChecks = 0
		m.NextLookup = b.now()
		return d.Move(m, states.Indeterminate, "broker", "awaiting_confirmation", b.now())
	})
}
func successors(d *store.Data, id string) []store.Session {
	out := []store.Session{}
	seen := map[string]bool{}
	for {
		s, ok := d.Sessions[id]
		if !ok || s.Successor == "" || seen[s.Successor] {
			return out
		}
		seen[s.Successor] = true
		next, ok := d.Sessions[s.Successor]
		if !ok {
			return out
		}
		out = append(out, next)
		id = next.ID
	}
}
func (b *Broker) lookup(ctx context.Context, d *store.Data, a store.Attempt, c policy.Config) (bool, error) {
	driver := b.Drivers[a.Session.Harness]
	if driver == nil {
		return false, errors.New("lookup driver unavailable")
	}
	call, cancel := context.WithTimeout(ctx, policy.Seconds(c.DriverTimeout))
	defer cancel()
	return bounded(call, func(ctx context.Context) (bool, error) {
		return driver.Lookup(ctx, LookupRequest{a, successors(d, a.Session.ID)})
	})
}
func (b *Broker) confirm(ctx context.Context, key string) error {
	d, c, e := b.Data(ctx)
	if e != nil {
		return e
	}
	m := d.Messages[key]
	if m == nil || states.Terminal(m.State) {
		return nil
	}
	if reason := lookupExpired(d, m, c, b.now()); reason != "" {
		return b.Store.Update(ctx, func(d *store.Data) error {
			current := d.Messages[key]
			if !sameLookup(m, current) {
				return nil
			}
			reason := lookupExpired(d, current, c, b.now())
			if reason == "" {
				return nil
			}
			return d.Move(current, states.Expired, "broker", reason, b.now())
		})
	}
	for _, a := range m.Attempts {
		if a.Positive {
			if e = b.Finish(ctx, a, Result{Outcome: Accepted}, nil); e != nil {
				return e
			}
			continue
		}
		if a.Outcome == NotSent || (!a.Started.IsZero() && !b.now().Before(a.Started.Add(policy.Seconds(c.LookupWindow)))) {
			continue
		}
		b.hook("before_lookup")
		found, e := b.lookup(ctx, d, a, c)
		if errors.Is(e, ErrRecipientGone) {
			return b.Store.Update(ctx, func(d *store.Data) error {
				current := d.Messages[key]
				if !sameLookup(m, current) {
					return nil
				}
				return d.Move(current, states.Expired, "broker", "unconfirmed; delivery not proven before recipient thread disappeared", b.now())
			})
		}
		if e != nil {
			continue
		}
		b.hook("after_lookup")
		if found {
			if e = b.Finish(ctx, a, Result{Outcome: Accepted}, nil); e != nil {
				return e
			}
		}
	}
	return b.Store.Update(ctx, func(d *store.Data) error {
		if current := d.Messages[key]; sameLookup(m, current) {
			current.LookupChecks++
			delay := policy.Seconds(c.LookupInterval) * time.Duration(1<<min(current.LookupChecks-1, 6))
			current.NextLookup = b.now().Add(min(delay, 10*time.Minute))
		}
		return nil
	})
}
func (b *Broker) maintain(ctx context.Context) error {
	b.sweepMu.Lock()
	defer b.sweepMu.Unlock()
	var retention bool
	var interval time.Duration
	e := b.withConfig(ctx, func(c policy.Config) error {
		interval = policy.Seconds(c.SweepInterval)
		return b.Store.Update(ctx, func(d *store.Data) error {
			now := b.now()
			deleted := 0
			if _, pending := d.Health["checkpoint"]; pending {
				retention = true
			}
			for key, m := range d.Messages {
				expiredClaim := false
				if m.State == states.Claimed {
					b.mu.Lock()
					started, exists := b.claimStarted[m.ClaimToken]
					b.mu.Unlock()
					expiredClaim = !exists || time.Since(started) >= policy.Seconds(c.Claim)
				}
				if m.State == states.Claimed && (m.BrokerEpoch != b.Epoch || !now.Before(m.Lease) || expiredClaim) {
					if e := d.Move(m, states.Queued, "broker", "claim lease recovered", now); e != nil {
						return e
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
				if e := settleStored(d, m, now); e != nil {
					return e
				}
				if reason := lookupExpired(d, m, c, now); reason != "" {
					if e := d.Move(m, states.Expired, "broker", reason, now); e != nil {
						return e
					}
				}
				if (m.State == states.Queued || m.State == states.Held) && m.Destination == c.Machine {
					s, ok := d.Sessions[m.Recipient]
					if ok && (s.State == "offline" || s.State == "detached" || s.State == "dead") && now.Sub(maxTime(s.LastSeen, m.Created)) >= policy.Seconds(c.OfflineGrace) {
						if e := d.Move(m, states.Expired, "broker", "recipient unavailable beyond offline grace", now); e != nil {
							return e
						}
					}
				}
				if m.State == states.Indeterminate || m.State == states.Dead {
					if now.Sub(m.Updated) >= policy.Seconds(c.NoticeGrace) {
						if h, exists := d.Health["message/"+key]; !exists || h.Detail != m.Error {
							d.Notice("message/"+key, m.Error, "input may still be pending; wait for the recipient turn boundary before considering herd-msg resolve "+m.ID+" resend|drop", now)
						}
					}
				}
				if deleted < 100 && states.Terminal(m.State) && now.Sub(m.TerminalAt) >= policy.Seconds(c.Retention) {
					b.hook("before_retention_commit")
					delete(d.Messages, key)
					delete(d.Health, "message/"+key)
					deleted++
					retention = true
				}
			}
			// Keep fences and successor chains, even after their sessions go stale.
			needed := map[string]bool{}
			for _, m := range d.Messages {
				if !states.Terminal(m.State) {
					needed[m.Recipient] = true
					needed[m.SenderID] = true
				}
			}
			for _, s := range d.Sessions {
				if s.Successor != "" {
					needed[s.Successor] = true
				}
			}
			pruned := 0
			for id, s := range d.Sessions {
				if pruned < 100 && s.State == "offline" && !s.Tombstone && s.PrunedAt.IsZero() && s.Successor == "" && len(s.Outstanding) == 0 && !needed[id] && now.Sub(s.LastSeen) >= policy.Seconds(c.SessionRetention) {
					delete(d.Sessions, id)
					pruned++
				}
			}
			dedupeDeleted := 0
			for key, dd := range d.Dedupe {
				if dedupeDeleted >= 100 {
					break
				}
				if !dd.TerminalAt.IsZero() && now.Sub(dd.TerminalAt) >= policy.Seconds(c.Dedupe) {
					if m := d.Messages[key]; m != nil {
						continue
					}
					if dd.Created.After(d.ReplayFloor) {
						d.ReplayFloor = dd.Created
					}
					delete(d.Dedupe, key)
					dedupeDeleted++
				}
			}
			b.hook("before_sweep_commit")
			if e := b.reconcile(d, c, now); e != nil {
				return e
			}
			return nil
		})
	})
	if e != nil {
		return e
	}
	b.mu.Lock()
	b.nextSweep = b.now().Add(interval)
	b.mu.Unlock()
	b.hook("after_sweep_commit")
	if retention {
		b.hook("after_retention_commit")
	}
	if e = b.Store.Checkpoint(ctx); e != nil {
		_ = b.Store.Update(ctx, func(d *store.Data) error {
			d.Notice("checkpoint", e.Error(), "retry retention sweep", b.now())
			return nil
		})
	} else {
		_ = b.Store.Update(ctx, func(d *store.Data) error { delete(d.Health, "checkpoint"); return nil })
	}
	return nil
}

func (b *Broker) Sweep(ctx context.Context) error {
	if e := b.maintain(ctx); e != nil {
		return e
	}
	return b.confirmDue(ctx)
}

func (b *Broker) confirmDue(ctx context.Context) error {
	d, c, e := b.Data(ctx)
	if e != nil {
		return e
	}
	var pending []*store.Message
	for _, m := range d.Messages {
		if (m.State == states.Indeterminate || m.State == states.Dead) && unresolved(m) && (!b.now().Before(m.NextLookup) || lookupExpired(d, m, c, b.now()) != "") {
			pending = append(pending, m)
		}
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].NextLookup.Equal(pending[j].NextLookup) {
			return pending[i].Sequence < pending[j].Sequence
		}
		return pending[i].NextLookup.Before(pending[j].NextLookup)
	})
	for i, m := range pending {
		if i >= 50 {
			break
		}
		if e = b.confirm(ctx, store.Key(m.Origin, m.ID)); e != nil {
			return e
		}
	}
	return nil
}
func (b *Broker) Tick(ctx context.Context) error {
	ctx = identity.CachedAgents(ctx)
	defer b.hook("after_tick")
	b.mu.Lock()
	due := !b.now().Before(b.nextSweep)
	b.mu.Unlock()
	if due {
		if e := b.maintain(ctx); e != nil {
			return e
		}
	}
	if e := b.recoverDue(ctx); e != nil {
		return e
	}
	if e := b.confirmDue(ctx); e != nil {
		return e
	}
	d, _, e := b.Data(ctx)
	if e != nil {
		return e
	}
	recipients := map[string]bool{}
	for _, m := range d.Messages {
		if m.State == states.Queued {
			s := d.Sessions[m.Recipient]
			if s.State == "offline" || s.State == "detached" || s.State == "dead" {
				continue
			}
			recipients[m.Recipient] = true
		}
	}
	for id := range recipients {
		go func(id string) { _ = b.Dispatch(ctx, id) }(id)
	}
	return nil
}

func settleStored(d *store.Data, m *store.Message, now time.Time) error {
	if states.Terminal(m.State) || m.State == states.Injecting {
		return nil
	}
	for _, a := range m.Attempts {
		if a.Positive {
			return d.Move(m, states.Injected, "broker", "stored acceptance proof", now)
		}
	}
	return nil
}
