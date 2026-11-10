package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func TestLookupWindowAndOfflineStopAllWork(t *testing.T) {
	for _, why := range []string{"window", "offline", "gone"} {
		t.Run(why, func(t *testing.T) {
			b, f := setup(t)
			now := b.now()
			b.Now = func() time.Time { return now }
			f.result = Result{Outcome: Unconfirmed}
			send(t, b, "uncertain")
			dispatch(t, b)
			lookups := 0
			f.lookup = func(LookupRequest) (bool, error) {
				lookups++
				if why == "gone" {
					return false, ErrRecipientGone
				}
				return false, nil
			}
			switch why {
			case "window":
				now = now.Add(time.Hour)
			case "offline":
				b.Store.Update(ctx, func(d *store.Data) error {
					s := d.Sessions["receiver"]
					s.State = "offline"
					d.Sessions[s.ID] = s
					return nil
				})
			}
			if e := b.Sweep(ctx); e != nil {
				t.Fatal(e)
			}
			m := message(t, b, "uncertain")
			if m.State != states.Expired || !strings.Contains(m.Error, "delivery not proven") {
				t.Fatal(m.State, m.Error)
			}
			expected := 0
			if why == "gone" {
				expected = 1
			}
			if lookups != expected {
				t.Fatal("unnecessary history lookup", lookups)
			}
			for i := 0; i < 10; i++ {
				if e := b.Tick(ctx); e != nil {
					t.Fatal(e)
				}
			}
			if lookups != expected {
				t.Fatal("terminal mail still costs lookups")
			}
		})
	}
}

func TestLookupBackoffAndSchedulerUsesDueTime(t *testing.T) {
	b, f := setup(t)
	now := b.now()
	b.Now = func() time.Time { return now }
	f.result = Result{Outcome: Unconfirmed}
	send(t, b, "first")
	dispatch(t, b)
	send(t, b, "second")
	calls := 0
	f.lookup = func(LookupRequest) (bool, error) { calls++; return false, nil }
	for i := 0; i < 7; i++ {
		if e := b.confirmDue(ctx); e != nil {
			t.Fatal(e)
		}
		m := message(t, b, "first")
		delay := m.NextLookup.Sub(now)
		want := min(10*time.Second*time.Duration(1<<i), 10*time.Minute)
		if delay != want {
			t.Fatalf("check %d backoff=%s want=%s", i, delay, want)
		}
		d, _ := b.Store.Read(ctx)
		sleep := b.tickDelay(d, 200*time.Millisecond)
		if sleep < min(delay, 30*time.Second) {
			t.Fatal("receipt or blocked successor retained a fast timer", sleep)
		}
		if e := b.confirmDue(ctx); e != nil {
			t.Fatal(e)
		}
		if calls != i+1 {
			t.Fatal("looked up before due")
		}
		now = m.NextLookup
		b.mu.Lock()
		b.nextSweep = now.Add(10 * time.Minute)
		b.mu.Unlock()
	}
}

func TestHygieneExpiresUnavailableAndPreservesFences(t *testing.T) {
	b, _ := setup(t)
	now := b.now()
	b.Now = func() time.Time { return now }
	for _, id := range []string{"queued", "held", "fresh", "live"} {
		send(t, b, id)
	}
	if e := b.Store.Update(ctx, func(d *store.Data) error {
		old := now.Add(-8 * 24 * time.Hour)
		for _, id := range []string{"stale", "tombstone", "outstanding", "successor", "pending", "detached"} {
			d.Sessions[id] = store.Session{ID: id, Machine: "local", Harness: "claude", State: "offline", LastSeen: old, Generation: "g"}
		}
		s := d.Sessions["tombstone"]
		s.Tombstone = true
		d.Sessions[s.ID] = s
		s = d.Sessions["outstanding"]
		s.Outstanding = []string{"external"}
		d.Sessions[s.ID] = s
		s = d.Sessions["successor"]
		s.Successor = "receiver"
		d.Sessions[s.ID] = s
		s = d.Sessions["detached"]
		s.State = "detached"
		d.Sessions[s.ID] = s
		for _, id := range []string{"queued", "held"} {
			m, _ := d.Find(id)
			m.Created = now.Add(-25 * time.Hour)
			m.Recipient = "detached"
			if id == "held" {
				if e := d.Move(m, states.Held, "broker", "held", now); e != nil {
					return e
				}
			}
		}
		fresh, _ := d.Find("fresh")
		fresh.Recipient = "pending"
		live, _ := d.Find("live")
		live.Created = old
		live.Expires = now.Add(time.Hour)
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e := b.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	for _, id := range []string{"queued", "held"} {
		if m := message(t, b, id); m.State != states.Expired || m.Error == "" {
			t.Fatal(id, m.State, m.Error)
		}
	}
	if message(t, b, "fresh").State != states.Queued || message(t, b, "live").State != states.Queued {
		t.Fatal("expired fresh or live mail")
	}
	d, _ := b.Store.Read(ctx)
	if _, ok := d.Sessions["stale"]; ok {
		t.Fatal("stale session retained")
	}
	for _, id := range []string{"tombstone", "outstanding", "successor", "pending", "receiver"} {
		if _, ok := d.Sessions[id]; !ok {
			t.Fatal("lost protocol/session dependency", id)
		}
	}
}

func TestRetentionDeletesOnlyOneBatchAndKeepsDedupe(t *testing.T) {
	b, _ := setup(t)
	now := b.now()
	b.Now = func() time.Time { return now }
	if e := b.Store.Update(ctx, func(d *store.Data) error {
		for i := 0; i < 205; i++ {
			id := fmt.Sprintf("old-%d", i)
			key := store.Key("local", id)
			d.Messages[key] = &store.Message{ID: id, Origin: "local", State: states.Injected, TerminalAt: now.Add(-8 * 24 * time.Hour)}
			d.Dedupe[key] = store.Dedupe{ID: id, Origin: "local", TerminalAt: now.Add(-8 * 24 * time.Hour), State: states.Injected}
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e := b.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	d, _ := b.Store.Read(ctx)
	if len(d.Messages) != 105 || len(d.Dedupe) != 205 {
		t.Fatal("unbounded deletes or lost replay protection", len(d.Messages), len(d.Dedupe))
	}
	for i := 0; i < 5; i++ {
		if e := b.Tick(ctx); e != nil {
			t.Fatal(e)
		}
	}
	d, _ = b.Store.Read(ctx)
	if len(d.Messages) != 105 {
		t.Fatal("maintenance ran on every tick")
	}
}

func TestDetachedDiscoveryDoesNotRenewOfflineGrace(t *testing.T) {
	b, _ := setup(t)
	now := b.now()
	b.Now = func() time.Time { return now }
	s := store.Session{ID: "detached", Name: "detached", Harness: "codex", State: "detached"}
	if e := b.Register(ctx, s); e != nil {
		t.Fatal(e)
	}
	d, _ := b.Store.Read(ctx)
	seen := d.Sessions[s.ID].LastSeen
	now = now.Add(25 * time.Hour)
	if e := b.RefreshHarness(ctx, "codex", []store.Session{s}); e != nil {
		t.Fatal(e)
	}
	d, _ = b.Store.Read(ctx)
	if !d.Sessions[s.ID].LastSeen.Equal(seen) {
		t.Fatal("discovery renewed detached grace")
	}
}

// Config changes should take effect at the next maintenance pass, while leaving
// unrelated delivery timers independent from the slow sweep.
func TestHygieneConfigDefaults(t *testing.T) {
	c := policy.Defaults()
	if c.LookupWindow != 3600 || c.SweepInterval != 600 || c.OfflineGrace != 86400 || c.SessionRetention != 604800 || c.Retention != 604800 {
		t.Fatal(c)
	}
}

type preparingDriver struct {
	Driver
	preparation string
	prepareErr  error
}

func (d preparingDriver) Prepare(context.Context, store.Session, string) (string, error) {
	return d.preparation, d.prepareErr
}

func TestPreparationProofIsCommittedBeforeInputAndSurvivesTimeout(t *testing.T) {
	b, f := setup(t)
	b.Drivers["claude"] = preparingDriver{Driver: f, preparation: "receipt-start"}
	f.inject = func(_ context.Context, _ store.Session, _, aid string) (Result, error) {
		d, e := b.Store.Read(ctx)
		if e != nil {
			t.Error(e)
			return Result{}, e
		}
		m, _ := d.Find("prepared")
		if len(m.Attempts) != 1 || m.Attempts[0].ID != aid || m.Attempts[0].Proof != "receipt-start" {
			t.Error("input started before preparation committed")
		}
		return Result{Outcome: Unconfirmed}, context.DeadlineExceeded
	}
	send(t, b, "prepared")
	dispatch(t, b)
	m := message(t, b, "prepared")
	if m.State != states.Indeterminate || m.Attempts[0].Proof != "receipt-start" {
		t.Fatal("timeout discarded durable preparation", m.State, m.Attempts)
	}
}

func TestPreparationFailureNeverStartsInput(t *testing.T) {
	b, f := setup(t)
	b.Drivers["claude"] = preparingDriver{Driver: f, prepareErr: errors.New("transcript unavailable")}
	send(t, b, "no-input")
	dispatch(t, b)
	m := message(t, b, "no-input")
	if m.State != states.Queued || len(m.Attempts) != 0 || len(f.calls) != 0 || !m.NextAttempt.After(b.now()) {
		t.Fatal("preparation failure started input or stranded claim", m)
	}
}
