package broker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func TestSameNameAfterClearAndPrune(t *testing.T) {
	b, f := setup(t)
	send(t, b, "pinned")
	if e := b.Rebind(ctx, "receiver", store.Session{ID: "next", Name: "reviewer", Harness: "claude", State: "live"}); e != nil {
		t.Fatal(e)
	}
	r := request(b, "by-name")
	r.To = "reviewer"
	if _, e := b.Send(ctx, r, agent); e != nil {
		t.Fatal(e)
	}
	if message(t, b, r.ID).Recipient != "next" || message(t, b, "pinned").Recipient != "receiver" {
		t.Fatal("name moved accepted mail")
	}
	f.availability = Availability{State: "offline"}
	if _, e := b.Prune(ctx, "receiver", true, nil, op); e != nil {
		t.Fatal(e)
	}
	r.ID = "after-prune"
	if _, e := b.Send(ctx, r, agent); e != nil {
		t.Fatal(e)
	}
	// Reactivating a tombstone preserves audit and gives it a new generation.
	d, _, _ := b.Data(ctx)
	pruned := d.Sessions["receiver"].PrunedAt
	if e := b.Register(ctx, store.Session{ID: "receiver", Name: "resumed", Harness: "claude", State: "live"}); e != nil {
		t.Fatal(e)
	}
	d, _, _ = b.Data(ctx)
	s := d.Sessions["receiver"]
	if s.Tombstone || s.Generation == "" || s.PrunedAt != pruned {
		t.Fatal(s)
	}
}
func TestOrphanSweepUsesStoredOlderAcceptance(t *testing.T) {
	b, f := setup(t)
	f.result = Result{Outcome: Unconfirmed}
	send(t, b, "orphan")
	dispatch(t, b)
	old := message(t, b, "orphan").Attempts[0]
	if e := b.Resolve(ctx, "orphan", "resend", true, "", op); e != nil {
		t.Fatal(e)
	}
	if e := b.Store.Update(ctx, func(d *store.Data) error {
		m, _ := d.Find("orphan")
		m.Attempts[0].Positive = true
		m.Attempts[0].Outcome = Accepted
		if e := d.Move(m, states.Claimed, "broker", "claim", b.now()); e != nil {
			return e
		}
		if e := d.Move(m, states.Injecting, "broker", "begin B", b.now()); e != nil {
			return e
		}
		m.ActiveAttempt = "orphan-B"
		m.NextLookup = b.now().Add(time.Hour)
		m.Attempts = append(m.Attempts, store.Attempt{ID: "orphan-B", MessageID: m.ID, Origin: m.Origin, Session: old.Session, BudgetEpoch: 2})
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	f.lookupErr = errors.New("lookup unavailable")
	checked := false
	b.Fault = func(point string) {
		if point == "after_sweep_commit" {
			checked = true
			if message(t, b, "orphan").State != states.Injected {
				t.Error("orphan recovery waited for lookup")
			}
		}
	}
	if e := b.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	if !checked || message(t, b, "orphan").State != states.Injected {
		t.Fatal("stored proof ignored after orphan recovery")
	}
	dispatch(t, b)
	if len(f.calls) != 1 {
		t.Fatal("orphan resent")
	}
}
func TestConfigPatchesAndHealthRecovery(t *testing.T) {
	b, f := setup(t)
	if _, e := b.Kill(ctx, true, op); e != nil {
		t.Fatal(e)
	}
	c, e := b.Config(ctx, map[string]json.RawMessage{"hop_limit": json.RawMessage(`5`)}, op)
	if e != nil || !c.Killed {
		t.Fatal("unrelated patch overwrote kill", c, e)
	}
	if _, e = b.Send(ctx, request(b, "refused"), agent); e == nil {
		t.Fatal("kill failed")
	}
	if _, e = b.Kill(ctx, false, op); e != nil {
		t.Fatal(e)
	}
	send(t, b, "valid")
	d, _, _ := b.Data(ctx)
	if _, ok := d.Health["admission"]; ok {
		t.Fatal("refusal notice did not clear")
	}
	if e = os.WriteFile(b.ConfigPath, []byte(`{broken`), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Send(ctx, request(b, "bad-config"), agent); e == nil {
		t.Fatal("invalid config admitted mail")
	}
	dispatchErr := b.Dispatch(ctx, "receiver")
	if dispatchErr == nil || len(f.calls) != 0 {
		t.Fatal("invalid config injected", dispatchErr)
	}
	d, display, e := b.Snapshot(ctx)
	if e != nil || !display.Killed || d.Health["config"].Check != "config" {
		t.Fatal(d, display, e)
	}
	if e = policy.Write(b.ConfigPath, policy.Defaults(), nil); e != nil {
		t.Fatal(e)
	}
	d, _, e = b.Data(ctx)
	if e != nil || d.Health["config"].Check != "" {
		t.Fatal("config notice not cleared", e)
	}
}
func TestRemoteNamePinnedAndDuplicateBeforeRename(t *testing.T) {
	b, _ := setup(t)
	config(t, b, func(c *policy.Config) { c.Links["peer"] = policy.Grant{} })
	wire := store.Message{ID: "named-remote", Origin: "peer", Sender: "writer.codex@peer", SenderID: "writer", Recipient: "receiver", Destination: "local", Address: "reviewer.claude@local", Body: "hello", Created: b.now()}
	if _, e := b.Accept(ctx, wire); e != nil {
		t.Fatal(e)
	}
	if message(t, b, wire.ID).Recipient != "receiver" {
		t.Fatal("remote ID was not pinned")
	}
	if e := b.Rebind(ctx, "receiver", store.Session{ID: "next", Name: "reviewer", Harness: "claude", State: "live"}); e != nil {
		t.Fatal(e)
	}
	if _, e := b.Accept(ctx, wire); e != nil {
		t.Fatal("duplicate re-resolved", e)
	}
	if message(t, b, wire.ID).Recipient != "receiver" {
		t.Fatal("wire retry moved to successor")
	}
}
func TestPaceAndPreconditionFailureBudgets(t *testing.T) {
	b, f := setup(t)
	config(t, b, func(c *policy.Config) { c.InjectionLimit = 1 })
	send(t, b, "paced-first")
	send(t, b, "paced-second")
	dispatch(t, b)
	dispatch(t, b)
	if len(f.calls) != 1 || message(t, b, "paced-second").State != states.Queued {
		t.Fatal("injection pace bypassed")
	}
	b2, f2 := setup(t)
	f2.result = Result{Outcome: NotSent, Class: "precondition_race", Proof: "tested stale turn refusal"}
	send(t, b2, "race")
	dispatch(t, b2)
	m := message(t, b2, "race")
	if m.Failures != 0 || m.State != states.Queued {
		t.Fatal("precondition race consumed budget")
	}
}
func TestReceiverOutstandingSurvivesRestart(t *testing.T) {
	home := t.TempDir()
	f := fake()
	b, e := New(home, filepath.Join(home, "config.json"), map[string]Driver{"claude": f})
	if e != nil {
		t.Fatal(e)
	}
	if e = b.Register(ctx, store.Session{ID: "receiver", Name: "receiver", Harness: "claude", State: "live", Outstanding: []string{"external-attempt"}}); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Send(ctx, request(b, "waiting"), op); e != nil {
		t.Fatal(e)
	}
	b.Close()
	b, e = New(home, filepath.Join(home, "config.json"), map[string]Driver{"claude": f})
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	dispatch(t, b)
	if len(f.calls) != 0 {
		t.Fatal("restart forgot external writer")
	}
	if e = b.Register(ctx, store.Session{ID: "receiver", Name: "receiver", Harness: "claude", State: "live"}); e != nil {
		t.Fatal(e)
	}
	dispatch(t, b)
	if len(f.calls) != 1 {
		t.Fatal("completed external writer did not unblock")
	}
}
func TestLateOutboxAcknowledgementsAndAudit(t *testing.T) {
	b, _ := setup(t)
	if e := b.Store.Update(ctx, func(d *store.Data) error {
		d.Links["peer"] = store.Link{Sessions: []store.Session{{ID: "remote-reviewer", Name: "reviewer", Machine: "peer", Harness: "claude"}}}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	config(t, b, func(c *policy.Config) { c.Links["peer"] = policy.Grant{} })
	for _, id := range []string{"dead-outbox", "dropped-outbox"} {
		r := request(b, id)
		r.To = "reviewer@peer"
		if _, e := b.Send(ctx, r, agent); e != nil {
			t.Fatal(e)
		}
	}
	if e := b.Store.Update(ctx, func(d *store.Data) error {
		m, _ := d.Find("dead-outbox")
		return d.Move(m, states.Dead, "transport", "expired", b.now())
	}); e != nil {
		t.Fatal(e)
	}
	if e := b.AcknowledgeOutbox(ctx, "dead-outbox"); e != nil {
		t.Fatal(e)
	}
	if message(t, b, "dead-outbox").State != states.Sent {
		t.Fatal("late dead ack not settled")
	}
	if e := b.Resolve(ctx, "dropped-outbox", "drop", false, "", op); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 3; i++ {
		if e := b.AcknowledgeOutbox(ctx, "dropped-outbox"); e != nil {
			t.Fatal(e)
		}
	}
	m := message(t, b, "dropped-outbox")
	if m.State != states.Dropped || len(m.History) != 3 {
		t.Fatal("late dropped ack lost or repeated audit", m)
	}
}
func TestMessageNoticeClearsOnExplicitResend(t *testing.T) {
	b, f := setup(t)
	now := time.Now().UTC()
	b.Now = func() time.Time { return now }
	f.result = Result{Outcome: Unconfirmed}
	send(t, b, "noticed")
	dispatch(t, b)
	now = now.Add(2 * time.Minute)
	if e := b.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	d, _, _ := b.Data(ctx)
	if d.Health["message/local/noticed"].Check == "" {
		t.Fatal("no stuck notice")
	}
	if e := b.Resolve(ctx, "noticed", "resend", true, "", op); e != nil {
		t.Fatal(e)
	}
	d, _, _ = b.Data(ctx)
	if _, ok := d.Health["message/local/noticed"]; ok {
		t.Fatal("resolved notice remained")
	}
}
func TestBoundedDrainReportsOutstandingWriter(t *testing.T) {
	b, f := setup(t)
	started := make(chan struct{})
	release := make(chan struct{})
	f.inject = func(context.Context, store.Session, string, string) (Result, error) {
		close(started)
		<-release
		return Result{Outcome: Unconfirmed}, nil
	}
	send(t, b, "draining")
	call, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- b.Dispatch(call, "receiver") }()
	<-started
	b.Stop()
	wait, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	if n := b.Drain(wait); n != 1 {
		t.Fatal(n)
	}
	stop()
	cancel()
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	close(release)
	wait, stop = context.WithTimeout(ctx, time.Second)
	defer stop()
	if n := b.Drain(wait); n != 0 {
		t.Fatal(n)
	}
}

func TestCreatedOffsetRetryUsesSameHash(t *testing.T) {
	b, _ := setup(t)
	r := request(b, "offset")
	r.Created = r.Created.In(time.FixedZone("offset", -5*60*60))
	if _, e := b.Send(ctx, r, agent); e != nil {
		t.Fatal(e)
	}
	r.Created = r.Created.UTC()
	if _, e := b.Send(ctx, r, agent); e != nil {
		t.Fatal("UTC retry changed identity", e)
	}
}

func TestInvalidConfigNoticeStableAndLockIsNotKill(t *testing.T) {
	b, _ := setup(t)
	now := time.Now().UTC()
	b.Now = func() time.Time { return now }
	if e := os.WriteFile(b.ConfigPath, []byte(`{broken`), 0600); e != nil {
		t.Fatal(e)
	}
	var since time.Time
	for i := 0; i < 5; i++ {
		if e := b.Tick(ctx); e == nil {
			t.Fatal("invalid config did not stop dispatch")
		}
		d, display, e := b.Snapshot(ctx)
		if e != nil || !display.Killed {
			t.Fatal(display, e)
		}
		h := d.Health["config"]
		if i == 0 {
			since = h.Since
		}
		if h.Count != 1 || h.Since != since {
			t.Fatal("notice churned", h)
		}
		now = now.Add(time.Minute)
	}
	if e := policy.Write(b.ConfigPath, policy.Defaults(), nil); e != nil {
		t.Fatal(e)
	}
	if _, _, e := b.Data(ctx); e != nil {
		t.Fatal(e)
	}
	lock, e := policy.Lock(ctx, b.ConfigPath+".lock")
	if e != nil {
		t.Fatal(e)
	}
	d, c, e := b.Snapshot(ctx)
	policy.Unlock(lock)
	if e != nil || c.Killed || d.Health["config-lock"].Check == "" {
		t.Fatal("lock timeout reported a false kill", c, e)
	}
}
func TestRepeatedPrunePreservesAudit(t *testing.T) {
	b, f := setup(t)
	now := time.Now().UTC()
	b.Now = func() time.Time { return now }
	f.availability = Availability{State: "offline"}
	if _, e := b.Prune(ctx, "receiver", true, nil, op); e != nil {
		t.Fatal(e)
	}
	d, _, _ := b.Data(ctx)
	first := d.Sessions["receiver"].PrunedAt
	now = now.Add(time.Hour)
	if _, e := b.Prune(ctx, "receiver", true, nil, op); e != nil {
		t.Fatal(e)
	}
	d, _, _ = b.Data(ctx)
	if d.Sessions["receiver"].PrunedAt != first {
		t.Fatal("repeat prune overwrote audit")
	}
}
func TestReceiverPauseVisibleAndCleared(t *testing.T) {
	b, _ := setup(t)
	if e := b.Register(ctx, store.Session{ID: "receiver", Name: "receiver", Harness: "claude", State: "live", Outstanding: []string{"external"}}); e != nil {
		t.Fatal(e)
	}
	send(t, b, "paused")
	dispatch(t, b)
	d, _, _ := b.Data(ctx)
	if d.Health["receiver/receiver"].Detail != receiverWaiting || message(t, b, "paused").Error != receiverWaiting {
		t.Fatal("receiver pause hidden")
	}
	if e := b.Register(ctx, store.Session{ID: "receiver", Name: "receiver", Harness: "claude", State: "live"}); e != nil {
		t.Fatal(e)
	}
	d, _, _ = b.Data(ctx)
	if _, exists := d.Health["receiver/receiver"]; exists || message(t, b, "paused").Error == receiverWaiting {
		t.Fatal("receiver pause did not clear")
	}
}
func TestRemoteCreatedOffsetRetry(t *testing.T) {
	b, _ := setup(t)
	config(t, b, func(c *policy.Config) { c.Links["peer"] = policy.Grant{} })
	m := store.Message{ID: "offset-remote", Origin: "peer", Sender: "writer.codex@peer", SenderID: "writer", Recipient: "receiver", Destination: "local", Created: b.now().In(time.FixedZone("offset", -5*60*60)), Body: "same instant"}
	if _, e := b.Accept(ctx, m); e != nil {
		t.Fatal(e)
	}
	m.Created = m.Created.UTC()
	if _, e := b.Accept(ctx, m); e != nil {
		t.Fatal("canonical wire timestamp changed hash", e)
	}
}
