package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

type FakeDriver struct {
	mu           sync.Mutex
	accepted     map[string]bool
	calls        []string
	result       Result
	err          error
	availability Availability
	lookupErr    error
	records      [][]byte
	inject       func(context.Context, store.Session, string, string) (Result, error)
	lookup       func(LookupRequest) (bool, error)
}

func fake() *FakeDriver {
	return &FakeDriver{accepted: map[string]bool{}, result: Result{Outcome: Accepted}, availability: Availability{State: "live"}}
}
func (f *FakeDriver) Probe(context.Context) error { return nil }
func (f *FakeDriver) SessionState(context.Context, store.Session) (Availability, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.availability, nil
}
func (f *FakeDriver) Inject(ctx context.Context, s store.Session, env, id string) (Result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, id)
	fn := f.inject
	r, e := f.result, f.err
	if r.Outcome == Accepted && fn == nil {
		f.accepted[id] = true
	}
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, s, env, id)
	}
	return r, e
}
func (f *FakeDriver) Lookup(ctx context.Context, r LookupRequest) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.lookup != nil {
		return f.lookup(r)
	}
	if f.records != nil {
		header, _, _ := strings.Cut(r.Attempt.Envelope, "\n")
		for _, row := range f.records {
			found, e := fakeClaudeLookup(row, header)
			if e != nil || found {
				return found, e
			}
		}
		return false, nil
	}
	return f.accepted[r.Attempt.ID], f.lookupErr
}
func setup(t *testing.T) (*Broker, *FakeDriver) {
	t.Helper()
	f := fake()
	home := t.TempDir()
	b, e := New(home, home+"/config.json", map[string]Driver{"codex": f, "claude": f})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { b.Close() })
	for _, s := range []store.Session{{ID: "sender", Name: "builder", Harness: "codex", State: "live"}, {ID: "receiver", Name: "reviewer", Harness: "claude", State: "live"}} {
		if e = b.Register(context.Background(), s); e != nil {
			t.Fatal(e)
		}
	}
	return b, f
}

var ctx = context.Background()
var op = Caller{ID: "operator"}
var agent = Caller{ID: "sender", Harness: "codex"}

func request(b *Broker, id string) SendRequest {
	return SendRequest{ID: id, Created: b.now(), To: "receiver", Body: "hello"}
}
func send(t *testing.T, b *Broker, id string) Receipt {
	t.Helper()
	r, e := b.Send(ctx, request(b, id), agent)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func message(t *testing.T, b *Broker, id string) *store.Message {
	t.Helper()
	d, e := b.Store.Read(ctx)
	if e != nil {
		t.Fatal(e)
	}
	m, e := d.Find(id)
	if e != nil {
		t.Fatal(e)
	}
	return m
}
func config(t *testing.T, b *Broker, fn func(*policy.Config)) {
	t.Helper()
	_, e := b.changeConfig(ctx, op, func(c *policy.Config) error { fn(c); return nil })
	if e != nil {
		t.Fatal(e)
	}
}
func dispatch(t *testing.T, b *Broker) {
	t.Helper()
	if e := b.Dispatch(ctx, "receiver"); e != nil {
		t.Fatal(e)
	}
}
func TestAdmissionDedupeAndConcurrentLimits(t *testing.T) {
	b, _ := setup(t)
	r := request(b, "same")
	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, e := b.Send(ctx, r, agent); errs <- e }()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	config(t, b, func(c *policy.Config) { c.Killed = true; c.OptOut["receiver"] = true; c.HopLimit = 1 })
	if _, e := b.Send(ctx, r, agent); e != nil {
		t.Fatal("duplicate blocked by gates", e)
	}
	r.ParentID = "different"
	if _, e := b.Send(ctx, r, agent); e == nil {
		t.Fatal("changed parent reused ID")
	}
	d, _, _ := b.Data(ctx)
	if len(d.Messages) != 1 {
		t.Fatal("dedupe created multiple rows")
	}
	config(t, b, func(c *policy.Config) { c.Killed = false; c.OptOut["receiver"] = false; c.PairLimit = 2 })
	errs = make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := b.Send(ctx, request(b, fmt.Sprint("rate-", i)), agent)
			errs <- e
		}(i)
	}
	wg.Wait()
	close(errs)
	accepted := 0
	for e := range errs {
		if e == nil {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("concurrent accepted %d", accepted)
	}
}
func TestFIFOAndUncertainty(t *testing.T) {
	b, f := setup(t)
	f.result = Result{Outcome: Unconfirmed}
	send(t, b, "first")
	send(t, b, "second")
	dispatch(t, b)
	if message(t, b, "first").State != states.Indeterminate {
		t.Fatal("uncertainty not parked")
	}
	f.result = Result{Outcome: Accepted}
	dispatch(t, b)
	if message(t, b, "second").State != states.Queued {
		t.Fatal("unconfirmed predecessor did not block FIFO")
	}
	dispatch(t, b)
	if len(f.calls) != 1 {
		t.Fatal("uncertain recipient got another injection")
	}
	a := message(t, b, "first").Attempts[0]
	f.accepted[a.ID] = true
	if e := b.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	if message(t, b, "first").State != states.Injected {
		t.Fatal("positive lookup did not settle")
	}
	dispatch(t, b)
	if message(t, b, "second").State != states.Injected || len(f.calls) != 2 {
		t.Fatal("confirmed predecessor stranded FIFO")
	}
}
func TestProvenFailuresAndBackoff(t *testing.T) {
	b, f := setup(t)
	now := time.Now()
	b.Now = func() time.Time { return now }
	send(t, b, "first")
	send(t, b, "later")
	f.result = Result{Outcome: NotSent, Proof: "no bytes written", Class: "transport"}
	dispatch(t, b)
	m := message(t, b, "first")
	if m.Failures != 0 || m.State != states.Queued {
		t.Fatal(m)
	}
	dispatch(t, b)
	if len(f.calls) != 1 {
		t.Fatal("backoff head overtaken")
	}
	f.result = Result{Outcome: NotSent, Proof: "tested rejection", Class: "delivery"}
	for i := 0; i < 4; i++ {
		now = now.Add(10 * time.Minute)
		dispatch(t, b)
	}
	m = message(t, b, "first")
	if m.State != states.Dead || m.Failures != 4 || len(m.Attempts) != 5 {
		t.Fatal(m)
	}
	dispatch(t, b)
	if message(t, b, "later").State != states.Queued {
		t.Fatal("unexpected later outcome")
	}
}
func TestResolveLooksUpAllEpochsAndRetainsBudget(t *testing.T) {
	b, f := setup(t)
	f.result = Result{Outcome: Unconfirmed}
	send(t, b, "uncertain")
	dispatch(t, b)
	if e := b.Resolve(ctx, "uncertain", "resend", false, "", op); e == nil {
		t.Fatal("resend lacked confirmation")
	}
	f.lookupErr = errors.New("offline")
	if e := b.Resolve(ctx, "uncertain", "resend", true, "", op); e == nil {
		t.Fatal("lookup error permitted resend")
	}
	f.lookupErr = nil
	if e := b.Resolve(ctx, "uncertain", "resend", true, "", op); e != nil {
		t.Fatal(e)
	}
	dispatch(t, b)
	m := message(t, b, "uncertain")
	if len(m.Attempts) != 2 || m.BudgetEpoch != 2 {
		t.Fatal(m)
	}
	f.accepted[m.Attempts[0].ID] = true
	if e := b.Resolve(ctx, "uncertain", "resend", true, "", op); e != nil {
		t.Fatal(e)
	}
	if message(t, b, "uncertain").State != states.Injected {
		t.Fatal("older epoch proof ignored")
	}
	if len(f.calls) != 2 {
		t.Fatal("lookup resent")
	}
}
func TestLatePositiveDuringNewAttempt(t *testing.T) {
	for _, outcome := range []Result{{Outcome: Accepted}, {Outcome: Unconfirmed}, {Outcome: NotSent, Proof: "no write", Class: "delivery"}} {
		t.Run(outcome.Outcome, func(t *testing.T) {
			b, f := setup(t)
			f.result = Result{Outcome: Unconfirmed}
			send(t, b, "late")
			dispatch(t, b)
			old := message(t, b, "late").Attempts[0]
			if e := b.Resolve(ctx, "late", "resend", true, "", op); e != nil {
				t.Fatal(e)
			}
			f.inject = func(_ context.Context, _ store.Session, _ string, _ string) (Result, error) {
				if e := b.Finish(ctx, old, Result{Outcome: Accepted}, nil); e != nil {
					t.Error(e)
				}
				return outcome, nil
			}
			dispatch(t, b)
			m := message(t, b, "late")
			if m.State != states.Injected || len(m.Attempts) != 2 {
				t.Fatal(m)
			}
			dispatch(t, b)
			if len(f.calls) != 2 {
				t.Fatal("third attempt after older proof")
			}
		})
	}
}
func TestRebindPruneAndGenerationFence(t *testing.T) {
	b, f := setup(t)
	send(t, b, "old-mail")
	d, _, _ := b.Data(ctx)
	oldGen := d.Sessions["receiver"].Generation
	if e := b.Rebind(ctx, "receiver", store.Session{ID: "successor", Name: "next", Harness: "claude", State: "live"}); e != nil {
		t.Fatal(e)
	}
	dispatch(t, b)
	if len(f.calls) != 0 {
		t.Fatal("old conversation followed successor")
	}
	if e := b.Register(ctx, store.Session{ID: "receiver", Name: "reviewer", Harness: "claude", State: "offline"}); e != nil {
		t.Fatal(e)
	}
	d, _, _ = b.Data(ctx)
	if d.Sessions["receiver"].Generation == oldGen {
		t.Fatal("generation reused")
	}
	if _, e := b.Prune(ctx, "receiver", true, nil, op); e == nil {
		t.Fatal("pruned live driver")
	}
	f.availability = Availability{State: "offline"}
	wrong := 0
	if _, e := b.Prune(ctx, "receiver", true, &wrong, op); e == nil {
		t.Fatal("stale unsent count accepted")
	}
	one := 1
	if n, e := b.Prune(ctx, "receiver", true, &one, op); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if message(t, b, "old-mail").State != states.Dropped {
		t.Fatal("prune lost unsent disposition")
	}
	if _, e := b.Send(ctx, request(b, "after-prune"), agent); e == nil {
		t.Fatal("admitted to tombstone")
	}
}
func TestClaimFencesAndKill(t *testing.T) {
	for _, gate := range []string{"generation", "kill", "expired"} {
		t.Run(gate, func(t *testing.T) {
			b, f := setup(t)
			send(t, b, "fenced")
			b.Fault = func(point string) {
				if point != "after_claim_commit" {
					return
				}
				switch gate {
				case "generation":
					_ = b.Store.Update(ctx, func(d *store.Data) error {
						s := d.Sessions["receiver"]
						s.Generation = store.Random()
						d.Sessions[s.ID] = s
						return nil
					})
				case "kill":
					_, _ = b.Kill(ctx, true, op)
				case "expired":
					_ = b.Store.Update(ctx, func(d *store.Data) error { m, _ := d.Find("fenced"); m.Lease = b.now().Add(-time.Second); return nil })
				}
			}
			dispatch(t, b)
			if len(f.calls) != 0 || len(message(t, b, "fenced").Attempts) != 0 {
				t.Fatal("fence started driver")
			}
		})
	}
}
func TestRetentionReplayFloorAndExpiredDuplicate(t *testing.T) {
	b, _ := setup(t)
	now := time.Now()
	b.Now = func() time.Time { return now }
	r := request(b, "retained")
	if _, e := b.Send(ctx, r, agent); e != nil {
		t.Fatal(e)
	}
	dispatch(t, b)
	config(t, b, func(c *policy.Config) {
		c.Retention = 1000
		c.TTL = 100
		c.OutboxTTL = 100
		c.Skew = 1
		c.Dedupe = 2000
	})
	now = now.Add(1001 * time.Second)
	if e := b.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	receipt, e := b.Send(ctx, r, agent)
	if e != nil || !receipt.Expired {
		t.Fatal(receipt, e)
	}
	now = now.Add(1000 * time.Second)
	if e = b.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	config(t, b, func(c *policy.Config) { c.Dedupe = 100000 })
	if _, e = b.Send(ctx, r, agent); e == nil {
		t.Fatal("expanded dedupe reopened swept replay")
	}
}
func TestRemoteReleaseRevocationAndOutbox(t *testing.T) {
	b, f := setup(t)
	config(t, b, func(c *policy.Config) { c.Links["peer"] = policy.Grant{} })
	wire := store.Message{ID: "remote", Origin: "peer", Sender: "writer.codex@peer", SenderID: "remote-session", Recipient: "receiver", Destination: "local", Address: "receiver@local", Body: "remote body", Created: b.now()}
	receipt, e := b.Accept(ctx, wire)
	if e != nil || receipt.State != states.Held {
		t.Fatal(receipt, e)
	}
	dispatch(t, b)
	if len(f.calls) != 0 {
		t.Fatal("held mail ran")
	}
	if e = b.Release(ctx, wire.ID, op); e != nil {
		t.Fatal(e)
	}
	dispatch(t, b)
	if message(t, b, wire.ID).State != states.Injected {
		t.Fatal("released grant ignored")
	}
	wire.ID = "revoke"
	config(t, b, func(c *policy.Config) { c.Links["peer"] = policy.Grant{AutoRun: true} })
	if _, e = b.Accept(ctx, wire); e != nil {
		t.Fatal(e)
	}
	config(t, b, func(c *policy.Config) { c.Links["peer"] = policy.Grant{} })
	if message(t, b, wire.ID).State != states.Held {
		t.Fatal("revocation not reconciled")
	}
	config(t, b, func(c *policy.Config) { delete(c.Links, "peer") })
	if message(t, b, wire.ID).State != states.Dead {
		t.Fatal("removed link not dead")
	}
	if _, e = b.Accept(ctx, wire); e != nil {
		t.Fatal("duplicate refused after unlink", e)
	}
	config(t, b, func(c *policy.Config) { c.Links["peer"] = policy.Grant{} })
	if e = b.Store.Update(ctx, func(d *store.Data) error {
		d.Links["peer"] = store.Link{Sessions: []store.Session{{ID: "remote-writer", Name: "writer", Machine: "peer", Harness: "codex"}}}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	r := request(b, "outbound")
	r.To = "writer@peer"
	if receipt, e = b.Send(ctx, r, agent); e != nil || receipt.State != states.Outbox {
		t.Fatal(receipt, e)
	}
	if e = b.Store.Update(ctx, func(d *store.Data) error {
		m, _ := d.Find(r.ID)
		return d.Move(m, states.Dead, "transport", "refused", b.now())
	}); e != nil {
		t.Fatal(e)
	}
	if e = b.Resolve(ctx, r.ID, "resend", false, "", op); e != nil {
		t.Fatal(e)
	}
	if message(t, b, r.ID).State != states.Outbox {
		t.Fatal("remote resend went local")
	}
	if e = b.AcknowledgeOutbox(ctx, r.ID); e != nil {
		t.Fatal(e)
	}
}

func TestOutstandingTimeoutFencesRecipient(t *testing.T) {
	b, f := setup(t)
	started := make(chan struct{})
	release := make(chan struct{})
	f.inject = func(context.Context, store.Session, string, string) (Result, error) {
		close(started)
		<-release
		return Result{Outcome: Accepted}, nil
	}
	send(t, b, "timeout")
	send(t, b, "later")
	call, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- b.Dispatch(call, "receiver") }()
	<-started
	cancel()
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	if message(t, b, "timeout").State != states.Indeterminate {
		t.Fatal("timeout not parked")
	}
	dispatch(t, b)
	if len(f.calls) != 1 {
		t.Fatal("overlapping recipient call")
	}
	if e := b.Resolve(ctx, "timeout", "resend", true, "", op); e == nil {
		t.Fatal("resend while writer outstanding")
	}
	if e := b.Register(ctx, store.Session{ID: "receiver", Name: "receiver", Harness: "claude", State: "live"}); e == nil {
		t.Fatal("registered while writer outstanding")
	}
	if e := b.Close(); e == nil {
		t.Fatal("closed broker released an outstanding writer")
	}
	if other, e := store.Open(b.Store.Home); e == nil {
		other.Close()
		t.Fatal("home lock released before outstanding writer returned")
	}
	close(release)
	deadline := time.Now().Add(time.Second)
	for b.Outstanding() > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if b.Outstanding() != 0 || message(t, b, "timeout").State != states.Injected {
		t.Fatal("late positive not settled")
	}
}
func TestPolicyStructuralAndHopInheritance(t *testing.T) {
	b, f := setup(t)
	large := request(b, "oversized")
	large.Body = string(make([]byte, 65537))
	if _, e := b.Send(ctx, large, agent); e == nil {
		t.Fatal("oversized body accepted")
	}
	config(t, b, func(c *policy.Config) { c.PendingLimit = 1 })
	send(t, b, "capacity")
	if _, e := b.Send(ctx, request(b, "over-capacity"), op); e == nil {
		t.Fatal("operator bypassed capacity")
	}
	config(t, b, func(c *policy.Config) { c.PendingLimit = 1000 })
	// An unconfirmed incoming attempt already carries a causal hop, even before lookup.
	f.result = Result{Outcome: Unconfirmed}
	hop := 6
	r := request(b, "high-hop")
	r.Hop = &hop
	r.To = "sender"
	if _, e := b.Send(ctx, r, op); e != nil {
		t.Fatal(e)
	}
	if e := b.Dispatch(ctx, "sender"); e != nil {
		t.Fatal(e)
	}
	if _, e := b.Send(ctx, request(b, "loop"), agent); e == nil {
		t.Fatal("unconfirmed incoming hop lost")
	}
	d, _, _ := b.Data(ctx)
	if len(d.Dedupe) != 2 {
		t.Fatal("refusal wrote dedupe/payload", len(d.Dedupe))
	}
}
func TestReceiverOutstandingAndRebindHints(t *testing.T) {
	b, f := setup(t)
	if e := b.Register(ctx, store.Session{ID: "receiver", Name: "receiver", Harness: "claude", State: "live", Outstanding: []string{"external-attempt"}}); e != nil {
		t.Fatal(e)
	}
	send(t, b, "external")
	dispatch(t, b)
	if len(f.calls) != 0 {
		t.Fatal("receiver outstanding ignored")
	}
	if e := b.Register(ctx, store.Session{ID: "receiver", Name: "receiver", Harness: "claude", State: "live"}); e != nil {
		t.Fatal(e)
	}
	f.result = Result{Outcome: Unconfirmed}
	dispatch(t, b)
	if e := b.Rebind(ctx, "receiver", store.Session{ID: "next", Name: "next", Harness: "claude", State: "live"}); e != nil {
		t.Fatal(e)
	}
	f.lookup = func(r LookupRequest) (bool, error) {
		if r.Attempt.Session.ID != "receiver" || len(r.Successors) != 1 || r.Successors[0].ID != "next" {
			t.Error("lost immutable identity or successors", r)
		}
		return true, nil
	}
	if e := b.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	if message(t, b, "external").State != states.Injected {
		t.Fatal("post-clear lookup failed")
	}
}
func TestDeadUncertainNeedsConfirmationAndReplayAcrossRename(t *testing.T) {
	b, f := setup(t)
	f.result = Result{Outcome: Unconfirmed}
	r := request(b, "same-after-rename")
	if _, e := b.Send(ctx, r, agent); e != nil {
		t.Fatal(e)
	}
	dispatch(t, b)
	if e := b.Store.Update(ctx, func(d *store.Data) error {
		m, _ := d.Find(r.ID)
		return d.Move(m, states.Dropped, "operator", "drop", b.now())
	}); e != nil {
		t.Fatal(e)
	}
	if e := b.Store.Update(ctx, func(d *store.Data) error {
		s := d.Sessions["receiver"]
		s.Name = "new-name"
		d.Sessions[s.ID] = s
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if _, e := b.Send(ctx, r, agent); e != nil {
		t.Fatal("retry recomputed resolved identity", e)
	}
	send(t, b, "dead-uncertain")
	dispatch(t, b)
	if e := b.Store.Update(ctx, func(d *store.Data) error {
		m, _ := d.Find("dead-uncertain") // Dead can retain uncertainty from an earlier epoch after a proven failure budget.
		m.State = states.Dead
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e := b.Resolve(ctx, "dead-uncertain", "resend", false, "", op); e == nil {
		t.Fatal("dead uncertainty bypassed confirmation")
	}
}

func TestLateNegativeDoesNotWeakenResendConfirmation(t *testing.T) {
	b, f := setup(t)
	f.result = Result{Outcome: Unconfirmed}
	send(t, b, "late-negative")
	dispatch(t, b)
	a := message(t, b, "late-negative").Attempts[0]
	if e := b.Finish(ctx, a, Result{Outcome: NotSent, Proof: "late zero write proof", Class: "transport"}, nil); e != nil {
		t.Fatal(e)
	}
	if message(t, b, "late-negative").State != states.Indeterminate {
		t.Fatal("late negative requeued row")
	}
	if e := b.Resolve(ctx, "late-negative", "resend", false, "", op); e == nil {
		t.Fatal("late negative bypassed confirmation")
	}
	f.lookupErr = errors.New("fresh lookup unavailable")
	if e := b.Resolve(ctx, "late-negative", "resend", true, "", op); e == nil {
		t.Fatal("late negative bypassed fresh lookup")
	}
}
func TestMonotonicClaimRecoveryAfterClockRollback(t *testing.T) {
	b, _ := setup(t)
	send(t, b, "clock")
	now := b.now()
	token := "claim-clock"
	if e := b.Store.Update(ctx, func(d *store.Data) error {
		m, _ := d.Find("clock")
		if e := d.Move(m, states.Claimed, "broker", "claim", now); e != nil {
			return e
		}
		m.ClaimToken = token
		m.BrokerEpoch = b.Epoch
		m.Lease = now.Add(24 * time.Hour)
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	b.mu.Lock()
	b.claimStarted[token] = time.Now().Add(-time.Hour)
	b.mu.Unlock()
	if e := b.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	if message(t, b, "clock").State != states.Queued {
		t.Fatal("wall clock rollback stranded expired claim")
	}
}

func TestFakeTranscriptLookupDoesNotAcceptQuotedMarker(t *testing.T) {
	b, f := setup(t)
	f.result = Result{Outcome: Unconfirmed}
	send(t, b, "transcript")
	dispatch(t, b)
	a := message(t, b, "transcript").Attempts[0]
	header, _, _ := strings.Cut(a.Envelope, "\n")
	quoted, _ := json.Marshal(map[string]any{"type": "user", "origin": map[string]string{"kind": "peer"}, "message": map[string]any{"content": []any{map[string]string{"type": "tool_result", "content": header}, map[string]string{"type": "text", "text": header}}}})
	f.records = [][]byte{quoted}
	if e := b.confirm(ctx, "local/transcript"); e != nil {
		t.Fatal(e)
	}
	if message(t, b, "transcript").State != states.Indeterminate {
		t.Fatal("quoted marker became acceptance")
	}
	accepted, _ := json.Marshal(map[string]any{"type": "user", "isMeta": true, "origin": map[string]string{"kind": "peer"}, "message": map[string]any{"content": "Another Claude session sent a message:\n" + a.Envelope}})
	f.records = append(f.records, accepted)
	if e := b.confirm(ctx, "local/transcript"); e != nil {
		t.Fatal(e)
	}
	if message(t, b, "transcript").State != states.Injected {
		t.Fatal("verified peer wrapper not accepted")
	}
}

func TestOperatorEnvelopeAndDetachedAdmissionRepairs(t *testing.T) {
	b, _ := setup(t)
	for _, sender := range []string{"operator", "sender"} {
		text := Envelope(store.Message{ID: "mail", SenderID: sender}, "attempt")
		if strings.Contains(text, "Reply with:") != (sender != "operator") {
			t.Fatal("envelope offered an unusable reply", text)
		}
	}
	if err := b.Register(ctx, store.Session{ID: "detached-thread", Name: "detached-reviewer", Harness: "codex", State: "detached"}); err != nil {
		t.Fatal(err)
	}
	r := request(b, "detached-mail")
	r.To = "detached-thread"
	receipt, err := b.Send(ctx, r, agent)
	if err != nil || receipt.State != states.Queued || !strings.Contains(receipt.Notice, "herd-msg doctor") {
		t.Fatalf("missing retained-mail repair: %+v %v", receipt, err)
	}
}

func TestRemoteOutboxDoesNotClaimLiveRecipientIsOffline(t *testing.T) {
	b, _ := setup(t)
	if err := b.InstallPeer(ctx, PrivatePeer{Machine: "peer", Key: strings.Repeat("a", 64), Path: "/tmp/herd-msg"}, nil, ""); err != nil {
		t.Fatal(err)
	}
	if err := b.Store.Update(ctx, func(d *store.Data) error {
		d.Links["peer"] = store.Link{Machine: "peer", Reachable: true, LastContact: b.now(), Sessions: []store.Session{{ID: "remote-writer", Name: "writer", Machine: "peer", Harness: "codex", State: "live"}}}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := request(b, "outbound-live")
	r.To = "writer@peer"
	receipt, err := b.Send(ctx, r, agent)
	if err != nil || receipt.State != states.Outbox || receipt.Notice != "" {
		t.Fatal(receipt, err)
	}
	if m := message(t, b, r.ID); m.Error != "" {
		t.Fatal("outbox wrongly reports recipient availability", m.Error)
	}
}

func TestQueuedReceiptKeepsSpecificRepair(t *testing.T) {
	b, _ := setup(t)
	id := "9b5e1a43-replaced"
	repair := "conversation changed; resume the pinned conversation or prune its mailbox"
	if err := b.Store.Update(ctx, func(d *store.Data) error {
		d.Sessions[id] = store.Session{ID: id, Name: "replaced-lane", Harness: "claude", State: "offline", LastSeen: b.now().Add(-time.Hour)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r := request(b, "mail-replaced")
	r.To = id
	if _, err := b.Send(ctx, r, agent); err != nil {
		t.Fatal(err)
	}
	if err := b.Store.Update(ctx, func(d *store.Data) error {
		m, err := d.Find(r.ID)
		if err != nil {
			return err
		}
		m.Error = repair
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	again, err := b.Send(ctx, r, agent)
	if err != nil || !strings.Contains(again.Notice, repair) || !strings.Contains(again.Notice, "replaced-lane") {
		t.Fatalf("%+v %v", again, err)
	}
}

func TestQueuedReceiptNamesWhoTheMailWaitsFor(t *testing.T) {
	b, _ := setup(t)
	for _, tc := range []struct{ id, name, state, want string }{
		{"7f3c9e21-offline", "old-throwaway", "offline", "queued for old-throwaway (7f3c9e21), last seen "},
		{"8a4d0f32-ended", "finished-lane", "dead", "may never return"},
	} {
		if err := b.Store.Update(ctx, func(d *store.Data) error {
			d.Sessions[tc.id] = store.Session{ID: tc.id, Name: tc.name, Harness: "claude", State: tc.state, LastSeen: b.now().Add(-23 * time.Hour)}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		r := request(b, "mail-"+tc.state)
		r.To = tc.id[:8]
		receipt, err := b.Send(ctx, r, agent)
		if err != nil || receipt.State != states.Queued || !strings.Contains(receipt.Notice, tc.want) {
			t.Fatalf("%s: %+v %v", tc.state, receipt, err)
		}
		// A retry of the same send reports the same thing.
		again, err := b.Send(ctx, r, agent)
		if err != nil || again.Notice != receipt.Notice {
			t.Fatalf("%s retry: %+v %v", tc.state, again, err)
		}
		d, err := b.Store.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		m, err := d.Find(r.ID)
		if err != nil || m.Recipient != tc.id || m.Error == receipt.Notice {
			t.Fatalf("%s: stored reason changed: %+v %v", tc.state, m, err)
		}
	}
}
func TestLimitRefusalsExplainWhenSendingUnlocks(t *testing.T) {
	b, f := setup(t)
	f.result = Result{Outcome: Unconfirmed}
	hop := 6
	r := request(b, "deep")
	r.Hop = &hop
	r.To = "sender"
	if _, e := b.Send(ctx, r, op); e != nil {
		t.Fatal(e)
	}
	if e := b.Dispatch(ctx, "sender"); e != nil {
		t.Fatal(e)
	}
	_, e := b.Send(ctx, request(b, "fresh"), agent)
	var be *Error
	if !errors.As(e, &be) || be.Retryable {
		t.Fatalf("hop refusal should be a non-retryable broker error: %#v", e)
	}
	for _, want := range []string{"hop limit exceeded", "hop 7", "limit is 6", "clears at", "raise hop_limit"} {
		if !strings.Contains(e.Error(), want) {
			t.Fatalf("hop refusal %q lacks %q", e.Error(), want)
		}
	}
	hc := policy.Config{HopLimit: 6, IncomingWindow: 600}
	if msg := hopRefusal(hc, SendRequest{ParentID: "x"}, 7, 6, time.Time{}); !strings.Contains(msg, "keep failing") {
		t.Fatal("reply to a parent at the limit should say it keeps failing", msg)
	}
	if msg := hopRefusal(hc, SendRequest{ParentID: "x"}, 7, 1, time.Now()); strings.Contains(msg, "keep failing") || !strings.Contains(msg, "clears at") {
		t.Fatal("reply blocked only by recent incoming mail should say when it clears", msg)
	}
	base := time.Date(2026, 11, 5, 20, 0, 0, 0, time.UTC)
	staggered := []time.Time{base.Add(2 * time.Minute), base, base.Add(time.Minute)}
	if msg := pairRefusal(policy.Config{PairLimit: 3, PairWindow: 600}, "r", staggered); !strings.Contains(msg, "fits after 20:10:00 UTC") {
		t.Fatal("pair retry time should be the oldest expiry at the limit", msg)
	}
	if msg := pairRefusal(policy.Config{PairLimit: 1, PairWindow: 600}, "r", staggered); !strings.Contains(msg, "fits after 20:12:00 UTC") {
		t.Fatal("pair retry time must wait until the count drops below a lowered limit", msg)
	}

	b, _ = setup(t)
	config(t, b, func(c *policy.Config) { c.PairLimit = 2 })
	send(t, b, "one")
	send(t, b, "two")
	_, e = b.Send(ctx, request(b, "three"), agent)
	if !errors.As(e, &be) || !be.Retryable {
		t.Fatalf("pair refusal should be retryable: %#v", e)
	}
	for _, want := range []string{"pair rate limit exceeded", "2 messages to receiver", "limit 2", "next send fits after"} {
		if !strings.Contains(e.Error(), want) {
			t.Fatalf("pair refusal %q lacks %q", e.Error(), want)
		}
	}
}

// A hub that answers many lanes continues the chain of its latest incoming
// message, not the deepest message in the window, while a loop still climbs.
func TestHopFollowsLatestIncomingMessage(t *testing.T) {
	b, f := setup(t)
	f.result = Result{Outcome: Accepted}
	deliver := func(id string, hop int) {
		t.Helper()
		r := request(b, id)
		r.Hop = &hop
		r.To = "sender"
		if _, e := b.Send(ctx, r, op); e != nil {
			t.Fatal(e)
		}
		if e := b.Dispatch(ctx, "sender"); e != nil {
			t.Fatal(e)
		}
		if m := message(t, b, id); len(m.Attempts) == 0 {
			t.Fatalf("%s was not attempted: %+v", id, m)
		}
	}
	deliver("deep-lane", 6)
	if _, e := b.Send(ctx, request(b, "blocked"), agent); e == nil {
		t.Fatal("send after the deep lane's message should exceed the hop limit")
	}
	deliver("shallow-lane", 0)
	send(t, b, "hub-reply")
	if got := message(t, b, "hub-reply").Hop; got != 1 {
		t.Fatalf("hub send hop = %d, want 1 from the latest incoming message", got)
	}
}

// Two agents that keep sending new (non-reply) mail to each other still climb
// one hop per turn and are refused once they pass hop_limit.
func TestHopLoopBetweenAgentsStillClimbs(t *testing.T) {
	b, f := setup(t)
	f.result = Result{Outcome: Accepted}
	peer := Caller{ID: "receiver", Harness: "claude"}
	callers := []Caller{agent, peer}
	targets := []string{"receiver", "sender"}
	limit := policy.Defaults().HopLimit
	for turn := 0; ; turn++ {
		r := request(b, fmt.Sprintf("loop-%d", turn))
		r.To = targets[turn%2]
		_, e := b.Send(ctx, r, callers[turn%2])
		if turn > limit {
			if e == nil || !strings.Contains(e.Error(), policy.HopLimitReason) {
				t.Fatalf("turn %d: loop was not stopped by the hop limit: %v", turn, e)
			}
			return
		}
		if e != nil {
			t.Fatalf("turn %d refused before the limit: %v", turn, e)
		}
		if got := message(t, b, r.ID).Hop; got != turn {
			t.Fatalf("turn %d hop = %d, want %d", turn, got, turn)
		}
		if e := b.Dispatch(ctx, r.To); e != nil {
			t.Fatal(e)
		}
	}
}
