package broker

import (
	"context"
	"github.com/JeremiahChurch/herd-msg/internal/store"
	"testing"
	"time"
)

func TestReceiverRegistrationCASAndImmutableHints(t *testing.T) {
	b, f := setup(t)
	d, _, _ := b.Data(ctx)
	s := d.Sessions["receiver"]
	s.Hints = map[string]string{"endpoint": "/tmp/test.sock", "transcript": "/tmp/test.jsonl", "instance": "child"}
	expected := s.Generation
	r := b.Handle(ctx, Request{Version: Protocol, Verb: "register", Caller: Caller{ID: s.ID, Harness: "claude"}, Session: &s, ExpectedGeneration: &expected})
	if r.Code != 0 {
		t.Fatal(r)
	}
	r = b.Handle(ctx, Request{Version: Protocol, Verb: "register", Caller: Caller{ID: s.ID, Harness: "claude"}, Session: &s, ExpectedGeneration: &expected})
	if r.Code == 0 {
		t.Fatal("stale delayed registration accepted")
	}
	f.availability = Availability{State: "unavailable", Reason: "temporary probe failure"}
	send(t, b, "waiting")
	dispatch(t, b)
	d, _, _ = b.Data(ctx)
	s = d.Sessions["receiver"]
	if s.Hints["endpoint"] == "" || s.Hints["transcript"] == "" {
		t.Fatal("probe erased receiver coordinates")
	}
	if len(message(t, b, "waiting").Attempts) != 0 {
		t.Fatal("unavailable recorded attempt")
	}
	f.availability = Availability{State: "live"}
	dispatch(t, b)
	m := message(t, b, "waiting")
	if len(m.Attempts) != 1 || m.Attempts[0].Session.Hints["transcript"] == "" {
		t.Fatal("attempt lost proof coordinates")
	}
}
func TestAttemptValidationRejectsEpochAndSettledWork(t *testing.T) {
	b, f := setup(t)
	send(t, b, "mail")
	f.inject = func(ctx context.Context, s store.Session, env, aid string) (Result, error) {
		q := Request{Version: Protocol, Verb: "validate-attempt", Caller: Caller{ID: s.ID, Harness: "claude"}, Generation: s.Generation, Epoch: b.Epoch, AttemptID: aid}
		if r := b.Handle(ctx, q); r.Code != 0 {
			t.Error(r)
		}
		q.Epoch = "old"
		if r := b.Handle(ctx, q); r.Code == 0 {
			t.Error("old epoch accepted")
		}
		return Result{Outcome: Unconfirmed}, nil
	}
	dispatch(t, b)
	m := message(t, b, "mail")
	q := Request{Version: Protocol, Verb: "validate-attempt", Caller: Caller{ID: "receiver", Harness: "claude"}, Generation: m.Attempts[0].Session.Generation, Epoch: b.Epoch, AttemptID: m.Attempts[0].ID}
	if r := b.Handle(ctx, q); r.Code == 0 {
		t.Fatal("settled attempt accepted")
	}
}
func TestDiscoveryPreservesGenerationAndReceiverWork(t *testing.T) {
	b, _ := setup(t)
	d, _, _ := b.Data(ctx)
	s := d.Sessions["sender"]
	generation := s.Generation
	s.Outstanding = []string{"work"}
	if e := b.Store.Update(ctx, func(d *store.Data) error { d.Sessions[s.ID] = s; return nil }); e != nil {
		t.Fatal(e)
	}
	if e := b.Refresh(ctx, []store.Session{{ID: s.ID, Name: "renamed", Harness: "codex", State: "live"}}); e != nil {
		t.Fatal(e)
	}
	d, _, _ = b.Data(ctx)
	s = d.Sessions[s.ID]
	if s.Generation != generation || len(s.Outstanding) != 1 || s.Name != "renamed" {
		t.Fatal(s)
	}
}

func TestMuseDiscoveryDoesNotClearCodexFaults(t *testing.T) {
	b, _ := setup(t)
	if e := b.DriverNotice(ctx, "codex", "socket missing"); e != nil {
		t.Fatal(e)
	}
	s := store.Session{ID: "muse-session", Name: "muse-name", Harness: "muse", Pane: "w1:p1", State: "live"}
	if e := b.RefreshHarness(ctx, "muse", []store.Session{s}); e != nil {
		t.Fatal(e)
	}
	d, _, _ := b.Data(ctx)
	if d.Health["driver/codex"].Detail != "socket missing" || d.Sessions[s.ID].Harness != "muse" {
		t.Fatal(d.Health, d.Sessions)
	}
	if e := (Caller{ID: s.ID, Harness: "muse"}).Validate(); e != nil {
		t.Fatal(e)
	}
	generation := d.Sessions[s.ID].Generation
	s.Pane = "w2:p1"
	if e := b.RefreshHarness(ctx, "muse", []store.Session{s}); e != nil {
		t.Fatal(e)
	}
	d, _, _ = b.Data(ctx)
	if d.Sessions[s.ID].Pane != s.Pane || d.Sessions[s.ID].Generation != generation {
		t.Fatal(d.Sessions[s.ID])
	}
	if e := b.RefreshHarness(ctx, "codex", []store.Session{s}); e == nil {
		t.Fatal("cross-harness discovery accepted")
	}
}
func TestReceiverCASWaitIsBounded(t *testing.T) {
	b, _ := setup(t)
	unlock, e := b.lock(ctx, "receiver")
	if e != nil {
		t.Fatal(e)
	}
	defer unlock()
	d, _, _ := b.Data(ctx)
	s := d.Sessions["receiver"]
	r := b.Handle(ctx, Request{Version: Protocol, Verb: "register", Caller: op, Session: &s})
	if r.Code == 0 {
		t.Fatal("acquired held delivery lock")
	}
}

func TestAvailabilityRefreshTellsSenderOfflineBeforeDispatch(t *testing.T) {
	b, f := setup(t)
	f.availability = Availability{State: "offline", Reason: "receiver exited"}
	if e := b.RefreshAvailability(ctx); e != nil {
		t.Fatal(e)
	}
	d, _, _ := b.Data(ctx)
	if d.Sessions["receiver"].State != "offline" {
		t.Fatal("live registration stayed stale")
	}
	r, e := b.Send(ctx, request(b, "offline"), agent)
	if e != nil || r.Notice == "" {
		t.Fatal("sender received no offline reason", r, e)
	}
	if len(message(t, b, "offline").Attempts) != 0 {
		t.Fatal("refresh created an attempt")
	}
}

func TestHealthyRefreshClearsNoticeAndBoundsSeenWrites(t *testing.T) {
	b, f := setup(t)
	if e := b.DriverNotice(ctx, "codex", "socket absent"); e != nil {
		t.Fatal(e)
	}
	if e := b.Refresh(ctx, nil); e != nil {
		t.Fatal(e)
	}
	d, _, _ := b.Data(ctx)
	if _, ok := d.Health["driver/codex"]; ok {
		t.Fatal("healthy empty discovery retained old socket notice")
	}
	before := d.Sessions["receiver"].LastSeen
	now := b.now()
	b.Now = func() time.Time { return now.Add(10 * time.Second) }
	if e := b.RefreshAvailability(ctx); e != nil {
		t.Fatal(e)
	}
	d, _, _ = b.Data(ctx)
	if !d.Sessions["receiver"].LastSeen.Equal(before) {
		t.Fatal("healthy refresh rewrote seen time inside one minute")
	}
	f.availability.Blocked = true
	if e := b.RefreshAvailability(ctx); e != nil {
		t.Fatal(e)
	}
	d, _, _ = b.Data(ctx)
	if !d.Sessions["receiver"].Blocked {
		t.Fatal("write limit hid a blocked-state change")
	}
	b.Now = func() time.Time { return now.Add(61 * time.Second) }
	if e := b.RefreshAvailability(ctx); e != nil {
		t.Fatal(e)
	}
	d, _, _ = b.Data(ctx)
	if !d.Sessions["receiver"].LastSeen.After(before) {
		t.Fatal("healthy seen time never advanced")
	}
}

func TestPausedDriverKeepsMailQueuedAndOtherHarnessAvailable(t *testing.T) {
	b, f := setup(t)
	b.PauseDriver("claude", "protocol probe failed")
	send(t, b, "mail")
	dispatch(t, b)
	if m := message(t, b, "mail"); len(m.Attempts) != 0 || m.State != "queued" {
		t.Fatal("paused driver attempted delivery", m)
	}
	a, e := b.availability(ctx, f, store.Session{Harness: "codex"}, b.displayConfig)
	if e != nil || a.State != "live" {
		t.Fatal("unrelated harness paused", a, e)
	}
	b.PauseDriver("claude", "")
	dispatch(t, b)
	if m := message(t, b, "mail"); len(m.Attempts) != 1 {
		t.Fatal("repaired driver did not resume", m)
	}
}

func TestSuccessfulDiscoveryClearsStaleSocketPause(t *testing.T) {
	b, f := setup(t)
	b.PauseDriver("codex", "socket absent")
	if e := b.Store.Update(ctx, func(d *store.Data) error { d.Notice("probe/codex-socket", "absent", "start", b.now()); return nil }); e != nil {
		t.Fatal(e)
	}
	if e := b.Refresh(ctx, nil); e != nil {
		t.Fatal(e)
	}
	d, _, e := b.Data(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, exists := d.Health["probe/codex-socket"]; exists {
		t.Fatal("recovered socket retained stale probe")
	}
	a, e := b.availability(ctx, f, store.Session{Harness: "codex"}, b.displayConfig)
	if e != nil || a.State != "live" {
		t.Fatal("recovered driver stayed paused", a, e)
	}
}
