package broker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func TestReceiverLongPollWaitsForOwnSessionAndCancels(t *testing.T) {
	b, _ := setup(t)
	q := Request{Version: Protocol, Verb: "receiver-state", Wait: true, Caller: Caller{ID: "receiver", Harness: "claude"}}
	r := b.Handle(ctx, q)
	var snap struct {
		Token string `json:"if_updated"`
	}
	if r.Code != 0 || json.Unmarshal(r.Data, &snap) != nil || snap.Token == "" {
		t.Fatal(r)
	}
	q.IfUpdated = snap.Token
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	reply := make(chan Response, 1)
	go func() { reply <- b.Handle(call, q) }()
	if e := b.Store.Update(ctx, func(d *store.Data) error {
		s := d.Sessions["sender"]
		s.Name = "changed"
		d.Sessions[s.ID] = s
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	select {
	case r := <-reply:
		t.Fatal("unrelated session ended long poll", r)
	case <-time.After(50 * time.Millisecond):
	}
	if e := b.Store.Update(ctx, func(d *store.Data) error {
		s := d.Sessions["receiver"]
		s.Outstanding = []string{"attempt"}
		d.Sessions[s.ID] = s
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	select {
	case r := <-reply:
		if r.Code != 0 {
			t.Fatal(r)
		}
	case <-time.After(time.Second):
		t.Fatal("session change did not wake poll")
	}
	r = b.Handle(ctx, q)
	json.Unmarshal(r.Data, &snap)
	q.IfUpdated = snap.Token
	go func() { reply <- b.Handle(call, q) }()
	cancel()
	select {
	case <-reply:
	case <-time.After(time.Second):
		t.Fatal("cancellation blocked")
	}
}

func TestIdleTicksNeverProbeDriverAndWakeForMail(t *testing.T) {
	b, f := setup(t)
	var probes, observations atomic.Int64
	b.Drivers["claude"] = observedDriver{Driver: f, calls: &observations}
	b.Drivers["codex"] = observedDriver{Driver: f, calls: &observations}
	before, e := os.Stat(filepath.Join(b.Store.Home, "mailbox.db-wal"))
	if e != nil {
		t.Fatal(e)
	}
	f.inject = func(context.Context, store.Session, string, string) (Result, error) {
		probes.Add(1)
		return Result{Outcome: Accepted}, nil
	}
	for i := 0; i < 30; i++ {
		if e := b.Tick(ctx); e != nil {
			t.Fatal(e)
		}
	}
	after, e := os.Stat(filepath.Join(b.Store.Home, "mailbox.db-wal"))
	if e != nil {
		t.Fatal(e)
	}
	if before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("idle ticks wrote the WAL")
	}
	if probes.Load() != 0 || observations.Load() != 0 {
		t.Fatal("idle driver call")
	}
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); b.RunTicks(call) }()
	time.Sleep(30 * time.Millisecond)
	send(t, b, "wake")
	limit := time.Now().Add(time.Second)
	for probes.Load() == 0 && time.Now().Before(limit) {
		time.Sleep(5 * time.Millisecond)
	}
	if probes.Load() != 1 {
		t.Fatal("idle scheduler did not wake for send", probes.Load())
	}
	cancel()
	<-done
}

type observedDriver struct {
	Driver
	calls *atomic.Int64
}

func (d observedDriver) Probe(ctx context.Context) error { d.calls.Add(1); return d.Driver.Probe(ctx) }

func TestDispatchDoesNotProbeBeforeHeadRetryIsDue(t *testing.T) {
	b, f := setup(t)
	var calls atomic.Int64
	b.Drivers["claude"] = observedDriver{Driver: f, calls: &calls}
	send(t, b, "delayed")
	if e := b.Store.Update(ctx, func(d *store.Data) error {
		m, e := d.Find("delayed")
		if e != nil {
			return e
		}
		m.NextAttempt = b.now().Add(time.Minute)
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e := b.Dispatch(ctx, "receiver"); e != nil {
		t.Fatal(e)
	}
	if calls.Load() != 0 {
		t.Fatal("delayed head probed driver")
	}
}

func TestClosingServerCancelsReceiverWait(t *testing.T) {
	b, _ := setup(t)
	call, cancel := context.WithCancel(ctx)
	defer cancel()
	server, e := b.Listen()
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close()
	go server.Serve(call)
	q := Request{Verb: "receiver-state", Wait: true, Caller: Caller{ID: "receiver", Harness: "claude"}}
	r, e := Call(call, b.Store.Home, q)
	if e != nil || r.Code != 0 {
		t.Fatal(r, e)
	}
	var snap struct {
		Token string `json:"if_updated"`
	}
	json.Unmarshal(r.Data, &snap)
	q.IfUpdated = snap.Token
	reply := make(chan error, 1)
	go func() { _, e := Call(call, b.Store.Home, q); reply <- e }()
	select {
	case e := <-reply:
		t.Fatal("wait returned early", e)
	case <-time.After(30 * time.Millisecond):
	}
	closed := make(chan struct{})
	go func() { server.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("server close blocked on long poll")
	}
	select {
	case e := <-reply:
		if e == nil {
			t.Fatal("closed connection returned success")
		}
	case <-time.After(time.Second):
		t.Fatal("receiver did not see broker disconnect")
	}
}

func TestPolicyResumeWakesBackedOffDelivery(t *testing.T) {
	for _, kind := range []string{"kill", "allow", "config"} {
		t.Run(kind, func(t *testing.T) {
			b, f := setup(t)
			send(t, b, "paused")
			if kind == "allow" {
				if e := b.Allow(ctx, "receiver", false, op); e != nil {
					t.Fatal(e)
				}
			} else {
				if _, e := b.Kill(ctx, true, op); e != nil {
					t.Fatal(e)
				}
			}
			injected := make(chan struct{}, 1)
			f.inject = func(context.Context, store.Session, string, string) (Result, error) {
				injected <- struct{}{}
				return Result{Outcome: Accepted}, nil
			}
			var sweeps atomic.Int64
			backedOff := make(chan struct{}, 1)
			b.Fault = func(point string) {
				if point == "after_tick" && sweeps.Add(1) == 7 {
					backedOff <- struct{}{}
				}
			}
			call, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() { defer close(done); b.RunTicks(call) }()
			defer func() { cancel(); <-done }()
			select {
			case <-backedOff:
			case <-time.After(10 * time.Second):
				t.Fatal("scheduler did not back off")
			}
			time.Sleep(50 * time.Millisecond)
			switch kind {
			case "kill":
				if _, e := b.Kill(ctx, false, op); e != nil {
					t.Fatal(e)
				}
			case "allow":
				if e := b.Allow(ctx, "receiver", true, op); e != nil {
					t.Fatal(e)
				}
			case "config":
				if _, e := b.Config(ctx, map[string]json.RawMessage{"killed": json.RawMessage("false")}, op); e != nil {
					t.Fatal(e)
				}
			}
			select {
			case <-injected:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("policy resume did not wake queued mail")
			}
		})
	}
}

func TestOverdueLookupWithInvalidConfigDoesNotSpin(t *testing.T) {
	b, f := setup(t)
	f.result = Result{Outcome: Unconfirmed}
	send(t, b, "overdue")
	dispatch(t, b)
	if e := os.WriteFile(b.ConfigPath, []byte("{invalid"), 0600); e != nil {
		t.Fatal(e)
	}
	var ticks atomic.Int64
	b.Fault = func(point string) {
		if point == "after_tick" {
			ticks.Add(1)
		}
	}
	call, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	b.RunTicks(call)
	if ticks.Load() > 3 {
		t.Fatal("failed overdue tick spun without backoff", ticks.Load())
	}
}
