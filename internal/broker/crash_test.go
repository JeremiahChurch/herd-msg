package broker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

// The acceptance ledger is independent of SQLite and survives SIGKILL.
type ledgerDriver struct {
	home, point, mode string
	broker            *Broker
	prior             *store.Attempt
}
type ledgerEntry struct {
	MessageID   string `json:"message_id"`
	BudgetEpoch int    `json:"budget_epoch"`
	AttemptID   string `json:"attempt_id"`
}

func (d ledgerDriver) Probe(context.Context) error { return nil }
func (d ledgerDriver) SessionState(context.Context, store.Session) (Availability, error) {
	if d.mode == "prune" {
		return Availability{State: "offline"}, nil
	}
	return Availability{State: "live"}, nil
}
func (d ledgerDriver) Inject(ctx context.Context, s store.Session, envelope, id string) (Result, error) {
	data, e := store.Snapshot(ctx, d.home)
	if e != nil {
		return Result{}, e
	}
	var entry ledgerEntry
	for _, m := range data.Messages {
		for _, a := range m.Attempts {
			if a.ID == id {
				entry = ledgerEntry{m.ID, a.BudgetEpoch, a.ID}
				if m.State != states.Injecting {
					return Result{}, errors.New("write preceded injecting commit")
				}
			}
		}
	}
	if entry.AttemptID == "" {
		return Result{}, errors.New("write preceded attempt commit")
	}
	if e = appendLedger(d.home, "invocation.jsonl", entry); e != nil {
		return Result{}, e
	}
	crash(d.point, "during_driver_write")
	if d.mode == "timeout" {
		select {}
	}
	if d.mode == "signal" {
		if e = os.WriteFile(filepath.Join(d.home, "signal-ready"), []byte("ready"), 0600); e != nil {
			return Result{}, e
		}
		<-ctx.Done()
		return Result{Outcome: Unconfirmed}, ctx.Err()
	}

	if d.mode == "older_positive" {
		if d.prior == nil {
			return Result{Outcome: Unconfirmed}, nil
		}
		if e = appendLedger(d.home, "acceptance.jsonl", ledgerEntry{d.prior.MessageID, d.prior.BudgetEpoch, d.prior.ID}); e != nil {
			return Result{}, e
		}
		if e = d.broker.Finish(ctx, *d.prior, Result{Outcome: Accepted}, nil); e != nil {
			return Result{}, e
		}
		crash(d.point, "after_older_positive")
	}
	if d.mode == "resolve" {
		return Result{Outcome: Unconfirmed}, nil
	}
	if d.mode == "not_sent" {
		crash(d.point, "after_not_sent_return")
		return Result{Outcome: NotSent, Proof: "fake writer did not write", Class: "delivery"}, nil
	}
	if e = appendLedger(d.home, "acceptance.jsonl", entry); e != nil {
		return Result{}, e
	}
	crash(d.point, "after_acceptance")
	if d.mode == "uncertain" || d.mode == "lookup" || d.mode == "resolve" {
		return Result{Outcome: Unconfirmed}, nil
	}
	return Result{Outcome: Accepted}, nil
}
func appendLedger(home, name string, entry ledgerEntry) error {
	f, e := os.OpenFile(filepath.Join(home, name), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	e = json.NewEncoder(f).Encode(entry)
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	return e
}
func (d ledgerDriver) Lookup(ctx context.Context, r LookupRequest) (bool, error) {
	b, e := os.ReadFile(filepath.Join(d.home, "acceptance.jsonl"))
	if os.IsNotExist(e) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var row ledgerEntry
		if json.Unmarshal([]byte(line), &row) == nil && row.AttemptID == r.Attempt.ID {
			return true, nil
		}
	}
	return false, nil
}
func crash(want, point string) {
	if want == point {
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	}
}
func TestCrashWorker(t *testing.T) {
	if os.Getenv("MAILBOX_CRASH_WORKER") != "1" {
		return
	}
	home := os.Getenv("MAILBOX_CRASH_HOME")
	point := os.Getenv("MAILBOX_CRASH_POINT")
	mode := os.Getenv("MAILBOX_CRASH_MODE")
	driver := &ledgerDriver{home: home, point: point, mode: mode}
	// Recovery hooks are exercised on a second process invocation with an injected row.
	b, e := New(home, filepath.Join(home, "config.json"), map[string]Driver{"codex": driver}, func(p string) {
		if strings.Contains(point, "recovery") {
			crash(point, p)
		}
	})
	if e != nil {
		t.Fatal(e)
	}
	driver.broker = b
	defer b.Close()
	if strings.Contains(point, "recovery") {
		t.Fatal("recovery hook was not hit")
	}
	if e = b.Register(ctx, store.Session{ID: "receiver", Name: "receiver", Harness: "codex", State: "live"}); e != nil {
		t.Fatal(e)
	}
	r := SendRequest{ID: "crash-message", Created: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), To: "receiver", Body: "durable crash payload"}
	now := r.Created
	b.Now = func() time.Time { return now }
	if mode == "timeout" {
		_, e = b.changeConfig(ctx, op, func(c *policy.Config) error { c.DriverTimeout = 1; c.Claim = 2; return nil })
		if e != nil {
			t.Fatal(e)
		}
	}
	if mode == "revoke" {
		_, e = b.changeConfig(ctx, op, func(c *policy.Config) error { c.Links["peer"] = policy.Grant{AutoRun: true}; return nil })
		if e != nil {
			t.Fatal(e)
		}
	}
	b.Fault = func(p string) { crash(point, p) }
	if mode == "revoke" {
		_, e = b.Accept(ctx, store.Message{ID: r.ID, Origin: "peer", Sender: "writer.codex@peer", SenderID: "writer", Recipient: "receiver", Destination: "local", Body: r.Body, Created: r.Created})
	} else {
		_, e = b.Send(ctx, r, op)
	}
	if e != nil {
		t.Fatal(e)
	}
	if mode == "signal" {
		call, cancel := signal.NotifyContext(ctx, syscall.SIGTERM)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- b.Dispatch(call, "receiver") }()
		<-call.Done()
		b.Stop()
		if e = <-done; e != nil {
			t.Fatal(e)
		}
		wait, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		if n := b.Drain(wait); n != 0 {
			t.Fatal("writer did not drain", n)
		}
		return
	}
	switch mode {
	case "prune":
		_, e = b.Prune(ctx, "receiver", true, nil, op)
	case "config":
		_, e = b.Kill(ctx, true, op)
	case "revoke":
		_, e = b.changeConfig(ctx, op, func(c *policy.Config) error { c.Links["peer"] = policy.Grant{}; return nil })
	default:
		e = b.Dispatch(ctx, "receiver")
	}
	if e != nil {
		t.Fatal(e)
	}
	switch mode {
	case "older_positive":
		a := message(t, b, r.ID).Attempts[0]
		if e = b.Resolve(ctx, r.ID, "resend", true, "", op); e == nil {
			driver.prior = &a
			e = b.Dispatch(ctx, "receiver")
		}
	case "resolve":
		e = b.Resolve(ctx, r.ID, "resend", true, "", op)
	case "retention":
		now = now.Add(31 * 24 * time.Hour)
		e = b.Sweep(ctx)
	case "lookup", "uncertain":
		e = b.Sweep(ctx)
	}
	if e != nil {
		t.Fatal(e)
	}
	t.Fatalf("fault hook %s was not reached", point)
}
func TestCrashBoundaries(t *testing.T) {
	cases := []struct{ point, mode string }{
		{"before_admission_commit", "accepted"}, {"after_admission_commit", "accepted"},
		{"before_claim_commit", "accepted"}, {"after_claim_commit", "accepted"},
		{"before_injecting_commit", "accepted"}, {"after_injecting_commit", "accepted"},
		{"during_driver_write", "accepted"}, {"after_acceptance", "accepted"},
		{"after_driver_return", "accepted"}, {"before_result_commit", "accepted"}, {"after_result_commit", "accepted"},
		{"after_not_sent_return", "not_sent"}, {"after_driver_return", "not_sent"}, {"before_result_commit", "not_sent"},
		{"before_lookup", "lookup"}, {"after_lookup", "lookup"},
		{"before_prune_commit", "prune"}, {"after_prune_commit", "prune"},
		{"before_retention_commit", "retention"}, {"after_retention_commit", "retention"},
		{"before_resolve_commit", "resolve"}, {"after_resolve_commit", "resolve"},
		{"before_config_rename", "config"}, {"after_config_rename", "config"},
		{"after_timeout_commit", "timeout"}, {"after_older_positive", "older_positive"}, {"before_config_rename", "revoke"}, {"after_config_rename", "revoke"},
		{"before_sweep_commit", "uncertain"}, {"after_sweep_commit", "uncertain"},
	}
	for _, test := range cases {
		t.Run(test.mode+"/"+test.point, func(t *testing.T) {
			home := t.TempDir()
			mode := test.mode
			if mode == "lookup" || mode == "resolve" {
				mode = "uncertain"
			}
			// Keep the fake's mode separate from the action selected by the worker.
			workerMode := test.mode
			cmd := exec.Command(os.Args[0], "-test.run=^TestCrashWorker$")
			cmd.Env = append(os.Environ(), "MAILBOX_CRASH_WORKER=1", "MAILBOX_CRASH_HOME="+home, "MAILBOX_CRASH_POINT="+test.point, "MAILBOX_CRASH_MODE="+workerMode)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || !exit.ProcessState.Sys().(syscall.WaitStatus).Signaled() || exit.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
				t.Fatalf("worker was not killed: %v\n%s", err, output)
			}
			beforeRead := map[string][]byte{}
			for _, suffix := range []string{"", "-wal", "-shm"} {
				blob, err := os.ReadFile(filepath.Join(home, "mailbox.db") + suffix)
				if err == nil {
					beforeRead[suffix] = blob
				} else if !os.IsNotExist(err) {
					t.Fatal(err)
				}
			}
			hot, e := store.Snapshot(ctx, home)
			for suffix, before := range beforeRead {
				after, err := os.ReadFile(filepath.Join(home, "mailbox.db") + suffix)
				if err != nil || len(before) != len(after) || (suffix != "-shm" && string(before) != string(after)) {
					t.Fatal("hot crash snapshot mutated files", suffix, err)
				}
			}
			if e != nil {
				t.Fatal(e)
			}
			hotKey := "local/crash-message"
			if test.mode == "revoke" {
				hotKey = "peer/crash-message"
			}
			hotMessage := hot.Messages[hotKey]
			if test.mode == "timeout" && hot.Health["outstanding/receiver"].Check == "" {
				t.Fatal("timed-out writer had no durable notice before restart")
			}
			if test.mode == "retention" {
				if test.point == "before_retention_commit" && hotMessage == nil {
					t.Fatal("retention committed before hook")
				}
				if test.point == "after_retention_commit" && hotMessage != nil {
					t.Fatal("retention kept payload")
				}
			}
			if test.mode == "prune" {
				after := test.point == "after_prune_commit"
				if hot.Sessions["receiver"].Tombstone != after || hotMessage == nil || (after && hotMessage.State != states.Dropped) || (!after && hotMessage.State != states.Queued) {
					t.Fatal("prune was not atomic", hotMessage)
				}
			}
			if test.mode == "resolve" {
				want := 1
				if test.point == "after_resolve_commit" {
					want = 2
				}
				if hotMessage == nil || hotMessage.BudgetEpoch != want {
					t.Fatal("resend budget was not atomic", hotMessage)
				}
			}
			var driver Driver = ledgerDriver{home: home, mode: mode}
			if test.mode == "older_positive" {
				if hotMessage == nil || hotMessage.State != states.Injecting || len(hotMessage.Attempts) != 2 || !hotMessage.Attempts[0].Positive {
					t.Fatal("missing durable older proof at death", hotMessage)
				}
				driver = &FakeDriver{lookupErr: errors.New("lookup must not be needed")}
			}
			b, e := NewWithClock(home, filepath.Join(home, "config.json"), map[string]Driver{"codex": driver}, func() time.Time { return time.Date(2030, 1, 1, 0, 0, 1, 0, time.UTC) })
			if e != nil {
				t.Fatal(e)
			}
			defer b.Close()
			if test.mode == "older_positive" && message(t, b, "crash-message").State != states.Injected {
				t.Fatal("recovery required a lookup despite stored positive proof")
			}
			if test.mode == "timeout" {
				d, _, err := b.Data(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if _, exists := d.Health["outstanding/receiver"]; exists {
					t.Fatal("restart retained obsolete outstanding notice")
				}
				if b.Outstanding() != 0 {
					t.Fatal("restart retained in-process writer")
				}
			}
			b.Now = func() time.Time { return time.Date(2030, 1, 1, 0, 0, 1, 0, time.UTC) }
			if e = b.Sweep(ctx); e != nil {
				t.Fatal(e)
			}
			data, _, e := b.Data(ctx)
			if e != nil {
				t.Fatal(e)
			}
			key := "local/crash-message"
			if test.mode == "revoke" {
				key = "peer/crash-message"
			}
			m := data.Messages[key]
			if m == nil && test.point != "before_admission_commit" && test.mode != "retention" {
				t.Fatal("accepted message silently disappeared")
			}
			if m != nil && m.State == states.Claimed {
				t.Fatal("claim was not recovered")
			}
			if m != nil && m.State == states.Injecting {
				t.Fatal("injecting was not parked")
			}
			if m != nil && m.State == states.Queued {
				if len(m.Attempts) > 0 {
					last := m.Attempts[len(m.Attempts)-1]
					if last.BudgetEpoch == m.BudgetEpoch && last.Outcome != NotSent {
						t.Fatal("uncertain attempt automatically returned to queue")
					}
				}
				if e = b.Dispatch(ctx, "receiver"); e != nil {
					t.Fatal(e)
				}
			}
			if e = b.Dispatch(ctx, "receiver"); e != nil {
				t.Fatal(e)
			}
			calls := ledgerFile(t, home, "invocation.jsonl")
			perEpoch := map[int]int{}
			for _, call := range calls {
				perEpoch[call.BudgetEpoch]++
			}
			for epoch, n := range perEpoch {
				if n > 1 {
					t.Fatalf("second automatic Inject in epoch %d: %+v", epoch, calls)
				}
			}
			entries := ledgerRows(t, home)
			if len(entries) > 1 {
				t.Fatalf("second automatic acceptance: %+v", entries)
			}
			data, _, _ = b.Data(ctx)
			m = data.Messages[key]
			if len(entries) == 1 && m != nil && m.State != states.Injected && m.State != states.Dropped && m.State != states.Indeterminate {
				t.Fatalf("accepted message became retryable: %s", m.State)
			}
			if len(entries) == 1 && m != nil && test.mode != "resolve" && m.State != states.Dropped && m.State != states.Injected {
				t.Fatal("positive ledger proof did not settle", m.State)
			}
			if test.mode == "revoke" && test.point == "after_config_rename" && m.State != states.Held {
				t.Fatal("revoked grant injected", m.State)
			}
			if test.mode == "config" && test.point == "after_config_rename" {
				if len(entries) != 0 || len(calls) != 0 {
					t.Fatal("kill allowed a new injection")
				}
				config, e := policy.Read(filepath.Join(home, "config.json"))
				if e != nil || !config.Killed {
					t.Fatal("lost committed kill", e)
				}
			}
		})
	}
}
func ledgerRows(t *testing.T, home string) []ledgerEntry {
	return ledgerFile(t, home, "acceptance.jsonl")
}
func ledgerFile(t *testing.T, home, name string) []ledgerEntry {
	t.Helper()
	b, e := os.ReadFile(filepath.Join(home, name))
	if os.IsNotExist(e) {
		return nil
	}
	if e != nil {
		t.Fatal(e)
	}
	var out []ledgerEntry
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var row ledgerEntry
		if e = json.Unmarshal([]byte(line), &row); e != nil {
			t.Fatal(e)
		}
		out = append(out, row)
	}
	return out
}

func TestCrashRecoveryTransactions(t *testing.T) {
	for _, state := range []states.State{states.Claimed, states.Injecting} {
		for _, point := range []string{"before_recovery_commit", "after_recovery_commit"} {
			t.Run(string(state)+"/"+point, func(t *testing.T) {
				home := t.TempDir()
				s, e := store.Open(home)
				if e != nil {
					t.Fatal(e)
				}
				now := time.Now().UTC()
				e = s.Update(ctx, func(d *store.Data) error {
					d.Sessions["receiver"] = store.Session{ID: "receiver", Name: "receiver", Harness: "codex", State: "live", Generation: "generation-a"}
					m := &store.Message{ID: "recover", Origin: "local", Sender: "operator@local", SenderID: "operator", Recipient: "receiver", Destination: "local", State: state, Created: now, Accepted: now, Updated: now, Expires: now.Add(time.Hour), BrokerEpoch: "old-epoch", ClaimToken: "old-token", Lease: now.Add(time.Minute), BudgetEpoch: 1}
					if state == states.Injecting {
						a := store.Attempt{ID: "old-attempt", MessageID: m.ID, Origin: m.Origin, Session: d.Sessions["receiver"], Started: now}
						m.Attempts = []store.Attempt{a}
						m.ActiveAttempt = a.ID
					}
					d.Messages["local/recover"] = m
					d.Dedupe["local/recover"] = store.Dedupe{ID: m.ID, Origin: m.Origin, Created: now, State: state}
					return nil
				})
				if e != nil {
					t.Fatal(e)
				}
				s.Close()
				cmd := exec.Command(os.Args[0], "-test.run=^TestCrashWorker$")
				cmd.Env = append(os.Environ(), "MAILBOX_CRASH_WORKER=1", "MAILBOX_CRASH_HOME="+home, "MAILBOX_CRASH_POINT="+point, "MAILBOX_CRASH_MODE=recovery")
				output, e := cmd.CombinedOutput()
				var exit *exec.ExitError
				if !errors.As(e, &exit) || !exit.ProcessState.Sys().(syscall.WaitStatus).Signaled() {
					t.Fatalf("recovery hook missed: %v %s", e, output)
				}
				b, e := New(home, filepath.Join(home, "config.json"), map[string]Driver{"codex": ledgerDriver{home: home}})
				if e != nil {
					t.Fatal(e)
				}
				defer b.Close()
				m := message(t, b, "recover")
				want := states.Queued
				if state == states.Injecting {
					want = states.Indeterminate
				}
				if m.State != want {
					t.Fatal(m.State, want)
				}
				if state == states.Injecting {
					dispatch(t, b)
					if len(ledgerRows(t, home)) != 0 {
						t.Fatal("recovered uncertainty resent")
					}
				}
			})
		}
	}
}

func TestActiveSignalDrain(t *testing.T) {
	home := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashWorker$")
	cmd.Env = append(os.Environ(), "MAILBOX_CRASH_WORKER=1", "MAILBOX_CRASH_HOME="+home, "MAILBOX_CRASH_POINT=", "MAILBOX_CRASH_MODE=signal")
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	ready := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, e := os.Stat(filepath.Join(home, "signal-ready")); e == nil {
			ready = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatal("active writer did not start")
	}
	if e := cmd.Process.Signal(syscall.SIGTERM); e != nil {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal("signal drain failed", e)
		}
	case <-time.After(3 * time.Second):
		cmd.Process.Kill()
		<-done
		t.Fatal("signal drain did not stop")
	}
	b, e := NewWithClock(home, filepath.Join(home, "config.json"), map[string]Driver{"codex": ledgerDriver{home: home}}, func() time.Time { return time.Date(2030, 1, 1, 0, 0, 1, 0, time.UTC) })
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	if message(t, b, "crash-message").State != states.Indeterminate || b.Outstanding() != 0 {
		t.Fatal("SIGTERM did not park and drain writer")
	}
	dispatch(t, b)
	if len(ledgerFile(t, home, "invocation.jsonl")) != 1 {
		t.Fatal("SIGTERM recovery re-injected")
	}
}
