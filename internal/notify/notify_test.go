package notify

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func TestNotificationDedupSurvivesRestartAndClearsAfterRecovery(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	bin := filepath.Join(home, "bin")
	if e := os.Mkdir(bin, 0700); e != nil {
		t.Fatal(e)
	}
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/alerts\"\n"
	if e := os.WriteFile(filepath.Join(bin, "herdr"), []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", bin+":/usr/bin:/bin")
	n := Notifier{Path: filepath.Join(home, "notifications.json")}
	d := store.Empty()
	now := time.Now()
	d.Notice("driver/codex", "missing socket", "codex --remote unix://", now)
	for i := 0; i < 2; i++ {
		if e := n.Sweep(context.Background(), d, now.Add(16*time.Minute)); e != nil {
			t.Fatal(e)
		}
	}
	data, _ := os.ReadFile(filepath.Join(home, "alerts"))
	first := string(data)
	if first == "" {
		t.Fatal("no alert")
	}
	d.Notice("driver/codex", "unsupported version", "upgrade", now)
	if e := n.Sweep(context.Background(), d, now.Add(16*time.Minute)); e != nil {
		t.Fatal(e)
	}
	data, _ = os.ReadFile(filepath.Join(home, "alerts"))
	if string(data) == first {
		t.Fatal("changed failure not alerted")
	}
	delete(d.Health, "driver/codex")
	if e := n.Sweep(context.Background(), d, now.Add(16*time.Minute)); e != nil {
		t.Fatal(e)
	}
	d.Notice("driver/codex", "unsupported version", "upgrade", now)
	before := len(data)
	if e := n.Sweep(context.Background(), d, now.Add(16*time.Minute)); e != nil {
		t.Fatal(e)
	}
	data, _ = os.ReadFile(filepath.Join(home, "alerts"))
	if len(data) <= before {
		t.Fatal("recurrent failure not alerted")
	}
}
func TestStuckMailAndLinkDelayAndFailedSend(t *testing.T) {
	now := time.Now()
	d := store.Empty()
	d.Links["node-b"] = store.Link{Machine: "node-b", Error: "offline"}
	calls := 0
	fail := true
	n := Notifier{Path: filepath.Join(t.TempDir(), "alerts.json"), Send: func(context.Context, string, string) error {
		calls++
		if fail {
			return errors.New("herdr unavailable")
		}
		return nil
	}}
	if e := n.Sweep(context.Background(), d, now); e != nil || calls != 0 {
		t.Fatal(e, calls)
	}
	d.Messages["mail"] = &store.Message{ID: "mail", Origin: "node-a", State: states.Indeterminate}
	d.Notice("message/"+store.Key("node-a", "mail"), "awaiting confirmation", "inspect", now)
	if e := n.Sweep(context.Background(), d, now.Add(59*time.Minute)); e == nil || calls != 1 {
		t.Fatal(e, calls)
	}
	fail = false
	if e := n.Sweep(context.Background(), d, now.Add(61*time.Minute)); e != nil || calls != 3 {
		t.Fatal(e, calls)
	}
	if e := n.Sweep(context.Background(), d, now.Add(62*time.Minute)); e != nil || calls != 3 {
		t.Fatal("duplicate alerts", e, calls)
	}
	d.Links["node-b"] = store.Link{Reachable: true, LastContact: now.Add(62 * time.Minute)}
	if e := n.Sweep(context.Background(), d, now.Add(62*time.Minute)); e != nil {
		t.Fatal(e)
	}
}

func TestPendingInputResolvingWithinFifteenMinutesDoesNotAlert(t *testing.T) {
	now := time.Now()
	d := store.Empty()
	key := store.Key("node-a", "pending")
	d.Messages[key] = &store.Message{ID: "pending", Origin: "node-a", State: states.Indeterminate, Updated: now}
	calls := 0
	n := Notifier{Path: filepath.Join(t.TempDir(), "alerts.json"), Send: func(context.Context, string, string) error { calls++; return nil }}
	if e := n.Sweep(context.Background(), d, now.Add(10*time.Second)); e != nil || calls != 0 {
		t.Fatal("normal pending input notified", calls, e)
	}
	d.Messages[key].State = states.Injected
	if e := n.Sweep(context.Background(), d, now.Add(3*time.Minute)); e != nil || calls != 0 {
		t.Fatal("accepted input notified", calls, e)
	}
	d.Messages[key].State = states.Indeterminate
	d.Notice("message/"+key, "still unconfirmed", "inspect", now.Add(time.Minute))
	if e := n.Sweep(context.Background(), d, now.Add(16*time.Minute)); e != nil || calls != 1 {
		t.Fatal("mature stuck notice did not notify", calls, e)
	}
	if e := n.Sweep(context.Background(), d, now.Add(17*time.Minute)); e != nil || calls != 1 {
		t.Fatal("stuck notice repeated", calls, e)
	}
}

func TestPausedTrafficConditionsNotifyAndIdleDetachedSessionsStayQuiet(t *testing.T) {
	for _, key := range []string{"receiver/test", "outstanding/test", "config"} {
		t.Run(key, func(t *testing.T) {
			d := store.Empty()
			now := time.Now()
			d.Notice(key, "traffic paused", "repair", now)
			d.Notice("probe/session/idle", "detached", "resume", now)
			calls := 0
			n := Notifier{Path: filepath.Join(t.TempDir(), "alerts.json"), Send: func(context.Context, string, string) error { calls++; return nil }}
			for i := 0; i < 2; i++ {
				if e := n.Sweep(context.Background(), d, now.Add(16*time.Minute)); e != nil {
					t.Fatal(e)
				}
			}
			if calls != 1 {
				t.Fatal("missing or duplicate pause alert", calls)
			}
		})
	}
}
func TestUnlinkedMachineDoesNotAlert(t *testing.T) {
	d := store.Empty()
	d.Links["node-b"] = store.Link{Error: "link removed"}
	calls := 0
	n := Notifier{Path: filepath.Join(t.TempDir(), "alerts.json"), ActiveLinks: map[string]bool{}, Send: func(context.Context, string, string) error { calls++; return nil }}
	now := time.Now()
	for _, at := range []time.Time{now, now.Add(2 * time.Hour)} {
		if e := n.Sweep(context.Background(), d, at); e != nil {
			t.Fatal(e)
		}
	}
	if calls != 0 {
		t.Fatal("deliberate unlink alerted")
	}
}

func TestBriefReceiverWorkDoesNotNotify(t *testing.T) {
	now := time.Now()
	d := store.Empty()
	d.Notice("receiver/session", "outstanding work", "wait", now)
	calls := 0
	n := Notifier{Path: filepath.Join(t.TempDir(), "alerts.json"), Send: func(context.Context, string, string) error { calls++; return nil }}
	if e := n.Sweep(context.Background(), d, now.Add(time.Second)); e != nil || calls != 0 {
		t.Fatal("ordinary receiver work notified", calls, e)
	}
	delete(d.Health, "receiver/session")
	if e := n.Sweep(context.Background(), d, now.Add(time.Minute)); e != nil || calls != 0 {
		t.Fatal("completed receiver work notified", calls, e)
	}
}

func TestDetachedSessionAlertsOnlyWhileMailWaits(t *testing.T) {
	now := time.Now()
	d := store.Empty()
	d.Sessions["thread-a"] = store.Session{ID: "thread-a", Harness: "codex", State: "detached"}
	d.Notice("probe/session/thread-a", "detached", "resume-thread-a", now)
	var bodies []string
	n := Notifier{Path: filepath.Join(t.TempDir(), "alerts.json"), Send: func(_ context.Context, _, body string) error { bodies = append(bodies, body); return nil }}
	if e := n.Sweep(context.Background(), d, now); e != nil || len(bodies) != 0 {
		t.Fatal("idle detached session alerted", bodies, e)
	}
	d.Messages["one"] = &store.Message{ID: "one", Recipient: "thread-a", State: states.Queued}
	for i := 0; i < 2; i++ {
		if e := n.Sweep(context.Background(), d, now); e != nil {
			t.Fatal(e)
		}
	}
	d.Messages["two"] = &store.Message{ID: "two", Recipient: "thread-a", State: states.Queued}
	if e := n.Sweep(context.Background(), d, now); e != nil {
		t.Fatal(e)
	}
	if len(bodies) != 1 || !strings.Contains(bodies[0], "resume-thread-a") {
		t.Fatal("queued detached mail missing or noisy", bodies)
	}
	d.Messages = map[string]*store.Message{}
	if e := n.Sweep(context.Background(), d, now); e != nil {
		t.Fatal(e)
	}
	d.Messages["three"] = &store.Message{ID: "three", Recipient: "thread-a", State: states.Queued}
	if e := n.Sweep(context.Background(), d, now); e != nil || len(bodies) != 2 {
		t.Fatal("detached failure did not rearm after recovery", bodies, e)
	}
}
func TestDeadMailAlertDoesNotClaimPendingAcceptance(t *testing.T) {
	d := store.Empty()
	d.Messages["dead"] = &store.Message{ID: "dead", Origin: "node-a", State: states.Dead, Error: "proven failure"}
	body := ""
	n := Notifier{Path: filepath.Join(t.TempDir(), "alerts.json"), Send: func(_ context.Context, _, text string) error { body = text; return nil }}
	if e := n.Sweep(context.Background(), d, time.Now()); e != nil || body == "" || strings.Contains(body, "pending") {
		t.Fatal(body, e)
	}
}

func TestTransientDiscoveryNoticeIsQuietWhileProtocolFailureIsImmediate(t *testing.T) {
	now := time.Now()
	d := store.Empty()
	d.Notice("driver/codex", "brief discovery timeout", "wait", now)
	calls := 0
	n := Notifier{Path: filepath.Join(t.TempDir(), "alerts.json"), Send: func(context.Context, string, string) error { calls++; return nil }}
	if e := n.Sweep(context.Background(), d, now.Add(time.Second)); e != nil || calls != 0 {
		t.Fatal("brief discovery timeout alerted", calls, e)
	}
	d.Notice("probe/codex-socket", "unsupported server", "upgrade", now)
	if e := n.Sweep(context.Background(), d, now.Add(time.Second)); e != nil || calls != 1 {
		t.Fatal("protocol failure did not alert immediately", calls, e)
	}
}

func TestStartupFailurePreservesUnrelatedAlertHistory(t *testing.T) {
	now := time.Now()
	path := filepath.Join(t.TempDir(), "notifications.json")
	original := State{Sent: map[string]string{"link/node-b": "existing outage"}, Down: map[string]time.Time{"link/node-b": now.Add(-2 * time.Hour)}}
	data, _ := json.Marshal(original)
	if e := os.WriteFile(path, data, 0600); e != nil {
		t.Fatal(e)
	}
	calls := 0
	n := Notifier{Path: path, Send: func(context.Context, string, string) error { calls++; return nil }}
	for i := 0; i < 2; i++ {
		if e := n.Failure(context.Background(), "config", "bad startup settings", "repair"); e != nil {
			t.Fatal(e)
		}
	}
	data, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	var state State
	if e = json.Unmarshal(data, &state); e != nil {
		t.Fatal(e)
	}
	if calls != 1 || state.Sent["link/node-b"] != original.Sent["link/node-b"] || !state.Down["link/node-b"].Equal(original.Down["link/node-b"]) {
		t.Fatal("startup erased unrelated outage state", calls, state)
	}
}
