//go:build live

package e2e

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/drivers/codex"
	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func TestSupportedCodexFloor(t *testing.T) {
	binary := os.Getenv("HERD_MSG_LIVE_CODEX_FLOOR_BINARY")
	if os.Getenv("HERD_MSG_LIVE") != "1" || binary == "" {
		t.Skip("set HERD_MSG_LIVE=1 and HERD_MSG_LIVE_CODEX_FLOOR_BINARY to an isolated 0.157.1 binary")
	}
	version, e := exec.Command(binary, "--version").Output()
	if e != nil || !strings.Contains(string(version), "0.157.1") {
		t.Fatal("expected Codex 0.157.1", string(version), e)
	}
	home, e := os.UserHomeDir()
	if e != nil {
		t.Fatal(e)
	}
	root, e := os.MkdirTemp(home, "hm-floor-")
	if e != nil {
		t.Fatal(e)
	}
	defer os.RemoveAll(root)
	codexHome := filepath.Join(root, "codex")
	socket := filepath.Join(codexHome, "app-server-control", "app-server-control.sock")
	os.MkdirAll(filepath.Dir(socket), 0700)
	auth, e := os.ReadFile(filepath.Join(home, ".codex", "auth.json"))
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(codexHome, "auth.json"), auth, 0600); e != nil {
		t.Fatal(e)
	}
	env := append(cleanEnv(), "CODEX_HOME="+codexHome, "DISABLE_AUTOUPDATER=1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	server := exec.Command(binary, "app-server", "--listen", "unix://"+socket)
	server.Env = env
	server.Dir = root
	log, e := os.Create(filepath.Join(root, "server.log"))
	if e != nil {
		t.Fatal(e)
	}
	defer log.Close()
	server.Stdout = log
	server.Stderr = log
	if e = server.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() { server.Process.Kill(); server.Wait() }()
	wait := func(label string, timeout time.Duration, fn func() bool) {
		t.Helper()
		until := time.Now().Add(timeout)
		for time.Now().Before(until) {
			if fn() {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(200 * time.Millisecond):
			}
		}
		t.Fatal("timed out:", label)
	}
	wait("old app-server socket", 10*time.Second, func() bool { _, e := os.Stat(socket); return e == nil })
	name := "mail-floor-codex"
	if exec.Command("tmux", "-L", "herd-msg-test", "has-session", "-t", name).Run() == nil {
		t.Fatal("floor test session already in use")
	}
	command := "cd " + quote(root) + " && exec " + quote(binary) + " --remote " + quote("unix://"+socket) + " -m gpt-6-luna -c 'model_reasoning_effort=\"low\"' --dangerously-bypass-approvals-and-sandbox --no-alt-screen " + quote("Reply exactly FLOOR_READY. The user authorizes harmless sleep commands for this disposable test.")
	tmux := exec.Command("tmux", "-L", "herd-msg-test", "new-session", "-d", "-s", name, "-x", "160", "-y", "50", command)
	tmux.Env = env
	if out, e := tmux.CombinedOutput(); e != nil {
		t.Fatal(e, string(out))
	}
	defer exec.Command("tmux", "-L", "herd-msg-test", "kill-session", "-t", name).Run()
	pane := func() string {
		b, _ := exec.Command("tmux", "-L", "herd-msg-test", "capture-pane", "-p", "-S", "-80", "-t", name).Output()
		return string(b)
	}
	driver := &codex.Driver{Socket: socket, Agents: func(context.Context, string) ([]identity.Agent, error) { return []identity.Agent{}, nil }}
	var recipient store.Session
	wait("attached floor thread", 30*time.Second, func() bool {
		text := pane()
		if strings.Contains(text, "Do you trust") || strings.Contains(text, "Trust this folder") {
			exec.Command("tmux", "-L", "herd-msg-test", "send-keys", "-t", name, "Enter").Run()
		}
		probe, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		sessions, e := driver.Discover(probe)
		if e != nil {
			return false
		}
		for _, s := range sessions {
			if s.Hints["cwd"] == root && s.Name != s.ID {
				recipient = s
				return true
			}
		}
		return false
	})
	wait("floor ready", 30*time.Second, func() bool { return strings.Contains(pane(), "FLOOR_READY") })
	idle := func() {
		wait("floor idle", 30*time.Second, func() bool {
			probe, stop := context.WithTimeout(ctx, time.Second)
			defer stop()
			c, e := codex.Open(probe, socket)
			if e != nil {
				return false
			}
			defer c.Close()
			thread, e := c.Read(recipient.ID, false)
			return e == nil && thread.Status.Type == "idle"
		})
	}
	idle()
	b, e := broker.New(filepath.Join(root, "mail"), filepath.Join(root, "config.json"), map[string]broker.Driver{"codex": driver})
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	if e = b.Register(ctx, recipient); e != nil {
		t.Fatal(e)
	}
	send := func(body string) string {
		id := broker.NewID(time.Now())
		if _, e := b.Send(ctx, broker.SendRequest{ID: id, Created: time.Now(), To: recipient.ID, Body: body}, broker.Caller{ID: "operator"}); e != nil {
			t.Fatal(e)
		}
		if e = b.Dispatch(ctx, recipient.ID); e != nil {
			t.Fatal(e)
		}
		return id
	}
	accepted := func(id string) {
		wait("floor positive acceptance", 40*time.Second, func() bool {
			b.Tick(ctx)
			d, e := b.Store.Read(ctx)
			if e != nil {
				return false
			}
			m, e := d.Find(id)
			return e == nil && m.State == "injected"
		})
	}
	typeDraft := func(text string) {
		if out, e := exec.Command("tmux", "-L", "herd-msg-test", "send-keys", "-t", name, "-l", text).CombinedOutput(); e != nil {
			t.Fatal(e, string(out))
		}
	}
	assertDraft := func(text string) {
		t.Helper()
		if !strings.Contains(pane(), text) {
			t.Fatal("floor composer draft lost", pane())
		}
		probe, stop := context.WithTimeout(ctx, 2*time.Second)
		defer stop()
		c, e := codex.Open(probe, socket)
		if e != nil {
			t.Fatal(e)
		}
		defer c.Close()
		var history json.RawMessage
		if e = c.Call("thread/read", map[string]any{"threadId": recipient.ID, "includeTurns": true}, &history); e != nil {
			t.Fatal(e)
		}
		if strings.Contains(string(history), text) {
			t.Fatal("floor draft entered accepted history")
		}
	}
	typeDraft("UNSENT_FLOOR_IDLE")
	id := send("Reply FLOOR_IDLE_DELIVERY only.")
	accepted(id)
	assertDraft("UNSENT_FLOOR_IDLE")
	exec.Command("tmux", "-L", "herd-msg-test", "send-keys", "-t", name, "C-u").Run()
	idle()
	t.Log("Codex 0.157.1 attached idle start accepted without resume; draft intact")
	probe, stop := context.WithTimeout(ctx, 3*time.Second)
	e = busyClient(probe, socket, map[string]any{"threadId": recipient.ID, "clientUserMessageId": "floor-busy-" + store.Random(), "input": []any{map[string]string{"type": "text", "text": "Execute shell sleep 12 once, then reply BUSY_DONE."}}, "approvalPolicy": "never", "sandboxPolicy": map[string]any{"type": "dangerFullAccess"}, "collaborationMode": map[string]any{"mode": "default", "settings": map[string]any{"model": "gpt-6-luna", "reasoning_effort": "low", "developer_instructions": "When asked for BUSY_DONE, execute shell sleep 12 once. Follow other messages normally. The user authorizes harmless sleep commands in this disposable test."}}})
	stop()
	if e != nil {
		t.Fatal(e)
	}
	wait("floor active", 15*time.Second, func() bool {
		probe, stop := context.WithTimeout(ctx, time.Second)
		defer stop()
		c, e := codex.Open(probe, socket)
		if e != nil {
			return false
		}
		defer c.Close()
		thread, e := c.Read(recipient.ID, false)
		return e == nil && thread.Status.Type == "active"
	})
	typeDraft("UNSENT_FLOOR_ACTIVE")
	id = send("After the sleep, reply FLOOR_ACTIVE_DELIVERY.")
	d, _ := b.Store.Read(ctx)
	waiting, _ := d.Find(id)
	if waiting.State != "queued" || len(waiting.Attempts) != 0 {
		t.Fatal("busy thread received an injection")
	}
	accepted(id)
	assertDraft("UNSENT_FLOOR_ACTIVE")
	t.Log("Codex 0.157.1 waited for idle, then delivered without resume; draft intact")
}
