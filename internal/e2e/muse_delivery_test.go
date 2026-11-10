//go:build live

package e2e

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/drivers/muse"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

// This uses Muse's echo provider, isolated session storage, and a disposable
// broker. It exercises the native transport without billing a model account.
func TestMuseNativeDelivery(t *testing.T) {
	if os.Getenv("HERD_MSG_LIVE") != "1" || os.Getenv("HERD_MSG_LIVE_MUSE") != "1" {
		t.Skip("set HERD_MSG_LIVE=1 and HERD_MSG_LIVE_MUSE=1 to create a disposable Muse workspace")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	root := t.TempDir()
	cwd := filepath.Join(root, "workspace")
	if e := os.Mkdir(cwd, 0700); e != nil {
		t.Fatal(e)
	}
	data := filepath.Join(root, "data")
	t.Setenv("XDG_DATA_HOME", data)
	t.Setenv("HERD_MSG_HOME", filepath.Join(root, "mailbox"))
	t.Setenv("MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS", "1")
	t.Setenv("MUSE_EXPERIMENTAL_LOCAL_SESSION_MESSAGING", "1")
	session := os.Getenv("HERDR_SESSION")
	herdr := func(ctx context.Context, args ...string) ([]byte, error) {
		if session != "" {
			args = append([]string{"--session", session}, args...)
		}
		return exec.CommandContext(ctx, "herdr", args...).CombinedOutput()
	}
	launch := "env -u CODEX_THREAD_ID -u CLAUDE_CODE_SESSION_ID -u MUSE_SESSION_ID -u MUSE_THREAD_ID XDG_DATA_HOME=" + quote(data) + " MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=1 MUSE_EXPERIMENTAL_LOCAL_SESSION_MESSAGING=1 muse --provider echo --echo-delay-ms 3000 --yolo -c local_session_messaging.enabled=true"
	cmd := exec.CommandContext(ctx, "herd-spawn", "-b", "-a", "muse", "-l", "herd-msg-muse-test", cwd, "MUSE_ECHO_READY")
	cmd.Env = append(cleanEnv(), "HERD_MUSE_CMD="+launch)
	if session != "" {
		cmd.Env = append(cmd.Env, "HERDR_SESSION="+session)
	}
	b, e := cmd.CombinedOutput()
	if e != nil {
		t.Fatalf("spawn: %v: %s", e, b)
	}
	match := regexp.MustCompile(`workspace (w[A-Za-z0-9]+) .*pane=(w[A-Za-z0-9]+:p[0-9]+)`).FindStringSubmatch(string(b))
	if len(match) != 3 {
		t.Fatalf("cannot parse spawned workspace: %s", b)
	}
	wid, pane := match[1], match[2]
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if b, e := herdr(cleanup, "workspace", "close", wid); e != nil {
			t.Errorf("close %s: %v: %s", wid, e, b)
		}
	})
	wait := func(label string, fn func() bool) {
		t.Helper()
		for {
			if fn() {
				return
			}
			select {
			case <-ctx.Done():
				b, _ := herdr(context.Background(), "pane", "read", pane, "--lines", "30")
				t.Fatalf("%s: %v\n%s", label, ctx.Err(), b)
			case <-time.After(300 * time.Millisecond):
			}
		}
	}
	d := &muse.Driver{HerdrSession: session}
	var target store.Session
	wait("Muse ready", func() bool {
		peers, e := d.Discover(ctx)
		if e != nil {
			return false
		}
		for _, s := range peers {
			if s.Pane == pane && s.State == "live" && !s.Blocked && (s.Reason == "idle" || s.Reason == "done") {
				target = s
				return true
			}
		}
		return false
	})
	home := os.Getenv("HERD_MSG_HOME")
	mailbox, e := broker.New(home, "", map[string]broker.Driver{"muse": d})
	if e != nil {
		t.Fatal(e)
	}
	defer mailbox.Close()
	if e := mailbox.RefreshHarness(ctx, "muse", []store.Session{target}); e != nil {
		t.Fatal(e)
	}
	send := func(body string) {
		t.Helper()
		id := broker.NewID(time.Now())
		if _, e := mailbox.Send(ctx, broker.SendRequest{ID: id, Created: time.Now(), To: target.ID, Body: body}, broker.Caller{ID: "operator"}); e != nil {
			t.Fatal(e)
		}
		wait("durable native acceptance", func() bool {
			if e := mailbox.Tick(ctx); e != nil {
				t.Fatal(e)
			}
			data, _, e := mailbox.Data(ctx)
			if e != nil {
				t.Fatal(e)
			}
			for _, m := range data.Messages {
				if m.ID == id && m.State == states.Injected && len(m.Attempts) == 1 {
					return true
				}
			}
			return false
		})
	}
	send("MUSE_BROKER_ECHO_IDLE")
	wait("first echo working", func() bool {
		// Muse's herdr Stop stamp can remain done during a peer-triggered
		// turn. Confirm actual activity from this isolated session's log.
		paths, _ := filepath.Glob(filepath.Join(data, "muse", "sessions", "*", "*", "*", target.ID, "session.jsonl"))
		if len(paths) != 1 {
			return false
		}
		b, e := os.ReadFile(paths[0])
		if e != nil {
			return false
		}
		active := false
		for _, line := range strings.Split(string(b), "\n") {
			var r struct {
				Type    string `json:"payload_type"`
				Payload struct {
					Kind  string `json:"kind"`
					Event struct {
						Kind   string `json:"kind"`
						Prompt string `json:"prompt"`
					} `json:"event"`
				} `json:"payload"`
			}
			if json.Unmarshal([]byte(line), &r) != nil || r.Type != "runtime.session" || r.Payload.Kind != "run" {
				continue
			}
			switch r.Payload.Event.Kind {
			case "started":
				active = r.Payload.Event.Prompt == ""
			case "terminal":
				active = false
			}
		}
		return active
	})
	send("MUSE_BROKER_ECHO_BUSY")
	wait("visible echo response", func() bool {
		b, e := herdr(ctx, "pane", "read", pane, "--lines", "60")
		// Echo repeats the initial user prompt for peer-triggered turns. The
		// native preview truncates bodies and can group consecutive messages.
		text := string(b)
		received := strings.Count(text, "Received message from external agent") >= 2 || strings.Contains(text, "Received 2 messages")
		return e == nil && received && strings.Count(text, "echo: MUSE_ECHO_READY") >= 3
	})
}
