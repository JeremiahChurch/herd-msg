//go:build live

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/drivers/codex"
	"github.com/JeremiahChurch/herd-msg/internal/store"
	"github.com/JeremiahChurch/herd-msg/internal/wsunix"
)

// The busy-turn fixture opts into collaboration settings only on its isolated socket.
func busyClient(ctx context.Context, socket string, params any) error {
	w, e := wsunix.Dial(ctx, socket)
	if e != nil {
		return e
	}
	defer w.Close()
	call := func(id int, method string, params any) error {
		b, _ := json.Marshal(map[string]any{"id": id, "method": method, "params": params})
		if e := w.WriteMessage(b); e != nil {
			return e
		}
		for {
			b, e := w.ReadMessage()
			if e != nil {
				return e
			}
			var r struct {
				ID    *int            `json:"id"`
				Error *codex.RPCError `json:"error"`
			}
			if e = json.Unmarshal(b, &r); e != nil {
				return e
			}
			if r.ID != nil && *r.ID == id {
				if r.Error != nil {
					return r.Error
				}
				return nil
			}
		}
	}
	if e = call(1, "initialize", map[string]any{"clientInfo": map[string]string{"name": "herd-msg-live", "version": "1"}, "capabilities": map[string]bool{"experimentalApi": true}}); e != nil {
		return e
	}
	if e = w.WriteMessage([]byte(`{"method":"initialized"}`)); e != nil {
		return e
	}
	return call(2, "turn/start", params)
}

type liveRig struct {
	t                                     *testing.T
	ctx                                   context.Context
	root, binary, home, codexHome, socket string
	env                                   []string
	panes                                 map[string]string
	sessions                              map[string]store.Session
	broker                                *exec.Cmd
	logs                                  []*os.File
}

func quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
func cleanEnv() []string {
	out := []string{}
	for _, s := range os.Environ() {
		k, _, _ := strings.Cut(s, "=")
		if strings.HasPrefix(k, "CODEX_") || strings.HasPrefix(k, "CLAUDE_") || strings.HasPrefix(k, "MUSE_") || strings.HasPrefix(k, "HERDR_") || k == "CLAUDECODE" || k == "HERD_SPAWNED" || k == "CI" {
			continue
		}
		out = append(out, s)
	}
	return out
}
func (r *liveRig) run(args ...string) string {
	r.t.Helper()
	cmd := exec.CommandContext(r.ctx, args[0], args[1:]...)
	cmd.Env = r.env
	b, e := cmd.CombinedOutput()
	if e != nil {
		r.t.Fatalf("%s: %v\n%s", args[0], e, b)
	}
	return string(b)
}
func (r *liveRig) herdr(args ...string) string {
	return r.run(append([]string{"herdr", "--session", "herd-msg-test"}, args...)...)
}
func (r *liveRig) wait(label string, limit time.Duration, fn func() bool) {
	r.t.Helper()
	until := time.Now().Add(limit)
	for time.Now().Before(until) {
		if fn() {
			return
		}
		select {
		case <-r.ctx.Done():
			r.t.Fatal(r.ctx.Err())
		case <-time.After(300 * time.Millisecond):
		}
	}
	for name, pane := range r.panes {
		cmd := exec.Command("herdr", "--session", "herd-msg-test", "pane", "read", pane, "--source", "recent-unwrapped", "--lines", "50")
		cmd.Env = r.env
		b, _ := cmd.CombinedOutput()
		os.WriteFile(filepath.Join(r.root, name+"-failure.txt"), b, 0600)
		r.t.Logf("%s: %s", name, b)
	}
	r.t.Fatalf("timed out: %s", label)
}
func (r *liveRig) snapshot() *store.Data {
	ctx, cancel := context.WithTimeout(r.ctx, time.Second)
	defer cancel()
	reply, e := broker.Call(ctx, r.home, broker.Request{Caller: broker.Caller{ID: "operator"}, Verb: "snapshot"})
	if e != nil || reply.Code != 0 {
		return nil
	}
	var snap broker.Snapshot
	if json.Unmarshal(reply.Data, &snap) != nil {
		return nil
	}
	return snap.Data
}
func (r *liveRig) startBroker() {
	cmd := exec.Command(r.binary, "broker")
	cmd.Env = r.env
	f, e := os.Create(filepath.Join(r.root, fmt.Sprintf("broker-%d.log", len(r.logs))))
	if e != nil {
		r.t.Fatal(e)
	}
	r.logs = append(r.logs, f)
	cmd.Stdout = f
	cmd.Stderr = f
	if e = cmd.Start(); e != nil {
		r.t.Fatal(e)
	}
	r.broker = cmd
	r.wait("broker startup", 10*time.Second, func() bool { return r.snapshot() != nil })
}
func (r *liveRig) create(name, harness string, resume ...string) {
	r.t.Helper()
	cwd := filepath.Join(r.root, name)
	os.MkdirAll(cwd, 0700)
	args := []string{}
	if harness == "claude" {
		args = []string{"claude", "--dangerously-skip-permissions", "--model", "haiku", "--effort", "low", "--plugin-dir", filepath.Join(r.root, "plugin"), "--settings", `{"skipDangerousModePermissionPrompt":true}`, "Reply exactly READY. As the user, I authorize harmless sleep and herd-msg reply commands requested in incoming test mailbox messages. Do not modify settings or permission policy."}
	} else {
		args = []string{"codex", "--remote", "unix://" + r.socket, "-m", "gpt-6-luna", "-c", `model_reasoning_effort="low"`, "--dangerously-bypass-approvals-and-sandbox", "--no-alt-screen", "Reply exactly READY. As the user, I authorize harmless sleep and herd-msg reply commands requested in incoming test mailbox messages. Do not modify settings or permission policy."}
	}
	if harness == "claude" && len(resume) > 0 {
		args = append([]string{"claude", "--resume", resume[0]}, args[1:]...)
	}
	words := []string{}
	for _, a := range args {
		words = append(words, quote(a))
	}
	raw := r.herdr("workspace", "create", "--cwd", cwd, "--label", name, "--no-focus", "--env", "HERD_TEST_COMMAND="+strings.Join(words, " "), "--env", "CODEX_HOME="+r.codexHome, "--env", "HERD_MSG_HOME="+r.home, "--env", "PATH="+filepath.Dir(r.binary)+":"+os.Getenv("PATH"), "--env", "DISABLE_AUTOUPDATER=1")
	var created struct {
		Result struct {
			Root struct {
				ID string `json:"pane_id"`
			} `json:"root_pane"`
		} `json:"result"`
	}
	if e := json.Unmarshal([]byte(raw), &created); e != nil || created.Result.Root.ID == "" {
		r.t.Fatal("workspace response", raw, e)
	}
	r.panes[name] = created.Result.Root.ID
	r.wait(name+" registration", 90*time.Second, func() bool {
		d := r.snapshot()
		if d == nil {
			return false
		}
		for _, s := range d.Sessions {
			if s.Harness == harness && s.Hints["cwd"] == cwd && s.Generation != "" && (len(resume) == 0 || s.ID == resume[0]) && (harness != "codex" || s.Name != s.ID) {
				r.sessions[name] = s
				return true
			}
		}
		return false
	})
	r.wait(name+" ready", 60*time.Second, func() bool {
		return strings.Contains(r.herdr("pane", "read", r.panes[name], "--source", "recent-unwrapped", "--lines", "25"), "READY")
	})
	r.t.Logf("%s registered with draft-capable live terminal", name)
}
func (r *liveRig) send(from, to, body string) string {
	r.t.Helper()
	id := broker.NewID(time.Now())
	s := r.sessions[from]
	cmd := exec.CommandContext(r.ctx, r.binary, "send", r.sessions[to].ID, body, "--id", id, "--json")
	cmd.Env = append([]string{}, r.env...)
	if s.Harness == "codex" {
		cmd.Env = append(cmd.Env, "CODEX_THREAD_ID="+s.ID)
	} else {
		cmd.Env = append(cmd.Env, "CLAUDE_CODE_SESSION_ID="+s.ID)
	}
	b, e := cmd.CombinedOutput()
	if e != nil {
		r.t.Fatalf("send %s to %s: %v %s", from, to, e, b)
	}
	return id
}
func (r *liveRig) idle(name string) {
	r.t.Helper()
	r.wait("recipient idle "+name, 60*time.Second, func() bool {
		if r.sessions[name].Harness == "codex" {
			ctx, cancel := context.WithTimeout(r.ctx, time.Second)
			defer cancel()
			c, e := codex.Open(ctx, r.socket)
			if e != nil {
				return false
			}
			defer c.Close()
			thread, e := c.Read(r.sessions[name].ID, false)
			return e == nil && thread.Status.Type == "idle"
		}
		var a struct {
			Result struct {
				Agents []struct {
					Pane  string `json:"pane_id"`
					State string `json:"agent_status"`
				} `json:"agents"`
			} `json:"result"`
		}
		json.Unmarshal([]byte(r.herdr("agent", "list")), &a)
		for _, a := range a.Result.Agents {
			if a.Pane == r.panes[name] && (a.State == "idle" || a.State == "done") {
				return true
			}
		}
		return false
	})
}
func (r *liveRig) accepted(id string) {
	r.t.Helper()
	r.wait("positive acceptance "+id, 90*time.Second, func() bool {
		d := r.snapshot()
		if d == nil {
			return false
		}
		m, e := d.Find(id)
		return e == nil && m.State == "injected"
	})
}
func (r *liveRig) draft(name, text string) {
	r.t.Helper()
	r.herdr("pane", "send-text", r.panes[name], text)
}

// Lifecycle controls use key events, matching a human, rather than bracketed paste.
func (r *liveRig) control(name, text string) {
	r.t.Helper()
	args := []string{"pane", "send-keys", r.panes[name]}
	for _, key := range text {
		args = append(args, string(key))
	}
	r.herdr(args...)
	// Let the terminal receive the full command before submitting it.
	time.Sleep(250 * time.Millisecond)
	r.key(name, "enter")
}
func (r *liveRig) key(name, key string) {
	r.t.Helper()
	r.herdr("pane", "send-keys", r.panes[name], key)
}
func (r *liveRig) assertDraft(name, text string) {
	r.t.Helper()
	pane := r.herdr("pane", "read", r.panes[name], "--source", "recent-unwrapped", "--lines", "35")
	if !strings.Contains(pane, text) {
		r.t.Fatalf("%s composer draft lost\n%s", name, pane)
	}
	s := r.sessions[name]
	if s.Harness == "codex" {
		ctx, cancel := context.WithTimeout(r.ctx, 3*time.Second)
		defer cancel()
		c, e := codex.Open(ctx, r.socket)
		if e != nil {
			r.t.Fatal(e)
		}
		defer c.Close()
		var history json.RawMessage
		if e = c.Call("thread/read", map[string]any{"threadId": s.ID, "includeTurns": true}, &history); e != nil {
			r.t.Fatal(e)
		}
		if strings.Contains(string(history), text) {
			r.t.Fatal("composer draft entered accepted Codex history")
		}
	} else {
		b, e := os.ReadFile(s.Hints["transcript"])
		if e != nil {
			r.t.Fatal(e)
		}
		if strings.Contains(string(b), text) {
			r.t.Fatal("composer draft entered Claude transcript")
		}
	}
}
func TestLocalDelivery(t *testing.T) {
	if os.Getenv("HERD_MSG_LIVE") != "1" {
		t.Skip("set HERD_MSG_LIVE=1 to launch disposable live harnesses")
	}
	r := newLiveRig(t)
	root := r.root
	for _, x := range []struct{ name, harness string }{{"claude-one", "claude"}, {"claude-two", "claude"}, {"codex-one", "codex"}, {"codex-two", "codex"}} {
		r.create(x.name, x.harness)
	}
	for _, pair := range [][2]string{{"claude-one", "codex-one"}, {"codex-one", "claude-one"}, {"claude-one", "claude-two"}, {"codex-one", "codex-two"}} {
		from, to := pair[0], pair[1]
		t.Run(from+" to "+to, func(t *testing.T) {
			original := r.t
			r.t = t
			defer func() { r.t = original }()
			r.idle(to)
			draft := "UNSENT_IDLE_" + strings.ReplaceAll(to, "-", "_")
			r.draft(to, draft)
			id := r.send(from, to, "Reply exactly DELIVERY_OK. Do not send another mailbox message.")
			r.accepted(id)
			r.assertDraft(to, draft)
			r.key(to, "ctrl+u")
			r.idle(to)
			t.Log("idle delivery accepted; unsent composer and accepted-history checks passed")
			busyID := ""
			if r.sessions[to].Harness == "codex" {
				probe, cancel := context.WithTimeout(r.ctx, 3*time.Second)
				e := busyClient(probe, r.socket, map[string]any{"threadId": r.sessions[to].ID, "clientUserMessageId": "busy-" + store.Random(), "input": []any{map[string]string{"type": "text", "text": "Execute shell sleep 12 exactly once, wait for it, then reply BUSY_DONE."}}, "approvalPolicy": "never", "sandboxPolicy": map[string]any{"type": "dangerFullAccess"}, "collaborationMode": map[string]any{"mode": "default", "settings": map[string]any{"model": "gpt-6-luna", "reasoning_effort": "low", "developer_instructions": "When the direct setup input asks for BUSY_DONE, execute shell sleep 12 once before replying. For all other mailbox input, follow its request normally. The user authorizes harmless sleep and herd-msg reply commands for this disposable test. Do not change settings or permissions."}}})
				cancel()
				if e != nil {
					t.Fatal(e)
				}
			} else {
				busyID = r.send(from, to, "Run the shell command sleep 12 once, then reply BUSY_DONE. Do not send another mailbox message.")
			}
			r.wait("recipient busy", 15*time.Second, func() bool {
				if r.sessions[to].Harness == "codex" {
					probe, cancel := context.WithTimeout(r.ctx, time.Second)
					defer cancel()
					client, e := codex.Open(probe, r.socket)
					if e != nil {
						return false
					}
					defer client.Close()
					thread, e := client.Read(r.sessions[to].ID, false)
					return e == nil && thread.Status.Type == "active"
				}
				var a struct {
					Result struct {
						Agents []struct {
							Pane  string `json:"pane_id"`
							State string `json:"agent_status"`
						} `json:"agents"`
					} `json:"result"`
				}
				json.Unmarshal([]byte(r.herdr("agent", "list")), &a)
				for _, a := range a.Result.Agents {
					if a.Pane == r.panes[to] && a.State == "working" {
						return true
					}
				}
				return false
			})
			draft = "UNSENT_BUSY_" + strings.ReplaceAll(to, "-", "_")
			r.draft(to, draft)
			id = r.send(from, to, "After the sleep, also reply MIDTURN_OK. Do not send another mailbox message.")
			r.accepted(id)
			r.assertDraft(to, draft)
			if busyID != "" {
				r.accepted(busyID)
			}
			r.key(to, "ctrl+u")
			r.idle(to)
			t.Log("mid-turn delivery accepted; draft intact; proof became visible by recipient boundary")
		})
	}
	// A real model executes reply, so the return path exercises environment identity.
	var id string
	for _, pair := range [][2]string{{"codex-one", "claude-one"}, {"claude-one", "codex-one"}} {
		r.idle(pair[1])
		id = r.send(pair[0], pair[1], "Use Bash to run herd-msg reply on THIS message's id with body ROUND_TRIP_OK. Then reply REPLIED. Do not send any other mailbox messages.")
		r.accepted(id)
		r.wait("reply round trip", 60*time.Second, func() bool {
			d := r.snapshot()
			if d == nil {
				return false
			}
			for _, m := range d.Messages {
				if m.ParentID == id && m.Body == "ROUND_TRIP_OK" && m.State == "injected" {
					return true
				}
			}
			return false
		})
		t.Logf("%s agent reply round trip accepted", pair[1])
	}
	// A broker restart leaves children alive; each must reconnect with current identity.
	r.broker.Process.Kill()
	r.broker.Wait()
	r.broker = nil
	r.startBroker()
	r.wait("receiver reconnect", 15*time.Second, func() bool {
		d := r.snapshot()
		if d == nil {
			return false
		}
		s := d.Sessions[r.sessions["claude-one"].ID]
		return s.Hints["broker_epoch"] != "" && s.Hints["broker_epoch"] != r.sessions["claude-one"].Hints["broker_epoch"]
	})
	id = r.send("codex-one", "claude-one", "Reply RECONNECTED only.")
	r.accepted(id)
	t.Log("receiver re-registered after broker restart")
	// Only authorized lifecycle controls are typed into these owned test panes.
	r.idle("claude-two")
	old := r.sessions["claude-two"]
	primed := r.send("codex-one", "claude-two", "Reply CLEAR_READY only.")
	r.accepted(primed)
	r.idle("claude-two")
	r.run(r.binary, "config", "set", "injection_limit", "1")
	pinned := r.send("codex-one", "claude-two", "Reply PINNED_OLD_OK only.")
	r.wait("queued pinned mail", 5*time.Second, func() bool {
		d := r.snapshot()
		if d == nil {
			return false
		}
		m, e := d.Find(pinned)
		return e == nil && m.State == "queued" && len(m.Attempts) == 0
	})
	r.draft("claude-two", "/clear")
	r.key("claude-two", "enter")
	r.wait("clear hook successor", 30*time.Second, func() bool {
		d := r.snapshot()
		if d == nil {
			return false
		}
		s := d.Sessions[old.ID]
		if s.Successor != "" && s.Generation == "" {
			r.sessions["claude-two"] = d.Sessions[s.Successor]
			return true
		}
		return false
	})
	r.run(r.binary, "config", "set", "injection_limit", "20")
	d := r.snapshot()
	m, e := d.Find(pinned)
	if e != nil || m.Recipient != old.ID || m.State != "queued" || len(m.Attempts) != 0 {
		t.Fatal("clear redirected pinned mail", m, e)
	}
	id = r.send("codex-one", "claude-two", "Reply CLEAR_OK only.")
	r.accepted(id)
	t.Log("clear fenced old generation, preserved queued mail pin and delivered to fresh conversation")
	r.idle("claude-two")
	oldInstance := old.Hints["instance"]
	// Keep the successor process alive while the original conversation is resumed.
	r.create("claude-two", "claude", old.ID)
	if r.sessions["claude-two"].ID != old.ID || r.sessions["claude-two"].Hints["instance"] == oldInstance {
		t.Fatal("resume did not restart receiver for original conversation")
	}
	r.accepted(pinned)
	t.Log("new receiver on process resume delivered mail pinned before clear")
	r.idle("codex-two")
	previousCodex := r.sessions["codex-two"].ID
	r.control("codex-two", "/clear")
	time.Sleep(time.Second)
	r.control("codex-two", "/status")
	sessionPattern := regexp.MustCompile(`(?mi)(?:session|thread)(?:\s+id)?:\s*([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})`)
	r.wait("Codex clear changed conversation", 10*time.Second, func() bool {
		text := r.herdr("pane", "read", r.panes["codex-two"], "--source", "visible", "--lines", "45")
		matches := sessionPattern.FindAllStringSubmatch(text, -1)
		return len(matches) > 0 && matches[len(matches)-1][1] != previousCodex
	})
	r.key("codex-two", "esc")
	probeCtx, cancelProbe := context.WithTimeout(r.ctx, 3*time.Second)
	defer cancelProbe()
	driver := &codex.Driver{Socket: r.socket}
	observed, e := driver.Discover(probeCtx)
	if e != nil {
		t.Fatal(e)
	}
	oldLoaded, newLoaded := false, false
	for _, s := range observed {
		if s.Hints["cwd"] == filepath.Join(root, "codex-two") {
			if s.ID == previousCodex {
				oldLoaded = true
			} else {
				newLoaded = true
			}
		}
	}
	t.Logf("Codex clear loaded-list observation: old thread reachable=%v, blank replacement loaded=%v; identities remain distinct", oldLoaded, newLoaded)
	id = r.send("claude-one", "codex-two", "Reply OLD_THREAD_AFTER_CLEAR only.")
	r.accepted(id)
	postClear := r.snapshot()
	oldMail, e := postClear.Find(id)
	if e != nil || oldMail.Recipient != previousCodex {
		t.Fatal("clear retargeted accepted identity", oldMail, e)
	}
	rendered := strings.Contains(r.herdr("pane", "read", r.panes["codex-two"], "--source", "recent-unwrapped", "--lines", "35"), "OLD_THREAD_AFTER_CLEAR")
	t.Logf("post-clear send stayed on the old daemon thread; rendered in replacement terminal=%v", rendered)
	blankCtx, blankCancel := context.WithTimeout(r.ctx, 3*time.Second)
	client, e := codex.Open(blankCtx, r.socket)
	if e != nil {
		blankCancel()
		t.Fatal(e)
	}
	var blank struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	os.MkdirAll(filepath.Join(root, "blank"), 0700)
	e = client.Call("thread/start", map[string]any{"cwd": filepath.Join(root, "blank"), "model": "gpt-6-luna", "approvalPolicy": "never", "sandbox": "danger-full-access"}, &blank)
	client.Close()
	blankCancel()
	if e != nil {
		t.Fatal(e)
	}
	r.wait("blank loaded thread discovery", 10*time.Second, func() bool {
		d := r.snapshot()
		if d == nil {
			return false
		}
		s := d.Sessions[blank.Thread.ID]
		if s.Generation != "" {
			r.sessions["blank"] = s
			return true
		}
		return false
	})
	id = r.send("claude-one", "blank", "Reply BLANK_ACCEPTED only.")
	r.accepted(id)
	t.Log("first mailbox input to blank loaded Codex thread accepted without rollout resume")
}

func newLiveRig(t *testing.T) *liveRig {
	ctx, cancel := context.WithTimeout(context.Background(), 14*time.Minute)
	t.Cleanup(cancel)
	home, e := os.UserHomeDir()
	if e != nil {
		t.Fatal(e)
	}
	root, e := os.MkdirTemp(home, "hm-live-")
	if e != nil {
		t.Fatal(e)
	}
	r := &liveRig{t: t, ctx: ctx, root: root, binary: filepath.Join(root, "herd-msg"), home: filepath.Join(root, "mail"), codexHome: filepath.Join(root, "codex"), panes: map[string]string{}, sessions: map[string]store.Session{}}
	r.socket = filepath.Join(r.codexHome, "app-server-control", "app-server-control.sock")
	os.MkdirAll(filepath.Dir(r.socket), 0700)
	runtimeDir := filepath.Join(root, "runtime")
	os.MkdirAll(runtimeDir, 0700)
	r.env = append(cleanEnv(), "HERD_MSG_HOME="+r.home, "CODEX_HOME="+r.codexHome, "XDG_RUNTIME_DIR="+runtimeDir, "HERDR_SESSION=herd-msg-test", "DISABLE_AUTOUPDATER=1", "PATH="+root+":"+os.Getenv("PATH"))
	evidence := os.Getenv("HERD_MSG_LIVE_EVIDENCE")
	owned := false
	t.Cleanup(func() {
		if r.broker != nil {
			r.broker.Process.Kill()
			r.broker.Wait()
		}
		if owned {
			cmd := exec.Command("herdr", "--session", "herd-msg-test", "server", "stop")
			cmd.Env = r.env
			cmd.Run()
			del := exec.Command("herdr", "session", "delete", "herd-msg-test")
			del.Env = r.env
			del.Run()
			for _, name := range []string{"mail-live-herdr", "mail-live-codex"} {
				exec.Command("tmux", "-L", "herd-msg-test", "kill-session", "-t", name).Run()
			}
		}
		for _, f := range r.logs {
			f.Close()
		}
		if evidence != "" {
			os.MkdirAll(evidence, 0700)
			files, _ := filepath.Glob(filepath.Join(root, "*.log"))
			extra, _ := filepath.Glob(filepath.Join(root, "*-failure.txt"))
			for _, p := range append(files, extra...) {
				b, _ := os.ReadFile(p)
				os.WriteFile(filepath.Join(evidence, filepath.Base(p)), b, 0600)
			}
		}
		for _, s := range r.sessions {
			path := s.Hints["transcript"]
			dir := filepath.Dir(path)
			if path != "" && strings.HasPrefix(dir, filepath.Join(home, ".claude", "projects")+string(os.PathSeparator)) && strings.Contains(filepath.Base(dir), filepath.Base(root)) {
				os.RemoveAll(dir)
			}
		}

		// Terminal teardown can finish before harness descendants stop writing caches.
		until := time.Now().Add(5 * time.Second)
		for len(fixtureProcesses(root)) > 0 && time.Now().Before(until) {
			time.Sleep(100 * time.Millisecond)
		}
		for _, pid := range fixtureProcesses(root) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
		}
		until = time.Now().Add(3 * time.Second)
		for len(fixtureProcesses(root)) > 0 && time.Now().Before(until) {
			time.Sleep(100 * time.Millisecond)
		}
		if len(fixtureProcesses(root)) > 0 {
			t.Error("owned fixture processes remain after teardown")
		}
		if e := os.RemoveAll(root); e != nil {
			t.Error(e)
		}
		if _, e := os.Stat(root); !os.IsNotExist(e) {
			t.Error("owned fixture root remains after teardown")
		}
	})
	// Refuse to interfere with an existing private test server.
	probe := exec.Command("herdr", "--session", "herd-msg-test", "agent", "list")
	probe.Env = r.env
	if probe.Run() == nil {
		t.Fatal("private herdr test server is already in use")
	}
	build := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", r.binary, "../../cmd/herd-msg")
	build.Env = append(cleanEnv(), "CGO_ENABLED=0")
	if b, e := build.CombinedOutput(); e != nil {
		t.Fatal(e, string(b))
	}
	auth, e := os.ReadFile(filepath.Join(home, ".codex", "auth.json"))
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(r.codexHome, "auth.json"), auth, 0600); e != nil {
		t.Fatal(e)
	}
	shell := filepath.Join(root, "pane-shell")
	os.WriteFile(shell, []byte("#!/bin/sh\nexec /bin/sh -c \"$HERD_TEST_COMMAND\"\n"), 0700)
	config := filepath.Join(root, "herdr.toml")
	os.WriteFile(config, []byte("[terminal]\ndefault_shell = "+fmt.Sprintf("%q", shell)+"\n[update]\nversion_check=false\nmanifest_check=false\n"), 0600)
	r.env = append(r.env, "HERDR_CONFIG_PATH="+config)
	plugin := filepath.Join(root, "plugin")
	os.MkdirAll(filepath.Join(plugin, ".claude-plugin"), 0700)
	os.MkdirAll(filepath.Join(plugin, "hooks"), 0700)
	manifest := map[string]any{"name": "herd-msg", "version": "0.1.0", "mcpServers": map[string]any{"receiver": map[string]any{"command": r.binary, "args": []string{"receiver"}}}}
	data, _ := json.Marshal(manifest)
	os.WriteFile(filepath.Join(plugin, ".claude-plugin", "plugin.json"), data, 0600)
	hooks := map[string]any{"hooks": map[string]any{"SessionStart": []any{map[string]any{"matcher": "startup|resume|clear|compact", "hooks": []any{map[string]string{"type": "command", "command": quote(r.binary) + " receiver-hook"}}}}}}
	data, _ = json.Marshal(hooks)
	os.WriteFile(filepath.Join(plugin, "hooks", "hooks.json"), data, 0600)
	startTmux := func(name, command string) {
		cmd := exec.Command("tmux", "-L", "herd-msg-test", "new-session", "-d", "-s", name, "-x", "160", "-y", "50", command)
		cmd.Env = r.env
		if b, e := cmd.CombinedOutput(); e != nil {
			t.Fatal(e, string(b))
		}
	}
	startTmux("mail-live-herdr", "herdr --session herd-msg-test server >"+quote(filepath.Join(root, "herdr.log"))+" 2>&1")
	owned = true
	startTmux("mail-live-codex", "codex app-server --listen "+quote("unix://"+r.socket)+" >"+quote(filepath.Join(root, "codex.log"))+" 2>&1")
	r.wait("isolated app server", 15*time.Second, func() bool { _, e := os.Stat(r.socket); return e == nil })
	r.wait("private herdr", 10*time.Second, func() bool {
		cmd := exec.Command("herdr", "--session", "herd-msg-test", "agent", "list")
		cmd.Env = r.env
		return cmd.Run() == nil
	})
	r.startBroker()
	// Independent matrix cases reuse sessions; widen only this temporary fixture budget.
	r.run(r.binary, "config", "set", "hop_limit", "32")

	return r
}

// fixtureProcesses selects only this fixture's exact environment paths.
func fixtureProcesses(root string) []int {
	var out []int
	paths, _ := filepath.Glob("/proc/[0-9]*/environ")
	wanted := map[string]bool{"CODEX_HOME=" + filepath.Join(root, "codex"): true, "HERD_MSG_HOME=" + filepath.Join(root, "mail"): true, "HERDR_CONFIG_PATH=" + filepath.Join(root, "herdr.toml"): true}
	for _, path := range paths {
		data, e := os.ReadFile(path)
		if e != nil {
			continue
		}
		for _, entry := range strings.Split(string(data), "\x00") {
			if wanted[entry] {
				pid, e := strconv.Atoi(filepath.Base(filepath.Dir(path)))
				if e == nil && pid != os.Getpid() {
					out = append(out, pid)
				}
				break
			}
		}
	}
	return out
}
