//go:build live

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func TestCrossMachine(t *testing.T) {
	label := os.Getenv("HERD_MSG_LIVE_MACHINE")
	if label == "" {
		t.Skip("set HERD_MSG_LIVE_MACHINE to a saved herdr machine label for isolated SSH delivery")
	}
	machines, e := identity.Machines(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	saved, e := identity.FindMachine(machines, label)
	if e != nil {
		t.Fatal(e)
	}
	r := newLiveRig(t)
	r.create("codex-one", "codex")
	remoteRun := func(args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		words := make([]string, len(args))
		for i, a := range args {
			words[i] = quote(a)
		}
		cmd := exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=8", saved.Target, strings.Join(words, " "))
		cmd.WaitDelay = time.Second
		return cmd.CombinedOutput()
	}
	mustRemote := func(args ...string) string {
		t.Helper()
		data, e := remoteRun(args...)
		if e != nil {
			t.Fatalf("remote fixture command failed: %v: %s", e, data)
		}
		return string(data)
	}
	// Choose an existing trusted ancestor without changing account-level trust.
	bootstrap := `import os,json,tempfile
home=os.path.expanduser('~')
with open(os.path.join(home,'.claude.json')) as f: settings=json.load(f)
parents=[p for p,v in settings.get('projects',{}).items() if v.get('hasTrustDialogAccepted') and os.path.isdir(p) and os.path.commonpath([home,os.path.realpath(p)])==home]
if not parents: raise RuntimeError('no trusted home project for disposable live fixture')
print(tempfile.mkdtemp(prefix='hm-cross-',dir=min(parents,key=len)))`
	raw, e := exec.Command("ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", saved.Target, "python3 -c "+quote(bootstrap)).Output()
	if e != nil {
		t.Fatal(e)
	}
	root := strings.TrimSpace(string(raw))
	if !filepath.IsAbs(root) || !strings.HasPrefix(filepath.Base(root), "hm-cross-") || strings.ContainsAny(root, "\r\n") {
		t.Fatal("invalid temporary remote root")
	}
	remoteBinary := filepath.Join(root, "herd-msg")
	remoteHome := filepath.Join(root, "mail")
	remoteEnv := []string{"env", "-u", "CODEX_THREAD_ID", "-u", "CLAUDE_CODE_SESSION_ID", "-u", "CLAUDECODE", "HERD_MSG_HOME=" + remoteHome, "CODEX_HOME=" + filepath.Join(root, "codex"), "HERDR_SESSION=herd-msg-cross", "HERDR_CONFIG_PATH=" + filepath.Join(root, "herdr.toml"), "DISABLE_AUTOUPDATER=1"}
	remote := func(args ...string) string { return mustRemote(append(append([]string{}, remoteEnv...), args...)...) }
	ownedServer := false
	ownedTmux := []string{}
	t.Cleanup(func() {
		if ownedServer {
			remoteRun(append(append([]string{}, remoteEnv...), "herdr", "--session", "herd-msg-cross", "server", "stop")...)
			remoteRun("herdr", "session", "delete", "herd-msg-cross")
		}
		for _, name := range ownedTmux {
			remoteRun("tmux", "-L", "herd-msg-test", "kill-session", "-t", name)
		}
		// Remove only fixture-owned process roots and matching transcript directories.
		script := `import os,sys,shutil,glob
root=sys.argv[1]
assert os.path.isabs(root) and os.path.basename(root).startswith('hm-cross-')
for p in glob.glob(os.path.expanduser('~/.claude/projects/*'+os.path.basename(root)+'*')):
 shutil.rmtree(p)
shutil.rmtree(root)
assert not os.path.exists(root)
leaks=[]
for path in glob.glob('/proc/[0-9]*/cmdline'):
 try:
  data=open(path,'rb').read()
  if root.encode() in data and b'python3' not in data and b'sshd:' not in data: leaks.append(path)
 except OSError: pass
if leaks: raise RuntimeError('owned processes remain: '+str(leaks))
print('remote fixture root, processes and transcripts removed')`
		out, err := remoteRun("python3", "-c", script, root)
		if err != nil {
			t.Errorf("remote cleanup failed: %v %s", err, out)
		} else {
			t.Log(strings.TrimSpace(string(out)))
		}
	})
	copyFile := func(local, destination string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "scp", "-O", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", local, saved.Target+":"+quote(destination))
		cmd.WaitDelay = time.Second
		if data, e := cmd.CombinedOutput(); e != nil {
			t.Fatal("temporary bootstrap copy failed", e, string(data))
		}
	}
	copyFile(r.binary, remoteBinary)
	writeRemote := func(name string, data []byte) {
		local := filepath.Join(r.root, "remote-"+filepath.Base(name))
		if e := os.WriteFile(local, data, 0600); e != nil {
			t.Fatal(e)
		}
		copyFile(local, filepath.Join(root, name))
	}
	writeRemote("pane-shell", []byte("#!/bin/sh\nexec /bin/sh -c \"$HERD_TEST_COMMAND\"\n"))
	remote("chmod", "0700", filepath.Join(root, "pane-shell"), remoteBinary)
	writeRemote("herdr.toml", []byte("[terminal]\ndefault_shell = "+fmt.Sprintf("%q", filepath.Join(root, "pane-shell"))+"\n[update]\nversion_check=false\nmanifest_check=false\n"))
	mcp, _ := json.Marshal(map[string]any{"mcpServers": map[string]any{"receiver": map[string]any{"command": remoteBinary, "args": []string{"receiver"}}}})
	writeRemote("receiver-config.json", mcp)
	remote("mkdir", "-p", filepath.Join(root, "codex"), filepath.Join(root, "claude"))
	if _, e := remoteRun(append(append([]string{}, remoteEnv...), "herdr", "--session", "herd-msg-cross", "agent", "list")...); e == nil {
		t.Fatal("remote private herdr server is already in use")
	}
	startTmux := func(name string, args ...string) {
		words := []string{}
		for _, a := range append(append([]string{}, remoteEnv...), args...) {
			words = append(words, quote(a))
		}
		remote("tmux", "-L", "herd-msg-test", "new-session", "-d", "-s", name, "-x", "160", "-y", "50", strings.Join(words, " "))
		ownedTmux = append(ownedTmux, name)
	}
	startTmux("mail-cross-herdr", "herdr", "--session", "herd-msg-cross", "server")
	ownedServer = true
	remote("sleep", "1")
	startTmux("mail-cross-broker", remoteBinary, "broker")
	var remoteSnap broker.Snapshot
	snapshotRemote := func() bool {
		data, e := remoteRun(append(append([]string{}, remoteEnv...), remoteBinary, "status", "--json")...)
		if e != nil {
			return false
		}
		var status struct {
			Machine string `json:"machine"`
			Health  []struct {
				Check string `json:"check"`
			} `json:"health"`
		}
		if json.Unmarshal(data, &status) != nil || status.Machine == "" {
			return false
		}
		for _, h := range status.Health {
			if h.Check == "broker" {
				return false
			}
		}
		return true
	}
	r.wait("remote temporary broker", 15*time.Second, snapshotRemote)
	hooks := map[string]any{"skipDangerousModePermissionPrompt": true, "hooks": map[string]any{"SessionStart": []any{map[string]any{"matcher": "startup|resume|clear|compact", "hooks": []any{map[string]string{"type": "command", "command": quote(remoteBinary) + " receiver-hook"}}}}}}
	settings, _ := json.Marshal(hooks)
	args := []string{"claude", "--dangerously-skip-permissions", "--model", "haiku", "--effort", "low", "--mcp-config", filepath.Join(root, "receiver-config.json"), "--strict-mcp-config", "--settings", string(settings), "Reply exactly CROSS_READY. The user authorizes harmless herd-msg reply commands requested in test mailbox messages. Do not change settings or permission policy."}
	words := []string{}
	for _, a := range args {
		words = append(words, quote(a))
	}
	created := remote("herdr", "--session", "herd-msg-cross", "workspace", "create", "--cwd", filepath.Join(root, "claude"), "--label", "cross-claude", "--no-focus", "--env", "HERD_TEST_COMMAND="+strings.Join(words, " "), "--env", "HERD_MSG_HOME="+remoteHome, "--env", "PATH="+root+":/usr/local/bin:/usr/bin:/bin:"+strings.TrimSpace(mustRemote("sh", "-c", "printf '%s' \"$PATH\"")), "--env", "DISABLE_AUTOUPDATER=1")
	var pane struct {
		Result struct {
			Root struct {
				ID string `json:"pane_id"`
			} `json:"root_pane"`
		} `json:"result"`
	}
	if e = json.Unmarshal([]byte(created), &pane); e != nil || pane.Result.Root.ID == "" {
		t.Fatal("remote pane creation failed", e)
	}
	readRemote := func() string {
		return remote("herdr", "--session", "herd-msg-cross", "pane", "read", pane.Result.Root.ID, "--source", "recent-unwrapped", "--lines", "35")
	}
	r.wait("remote Claude ready", 60*time.Second, func() bool { text := readRemote(); return strings.Contains(text, "CROSS_READY") })
	// Use the private socket RPC through a read-only fixture helper, never histories outside this home.
	getRemote := func() bool {
		data, e := remoteRun(append(append([]string{}, remoteEnv...), remoteBinary, "peers", "--json")...)
		if e != nil {
			return false
		}
		var sessions []store.Session
		if json.Unmarshal(data, &sessions) != nil {
			return false
		}
		for _, s := range sessions {
			if s.Harness == "claude" && s.Machine != "" {
				remoteSnap.Config.Machine = s.Machine
				remoteSnap.Data = store.Empty()
				remoteSnap.Data.Sessions[s.ID] = s
				return true
			}
		}
		return false
	}
	r.wait("remote Claude receiver registration", 30*time.Second, getRemote)
	var recipient store.Session
	for _, s := range remoteSnap.Data.Sessions {
		recipient = s
	}
	// Explicit bootstrap options apply only to these disposable homes.
	linkOutput := r.run(r.binary, "link", label, "--remote-path", remoteBinary, "--remote-home", remoteHome, "--auto-run", "--json")
	var direction struct {
		ReverseError string `json:"reverse_error"`
	}
	if json.Unmarshal([]byte(linkOutput), &direction) != nil {
		t.Fatal("invalid link result")
	}
	if direction.ReverseError != "" {
		t.Log("real link negotiated one-way SSH with polling")
	} else {
		t.Log("real link negotiated two reachable directions")
	}
	remote(remoteBinary, "link", r.snapshot().Sessions[r.sessions["codex-one"].ID].Machine, "--auto-run", "--json")
	r.wait("cross-machine peer cache", 40*time.Second, func() bool { data := r.run(r.binary, "peers", "--json"); return strings.Contains(data, recipient.ID) })
	localMachine := r.sessions["codex-one"].Machine
	if localMachine == "" {
		var info struct {
			Machine string `json:"machine"`
		}
		json.Unmarshal([]byte(r.run(r.binary, "status", "--json")), &info)
		localMachine = info.Machine
	}
	id := broker.NewID(time.Now())
	send := exec.CommandContext(r.ctx, r.binary, "send", recipient.ID+"@"+recipient.Machine, "Use Bash to run "+quote(remoteBinary)+" reply on THIS message's id with body CROSS_MACHINE_REPLY. Then reply CROSS_REPLIED. Do not send any other mailbox message.", "--id", id, "--json")
	send.Env = append(append([]string{}, r.env...), "CODEX_THREAD_ID="+r.sessions["codex-one"].ID)
	if output, e := send.CombinedOutput(); e != nil {
		t.Fatal(e, string(output))
	}
	r.wait("real remote Claude reply accepted by local Codex", 120*time.Second, func() bool {
		d := r.snapshot()
		if d == nil {
			return false
		}
		for _, m := range d.Messages {
			if m.ParentID == id && m.Body == "CROSS_MACHINE_REPLY" && m.State == "injected" && m.Destination == localMachine {
				return true
			}
		}
		return false
	})
	t.Log("local attached Codex to remote Claude and real reply accepted; strict SSH, temporary homes and receiving-only grants verified")
	t.Log("remote machine and session names redacted; return transfer uses commit-before-ack and receiving-side policy")
}
