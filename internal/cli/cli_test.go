package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func cli(t *testing.T, args ...string) (int, string) {
	t.Helper()
	var out, err bytes.Buffer
	code := Run(context.Background(), args, &out, &err, "test")
	if !json.Valid(bytes.TrimSpace(out.Bytes())) {
		t.Fatalf("invalid JSON from %v: %s stderr %s", args, out.String(), err.String())
	}
	return code, out.String()
}
func TestOfflineCommandsAndIdentity(t *testing.T) {
	home := filepath.Join(t.TempDir(), "missing")
	t.Setenv("HERD_MSG_HOME", home)
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	if code, text := cli(t, "status", "--json"); code != 0 || !strings.Contains(text, `"check":"broker"`) {
		t.Fatal(code, text)
	}
	if _, e := os.Stat(home); !os.IsNotExist(e) {
		t.Fatal("offline status created home")
	}
	if code, text := cli(t, "send", "receiver", "hello", "--json"); code != 4 || !strings.Contains(text, `"id":`) {
		t.Fatal(code, text)
	}
	if code, text := cli(t, "kill", "on", "--json"); code != 0 {
		t.Fatal(code, text)
	}
	c, e := policy.Read(filepath.Join(home, "config.json"))
	if e != nil || !c.Killed {
		t.Fatal(c, e)
	}
	if code, text := cli(t, "config", "set", "hop_limit", "5", "--json"); code != 0 {
		t.Fatal(code, text)
	}
	if code, text := cli(t, "config", "set", "dedupe_seconds", "1", "--json"); code != 1 {
		t.Fatal(code, text)
	}
	t.Setenv("CODEX_THREAD_ID", "agent")
	if code, text := cli(t, "kill", "off", "--json"); code != 2 {
		t.Fatal(code, text)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "other")
	if code, text := cli(t, "status", "--json"); code != 1 || !strings.Contains(text, "both harness") {
		t.Fatal(code, text)
	}
}

func TestMuseCallerIdentityAndInheritedConflicts(t *testing.T) {
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	t.Setenv("MUSE_SESSION_ID", "muse-session")
	c, e := identity()
	if e != nil || c.ID != "muse-session" || c.Harness != "muse" || c.Operator() {
		t.Fatal(c, e)
	}
	t.Setenv("HERD_MSG_HOME", t.TempDir())
	if code, _ := cli(t, "kill", "on", "--json"); code != 2 {
		t.Fatal("Muse caller obtained operator authority", code)
	}
	for _, variable := range []string{"CODEX_THREAD_ID", "CLAUDE_CODE_SESSION_ID"} {
		t.Setenv(variable, "inherited")
		if _, e := identity(); e == nil {
			t.Fatal("accepted conflicting identity", variable)
		}
		t.Setenv(variable, "")
	}
}
func TestSocketCommandsAndOfflineWAL(t *testing.T) {
	home := t.TempDir()
	runtimeDir := t.TempDir()
	t.Setenv("HERD_MSG_HOME", home)
	t.Setenv("XDG_RUNTIME_DIR", runtimeDir)
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	b, e := broker.New(home, "", map[string]broker.Driver{})
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	if e = b.Register(context.Background(), store.Session{ID: "receiver", Name: "reviewer", Harness: "claude", State: "offline"}); e != nil {
		t.Fatal(e)
	}
	server, e := b.Listen()
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx) }()
	defer server.Close()
	infoBytes, _ := os.ReadFile(filepath.Join(home, "socket.json"))
	var info broker.SocketInfo
	json.Unmarshal(infoBytes, &info)
	stat, e := os.Stat(info.Path)
	if e != nil || stat.Mode().Perm() != 0600 {
		t.Fatal(stat, e)
	}
	// Discovery follows the canonical home file even with a different runtime environment.
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(t.TempDir(), "other-runtime"))
	created := time.Now().UTC().Format(time.RFC3339Nano)
	if code, text := cli(t, "send", "reviewer", "hello", "--id", "frozen", "--created", created, "--json"); code != 0 || !strings.Contains(text, `"state":"queued"`) {
		t.Fatal(code, text)
	}
	if code, text := cli(t, "send", "reviewer", "hello", "--id", "frozen", "--created", created, "--json"); code != 0 {
		t.Fatal(code, text)
	}
	for _, verb := range []string{"read", "show"} {
		if code, text := cli(t, verb, "frozen", "--json"); code != 0 || !strings.Contains(text, `"body":"hello"`) || !strings.Contains(text, `"history"`) {
			t.Fatal(code, text)
		}
	}
	if code, text := cli(t, "log", "--json"); code != 0 || !strings.Contains(text, "frozen") {
		t.Fatal(code, text)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "receiver")
	if code, text := cli(t, "inbox", "--json"); code != 0 || !strings.Contains(text, "frozen") {
		t.Fatal(code, text)
	}
	if code, text := cli(t, "allow", "--session", "receiver", "off", "--json"); code != 0 {
		t.Fatal(code, text)
	}
	if code, text := cli(t, "resolve", "frozen", "drop", "--json"); code != 2 {
		t.Fatal(code, text)
	}
	if code, text := cli(t, "reply", "frozen", "reply", "--json"); code != 1 || !strings.Contains(text, "operator messages") {
		t.Fatal(code, text)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	if code, text := cli(t, "resolve", "frozen", "drop", "--json"); code != 0 {
		t.Fatal(code, text)
	}
	// A hot WAL read must preserve database bytes, WAL frames and sidecar sizes.
	before := fileState(t, home)
	cancel()
	if e = <-serveDone; e != nil {
		t.Fatal(e)
	}
	if code, text := cli(t, "status", "--json"); code != 0 || !strings.Contains(text, `"state":"offline"`) {
		t.Fatal(code, text)
	}
	after := fileState(t, home)
	if before != after {
		t.Fatal("offline read modified database or WAL", before, after)
	}
	raw, e := broker.Call(context.Background(), home, broker.Request{Version: broker.Protocol, Verb: "snapshot", Caller: broker.Caller{ID: "operator"}})
	if e == nil {
		t.Fatal("dead socket appeared online", raw)
	}
}

type files struct {
	db, wal                  [32]byte
	dbSize, walSize, shmSize int64
}

func fileState(t *testing.T, home string) files {
	t.Helper()
	var out files
	for i, suffix := range []string{"", "-wal", "-shm"} {
		path := filepath.Join(home, "mailbox.db") + suffix
		b, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		switch i {
		case 0:
			out.db = sha256.Sum256(b)
			out.dbSize = int64(len(b))
		case 1:
			out.wal = sha256.Sum256(b)
			out.walSize = int64(len(b))
		case 2:
			out.shmSize = int64(len(b))
		}
	}
	return out
}
func TestWatchContinuesOfflineAndCancels(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HERD_MSG_HOME", home)
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	ctx, cancel := context.WithCancel(context.Background())
	reader, writer, pipeErr := os.Pipe()
	if pipeErr != nil {
		t.Fatal(pipeErr)
	}
	defer reader.Close()
	defer writer.Close()
	done := make(chan int, 1)
	go func() { done <- Run(ctx, []string{"watch", "--json"}, writer, writer, "test") }()
	dec := json.NewDecoder(reader)
	for i := 0; i < 2; i++ {
		var status Status
		if e := dec.Decode(&status); e != nil {
			t.Fatal(e)
		}
		if status.Schema != 1 || len(status.Health) != 1 {
			t.Fatal(status)
		}
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatal(code)
		}
	case <-time.After(time.Second):
		t.Fatal("watch did not cancel")
	}
}

func TestHelpInMessageBodyReachesBroker(t *testing.T) {
	t.Setenv("HERD_MSG_HOME", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	for _, body := range [][]string{{"help"}, {"please", "help", "me"}, {"--", "--help"}} {
		args := append([]string{"--json", "send", "recipient"}, body...)
		if code, text := cli(t, args...); code != 4 || !strings.Contains(text, "broker not running") {
			t.Fatal(code, text)
		}
	}
}

func TestInstallCommandsRequireOperatorBeforeMutating(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("HERD_MSG_HOME", t.TempDir())
	t.Setenv("CODEX_THREAD_ID", "test-agent")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	for _, verb := range []string{"setup", "uninstall"} {
		if code, text := cli(t, verb, "--json"); code != 2 || !strings.Contains(text, "operator identity") {
			t.Fatal(verb, code, text)
		}
	}
}

func TestBrokerStartupHealthPublishesFailuresAndNotifications(t *testing.T) {
	t.Setenv("HERD_MSG_HOME", "")
	home := t.TempDir()
	bin := filepath.Join(home, "bin")
	if e := os.Mkdir(bin, 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("HOME", home)
	t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, "claude"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(home, "data"))
	for name, body := range map[string]string{"systemctl": "exit 0", "loginctl": "echo yes", "herdr": "case \"$1\" in --version) echo 'herdr 0.9.3';; esac", "claude": "echo '[]'", "bash": "echo forbidden >> \"$HOME/interactive-shell\"; exit 1"} {
		if e := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+body+"\n"), 0700); e != nil {
			t.Fatal(e)
		}
	}
	t.Setenv("PATH", bin+":/usr/bin:/bin")
	mailbox := filepath.Join(home, "mailbox")
	b, e := broker.New(mailbox, filepath.Join(home, "config.json"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	var errout bytes.Buffer
	c := command{home: mailbox, configPath: b.ConfigPath, err: &errout}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); c.monitor(ctx, b) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("health worker did not stop")
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		d, _, e := b.Data(ctx)
		_, notice := d.Health["probe/codex-socket"]
		_, fileErr := os.Stat(filepath.Join(mailbox, "notifications.json"))
		if e == nil && notice && fileErr == nil {
			if _, err := os.Stat(filepath.Join(home, "interactive-shell")); !os.IsNotExist(err) {
				t.Fatal("broker opened interactive shell", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("startup health did not reach durable health and notifier")
}

func TestIsolatedBrokerSkipsAccountChecksAndNotifications(t *testing.T) {
	home := t.TempDir()
	mailbox := filepath.Join(home, "mailbox")
	t.Setenv("HOME", home)
	t.Setenv("HERD_MSG_HOME", mailbox)
	t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))
	bin := filepath.Join(home, "bin")
	if e := os.Mkdir(bin, 0700); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"herdr", "systemctl", "claude", "loginctl", "bash"} {
		if e := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\necho called >> \"$HOME/account-checks\"\nexit 1\n"), 0700); e != nil {
			t.Fatal(e)
		}
	}
	t.Setenv("PATH", bin+":/usr/bin:/bin")
	b, e := broker.New(mailbox, filepath.Join(mailbox, "config.json"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	var errout bytes.Buffer
	c := command{home: mailbox, configPath: b.ConfigPath, err: &errout}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.monitor(ctx, b) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(2 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		d, _, e := b.Data(ctx)
		if e == nil {
			if _, ok := d.Health["probe/codex-socket"]; ok {
				ready = true
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("isolated socket probe did not run")
	}
	cancel()
	<-done
	for _, path := range []string{filepath.Join(home, "account-checks"), filepath.Join(mailbox, "notifications.json")} {
		if _, e := os.Stat(path); !os.IsNotExist(e) {
			t.Fatal("isolated broker touched account integration", path, e)
		}
	}
}

func TestInvalidConfigStillReachesNotificationMonitor(t *testing.T) {
	home := t.TempDir()
	mailbox := filepath.Join(home, "mailbox")
	t.Setenv("HERD_MSG_HOME", "")
	t.Setenv("HOME", home)
	t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, "claude"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, "config"))
	bin := filepath.Join(home, "bin")
	if e := os.Mkdir(bin, 0700); e != nil {
		t.Fatal(e)
	}
	for name, body := range map[string]string{"herdr": `if test "$1" = --version; then echo 'herdr 0.9.3'; else printf '%s\n' "$*" >> "$HOME/alerts"; fi`, "systemctl": "exit 0", "loginctl": "echo yes", "claude": "echo '[]'", "bash": "echo 'codex-cli 0.166.0'"} {
		if e := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+body+"\n"), 0700); e != nil {
			t.Fatal(e)
		}
	}
	t.Setenv("PATH", bin+":/usr/bin:/bin")
	config := filepath.Join(home, "config.json")
	b, e := broker.New(mailbox, config, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	if e = policy.Write(config, policy.Defaults(), nil); e != nil {
		t.Fatal(e)
	}
	var errout bytes.Buffer
	c := command{home: mailbox, configPath: config, err: &errout}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.monitorInterval(ctx, b, 20*time.Millisecond) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, e = os.Stat(filepath.Join(mailbox, "notifications.json")); e == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if e = os.WriteFile(config, []byte("{"), 0600); e != nil {
		t.Fatal(e)
	}
	for time.Now().Before(deadline) {
		data, _ := os.ReadFile(filepath.Join(home, "alerts"))
		if strings.Contains(string(data), "invalid config") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("invalid config suppressed its own notification")
}

func TestBadConfigurationAtBrokerStartupNotifiesOnce(t *testing.T) {
	for _, bad := range []string{"config", "peers"} {
		t.Run(bad, func(t *testing.T) {
			home := t.TempDir()
			mailbox := filepath.Join(home, "mailbox")
			config := filepath.Join(home, "config/config.json")
			t.Setenv("HERD_MSG_HOME", "")
			t.Setenv("HOME", home)
			t.Setenv("CODEX_THREAD_ID", "")
			t.Setenv("CLAUDE_CODE_SESSION_ID", "")
			if e := os.MkdirAll(filepath.Dir(config), 0700); e != nil {
				t.Fatal(e)
			}
			if e := os.MkdirAll(mailbox, 0700); e != nil {
				t.Fatal(e)
			}
			if bad == "config" {
				if e := os.WriteFile(config, []byte("{"), 0600); e != nil {
					t.Fatal(e)
				}
			} else {
				if e := policy.Write(config, policy.Defaults(), nil); e != nil {
					t.Fatal(e)
				}
				if e := os.WriteFile(filepath.Join(mailbox, "peers.json"), []byte("{}"), 0644); e != nil {
					t.Fatal(e)
				}
			}
			bin := filepath.Join(home, "bin")
			if e := os.Mkdir(bin, 0700); e != nil {
				t.Fatal(e)
			}
			script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$HOME/alerts\"\n"
			if e := os.WriteFile(filepath.Join(bin, "herdr"), []byte(script), 0700); e != nil {
				t.Fatal(e)
			}
			t.Setenv("PATH", bin+":/usr/bin:/bin")
			var errout bytes.Buffer
			c := command{ctx: context.Background(), home: mailbox, configPath: config, caller: broker.Caller{ID: "operator"}, err: &errout}
			for i := 0; i < 2; i++ {
				if e := c.runBroker(); e == nil {
					t.Fatal("bad startup configuration was accepted")
				}
			}
			data, e := os.ReadFile(filepath.Join(home, "alerts"))
			if e != nil {
				t.Fatal(e)
			}
			if strings.Count(string(data), "notification show") != 1 {
				t.Fatal("startup failure did not notify exactly once", string(data))
			}
			if bad == "peers" && !strings.Contains(string(data), "chmod 600") {
				t.Fatal("wrong private-file repair", string(data))
			}
		})
	}
}

func TestPeersRejectsUnknownArgumentsAndShowsEveryoneWithoutBroker(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HERD_MSG_HOME", home)
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	s, e := store.Open(home)
	if e != nil {
		t.Fatal(e)
	}
	e = s.Update(context.Background(), func(d *store.Data) error {
		d.Sessions["ended-session"] = store.Session{ID: "ended-session", Name: "ended", Harness: "claude", State: "dead"}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if code, text := cli(t, "peers", "--everyone", "--json"); code == 0 || !strings.Contains(text, "--all") {
		t.Fatal(code, text)
	}
	// With the broker down, the plain listing must not hide saved peers.
	for _, args := range [][]string{{"peers", "--json"}, {"peers", "--all", "--json"}} {
		if code, text := cli(t, args...); code != 0 || !strings.Contains(text, "ended-session") {
			t.Fatal(args, code, text)
		}
	}
}
