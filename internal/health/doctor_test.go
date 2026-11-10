package health

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/setup"
	"github.com/JeremiahChurch/herd-msg/internal/store"
	"github.com/JeremiahChurch/herd-msg/internal/transport"
)

func TestVersionsRejectOldUnknownAndPrerelease(t *testing.T) {
	for _, tc := range []struct {
		v, min string
		ok     bool
	}{{"0.157.1", MinCodex, true}, {"0.157.0", MinCodex, false}, {"0.9.2", MinHerdr, false}, {"0.9.3", MinHerdr, true}, {"0.166.0", MinCodex, true}, {"0.166.0-alpha", MinCodex, false}, {"garbage", MinCodex, false}} {
		if got := AtLeast(tc.v, tc.min); got != tc.ok {
			t.Errorf("%s >= %s: %v", tc.v, tc.min, got)
		}
	}
}
func TestDoctorReportsIndependentFailuresWithRepairs(t *testing.T) {
	home := t.TempDir()
	bin := filepath.Join(home, "bin")
	if e := os.Mkdir(bin, 0700); e != nil {
		t.Fatal(e)
	}
	for _, key := range []string{"HERD_MSG_HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "CODEX_HOME", "CLAUDE_CONFIG_DIR"} {
		t.Setenv(key, "")
	}
	t.Setenv("HOME", home)
	t.Setenv("PATH", bin+":/usr/bin:/bin")
	scripts := map[string]string{"systemctl": "exit 1", "loginctl": "echo no", "herdr": "echo 'herdr 0.9.2'", "claude": "echo '[]'", "bash": "test \"$1\" = -ic || exit 1; echo 'codex-cli 0.157.0'"}
	for name, body := range scripts {
		if e := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+body+"\n"), 0700); e != nil {
			t.Fatal(e)
		}
	}
	p, e := setup.Locations()
	if e != nil {
		t.Fatal(e)
	}
	if e = os.MkdirAll(filepath.Dir(p.Config), 0700); e != nil {
		t.Fatal(e)
	}
	mailbox, e := store.Home("")
	if e != nil {
		t.Fatal(e)
	}
	b, e := broker.New(mailbox, p.Config, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	secret := strings.Repeat("a", 64)
	for _, peer := range []broker.PrivatePeer{{Machine: "node-b", Target: "fake-route", Path: "/tmp/bin/herd-msg", Key: secret}, {Machine: "node-c", Path: "/tmp/bin/herd-msg", Key: secret}} {
		if e = b.InstallPeer(context.Background(), peer, nil, ""); e != nil {
			t.Fatal(e)
		}
	}
	d := store.Empty()
	d.Links["node-c"] = store.Link{Reachable: true, LastContact: time.Now()}
	d.Sessions["session-a"] = store.Session{ID: "session-a", State: "detached"}
	r := Doctor(context.Background(), Options{Data: d, SocketProbe: func(context.Context) error { return errors.New("missing socket") }, LinkProbe: func(_ context.Context, p broker.PrivatePeer) error {
		if p.Machine == "node-b" {
			return errors.New("host key changed")
		}
		return nil
	}})
	if r.OK {
		t.Fatal("bad install passed")
	}
	checks := map[string]Check{}
	for _, c := range r.Checks {
		checks[c.Check] = c
		if !c.OK && c.Fix == "" {
			t.Fatal("missing repair", c)
		}
	}
	if fix := checks["session/session-a"].Fix; !strings.Contains(fix, "resume") || !strings.Contains(fix, "HERD_CODEX_CMD") {
		t.Fatal("detached repair must cover the session and new sessions", fix)
	}
	for _, name := range []string{"unit-active", "linger", "herdr-version", "codex-version", "codex-socket", "claude-plugin", "session/session-a", "link/node-b"} {
		if checks[name].OK || checks[name].Check == "" {
			t.Fatal("missing failure", name)
		}
	}
	if !strings.Contains(checks["claude-plugin"].Detail, "missing") || !strings.Contains(checks["unit-active"].Detail, "inactive") {
		t.Fatal("failed check claimed success", checks)
	}
	if checks["codex-socket"].Fix != "bash -ic 'codex'" {
		t.Fatal("missing daemon repair must start a plain TUI", checks["codex-socket"])
	}
	if !strings.Contains(checks["session/session-a"].Fix, "session-a") {
		t.Fatal("resume repair omitted thread")
	}
	if !checks["link/node-c"].OK {
		t.Fatal("one failed link stopped next check")
	}
	data, _ := json.Marshal(r)
	if strings.Contains(string(data), secret) {
		t.Fatal("doctor exposed key")
	}
	d.Links["node-c"] = store.Link{Reachable: false, Error: "broker auth unavailable"}
	again := Doctor(context.Background(), Options{Home: mailbox, Config: p.Config, Data: d, SocketProbe: func(context.Context) error { return nil }, LinkProbe: func(context.Context, broker.PrivatePeer) error { return nil }})
	for _, check := range again.Checks {
		if check.Check == "link/node-c" && (check.OK || !strings.Contains(check.Detail, "broker auth unavailable")) {
			t.Fatal("interactive SSH hid broker failure", check)
		}
	}

	// The ordinary SSH probe must export public sessions rather than wiping the peer cache.
	d.Sessions["local-session"] = store.Session{ID: "local-session", Name: "agent", Machine: "local", Harness: "codex", State: "live", Hints: map[string]string{"secret": "receiver-private"}}
	fake := filepath.Join(bin, "ssh")
	executable, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	script := "#!/bin/sh\nexec env HERD_MSG_TEST_SSH=1 " + transport.Quote(executable) + " -test.run='^TestDoctorSSHHelper$'\n"
	if e = os.WriteFile(fake, []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	_ = Doctor(context.Background(), Options{Home: mailbox, Config: p.Config, Machine: "local", Data: d, SocketProbe: func(context.Context) error { return nil }})
	wire, e := os.ReadFile(filepath.Join(home, "probe-input"))
	if e != nil {
		t.Fatal(e)
	}
	var request broker.PeerRequest
	if e = json.Unmarshal(wire, &request); e != nil {
		t.Fatal(e)
	}
	found := false
	for _, session := range request.Sessions {
		if session.ID == "local-session" {
			found = true
			if len(session.Hints) > 0 {
				t.Fatal("probe exposed receiver hints")
			}
		}
	}
	if !found {
		t.Fatal("probe omitted local session catalog")
	}
	now := time.Now()
	Apply(d, r, now)
	Apply(d, r, now.Add(time.Minute))
	if !d.Health["probe/codex-socket"].Since.Equal(now) {
		t.Fatal("failure time changed")
	}
	Apply(d, Report{OK: true, Checks: []Check{{Check: "codex-socket", OK: true}}}, now)
	if len(d.Health) != 0 {
		t.Fatal("recovery left stale health")
	}
}

// Run as the fake SSH process so doctor exercises both the route challenge and
// the subsequent public-session probe without depending on external crypto tools.
func TestDoctorSSHHelper(t *testing.T) {
	if os.Getenv("HERD_MSG_TEST_SSH") != "1" {
		return
	}
	var r broker.PeerRequest
	if e := json.NewDecoder(os.Stdin).Decode(&r); e != nil {
		os.Exit(1)
	}
	var reply any = broker.PeerReply{}
	if r.Nonce != "" {
		secret, _ := hex.DecodeString(strings.Repeat("a", 64))
		transcript, _ := json.Marshal([]string{"herd-msg", "route-proof-v1", r.Machine, "node-b", r.Nonce})
		mac := hmac.New(sha256.New, secret)
		mac.Write(transcript)
		reply = broker.RouteProof{Machine: "node-b", MAC: hex.EncodeToString(mac.Sum(nil))}
	} else {
		data, _ := json.Marshal(r)
		if e := os.WriteFile(filepath.Join(os.Getenv("HOME"), "probe-input"), data, 0600); e != nil {
			os.Exit(1)
		}
	}
	data, _ := json.Marshal(reply)
	if e := json.NewEncoder(os.Stdout).Encode(broker.Response{Data: data}); e != nil {
		os.Exit(1)
	}
	os.Exit(0)
}

func TestSuccessfulVersionSurvivesInheritedOutputPipe(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CODEX_HOME", filepath.Join(home, "codex"))
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, "claude"))
	bin := filepath.Join(home, "bin")
	if e := os.Mkdir(bin, 0700); e != nil {
		t.Fatal(e)
	}
	// Interactive startup may leave a short-lived background child holding stdout.
	script := "#!/bin/sh\nsleep 2 &\necho 'codex-cli 0.166.0'\n"
	if e := os.WriteFile(filepath.Join(bin, "bash"), []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"systemctl", "loginctl", "herdr", "claude"} {
		if e := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\nexit 1\n"), 0700); e != nil {
			t.Fatal(e)
		}
	}
	t.Setenv("PATH", bin+":/usr/bin:/bin")
	r := Doctor(context.Background(), Options{SocketProbe: func(context.Context) error { return nil }})
	for _, c := range r.Checks {
		if c.Check == "codex-version" {
			if !c.OK {
				t.Fatal("successful version process treated as failed", c)
			}
			return
		}
	}
	t.Fatal("version check missing")
}

func TestBackgroundHealthNeverOpensShellEvenWhenClaudeIsMissing(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("CLAUDE_CONFIG_DIR", filepath.Join(home, "claude"))
	bin := filepath.Join(home, "bin")
	if err := os.Mkdir(bin, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"bash", "systemctl", "loginctl", "herdr"} {
		body := "#!/bin/sh\nexit 1\n"
		if name == "bash" {
			body = "#!/bin/sh\necho forbidden > \"$HOME/shell-started\"\nexit 1\n"
		}
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin)
	for range 2 {
		r := Doctor(context.Background(), Options{Background: true, SocketProbe: func(context.Context) error { return nil }})
		found := false
		for _, c := range r.Checks {
			if c.Check == "codex-version" {
				t.Fatal("background ran operator version probe")
			}
			if c.Check == "claude-plugin" {
				found = true
				if c.OK {
					t.Fatal("missing Claude passed")
				}
			}
		}
		if !found {
			t.Fatal("plugin check missing")
		}
	}
	if _, err := os.Stat(filepath.Join(home, "shell-started")); !os.IsNotExist(err) {
		t.Fatal("background started bash", err)
	}
}
