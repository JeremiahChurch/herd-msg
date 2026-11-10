package setup

import (
	"context"
	"errors"
	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T) (Paths, string) {
	t.Helper()
	home := t.TempDir()
	bin := filepath.Join(home, "fake-bin")
	if e := os.MkdirAll(bin, 0700); e != nil {
		t.Fatal(e)
	}
	for _, key := range []string{"HERD_MSG_HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "CODEX_HOME", "CLAUDE_CONFIG_DIR", "FAIL_AT"} {
		t.Setenv(key, "")
	}
	t.Setenv("HOME", home)
	t.Setenv("PATH", bin+":/usr/bin:/bin")
	t.Setenv("FIXTURE", home)
	scripts := map[string]string{
		"systemctl": `case "$*" in
 *show-environment*) test "$FAIL_AT" != systemd;;
 *is-active*) test -f "$FIXTURE/active";;
 *is-enabled*) test -f "$FIXTURE/enabled";;
 *restart*) test "$FAIL_AT" != restart || exit 1; touch "$FIXTURE/active";;
 *enable*) touch "$FIXTURE/enabled";;
 *disable*) rm -f "$FIXTURE/enabled";;
 *stop*) rm -f "$FIXTURE/active";;
 esac`,
		"loginctl": `case "$*" in *show-user*) if test -f "$FIXTURE/linger"; then echo yes; else echo no; fi;; *enable-linger*) test "$FAIL_AT" != linger || exit 1; touch "$FIXTURE/linger";; esac`,
		"herdr": `case "$*" in
 --version) echo 'herdr 0.9.3';;
 'plugin list --json') if test -f "$FIXTURE/herdr"; then echo '{"result":{"plugins":[{"plugin_id":"herd-msg"}]}}';else echo '{"result":{"plugins":[]}}';fi;;
 'plugin link '*) touch "$FIXTURE/herdr";;
 'plugin unlink '*) rm -f "$FIXTURE/herdr";;
 esac`,
		"claude": `case "$*" in
 'plugin list --json') if test -f "$FIXTURE/plugin";then echo '[{"id":"herd-msg@herd-msg-local"}]';else echo '[]';fi;;
 'plugin marketplace list --json') if test -f "$FIXTURE/market";then echo '[{"name":"herd-msg-local"}]';else echo '[]';fi;;
 'plugin marketplace add '*) touch "$FIXTURE/market";;
 'plugin marketplace remove '*) rm -f "$FIXTURE/market";;
 'plugin install '*) touch "$FIXTURE/plugin";test "$FAIL_AT" != claude || exit 1;;
 'plugin uninstall '*) rm -f "$FIXTURE/plugin";;
 esac`,
	}
	for name, script := range scripts {
		if e := os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\nset -e\n"+script+"\n"), 0700); e != nil {
			t.Fatal(e)
		}
	}
	source := filepath.Join(home, "source")
	if e := os.WriteFile(source, []byte("binary"), 0700); e != nil {
		t.Fatal(e)
	}
	p, e := Locations()
	if e != nil {
		t.Fatal(e)
	}
	return p, source
}
func TestInstallRemovalRetainsMailAndPurgeDeletesIt(t *testing.T) {
	p, source := fixture(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		r, e := installFixture(ctx, source)
		if e != nil || !r.Installed || !r.Linger {
			t.Fatalf("install %d: %+v %v", i, r, e)
		}
	}
	unit, e := os.ReadFile(p.Unit)
	if e != nil || !strings.Contains(string(unit), "Restart=on-failure") {
		t.Fatalf("unit: %s %v", unit, e)
	}
	hook, e := os.ReadFile(filepath.Join(p.Root, "claude-plugin/hooks/hooks.json"))
	if e != nil || !strings.Contains(string(hook), p.Binary) {
		t.Fatalf("hook %s %v", hook, e)
	}
	if e = os.WriteFile(filepath.Join(p.Data, "mailbox.db"), []byte("mail"), 0600); e != nil {
		t.Fatal(e)
	}
	shared := filepath.Join(p.Claude, "plugins/cache/other/keep")
	if e = os.MkdirAll(filepath.Dir(shared), 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(shared, []byte("keep"), 0600); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if _, e = Remove(ctx, false); e != nil {
			t.Fatal(e)
		}
	}
	for _, path := range []string{p.Binary, p.Unit, p.Root, p.Skill} {
		if _, e = os.Stat(path); !os.IsNotExist(e) {
			t.Fatalf("leftover %s: %v", path, e)
		}
	}
	for _, path := range []string{filepath.Join(p.Data, "mailbox.db"), p.Config, shared, filepath.Join(p.Home, "linger")} {
		if _, e = os.Stat(path); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = Remove(ctx, true); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(p.Data); !os.IsNotExist(e) {
		t.Fatal("purge retained mail")
	}
	if _, e = os.Stat(shared); e != nil {
		t.Fatal("purge touched shared cache", e)
	}
}
func TestInstallRollsBackPartialNativeFailure(t *testing.T) {
	for _, failure := range []string{"claude", "restart", "ready"} {
		t.Run(failure, func(t *testing.T) {
			p, source := fixture(t)
			t.Setenv("FAIL_AT", failure)
			if _, e := installFixture(context.Background(), source); e == nil {
				t.Fatal("expected failure")
			}
			for _, path := range []string{p.Binary, p.Unit, p.Skill, p.Config, filepath.Join(p.Root, "herdr-plugin.toml"), filepath.Join(p.Home, "plugin"), filepath.Join(p.Home, "market"), filepath.Join(p.Home, "herdr"), filepath.Join(p.Home, "active"), filepath.Join(p.Home, "enabled")} {
				if _, e := os.Stat(path); !os.IsNotExist(e) {
					t.Fatalf("partial install leftover %s: %v", path, e)
				}
			}
		})
	}
}
func TestUpgradeRollbackRestoresPreviousFilesAndService(t *testing.T) {
	p, source := fixture(t)
	if _, e := installFixture(context.Background(), source); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(source, []byte("new binary"), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("FAIL_AT", "claude")
	if _, e := installFixture(context.Background(), source); e == nil {
		t.Fatal("expected error")
	}
	data, e := os.ReadFile(p.Binary)
	if e != nil || string(data) != "binary" {
		t.Fatalf("previous binary lost %q %v", data, e)
	}
	if _, e = os.Stat(filepath.Join(p.Home, "active")); e != nil {
		t.Fatal("previous service lost", e)
	}
}
func TestMissingSystemdRefusesBeforeWriting(t *testing.T) {
	p, source := fixture(t)
	t.Setenv("FAIL_AT", "systemd")
	_, e := installFixture(context.Background(), source)
	if e == nil || !strings.Contains(e.Error(), "systemd user manager is required") {
		t.Fatal(e)
	}
	if _, e = os.Stat(p.Binary); !os.IsNotExist(e) {
		t.Fatal("binary written before preflight")
	}
}
func TestLingerFailureReportsExactCommand(t *testing.T) {
	_, source := fixture(t)
	t.Setenv("FAIL_AT", "linger")
	r, e := installFixture(context.Background(), source)
	if e != nil || r.Linger || !strings.Contains(strings.Join(r.Notes, " "), "loginctl enable-linger") {
		t.Fatalf("%+v %v", r, e)
	}
}

func installFixture(ctx context.Context, source string) (Result, error) {
	return install(ctx, source, func(context.Context, string, string) error {
		if os.Getenv("FAIL_AT") == "ready" {
			return errors.New("broker not ready")
		}
		return nil
	})
}
func TestReadinessRequiresARespondingNewBroker(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	home := t.TempDir()
	b, e := broker.New(home, filepath.Join(home, "config.json"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	server, e := b.Listen()
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.Serve(ctx)
	if e = waitBroker(ctx, home, ""); e != nil {
		t.Fatal(e)
	}
	short, stop := context.WithTimeout(ctx, 100*time.Millisecond)
	defer stop()
	if e = waitBroker(short, home, b.Epoch); e == nil {
		t.Fatal("old manual broker counted as restarted service")
	}
}
func TestNativeWarningsDoNotBreakRegistryJSON(t *testing.T) {
	p, source := fixture(t)
	fake := filepath.Join(p.Home, "fake-bin/claude")
	data, e := os.ReadFile(fake)
	if e != nil {
		t.Fatal(e)
	}
	data = []byte(strings.Replace(string(data), "set -e", "set -e\necho harmless-warning >&2", 1))
	if e = os.WriteFile(fake, data, 0700); e != nil {
		t.Fatal(e)
	}
	if _, e = installFixture(context.Background(), source); e != nil {
		t.Fatal(e)
	}
	if _, e = Remove(context.Background(), false); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(p.Home, "plugin")); !os.IsNotExist(e) {
		t.Fatal("plugin registration survived removal")
	}
}

func TestRemovalRefusesMalformedRegistryAndKeepsRepairResources(t *testing.T) {
	p, source := fixture(t)
	if _, e := installFixture(context.Background(), source); e != nil {
		t.Fatal(e)
	}
	fake := filepath.Join(p.Home, "fake-bin/claude")
	if e := os.WriteFile(fake, []byte("#!/bin/sh\necho malformed\n"), 0700); e != nil {
		t.Fatal(e)
	}
	if _, e := Remove(context.Background(), false); e == nil {
		t.Fatal("malformed registry reported removed")
	}
	for _, path := range []string{p.Binary, p.Root, filepath.Join(p.Home, "plugin"), filepath.Join(p.Home, "active")} {
		if _, e := os.Stat(path); e != nil {
			t.Fatal("repair resource removed", path, e)
		}
	}
}

func TestServicePreservesInstalledHomesAndResolvedToolPaths(t *testing.T) {
	p, source := fixture(t)
	codex := filepath.Join(p.Home, "custom-codex")
	conf := filepath.Join(p.Home, "custom-config")
	claude := filepath.Join(p.Home, "custom-claude")
	data := filepath.Join(p.Home, "custom-data")
	t.Setenv("CODEX_HOME", codex)
	t.Setenv("XDG_CONFIG_HOME", conf)
	t.Setenv("CLAUDE_CONFIG_DIR", claude)
	t.Setenv("XDG_DATA_HOME", data)
	if _, e := installFixture(context.Background(), source); e != nil {
		t.Fatal(e)
	}
	p, e := Locations()
	if e != nil {
		t.Fatal(e)
	}
	unit, e := os.ReadFile(p.Unit)
	if e != nil {
		t.Fatal(e)
	}
	for _, want := range []string{"CODEX_HOME=" + codex, "CLAUDE_CONFIG_DIR=" + claude, "XDG_CONFIG_HOME=" + conf, "XDG_DATA_HOME=" + data, filepath.Join(p.Home, "fake-bin")} {
		if !strings.Contains(string(unit), want) {
			t.Fatal("service lost installation setting", want, string(unit))
		}
	}
}
