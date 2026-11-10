package muse

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

// This is the fake native command in the crash test. It appends a receipt for
// the exact input the broker actually sent, including its generated attempt ID.
func TestMuseReceiptCommand(t *testing.T) {
	path := os.Getenv("HERD_MSG_MUSE_RECEIPT_LOG")
	if path == "" {
		t.Skip("native command helper")
	}
	body, e := io.ReadAll(os.Stdin)
	if e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if _, e = f.Write(append(encode(admission(testID, string(body), "accepted")), '\n')); e != nil {
		t.Fatal(e)
	}
}

func TestMuseBrokerCrashBeforeResultCommit(t *testing.T) {
	if home := os.Getenv("HERD_MSG_MUSE_CRASH_HOME"); home != "" {
		b, e := broker.New(home, filepath.Join(home, "config.json"), map[string]broker.Driver{"muse": &Driver{}})
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if e = b.Register(ctx, store.Session{ID: testID, Name: "muse-test", Harness: "muse", State: "live", Pane: "w1:p1"}); e != nil {
			t.Fatal(e)
		}
		if _, e = b.Send(ctx, broker.SendRequest{ID: "crash-offset", Created: time.Now(), To: testID, Body: "tiny crash test"}, broker.Caller{ID: "operator"}); e != nil {
			t.Fatal(e)
		}
		b.Fault = func(point string) {
			if point == "after_driver_return" {
				os.Exit(87)
			}
		}
		if e = b.Dispatch(ctx, testID); e != nil {
			t.Fatal(e)
		}
		t.Fatal("crash boundary was not reached")
	}
	f := setup(t)
	home := t.TempDir()
	log, e := os.OpenFile(f.log, os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	if e = log.Truncate(537 << 20); e != nil {
		t.Fatal(e)
	}
	if _, e = log.WriteAt([]byte("\n"), (537<<20)-1); e != nil {
		t.Fatal(e)
	}
	log.Close()
	exe, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	scriptPath := filepath.Join(f.root, "bin", "muse")
	script, e := os.ReadFile(scriptPath)
	if e != nil {
		t.Fatal(e)
	}
	scriptText := strings.Replace(string(script), "cat > "+shellQuote(filepath.Join(f.root, "sent")), shellQuote(exe)+" -test.run=^TestMuseReceiptCommand$", 1)
	if scriptText == string(script) {
		t.Fatal("native fixture command was not replaced")
	}
	write(t, scriptPath, []byte(scriptText), 0700)
	cmd := exec.Command(exe, "-test.run=^TestMuseBrokerCrashBeforeResultCommit$")
	cmd.Env = append(os.Environ(), "HERD_MSG_MUSE_CRASH_HOME="+home, "HERD_MSG_MUSE_RECEIPT_LOG="+f.log)
	output, e := cmd.CombinedOutput()
	exit, ok := e.(*exec.ExitError)
	if !ok || exit.ExitCode() != 87 {
		t.Fatalf("child did not crash at result boundary: %v %s", e, output)
	}
	b, e := broker.New(home, filepath.Join(home, "config.json"), map[string]broker.Driver{"muse": &Driver{}})
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	d, e := b.Store.Read(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	m, e := d.Find("crash-offset")
	if e != nil {
		t.Fatal(e)
	}
	if m.State != states.Indeterminate || len(m.Attempts) != 1 || !strings.HasPrefix(m.Attempts[0].Proof, "muse-offset:") {
		t.Fatal("pre-send offset was not committed before input", m)
	}
	if e = b.Sweep(context.Background()); e != nil {
		t.Fatal(e)
	}
	d, e = b.Store.Read(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	m, e = d.Find("crash-offset")
	if e != nil {
		t.Fatal(e)
	}
	if m.State != states.Injected || len(m.Attempts) != 1 {
		t.Fatal("restart missed receipt or sent twice", m.State, len(m.Attempts))
	}
	commands, e := os.ReadFile(filepath.Join(f.root, "commands"))
	if e != nil {
		t.Fatal(e)
	}
	if strings.Count(string(commands), "session-message send") != 1 {
		t.Fatal("native input was repeated", string(commands))
	}
}
