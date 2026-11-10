package main

import (
	"bytes"
	"context"
	"github.com/JeremiahChurch/herd-msg/internal/store"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"
)

func TestVersion(t *testing.T) {
	t.Setenv("CGO_ENABLED", "0")
	binary := filepath.Join(t.TempDir(), "herd-msg")
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	build := exec.Command(goBinary, "build", "-trimpath", "-ldflags", "-X main.version=v0.0.0-test", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build executable: %v\n%s", err, output)
	}
	output, err := exec.Command(binary).CombinedOutput()
	if err != nil {
		t.Fatalf("run executable: %v\n%s", err, output)
	}
	if got, want := string(output), "v0.0.0-test\n"; got != want {
		t.Fatalf("version output = %q, want %q", got, want)
	}
}

func TestBrokerSIGTERM(t *testing.T) {
	t.Setenv("CGO_ENABLED", "0")
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	home := filepath.Join(t.TempDir(), "mail")
	t.Setenv("HERD_MSG_HOME", home)
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	binary := filepath.Join(t.TempDir(), "herd-msg")
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	if output, e := exec.Command(goBinary, "build", "-o", binary, ".").CombinedOutput(); e != nil {
		t.Fatalf("build broker: %v %s", e, output)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "broker")
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	ready := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, e := os.Stat(filepath.Join(home, "socket.json")); e == nil {
			ready = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatal("broker never listened", output.String())
	}
	if e := cmd.Process.Signal(syscall.SIGTERM); e != nil {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatal(e)
	}
	if e := cmd.Wait(); e != nil {
		t.Fatalf("SIGTERM did not drain: %v %s", e, output.String())
	}
	s, e := store.Open(home)
	if e != nil {
		t.Fatal("shutdown did not release mailbox", e)
	}
	s.Close()
}
