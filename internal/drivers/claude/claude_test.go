package claude

import (
	"context"
	"encoding/json"
	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/receiver"
	"github.com/JeremiahChurch/herd-msg/internal/store"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFencedAndMissingReceiversAreOffline(t *testing.T) {
	d := &Driver{}
	for _, s := range []store.Session{{ID: "old", Hints: map[string]string{"endpoint": "/tmp/unused"}}, {ID: "missing", Generation: "generation"}} {
		a, e := d.SessionState(context.Background(), s)
		if e != nil || a.State != "offline" {
			t.Fatal(a, e)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, e := d.Inject(ctx, store.Session{ID: "offline", Generation: "generation", Hints: map[string]string{"endpoint": filepath.Join(t.TempDir(), "missing.sock")}}, "body", "attempt")
	if e != nil || r.Outcome != broker.NotSent {
		t.Fatal(r, e)
	}
}
func TestReceiverEOFIsAmbiguous(t *testing.T) {
	path := filepath.Join(t.TempDir(), "child.sock")
	l, e := net.Listen("unix", path)
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, e := l.Accept()
		if e != nil {
			return
		}
		defer c.Close()
		var q receiver.Control
		json.NewDecoder(c).Decode(&q)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, e := (&Driver{}).Inject(ctx, store.Session{ID: "recipient", Generation: "generation", Hints: map[string]string{"endpoint": path, "instance": "child"}}, "body", "attempt")
	if e != nil || r.Outcome != broker.Unconfirmed {
		t.Fatal(r, e)
	}
	<-done
}
func TestShippedPluginBundlesReceiverAndBindingHook(t *testing.T) {
	b, e := os.ReadFile("../../../claude-plugin/.claude-plugin/plugin.json")
	if e != nil {
		t.Fatal(e)
	}
	var manifest struct {
		Servers map[string]struct {
			Command string   `json:"command"`
			Args    []string `json:"args"`
		} `json:"mcpServers"`
	}
	if e = json.Unmarshal(b, &manifest); e != nil {
		t.Fatal(e)
	}
	s := manifest.Servers["herd-msg-receiver"]
	if s.Command != "herd-msg" || len(s.Args) != 1 || s.Args[0] != "receiver" {
		t.Fatal(manifest)
	}
	b, e = os.ReadFile("../../../claude-plugin/hooks/hooks.json")
	if e != nil {
		t.Fatal(e)
	}
	var hooks struct {
		Hooks map[string][]struct {
			Matcher string `json:"matcher"`
			Hooks   []struct {
				Command string `json:"command"`
			} `json:"hooks"`
		} `json:"hooks"`
	}
	if e = json.Unmarshal(b, &hooks); e != nil {
		t.Fatal(e)
	}
	start := hooks.Hooks["SessionStart"]
	if len(start) != 1 || !strings.Contains(start[0].Matcher, "clear") || len(start[0].Hooks) != 1 || start[0].Hooks[0].Command != "herd-msg receiver-hook" {
		t.Fatal(hooks)
	}
}

func TestStalledAndMalformedReceiverStayUnavailable(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "child.sock")
		l, e := net.Listen("unix", path)
		if e != nil {
			t.Fatal(e)
		}
		release := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			c, e := l.Accept()
			if e != nil {
				return
			}
			defer c.Close()
			if malformed {
				c.Write([]byte("not-json\n"))
				return
			}
			<-release
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		a, e := (&Driver{}).SessionState(ctx, store.Session{ID: "session", Generation: "generation", Hints: map[string]string{"endpoint": path}})
		cancel()
		close(release)
		l.Close()
		<-done
		if e == nil || a.State == "offline" {
			t.Fatal("stalled or malformed receiver authorized offline prune", a, e)
		}
	}
}
