package identity

import (
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestAgentSubscriptionWakesAndClosesOnCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "herdr.sock")
	l, e := net.Listen("unix", path)
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	call, cancel := context.WithCancel(context.Background())
	defer cancel()
	wakes := make(chan struct{}, 4)
	done := make(chan error, 1)
	go func() { done <- watchAgents(call, path, func() { wakes <- struct{}{} }) }()
	conn, e := l.Accept()
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Second))
	var q struct {
		Method string `json:"method"`
		Params struct {
			Subscriptions []map[string]string `json:"subscriptions"`
		} `json:"params"`
	}
	if e = json.NewDecoder(conn).Decode(&q); e != nil || q.Method != "events.subscribe" || len(q.Params.Subscriptions) == 0 {
		t.Fatal(q, e)
	}
	enc := json.NewEncoder(conn)
	if e = enc.Encode(map[string]any{"result": map[string]string{"type": "subscription_started"}}); e != nil {
		t.Fatal(e)
	}
	select {
	case <-wakes:
	case <-time.After(time.Second):
		t.Fatal("no reconnect wake")
	}
	if e = enc.Encode(map[string]any{"event": "pane_updated", "data": map[string]any{"pane": map[string]string{"pane_id": "w1:p1"}}}); e != nil {
		t.Fatal(e)
	}
	select {
	case <-wakes:
	case <-time.After(time.Second):
		t.Fatal("no event wake")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("subscription did not cancel")
	}
}

func TestSocketPathPrecedence(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", "/tmp/config")
	t.Setenv("HERDR_SESSION", "default-session")
	t.Setenv("HERDR_SOCKET_PATH", "/tmp/override.sock")
	if got := SocketPath(""); got != "/tmp/override.sock" {
		t.Fatal(got)
	}
	if got := SocketPath("default"); got != "/tmp/config/herdr/herdr.sock" {
		t.Fatal(got)
	}
	if got := SocketPath("named"); got != "/tmp/config/herdr/sessions/named/herdr.sock" {
		t.Fatal(got)
	}
}
