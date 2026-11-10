package codex

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Explicit opt-in only. Every thread, process, socket and CODEX_HOME is owned by
// this test; authentication is copied without loading the user's configuration.
func TestDisposableSteerProbe(t *testing.T) {
	binary := os.Getenv("HERD_MSG_STEER_PROBE_BINARY")
	if binary == "" {
		t.Skip("set HERD_MSG_STEER_PROBE_BINARY")
	}
	root := t.TempDir()
	home, _ := os.UserHomeDir()
	auth, err := os.ReadFile(filepath.Join(home, ".codex/auth.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "auth.json"), auth, 0600); err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(root, "server.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	socket := filepath.Join(root, "api.sock")
	cmd := exec.Command(binary, "app-server", "--listen", "unix://"+socket)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "CODEX_HOME="+root)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		done := make(chan struct{})
		go func() { cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			<-done
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	var c *Client
	for i := 0; i < 100; i++ {
		c, err = Open(ctx, socket)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.ws.SetDeadline(time.Now().Add(50 * time.Second))
	t.Logf("server version=%s", c.Version)
	var models struct {
		Data []struct {
			ID      string `json:"id"`
			Model   string `json:"model"`
			Default bool   `json:"isDefault"`
		} `json:"data"`
	}
	if err = c.Call("model/list", map[string]any{}, &models); err != nil {
		t.Fatal(err)
	}
	t.Logf("models=%+v", models.Data)
	var start struct {
		Thread Thread `json:"thread"`
	}
	if err = c.Call("thread/start", map[string]any{"cwd": root, "approvalPolicy": "never", "sandbox": "danger-full-access", "experimentalRawEvents": false, "persistExtendedHistory": true}, &start); err != nil {
		t.Fatal(err)
	}
	id := start.Thread.ID
	var turn struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	input := func(text string) []any { return []any{map[string]string{"type": "text", "text": text}} }
	if err = c.Call("turn/start", map[string]any{"threadId": id, "clientUserMessageId": "mail-probe-start", "input": input("Run sleep 8, then reply OK. Do nothing else.")}, &turn); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	active, err := c.Read(id, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pre-steer status=%s turn=%s", active.Status.Type, turn.Turn.ID)
	if active.Status.Type != "active" {
		var raw json.RawMessage
		c.Call("thread/read", map[string]any{"threadId": id, "includeTurns": true}, &raw)
		t.Fatalf("turn failed before steer: %s", raw)
	}
	var result json.RawMessage
	err = c.Call("turn/steer", map[string]any{"threadId": id, "expectedTurnId": turn.Turn.ID, "clientUserMessageId": "mail-probe-steer", "input": input("After sleeping reply STEER_OK instead.")}, &result)
	t.Logf("steer response=%s error=%v", result, err)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 35; i++ {
		time.Sleep(time.Second)
		tr, e := c.Read(id, false)
		if e != nil {
			t.Fatal(e)
		}
		if tr.Status.Type == "idle" {
			break
		}
	}
	var items json.RawMessage
	err = c.Call("thread/items/list", map[string]any{"threadId": id, "sortDirection": "desc", "limit": 100}, &items)
	t.Logf("items=%s error=%v", items, err)
	if err != nil || !strings.Contains(string(items), "mail-probe-start") || !strings.Contains(string(items), "mail-probe-steer") {
		t.Fatal("item client IDs missing", err)
	}
	tr, err := c.Read(id, true)
	if err != nil {
		t.Fatal(err)
	}
	var clients []string
	for _, turn := range tr.Turns {
		for _, item := range turn.Items {
			if item.Type == "userMessage" {
				clients = append(clients, item.ClientID)
			}
		}
	}
	t.Logf("persisted user clientIds=%q", clients)
	if !slices.Contains(clients, "mail-probe-start") || !slices.Contains(clients, "mail-probe-steer") {
		t.Fatal("persisted client IDs missing", clients)
	}
}
