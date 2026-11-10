package identity

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"time"
)

// SocketPath follows herdr's explicit session and socket override precedence.
func SocketPath(session string) string {
	if session == "" {
		if path := os.Getenv("HERDR_SOCKET_PATH"); path != "" {
			return path
		}
		session = os.Getenv("HERDR_SESSION")
	}
	root := os.Getenv("XDG_CONFIG_HOME")
	if root == "" {
		home, _ := os.UserHomeDir()
		root = filepath.Join(home, ".config")
	}
	root = filepath.Join(root, "herdr")
	if session != "" && session != "default" {
		root = filepath.Join(root, "sessions", session)
	}
	return filepath.Join(root, "herdr.sock")
}

// WatchAgents uses one event subscription, reconnecting after server replacement.
// The caller still performs a slow resync for old servers and lost events.
func WatchAgents(ctx context.Context, session string, wake func()) {
	for ctx.Err() == nil {
		_ = watchAgents(ctx, SocketPath(session), wake)
		timer := time.NewTimer(30 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func watchAgents(ctx context.Context, path string, wake func()) error {
	conn, e := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if e != nil {
		return e
	}
	defer conn.Close()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	subscriptions := []map[string]string{}
	for _, kind := range []string{"pane.created", "pane.updated", "pane.closed", "pane.exited", "pane.agent_detected", "workspace.updated", "layout.updated"} {
		subscriptions = append(subscriptions, map[string]string{"type": kind})
	}
	if e = json.NewEncoder(conn).Encode(map[string]any{"id": "herd-msg", "method": "events.subscribe", "params": map[string]any{"subscriptions": subscriptions}}); e != nil {
		return e
	}
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), 4<<20)
	if !scanner.Scan() {
		return errors.New("herdr subscription closed")
	}
	var ack struct {
		Error  json.RawMessage `json:"error"`
		Result struct {
			Type string `json:"type"`
		} `json:"result"`
	}
	if json.Unmarshal(scanner.Bytes(), &ack) != nil || len(ack.Error) != 0 || ack.Result.Type != "subscription_started" {
		return errors.New("herdr subscription unavailable")
	}
	conn.SetDeadline(time.Time{})
	wake() // Reconnect may have missed changes.
	for scanner.Scan() {
		wake()
	}
	return scanner.Err()
}
