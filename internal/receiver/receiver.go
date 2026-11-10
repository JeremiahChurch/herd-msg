package receiver

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

type Work struct {
	Session    string    `json:"session"`
	Generation string    `json:"generation"`
	Epoch      string    `json:"epoch"`
	Instance   string    `json:"instance"`
	Attempt    string    `json:"attempt"`
	Deadline   time.Time `json:"deadline"`
}
type Status struct {
	Session     string   `json:"session"`
	Generation  string   `json:"generation"`
	Epoch       string   `json:"epoch"`
	Instance    string   `json:"instance"`
	Outstanding []string `json:"outstanding"`
}
type Control struct {
	Verb string `json:"verb"`
	Work Work   `json:"work"`
}
type Reply struct {
	Status Status        `json:"status"`
	Result broker.Result `json:"result"`
}

func Call(ctx context.Context, path string, q Control) (Reply, error) {
	var r Reply
	c, e := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if e != nil {
		return r, e
	}
	defer c.Close()
	deadline := time.Now().Add(time.Second)
	if d, ok := ctx.Deadline(); ok {
		deadline = d
	}
	c.SetDeadline(deadline)
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			c.Close()
		case <-done:
		}
	}()
	if e = json.NewEncoder(c).Encode(q); e != nil {
		return r, e
	}
	e = json.NewDecoder(c).Decode(&r)
	return r, e
}

type child struct {
	home, socket, token, endpoint, instance, pane, herdr string
	op                                                   sync.Mutex
	mu                                                   sync.Mutex
	status                                               Status
	session                                              store.Session
	bindingKey                                           string
	active                                               map[string]bool
	pollToken, pollBinding                               string
}

func (c *child) state() Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.status
	s.Outstanding = []string{}
	for id := range c.active {
		s.Outstanding = append(s.Outstanding, id)
	}
	return s
}
func (c *child) rpc(ctx context.Context, r broker.Request) (json.RawMessage, error) {
	reply, e := broker.Call(ctx, c.home, r)
	if e != nil {
		return nil, e
	}
	if reply.Code != 0 {
		return nil, errors.New(reply.Error)
	}
	return reply.Data, nil
}
func (c *child) sync(ctx context.Context) error {
	bindings, e := readBindings(c.home, c.socket, c.token)
	if e != nil {
		return e
	}
	current := bindings.History[len(bindings.History)-1]
	encoded, _ := json.Marshal(bindings)
	key := fingerprint(string(encoded))
	if c.pollBinding != key {
		c.pollToken = ""
		c.pollBinding = key
	}
	raw, e := c.rpc(ctx, broker.Request{Verb: "receiver-state", Wait: true, IfUpdated: c.pollToken, Caller: broker.Caller{ID: current.ID, Harness: "claude"}})
	if e != nil {
		return e
	}
	var snap struct {
		Data  *store.Data `json:"data"`
		Epoch string      `json:"epoch"`
		Token string      `json:"if_updated"`
	}
	if e = json.Unmarshal(raw, &snap); e != nil {
		return e
	}
	if snap.Data == nil {
		return errors.New("missing receiver state")
	}
	c.pollToken = ""
	ctx, stop := context.WithTimeout(ctx, 900*time.Millisecond)
	defer stop()
	st := c.state()
	c.mu.Lock()
	boundKey := c.bindingKey
	c.mu.Unlock()
	observed, exists := snap.Data.Sessions[current.ID]
	if exists && observed.Hints["instance"] == c.instance && observed.Generation == st.Generation && st.Epoch == snap.Epoch && len(observed.Outstanding) == 0 && boundKey == key {
		c.pollToken = snap.Token
		return nil
	}
	if !c.op.TryLock() {
		return nil
	}
	defer c.op.Unlock()
	fresh, e := readBindings(c.home, c.socket, c.token)
	if e != nil {
		return e
	}
	encoded, _ = json.Marshal(fresh)
	if fingerprint(string(encoded)) != key {
		return nil
	}
	a, e := identity.Pane(ctx, store.Session{Pane: c.pane, Hints: map[string]string{"herdr_session": c.herdr}})
	if e != nil {
		return e
	}
	name := identity.Name(ctx, a, c.herdr)
	makeSession := func(b Binding) store.Session {
		return store.Session{ID: b.ID, Name: name, Harness: "claude", State: "live", Pane: c.pane, Outstanding: []string{}, Hints: map[string]string{"endpoint": c.endpoint, "instance": c.instance, "transcript": b.Transcript, "cwd": b.Cwd, "herdr_session": c.herdr}}
	}
	alive := func(s store.Session) bool {
		path := s.Hints["endpoint"]
		if path == "" || s.Generation == "" {
			return false
		}
		probe, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
		defer cancel()
		reply, e := Call(probe, path, Control{Verb: "status"})
		return e == nil && reply.Status.Instance == s.Hints["instance"] && reply.Status.Session == s.ID
	}
	// Replay only uncompleted links. A separately resumed, reachable owner wins.
	for i := 0; i+1 < len(bindings.History); i++ {
		oldB, nextB := bindings.History[i], bindings.History[i+1]
		if oldB.ID == current.ID {
			continue
		}
		old, exists := snap.Data.Sessions[oldB.ID]
		if !exists {
			continue
		}
		if old.Successor == nextB.ID && old.Generation == "" {
			continue
		}
		if old.Hints["instance"] != c.instance && alive(old) {
			continue
		}
		if old.Generation == "" {
			continue
		}
		if len(old.Outstanding) > 0 {
			clean := old
			clean.Outstanding = []string{}
			expected := old.Generation
			raw, e = c.rpc(ctx, broker.Request{Verb: "register", Caller: broker.Caller{ID: old.ID, Harness: "claude"}, Session: &clean, ExpectedGeneration: &expected})
			if e != nil {
				return e
			}
			var registered struct {
				Session store.Session `json:"session"`
			}
			if e = json.Unmarshal(raw, &registered); e != nil {
				return e
			}
			old = registered.Session
		}
		if existing, ok := snap.Data.Sessions[nextB.ID]; ok && existing.Hints["instance"] != c.instance && alive(existing) {
			return errors.New("successor has another reachable receiver")
		}
		next := makeSession(nextB)
		expected := old.Generation
		raw, e = c.rpc(ctx, broker.Request{Verb: "rebind", Caller: broker.Caller{ID: old.ID, Harness: "claude"}, OldID: old.ID, Session: &next, ExpectedGeneration: &expected})
		if e != nil {
			return e
		}
		var registered struct {
			Session store.Session `json:"session"`
		}
		if e = json.Unmarshal(raw, &registered); e != nil {
			return e
		}
		snap.Data.Sessions[next.ID] = registered.Session
		old.Generation = ""
		old.Successor = next.ID
		snap.Data.Sessions[old.ID] = old
	}
	s, exists := snap.Data.Sessions[current.ID]
	st = c.state()
	if exists && s.Hints["instance"] != c.instance && alive(s) {
		return errors.New("another reachable receiver owns this conversation")
	}
	if !exists || s.Hints["instance"] != c.instance || st.Epoch != snap.Epoch || len(s.Outstanding) > 0 || s.Generation == "" {
		next := makeSession(current)
		expected := s.Generation
		raw, e = c.rpc(ctx, broker.Request{Verb: "register", Caller: broker.Caller{ID: current.ID, Harness: "claude"}, Session: &next, ExpectedGeneration: &expected})
		if e != nil {
			return e
		}
		var registered struct {
			Session store.Session `json:"session"`
			Epoch   string        `json:"epoch"`
		}
		if e = json.Unmarshal(raw, &registered); e != nil {
			return e
		}
		s = registered.Session
		snap.Epoch = registered.Epoch
	}
	c.mu.Lock()
	c.session = s
	c.bindingKey = key
	c.status = Status{Session: s.ID, Generation: s.Generation, Epoch: snap.Epoch, Instance: c.instance, Outstanding: []string{}}
	c.mu.Unlock()
	return nil
}
func notSent(reason string) broker.Result {
	return broker.Result{Outcome: broker.NotSent, Class: "precondition_race", Proof: "no parent user bytes written: " + reason}
}
func (c *child) inject(ctx context.Context, w Work) broker.Result {
	if !c.op.TryLock() {
		return notSent("receiver is registering or rebinding")
	}
	defer c.op.Unlock()
	st := c.state()
	if w.Instance != c.instance || w.Session != st.Session || w.Generation != st.Generation || w.Epoch != st.Epoch {
		return notSent("receiver fence changed")
	}
	c.mu.Lock()
	c.active[w.Attempt] = true
	s := c.session
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.active, w.Attempt); c.mu.Unlock() }()
	deadline := w.Deadline
	if deadline.IsZero() || time.Until(deadline) <= 0 {
		return notSent("deadline expired")
	}
	call, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	pre, stop := context.WithDeadline(call, time.Now().Add(time.Until(deadline)/2))
	defer stop()
	a, e := identity.Pane(pre, s)
	if e != nil {
		return notSent(e.Error())
	}
	if a.State == "blocked" || a.State == "unknown" {
		return notSent("recipient blocked or state unknown")
	}
	raw, e := c.rpc(pre, broker.Request{Verb: "validate-attempt", Caller: broker.Caller{ID: s.ID, Harness: "claude"}, Epoch: w.Epoch, Generation: w.Generation, AttemptID: w.Attempt})
	if e != nil {
		return notSent(e.Error())
	}
	var validated struct {
		Attempt store.Attempt `json:"attempt"`
	}
	if e = json.Unmarshal(raw, &validated); e != nil {
		return notSent(e.Error())
	}
	if validated.Attempt.ID != w.Attempt {
		return notSent("attempt response mismatch")
	}
	parent, e := (&net.Dialer{}).DialContext(pre, "unix", c.socket)
	if e != nil {
		return notSent(e.Error())
	}
	defer parent.Close()
	parent.SetDeadline(deadline)
	enc := json.NewEncoder(parent)
	if e = enc.Encode(map[string]any{"type": "auth", "token": c.token}); e != nil {
		return notSent(e.Error())
	}
	binding, e := readBindings(c.home, c.socket, c.token)
	if e != nil {
		return notSent(e.Error())
	}
	if binding.History[len(binding.History)-1].ID != w.Session {
		return notSent("current hook binding changed")
	}
	if pre.Err() != nil || call.Err() != nil || !time.Now().Before(deadline) {
		return notSent("deadline elapsed before input")
	}
	// A successful write is enqueue evidence only. Transcript lookup proves acceptance.
	_ = enc.Encode(map[string]any{"type": "user", "message": map[string]string{"role": "user", "content": validated.Attempt.Envelope}})
	return broker.Result{Outcome: broker.Unconfirmed}
}
func Run(ctx context.Context, home string, in io.Reader, out, errOut io.Writer) error {
	socket, token := os.Getenv("CLAUDE_CODE_MESSAGING_SOCKET"), os.Getenv("CLAUDE_CODE_MESSAGING_TOKEN")
	if socket == "" || token == "" {
		return errors.New("receiver requires a Claude MCP parent")
	}
	if e := store.PrivateDir(filepath.Join(home, "receivers")); e != nil {
		return e
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := &child{home: home, socket: socket, token: token, instance: store.Random(), pane: os.Getenv("HERDR_PANE_ID"), herdr: os.Getenv("HERDR_SESSION"), active: map[string]bool{}}
	stdio := make(chan error, 1)
	go func() { stdio <- mcp(in, out); cancel() }()
	var owner *os.File
	for owner == nil {
		attempt, stop := context.WithTimeout(ctx, 100*time.Millisecond)
		f, e := policy.Lock(attempt, bindingPath(home, socket)+".owner.lock")
		stop()
		if e == nil {
			owner = f
			break
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(100 * time.Millisecond):
		}
	}
	defer policy.Unlock(owner)
	path, e := broker.SocketPath(home)
	if e != nil {
		return e
	}
	c.endpoint = filepath.Join(filepath.Dir(path), "herd-msg-r-"+c.instance[:16]+".sock")
	listener, e := net.Listen("unix", c.endpoint)
	if e != nil {
		return e
	}
	defer listener.Close()
	defer os.Remove(c.endpoint)
	if e = os.Chmod(c.endpoint, 0600); e != nil {
		return e
	}
	var workers sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() { <-ctx.Done(); listener.Close() }()
	go func() {
		defer close(acceptDone)
		for {
			conn, e := listener.Accept()
			if e != nil {
				return
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				var q Control
				if json.NewDecoder(io.LimitReader(conn, broker.MaxFrame)).Decode(&q) != nil {
					return
				}
				reply := Reply{Status: c.state()}
				if q.Verb == "inject" {
					reply.Result = c.inject(ctx, q.Work)
				}
				json.NewEncoder(conn).Encode(reply)
			}()
		}
	}()
	lastError := ""
	for {
		call, stop := context.WithTimeout(ctx, 28*time.Second)
		// Hook bindings can change while the broker is waiting. Stat the small
		// binding file locally; cancel the request promptly on an atomic replace.
		bindingDone := make(chan struct{})
		before, _ := os.Stat(bindingPath(home, socket))
		go func() {
			defer close(bindingDone)
			watchBinding(call, bindingPath(home, socket), before, stop)
		}()
		e = c.sync(call)
		stop()
		<-bindingDone
		if e != nil && ctx.Err() == nil && e.Error() != lastError {
			fmt.Fprintln(errOut, "receiver waiting:", e)
			lastError = e.Error()
		} else if e == nil {
			lastError = ""
		}
		select {
		case <-ctx.Done():
			listener.Close()
			<-acceptDone
			workers.Wait()
			return nil
		case e := <-stdio:
			listener.Close()
			<-acceptDone
			workers.Wait()
			return e
		case <-time.After(300 * time.Millisecond):
		}
	}
}
func mcp(in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 4096), broker.MaxFrame)
	for scanner.Scan() {
		var q struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if json.Unmarshal(scanner.Bytes(), &q) != nil || len(q.ID) == 0 {
			continue
		}
		var result any
		switch q.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2024-11-05", "capabilities": map[string]any{}, "serverInfo": map[string]string{"name": "herd-msg", "version": "1"}}
		case "tools/list":
			result = map[string]any{"tools": []any{}}
		case "resources/list":
			result = map[string]any{"resources": []any{}}
		case "prompts/list":
			result = map[string]any{"prompts": []any{}}
		default:
			result = map[string]any{}
		}
		if e := json.NewEncoder(out).Encode(map[string]any{"jsonrpc": "2.0", "id": q.ID, "result": result}); e != nil {
			return e
		}
	}
	return scanner.Err()
}

// watchBinding checks metadata only. No broker call, JSON decode or subprocess
// is needed to notice a hook rebind during the long poll.
func watchBinding(ctx context.Context, path string, before os.FileInfo, wake context.CancelFunc) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			after, e := os.Stat(path)
			if e != nil || before == nil || !os.SameFile(before, after) || before.Size() != after.Size() || before.ModTime() != after.ModTime() {
				wake()
				return
			}
		}
	}
}
