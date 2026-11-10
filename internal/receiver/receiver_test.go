package receiver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func TestTranscriptOnlyAcceptedExactPeerRecords(t *testing.T) {
	header := "[herd-msg] from builder.codex@node | id mail | attempt attempt | hop 1"
	user := func(content any, origin string) map[string]any {
		return map[string]any{"type": "user", "sessionId": "current", "origin": map[string]string{"kind": origin}, "message": map[string]any{"content": content}, "isMeta": true}
	}
	attachment := map[string]any{"type": "attachment", "sessionId": "current", "session_id": "stale-startup", "attachment": map[string]any{"type": "queued_command", "origin": map[string]string{"kind": "peer"}, "prompt": header + "\nbody"}, "renderedRole": "system"}
	cases := []struct {
		row        any
		found, err bool
	}{{user("Another Claude session sent a message:\n"+header+"\nbody", "peer"), true, false}, {attachment, true, false}, {user("quoted:\n"+header, "peer"), false, true}, {user("unrelated wrapper\nother message", "peer"), false, false}, {user("Another Claude session sent a message:\n"+header+"x", "peer"), false, false}, {user(header, "user"), false, false}, {user([]any{map[string]string{"type": "tool_result", "content": header}}, "peer"), false, false}, {map[string]any{"type": "queue-operation", "sessionId": "current", "content": header}, false, false}, {map[string]any{"type": "api-request-blob", "sessionId": "current", "message": map[string]string{"content": header}}, false, false}}
	for i, tc := range cases {
		b, _ := json.Marshal(tc.row)
		found, e := record(b, "current", header)
		if found != tc.found || (e != nil) != tc.err {
			t.Fatalf("record %d: %v %v", i, found, e)
		}
		found, _ = record(b, "other", header)
		if found {
			t.Fatal("wrong outer session")
		}
	}
	unknown, _ := json.Marshal(user("new wrapper\n"+header, "peer"))
	positive, _ := json.Marshal(attachment)
	data := append(append(unknown, '\n'), positive...)
	data = append(data, '\n')
	found, e := scan(context.Background(), bytes.NewReader(data), "current", header)
	if e != nil || !found {
		t.Fatal(found, e)
	}
	found, e = scan(context.Background(), bytes.NewReader(positive), "current", header)
	if e != nil || found {
		t.Fatal("partial record accepted", found, e)
	}
}
func TestHookFingerprintAndHistory(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CLAUDE_CODE_MESSAGING_SOCKET", "/tmp/test-parent.sock")
	t.Setenv("CLAUDE_CODE_MESSAGING_TOKEN", "token-first")
	for _, id := range []string{"old", "new", "old"} {
		e := Hook(context.Background(), home, strings.NewReader(`{"session_id":"`+id+`","transcript_path":"/tmp/`+id+`.jsonl","cwd":"/tmp/project"}`))
		if e != nil {
			t.Fatal(e)
		}
	}
	b, e := readBindings(home, "/tmp/test-parent.sock", "token-first")
	if e != nil || len(b.History) != 3 {
		t.Fatal(b, e)
	}
	if _, e = readBindings(home, "/tmp/test-parent.sock", "token-second"); e == nil {
		t.Fatal("stale parent token accepted")
	}
	t.Setenv("CLAUDE_CODE_MESSAGING_TOKEN", "token-second")
	if e = Hook(context.Background(), home, strings.NewReader(`{"session_id":"fresh","transcript_path":"/tmp/fresh.jsonl"}`)); e != nil {
		t.Fatal(e)
	}
	b, e = readBindings(home, "/tmp/test-parent.sock", "token-second")
	if e != nil || len(b.History) != 1 || b.History[0].ID != "fresh" {
		t.Fatal(b, e)
	}
}
func fakeHerdr(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\nprintf '%s\\n' '{\"result\":{\"agents\":[{\"pane_id\":\"test-pane\",\"agent\":\"claude\",\"agent_status\":\"idle\"}]}}'\n"
	if e := os.WriteFile(filepath.Join(dir, "herdr"), []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
}
func TestReceiverFencesBeforeParentUserBytes(t *testing.T) {
	fakeHerdr(t)
	home := t.TempDir()
	b, e := broker.New(home, filepath.Join(home, "config.json"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, e := b.Listen()
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close()
	go server.Serve(ctx)
	parentPath := filepath.Join(t.TempDir(), "parent.sock")
	listener, e := net.Listen("unix", parentPath)
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	records := make(chan map[string]any, 10)
	go func() {
		for {
			conn, e := listener.Accept()
			if e != nil {
				return
			}
			go func() {
				defer conn.Close()
				d := json.NewDecoder(conn)
				for {
					var row map[string]any
					if d.Decode(&row) != nil {
						return
					}
					records <- row
				}
			}()
		}
	}()
	s := store.Session{ID: "recipient", Name: "receiver", Harness: "claude", State: "live", Pane: "test-pane", Hints: map[string]string{"instance": "instance", "transcript": "/tmp/transcript.jsonl"}}
	if e = b.Register(ctx, s); e != nil {
		t.Fatal(e)
	}
	data, _ := b.Store.Read(ctx)
	s = data.Sessions[s.ID]
	a := store.Attempt{ID: "attempt", Session: s, Envelope: "committed envelope"}
	if e = b.Store.Update(ctx, func(d *store.Data) error {
		d.Messages["mail"] = &store.Message{ID: "mail", Recipient: s.ID, State: states.Injecting, ActiveAttempt: a.ID, Attempts: []store.Attempt{a}}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e = os.MkdirAll(filepath.Join(home, "receivers"), 0700); e != nil {
		t.Fatal(e)
	}
	binding := Bindings{Fingerprint: fingerprint("token"), History: []Binding{{ID: s.ID, Transcript: "/tmp/transcript.jsonl"}}}
	if e = atomicJSON(bindingPath(home, parentPath), binding); e != nil {
		t.Fatal(e)
	}
	c := &child{home: home, socket: parentPath, token: "token", instance: "instance", session: s, active: map[string]bool{}, status: Status{Session: s.ID, Generation: s.Generation, Epoch: b.Epoch, Instance: "instance"}}
	work := Work{Session: s.ID, Generation: s.Generation, Epoch: b.Epoch, Instance: "instance", Attempt: a.ID, Deadline: time.Now().Add(time.Second)}
	stale := work
	stale.Generation = "old"
	if r := c.inject(ctx, stale); r.Outcome != broker.NotSent {
		t.Fatal(r)
	}
	c.op.Lock()
	if r := c.inject(ctx, work); r.Outcome != broker.NotSent {
		t.Fatal(r)
	}
	c.op.Unlock()
	binding.History = append(binding.History, Binding{ID: "cleared", Transcript: "/tmp/clear.jsonl"})
	atomicJSON(bindingPath(home, parentPath), binding)
	if r := c.inject(ctx, work); r.Outcome != broker.NotSent {
		t.Fatal(r)
	}
	// Authentication bytes are harmless; a changed binding must never emit a user record.
	select {
	case row := <-records:
		if row["type"] == "user" {
			t.Fatal("old mail crossed published clear")
		}
	case <-time.After(time.Second):
	}
	atomicJSON(bindingPath(home, parentPath), Bindings{Fingerprint: fingerprint("token"), History: binding.History[:1]})
	if r := c.inject(ctx, work); r.Outcome != broker.Unconfirmed {
		t.Fatal(r)
	}
	gotUser := false
	deadline := time.After(time.Second)
	for !gotUser {
		select {
		case row := <-records:
			if row["type"] == "user" {
				gotUser = true
				message := row["message"].(map[string]any)
				if message["content"] != "committed envelope" {
					t.Fatal(row)
				}
			}
		case <-deadline:
			t.Fatal("no parent input")
		}
	}
	if len(c.state().Outstanding) != 0 {
		t.Fatal("completed call retained")
	}
	// Broker recovery/fencing is checked independently of the receiver's local state.
	work.Epoch = "old-broker"
	if r := c.inject(ctx, work); r.Outcome != broker.NotSent {
		t.Fatal(r)
	}
}
func TestMCPHasNoToolsAndEOFCancelsOwnerWait(t *testing.T) {
	var out bytes.Buffer
	if e := mcp(strings.NewReader("{\"id\":1,\"method\":\"initialize\"}\n{\"id\":2,\"method\":\"tools/list\"}\n"), &out); e != nil {
		t.Fatal(e)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[1], `"tools":[]`) {
		t.Fatal(out.String())
	}
	home := t.TempDir()
	t.Setenv("CLAUDE_CODE_MESSAGING_SOCKET", "/tmp/parent.sock")
	t.Setenv("CLAUDE_CODE_MESSAGING_TOKEN", "token")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e := Run(ctx, home, strings.NewReader(""), io.Discard, io.Discard); e != nil {
		t.Fatal(e)
	}
}

func TestChildWaitsForHookBeforeRegistration(t *testing.T) {
	c := &child{home: t.TempDir(), socket: "/tmp/unpublished.sock", token: "fresh", active: map[string]bool{}}
	if e := c.sync(context.Background()); e == nil {
		t.Fatal("registered without hook identity")
	}
	if c.state().Generation != "" {
		t.Fatal("generation exists without known transcript")
	}
}

func receiverSetup(t *testing.T) (string, *broker.Broker, context.Context) {
	home := t.TempDir()
	b, e := broker.New(home, filepath.Join(home, "config.json"), nil)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { b.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	server, e := b.Listen()
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { server.Close() })
	go server.Serve(ctx)
	os.MkdirAll(filepath.Join(home, "receivers"), 0700)
	return home, b, ctx
}
func bindHistory(t *testing.T, home, sock, token string, ids ...string) {
	h := []Binding{}
	for _, id := range ids {
		h = append(h, Binding{ID: id, Transcript: "/tmp/" + id + ".jsonl", Cwd: "/tmp"})
	}
	if e := atomicJSON(bindingPath(home, sock), Bindings{Fingerprint: fingerprint(token), History: h}); e != nil {
		t.Fatal(e)
	}
}

// Old process cleared A to B and is still running. Operator explicitly resumes A in a new process.
func TestResumeFencedConversationWhilePreviousProcessLives(t *testing.T) {
	fakeHerdr(t)
	home, b, ctx := receiverSetup(t)
	oldSock := filepath.Join(t.TempDir(), "old.sock")
	l, e := net.Listen("unix", oldSock)
	if e != nil {
		t.Fatal(e)
	}
	defer l.Close()
	go func() {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			var q Control
			json.NewDecoder(c).Decode(&q)
			json.NewEncoder(c).Encode(Reply{Status: Status{Session: "B", Generation: "gen-b", Instance: "old-inst"}})
			c.Close()
		}
	}()
	mk := func(id string) store.Session {
		return store.Session{ID: id, Name: "pane", Harness: "claude", State: "live", Pane: "test-pane", Hints: map[string]string{"endpoint": oldSock, "instance": "old-inst", "transcript": "/tmp/" + id}}
	}
	if e = b.Register(ctx, mk("A")); e != nil {
		t.Fatal(e)
	}
	if e = b.Rebind(ctx, "A", mk("B")); e != nil {
		t.Fatal(e)
	}
	parent := filepath.Join(t.TempDir(), "new-parent.sock")
	bindHistory(t, home, parent, "tok", "A")
	c := &child{home: home, socket: parent, token: "tok", instance: "new-inst", endpoint: "/tmp/new", pane: "test-pane", active: map[string]bool{}}
	e = c.sync(ctx)
	d, _ := b.Store.Read(ctx)
	t.Logf("resume A sync err=%v; A.Generation=%q instance=%q child.status=%+v", e, d.Sessions["A"].Generation, d.Sessions["A"].Hints["instance"], c.state())
	if e != nil {
		t.Errorf("explicit resume of fenced A refused: %v", e)
	}
}

func TestBindingMonitorCancelsWaitOnAtomicReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "binding.json")
	if e := os.WriteFile(path, []byte("before"), 0600); e != nil {
		t.Fatal(e)
	}
	before, e := os.Stat(path)
	if e != nil {
		t.Fatal(e)
	}
	call, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { watchBinding(call, path, before, cancel); close(done) }()
	next := path + ".next"
	if e = os.WriteFile(next, []byte("after"), 0600); e != nil {
		t.Fatal(e)
	}
	if e = os.Rename(next, path); e != nil {
		t.Fatal(e)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("hook rewrite did not interrupt long poll")
	}
}

func TestRegistrationFailureDoesNotLongPollBeforeRetry(t *testing.T) {
	fakeHerdr(t)
	home, _, ctx := receiverSetup(t)
	socket := filepath.Join(home, "parent.sock")
	bindHistory(t, home, socket, "token", "unregistered")
	// A pane absent from the synthetic herdr snapshot cannot register yet.
	c := &child{home: home, socket: socket, token: "token", pane: "absent", active: map[string]bool{}}
	if e := c.sync(ctx); e == nil {
		t.Fatal("missing pane registered")
	}
	if c.pollToken != "" {
		t.Fatal("registration failure armed a long poll")
	}
}
