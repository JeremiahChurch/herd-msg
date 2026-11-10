package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func TestExactStatusContract(t *testing.T) {
	fixture, e := os.ReadFile("testdata/status.json")
	if e != nil {
		t.Fatal(e)
	}
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	d := store.Empty()
	c := policy.Defaults()
	c.Links["peer"] = policy.Grant{}
	d.Sessions["receiver"] = store.Session{ID: "receiver", Name: "reviewer", Harness: "claude", State: "live", Blocked: true}
	d.Messages["local/pending"] = &store.Message{ID: "pending", Sender: "builder.codex@local", Recipient: "receiver", Destination: "local", State: states.Queued, Created: now, Updated: now, Body: "hello\nnext line", Error: "recipient blocked"}
	d.Messages["local/accepted"] = &store.Message{ID: "accepted", Sender: "builder.codex@local", Recipient: "receiver", Destination: "local", State: states.Injected, Created: now, Updated: now, TerminalAt: now, Body: "accepted body", Attempts: []store.Attempt{{ID: "attempt"}}}
	d.Health["driver"] = store.Health{Check: "driver", Detail: "probe passed", Since: now}
	got, _ := json.Marshal(BuildStatus(d, c, "test", false, now))
	var gotObject, wantObject any
	json.Unmarshal(got, &gotObject)
	json.Unmarshal(fixture, &wantObject)
	if !reflect.DeepEqual(gotObject, wantObject) {
		t.Fatalf("status contract mismatch\ngot %s\nwant %s", got, fixture)
	}
	// This explicit shape assertion catches coordinated accidental drift of code and fixture.
	root := wantObject.(map[string]any)
	keys(t, root, "schema machine version killed counts sessions links pending recent health")
	keys(t, root["counts"].(map[string]any), "outbox held queued claimed injecting indeterminate dead sent injected dropped expired")
	for _, kind := range []string{"pending", "recent"} {
		keys(t, root[kind].([]any)[0].(map[string]any), "id from to state created updated first_line attempts error")
	}
	keys(t, root["sessions"].([]any)[0].(map[string]any), "id name harness state pane last_seen opted_out unsent blocked")
	keys(t, root["links"].([]any)[0].(map[string]any), "machine auto_run mode reachable last_contact error")
	keys(t, root["health"].([]any)[0].(map[string]any), "check detail since fix")
	if _, e = DecodeStatus([]byte(`{"schema":2}`)); e == nil {
		t.Fatal("newer schema accepted")
	}
	if _, e = DecodeStatus(append([]byte(`{"extra":true,`), fixture[1:]...)); e != nil {
		t.Fatal("additive fields rejected", e)
	}
}
func keys(t *testing.T, m map[string]any, want string) {
	t.Helper()
	list := strings.Fields(want)
	if len(m) != len(list) {
		t.Fatal("wrong contract keys", m, want)
	}
	for _, k := range list {
		if _, ok := m[k]; !ok {
			t.Fatal("missing", k)
		}
	}
}
func TestCompletePendingBoundedRecentAndOffline(t *testing.T) {
	now := time.Now()
	d := store.Empty()
	c := policy.Defaults()
	for i := 0; i < 1500; i++ {
		id := fmt.Sprintf("pending-%04d", i)
		d.Messages[id] = &store.Message{ID: id, State: states.Indeterminate, Sequence: int64(i)}
	}
	for i := 0; i < 110; i++ {
		id := fmt.Sprint("recent-", i)
		d.Messages[id] = &store.Message{ID: id, State: states.Dropped, TerminalAt: now.Add(time.Duration(i) * time.Second)}
	}
	d.Sessions["old"] = store.Session{ID: "old", State: "dead"}
	status := BuildStatus(d, c, "test", true, now)
	if len(status.Pending) != 1500 || len(status.Recent) != 100 || status.Recent[0].ID != "recent-109" || status.Pending[0].ID != "pending-0000" || status.Sessions[0].State != "offline" {
		t.Fatal("status truncated or reordered")
	}
	if got := preview("a\t\x1b" + strings.Repeat("界", 200) + "\nsecret"); len([]rune(got)) != 160 || strings.ContainsAny(got, "\t\x1b\n") {
		t.Fatal("unsafe preview", got)
	}
	b, _ := json.Marshal(BuildStatus(store.Empty(), c, "test", true, now))
	if strings.Contains(string(b), "null") {
		t.Fatal("null array", string(b))
	}
}

func TestWatchTextIsCompactAndKeepsPendingMailVisible(t *testing.T) {
	s := Status{Machine: "node-a", Version: "test", Counts: map[states.State]int{states.Queued: 12}, Pending: []MessageStatus{}}
	for i := 0; i < 12; i++ {
		s.Pending = append(s.Pending, MessageStatus{ID: fmt.Sprintf("mail-%d", i), State: states.Queued, FirstLine: "hello", To: "receiver"})
	}
	var out bytes.Buffer
	if e := RenderWatch(&out, s); e != nil {
		t.Fatal(e)
	}
	text := out.String()
	if !strings.Contains(text, "Queued 12") || !strings.Contains(text, "mail-0") || !strings.Contains(text, "4 more") || strings.Count(text, "\n") > 24 || strings.Contains(text, `"schema"`) {
		t.Fatal("unbounded or missing viewer text", text)
	}
}

func TestViewerShowsPausedReceiverBeforeIdleDetachedSessions(t *testing.T) {
	s := Status{Machine: "node-a", Counts: map[states.State]int{}, Health: []HealthStatus{}}
	for i := 0; i < 6; i++ {
		s.Health = append(s.Health, HealthStatus{Check: fmt.Sprintf("probe/session/%d", i), Detail: "detached"})
	}
	s.Health = append(s.Health, HealthStatus{Check: "receiver/stuck", Detail: "paused receiver"})
	var out bytes.Buffer
	if e := RenderWatch(&out, s); e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(out.String(), "paused receiver") || !strings.Contains(out.String(), "status --json") {
		t.Fatal(out.String())
	}
}

func TestExpiredMailIsRecentAndNotWaiting(t *testing.T) {
	now := time.Now()
	d := store.Empty()
	c := policy.Defaults()
	d.Sessions["receiver"] = store.Session{ID: "receiver", State: "offline"}
	m := &store.Message{ID: "expired", Origin: "local", Recipient: "receiver", Destination: "local", State: states.Queued, Created: now}
	d.Messages["local/expired"] = m
	if e := d.Move(m, states.Expired, "broker", "recipient unavailable beyond offline grace", now); e != nil {
		t.Fatal(e)
	}
	s := BuildStatus(d, c, "test", false, now)
	if s.Counts[states.Expired] != 1 || len(s.Pending) != 0 || len(s.Recent) != 1 || s.Recent[0].Error == "" || s.Sessions[0].Unsent != 0 {
		t.Fatal(s)
	}
}
