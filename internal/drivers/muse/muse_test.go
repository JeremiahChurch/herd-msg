package muse

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

const testID = "01a1ad56-aebc-7d03-ac6f-375ac3d6b9af"

func admission(session, body, status string) map[string]any {
	return map[string]any{
		"schema_version": 1, "stream": map[string]string{"kind": "session", "id": session}, "durability": "durable",
		"payload_type": "peer_message.admission.settled", "payload_schema_version": 2,
		"payload": map[string]any{"schema_version": 2, "cause": "authenticated_external_agent", "outcomes": []any{map[string]any{
			"status": status, "inbox_item": map[string]any{"body": body, "source": map[string]any{
				"schema_version": 2, "authority": "runtime_context", "source": "session_message", "ingress_provenance": "external_agent", "target_session_id": session,
			}},
		}}},
	}
}

func encode(v any) []byte { b, _ := json.Marshal(v); return b }

func TestAcceptanceRequiresExactDurableTargetAdmission(t *testing.T) {
	body := "[herd-msg] from sender | id mail | attempt attempt\n---\nhello"
	for _, tc := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"other session", func(r map[string]any) { r["stream"].(map[string]string)["id"] = "other" }},
		{"not durable", func(r map[string]any) { r["durability"] = "live_only" }},
		{"unknown schema", func(r map[string]any) { r["payload_schema_version"] = 3 }},
		{"quoted in tool output", func(r map[string]any) { r["payload_type"] = "runtime.session" }},
		{"rejected", func(r map[string]any) { outcome(r)["status"] = "rejected" }},
		{"other body", func(r map[string]any) { item(r)["body"] = "quote: " + body }},
		{"wrong target", func(r map[string]any) { source(r)["target_session_id"] = "other" }},
		{"peer provenance", func(r map[string]any) { source(r)["ingress_provenance"] = "muse_peer" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := admission(testID, body, "accepted")
			tc.edit(r)
			if ok, e := accepted(encode(r), testID, body, 0); e != nil || ok {
				t.Fatal(ok, e)
			}
		})
	}
	r := admission(testID, body, "accepted")
	if ok, e := accepted(encode(r), testID, body, 0); e != nil || !ok {
		t.Fatal(ok, e)
	}
	frame := map[string]any{"schema_version": 1, "stream": map[string]string{"kind": "session", "id": testID}, "durability": "durable", "payload_type": "retained_frame", "payload": map[string]any{"children": []any{map[string]string{"record_json": string(encode(r))}}}}
	if ok, e := accepted(encode(frame), testID, body, 0); e != nil || !ok {
		t.Fatal("retained admission", ok, e)
	}
}

func outcome(r map[string]any) map[string]any {
	return r["payload"].(map[string]any)["outcomes"].([]any)[0].(map[string]any)
}
func item(r map[string]any) map[string]any   { return outcome(r)["inbox_item"].(map[string]any) }
func source(r map[string]any) map[string]any { return item(r)["source"].(map[string]any) }

type fixture struct {
	root, log string
	process   *exec.Cmd
}

func write(t *testing.T, path string, b []byte, mode os.FileMode) {
	t.Helper()
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(path, b, mode); e != nil {
		t.Fatal(e)
	}
}
func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }

func setup(t *testing.T, optIn ...string) fixture {
	t.Helper()
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", root)
	t.Setenv("MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS", "1")
	p := exec.Command("sleep", "300")
	flag := "1"
	if len(optIn) > 0 {
		flag = optIn[0]
	}
	p.Env = append(os.Environ(), "MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS="+flag)
	if e := p.Start(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = p.Process.Kill(); _ = p.Wait() })
	log := filepath.Join(root, "muse", "sessions", "2026", "11", "05", testID, "session.jsonl")
	write(t, log, nil, 0600)
	db, e := sql.Open("sqlite", filepath.Join(root, "muse", "session-index.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if _, e = db.Exec("CREATE TABLE sessions (session_id TEXT PRIMARY KEY, session_log_path TEXT)"); e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec("INSERT INTO sessions VALUES (?, ?)", testID, log); e != nil {
		t.Fatal(e)
	}
	write(t, filepath.Join(root, "muse", "runtime", "muse", "sessions", testID+".json"), encode(map[string]any{"schema_version": 1, "session_id": testID, "process_generation_hint": "pid=" + strconv.Itoa(p.Process.Pid)}), 0600)
	t.Setenv("PATH", filepath.Join(root, "bin")+":"+os.Getenv("PATH"))
	herdr := "#!/bin/sh\ncase \"$1 $2\" in\n'agent list') cat " + shellQuote(filepath.Join(root, "agents.json")) + ";;\n'pane process-info') printf '%s\\n' " + shellQuote(string(encode(map[string]any{"result": map[string]any{"process_info": map[string]any{"pane_id": "w1:p1", "foreground_processes": []any{map[string]any{"pid": p.Process.Pid}}}}}))) + ";;\n*) exit 2;;\nesac\n"
	write(t, filepath.Join(root, "bin", "herdr"), []byte(herdr), 0700)
	f := fixture{root, log, p}
	f.state(t, "idle")
	write(t, filepath.Join(root, "list.json"), encode(map[string]any{"schema_version": 1, "sessions": []any{map[string]string{"session_id": testID, "session_name": "muse-test"}}}), 0600)
	f.command(t, false)
	return f
}

func (f fixture) state(t *testing.T, state string) {
	write(t, filepath.Join(f.root, "agents.json"), encode(map[string]any{"result": map[string]any{"agents": []any{map[string]string{"agent": "muse", "agent_status": state, "pane_id": "w1:p1"}}}}), 0600)
}
func (f fixture) command(t *testing.T, accept bool) {
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> " + shellQuote(filepath.Join(f.root, "commands")) + "\ncase \"$1 $2\" in\n'--version ') printf '%s\\n' " + shellQuote(version) + ";;\n'session-message list') cat " + shellQuote(filepath.Join(f.root, "list.json")) + ";;\n'session-message send') cat > " + shellQuote(filepath.Join(f.root, "sent")) + "\n"
	if accept {
		script += "cat " + shellQuote(filepath.Join(f.root, "admission")) + " >> " + shellQuote(f.log) + "\n"
	}
	script += "printf '%s\\n' '{\"status\":\"unavailable\",\"error_code\":\"unverified_target_receipt\"}'\nexit 1;;\n*) exit 2;;\nesac\n"
	write(t, filepath.Join(f.root, "bin", "muse"), []byte(script), 0700)
}

func TestDiscoveryUsesProcessIdentityAndBlocksUnreadyReceivers(t *testing.T) {
	f := setup(t)
	d := &Driver{}
	for _, state := range []string{"idle", "working", "done", "blocked", "unknown"} {
		f.state(t, state)
		peers, e := d.Discover(context.Background())
		if e != nil || len(peers) != 1 {
			t.Fatal(peers, e)
		}
		if s := peers[0]; s.ID != testID || s.Pane != "w1:p1" || s.State != "live" || s.Blocked != (state == "working" || state == "blocked" || state == "unknown") {
			t.Fatal(s)
		}
	}
	// A registry PID which does not belong to the pane must not bind by name/cwd.
	write(t, filepath.Join(f.root, "muse", "runtime", "muse", "sessions", testID+".json"), encode(map[string]any{"schema_version": 1, "session_id": testID, "process_generation_hint": "pid=1"}), 0600)
	peers, e := d.Discover(context.Background())
	if e != nil || peers[0].State != "detached" || peers[0].Pane != "" {
		t.Fatal(peers, e)
	}
}

func TestClosedIngressAndUnknownVersionsHoldMail(t *testing.T) {
	f := setup(t, "0")
	d := &Driver{}
	peers, e := d.Discover(context.Background())
	if e != nil || len(peers) != 1 || !peers[0].Blocked || !strings.Contains(peers[0].Reason, "per-process") {
		t.Fatal(peers, e)
	}
	r, e := d.Inject(context.Background(), peers[0], "never sent", "attempt")
	if e != nil || r.Outcome != broker.NotSent {
		t.Fatal(r, e)
	}
	if _, e := os.Stat(filepath.Join(f.root, "sent")); !os.IsNotExist(e) {
		t.Fatal("closed ingress was sent input", e)
	}
	path := filepath.Join(f.root, "bin", "muse")
	b, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	write(t, path, []byte(strings.ReplaceAll(string(b), version, "Muse Code 1.4.15")), 0700)
	if _, e := d.Discover(context.Background()); e == nil || !strings.Contains(e.Error(), "unverified") {
		t.Fatal("unverified version accepted", e)
	}
}

func TestSendNeedsTranscriptProofEvenWhenCLIClaimsAcceptance(t *testing.T) {
	f := setup(t)
	d := &Driver{}
	s := store.Session{ID: testID, Harness: "muse"}
	body := "[herd-msg] attempt unique\n---\nbody with --json and quotes ' \""
	r, e := d.Inject(context.Background(), s, body, "unique")
	if e != nil || r.Outcome != broker.Unconfirmed {
		t.Fatal(r, e)
	}
	b, e := os.ReadFile(filepath.Join(f.root, "sent"))
	if e != nil || string(b) != body {
		t.Fatal(string(b), e)
	}
	write(t, filepath.Join(f.root, "admission"), append(encode(admission(testID, body, "accepted")), '\n'), 0600)
	f.command(t, true)
	r, e = d.Inject(context.Background(), s, body, "unique")
	if e != nil || r.Outcome != broker.Accepted {
		t.Fatal(r, e)
	}
	f.state(t, "blocked")
	_ = os.Remove(filepath.Join(f.root, "sent"))
	r, e = d.Inject(context.Background(), s, body, "other")
	if e != nil || r.Outcome != broker.NotSent {
		t.Fatal(r, e)
	}
	if _, e := os.Stat(filepath.Join(f.root, "sent")); !os.IsNotExist(e) {
		t.Fatal("blocked receiver was sent input", e)
	}
}

func TestLookupRejectsUnrelatedIndexPathsAndUnavailableHistory(t *testing.T) {
	f := setup(t)
	d := &Driver{}
	r := broker.LookupRequest{Attempt: store.Attempt{Session: store.Session{ID: testID}, Envelope: "exact body"}}
	write(t, f.log, []byte("{partial"), 0600)
	if ok, e := d.Lookup(context.Background(), r); e == nil || ok {
		t.Fatal("partial transcript confirmed", ok, e)
	}
	db, e := sql.Open("sqlite", filepath.Join(f.root, "muse", "session-index.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if _, e = db.Exec("UPDATE sessions SET session_log_path = ?", filepath.Join(f.root, "unrelated", testID, "session.jsonl")); e != nil {
		t.Fatal(e)
	}
	if ok, e := d.Lookup(context.Background(), r); e == nil || ok {
		t.Fatal("unexpected transcript path accepted", ok, e)
	}
}

func TestMissingTranscriptHoldsMailBeforeSend(t *testing.T) {
	f := setup(t)
	if e := os.Remove(f.log); e != nil {
		t.Fatal(e)
	}
	d := &Driver{}
	peers, e := d.Discover(context.Background())
	if e != nil || len(peers) != 1 || !peers[0].Blocked || !strings.Contains(peers[0].Reason, "transcript") {
		t.Fatal(peers, e)
	}
	r, e := d.Inject(context.Background(), peers[0], "never sent", "attempt")
	if e != nil || r.Outcome != broker.NotSent {
		t.Fatal(r, e)
	}
	if _, e := os.Stat(filepath.Join(f.root, "sent")); !os.IsNotExist(e) {
		t.Fatal("unverifiable receiver was sent input", e)
	}
}

func TestBrokerRefreshSharesDiscoveryButSendRechecks(t *testing.T) {
	f := setup(t)
	d := &Driver{}
	home := filepath.Join(t.TempDir(), "mailbox")
	b, e := broker.New(home, filepath.Join(home, "config.json"), map[string]broker.Driver{"muse": d})
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	s := store.Session{ID: testID, Name: "target", Harness: "muse", State: "live"}
	for _, id := range []string{testID, "01a1ad56-aebc-7d03-ac6f-375ac3d6b9aa", "01a1ad56-aebc-7d03-ac6f-375ac3d6b9ab"} {
		peer := s
		peer.ID, peer.Name = id, id
		if e := b.Register(context.Background(), peer); e != nil {
			t.Fatal(e)
		}
	}
	if e := b.RefreshAvailability(context.Background()); e != nil {
		t.Fatal(e)
	}
	commands, e := os.ReadFile(filepath.Join(f.root, "commands"))
	if e != nil || strings.Count(string(commands), "--version\n") != 1 || strings.Count(string(commands), "session-message list --json\n") != 1 {
		t.Fatal("refresh repeated discovery", string(commands), e)
	}
	pass := d.AvailabilityContext(context.Background())
	if _, e := d.SessionState(pass, s); e != nil {
		t.Fatal(e)
	}
	f.state(t, "blocked")
	r, e := d.Inject(pass, s, "never sent", "attempt")
	if e != nil || r.Outcome != broker.NotSent {
		t.Fatal("send reused stale availability", r, e)
	}
	if _, e := os.Stat(filepath.Join(f.root, "sent")); !os.IsNotExist(e) {
		t.Fatal("blocked receiver was sent input", e)
	}
	// Simulate reopening this mailbox without the opt-in. Stored peers must
	// remain visible without launching even a version probe.
	t.Setenv("MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS", "")
	_ = os.Remove(filepath.Join(f.root, "commands"))
	if e := b.RefreshAvailability(context.Background()); e != nil {
		t.Fatal(e)
	}
	r, e = d.Inject(context.Background(), s, "never sent", "attempt")
	if e != nil || r.Outcome != broker.NotSent {
		t.Fatal(r, e)
	}
	if _, e := os.Stat(filepath.Join(f.root, "commands")); !os.IsNotExist(e) {
		t.Fatal("opted-out broker launched Muse", e)
	}
}

func TestVersionCacheInvalidatesAndDiscoverySkipsAbsentMusePanes(t *testing.T) {
	f := setup(t)
	d := &Driver{}
	for i := 0; i < 5; i++ {
		if e := d.Probe(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	commands, _ := os.ReadFile(filepath.Join(f.root, "commands"))
	if strings.Count(string(commands), "--version\n") != 1 {
		t.Fatal(string(commands))
	}
	// Updating the payload also invalidates an unchanged launcher.
	write(t, filepath.Join(f.root, "bin", "muse-bin-test"), []byte("payload"), 0700)
	if e := d.Probe(context.Background()); e != nil {
		t.Fatal(e)
	}
	commands, _ = os.ReadFile(filepath.Join(f.root, "commands"))
	if strings.Count(string(commands), "--version\n") != 2 {
		t.Fatal(string(commands))
	}
	write(t, filepath.Join(f.root, "agents.json"), encode(map[string]any{"result": map[string]any{"agents": []any{}}}), 0600)
	os.Remove(filepath.Join(f.root, "commands"))
	peers, e := d.Discover(context.Background())
	if e != nil || len(peers) != 0 {
		t.Fatal(peers, e)
	}
	if commands, e := os.ReadFile(filepath.Join(f.root, "commands")); !os.IsNotExist(e) {
		t.Fatal("Muse launched with no panes", string(commands), e)
	}
}

func TestLookupSeeksPastLargeHistoryAndSurvivesRestart(t *testing.T) {
	f := setup(t)
	file, e := os.OpenFile(f.log, os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	if e = file.Truncate(537 << 20); e != nil {
		t.Fatal(e)
	}
	if _, e = file.WriteAt([]byte("\n"), (537<<20)-1); e != nil {
		t.Fatal(e)
	}
	file.Close()
	d := &Driver{}
	proof, e := d.seedLookup(context.Background(), testID, "large-history")
	if e != nil {
		t.Fatal(e)
	}
	body := "[herd-msg] attempt large-history\nbody"
	file, e = os.OpenFile(f.log, os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		t.Fatal(e)
	}
	_, e = file.Write(append(encode(admission(testID, body, "accepted")), '\n'))
	file.Close()
	if e != nil {
		t.Fatal(e)
	}
	// A fresh driver must use the durable pre-send offset, not scan 537 MB.
	fresh := &Driver{}
	r := broker.LookupRequest{Attempt: store.Attempt{ID: "large-history", Session: store.Session{ID: testID}, Envelope: body, Proof: proof}}
	if ok, e := fresh.Lookup(context.Background(), r); !ok || e != nil {
		t.Fatal(ok, e)
	}
	cursor := fresh.lookups[testID+"/large-history"]
	if cursor.Offset < 537<<20 || cursor.Offset > (537<<20)+4096 {
		t.Fatal("did not seek to receipt", cursor.Offset)
	}
}

func TestLookupAdvancesWithinBudgetAndRetriesPartialAppend(t *testing.T) {
	f := setup(t)
	d := &Driver{}
	body := "exact admission"
	line := []byte("{\"padding\":\"" + strings.Repeat("x", 1024) + "\"}\n")
	file, e := os.OpenFile(f.log, os.O_WRONLY|os.O_TRUNC, 0600)
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 5000; i++ {
		if _, e = file.Write(line); e != nil {
			t.Fatal(e)
		}
	}
	file.Close()
	r := broker.LookupRequest{Attempt: store.Attempt{ID: "bounded", Session: store.Session{ID: testID}, Envelope: body}}
	if found, _ := d.Lookup(context.Background(), r); found {
		t.Fatal("unrelated history confirmed")
	}
	cursor := d.lookups[testID+"/bounded"]
	first := cursor.Offset
	if first == 0 || first > transcriptReadLimit {
		t.Fatal("lookup exceeded read budget", first)
	}
	admission := append(encode(admission(testID, body, "accepted")), '\n')
	file, e = os.OpenFile(f.log, os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		t.Fatal(e)
	}
	file.Write(admission[:len(admission)/2])
	file.Close()
	if found, e := d.Lookup(context.Background(), r); found || e == nil {
		t.Fatal("partial admission accepted", found, e)
	}
	beforePartial := cursor.Offset
	if beforePartial <= first {
		t.Fatal("lookup replayed old bytes")
	}
	file, e = os.OpenFile(f.log, os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		t.Fatal(e)
	}
	file.Write(admission[len(admission)/2:])
	file.Close()
	if found, e := d.Lookup(context.Background(), r); !found || e != nil {
		t.Fatal("appended receipt missed", found, e)
	}
	// Replacement must clear the old positive cache and byte offset.
	if e = os.Rename(f.log, f.log+".old"); e != nil {
		t.Fatal(e)
	}
	write(t, f.log, []byte("{}\n"), 0600)
	if found, e := d.Lookup(context.Background(), r); found || e != nil {
		t.Fatal("rotated file reused proof", found, e)
	}
}

func TestPreSendPartialRecordIsSkippedAfterRestart(t *testing.T) {
	f := setup(t)
	write(t, f.log, []byte("{\"padding\":\"unfinished"), 0600)
	proof, e := (&Driver{}).seedLookup(context.Background(), testID, "partial-start")
	if e != nil {
		t.Fatal(e)
	}
	body := "receipt after partial pre-send record"
	file, e := os.OpenFile(f.log, os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		t.Fatal(e)
	}
	file.Write([]byte(" suffix\"}\n"))
	file.Write(append(encode(admission(testID, body, "accepted")), '\n'))
	file.Close()
	fresh := &Driver{}
	r := broker.LookupRequest{Attempt: store.Attempt{ID: "partial-start", Session: store.Session{ID: testID}, Envelope: body, Proof: proof}}
	if found, e := fresh.Lookup(context.Background(), r); !found || e != nil {
		t.Fatal("pre-send suffix blocked receipt", found, e)
	}
}

func TestLargeRecordContinuesAcrossLookupBudgets(t *testing.T) {
	f := setup(t)
	body := "receipt after large record"
	d := &Driver{}
	record := append(encode(map[string]string{"padding": strings.Repeat("x", 6<<20)}), '\n')
	data := append(record, append(encode(admission(testID, body, "accepted")), '\n')...)
	write(t, f.log, data, 0600)
	r := broker.LookupRequest{Attempt: store.Attempt{ID: "large-record", Session: store.Session{ID: testID}, Envelope: body}}
	if found, _ := d.Lookup(context.Background(), r); found {
		t.Fatal("scan exceeded first byte budget")
	}
	cursor := d.lookups[testID+"/large-record"]
	if cursor.Offset != transcriptReadLimit || len(cursor.Pending) != transcriptReadLimit {
		t.Fatal("lost large-record progress", cursor.Offset, len(cursor.Pending))
	}
	if found, e := d.Lookup(context.Background(), r); !found || e != nil {
		t.Fatal("large record hid later receipt", found, e)
	}
}
