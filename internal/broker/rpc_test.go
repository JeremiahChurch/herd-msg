package broker

import (
	"context"
	"database/sql"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRPCVersionAndFrameLimit(t *testing.T) {
	b, _ := setup(t)
	if r := b.Handle(ctx, Request{Version: Protocol + 1, Verb: "send", Caller: op, Send: request(b, "bad-version")}); r.Code == 0 {
		t.Fatal("unsupported version admitted")
	}
	d, _, _ := b.Data(ctx)
	if len(d.Messages) != 0 {
		t.Fatal("bad version mutated store")
	}
	if _, e := readFrame(strings.NewReader(strings.Repeat("x", MaxFrame) + "\n")); e == nil {
		t.Fatal("oversized frame accepted")
	}
	if _, e := readFrame(strings.NewReader("{}\n")); e != nil {
		t.Fatal(e)
	}
}
func TestCheckpointBusyHealthAndRetry(t *testing.T) {
	b, _ := setup(t)
	now := time.Now().UTC()
	b.Now = func() time.Time { return now }
	send(t, b, "sensitive")
	dispatch(t, b)
	u := url.URL{Scheme: "file", Path: filepath.Join(b.Store.Home, "mailbox.db")}
	reader, e := sql.Open("sqlite", u.String()+"?mode=ro")
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	tx, e := reader.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	var count int
	if e = tx.QueryRow("SELECT count(*) FROM messages").Scan(&count); e != nil {
		t.Fatal(e)
	}
	now = now.Add(31 * 24 * time.Hour)
	if e = b.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	d, _, _ := b.Data(ctx)
	if d.Health["checkpoint"].Check == "" {
		t.Fatal("busy checkpoint not reported")
	}
	tx.Rollback()
	if e = b.Sweep(ctx); e != nil {
		t.Fatal(e)
	}
	d, _, _ = b.Data(ctx)
	if _, exists := d.Health["checkpoint"]; exists {
		t.Fatal("checkpoint retry did not clear health")
	}
	if _, e = d.Find("sensitive"); e == nil {
		t.Fatal("retained swept body")
	}
	var attempts int
	db, e := sql.Open("sqlite", u.String()+"?mode=ro")
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	if e = db.QueryRow("SELECT count(*) FROM attempts").Scan(&attempts); e != nil || attempts != 0 {
		t.Fatal("retention left attempts", attempts, e)
	}
}
func TestResolveReportsAlreadyInjected(t *testing.T) {
	b, f := setup(t)
	f.result = Result{Outcome: Unconfirmed}
	send(t, b, "proof")
	dispatch(t, b)
	a := message(t, b, "proof").Attempts[0]
	f.accepted[a.ID] = true
	response := b.Handle(ctx, Request{Version: Protocol, Verb: "resolve", Caller: op, ID: "proof", Action: "resend", Confirm: true})
	if response.Code != 0 || !strings.Contains(string(response.Data), `"already_injected":true`) {
		t.Fatal(response)
	}
	if len(f.calls) != 1 {
		t.Fatal("settled proof resent")
	}
}
