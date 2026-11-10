package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/states"
)

func TestLookupSchedulingDoesNotRewriteAttemptHistory(t *testing.T) {
	ctx := context.Background()
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.Update(ctx, func(d *Data) error {
		d.Messages["local/m"] = &Message{ID: "m", Origin: "local", Recipient: "r", State: states.Indeterminate, Attempts: []Attempt{{ID: "a", Envelope: strings.Repeat("x", 32768), Outcome: "unconfirmed"}}, History: []History{{From: states.Injecting, To: states.Indeterminate, Reason: strings.Repeat("h", 8192)}}}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.db.Exec("CREATE TABLE writes (name TEXT)"); e != nil {
		t.Fatal(e)
	}
	for _, table := range []string{"messages", "attempts", "state_history", "dedupe", "schema_version", "write_probe"} {
		for _, op := range []string{"INSERT", "UPDATE", "DELETE"} {
			query := fmt.Sprintf("CREATE TRIGGER count_%s_%s AFTER %s ON %s BEGIN INSERT INTO writes VALUES ('%s:%s'); END", table, op, op, table, table, op)
			if _, e = s.db.Exec(query); e != nil {
				t.Fatal(e)
			}
		}
	}
	for i := 0; i < 20; i++ {
		if e = s.Update(ctx, func(d *Data) error {
			d.Messages["local/m"].NextLookup = time.Now().Add(time.Duration(i) * time.Second)
			return nil
		}); e != nil {
			t.Fatal(e)
		}
	}
	rows, e := s.db.Query("SELECT name,count(*) FROM writes GROUP BY name ORDER BY name")
	if e != nil {
		t.Fatal(e)
	}
	childWrites := 0
	for rows.Next() {
		var name string
		var n int
		if e = rows.Scan(&name, &n); e != nil {
			t.Fatal(e)
		}
		t.Logf("%s=%d", name, n)
		if strings.HasPrefix(name, "attempts:") || strings.HasPrefix(name, "state_history:") {
			childWrites += n
		}
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	rows.Close()
	info, e := os.Stat(filepath.Join(s.Home, "mailbox.db-wal"))
	if e != nil {
		t.Fatal(e)
	}
	t.Logf("WAL after 20 receipt timer updates=%d bytes", info.Size())
	if childWrites != 0 {
		t.Fatal("receipt timer rewrote immutable attempt or history", childWrites)
	}
	if e = s.Checkpoint(ctx); e != nil {
		t.Fatal(e)
	}
	info, e = os.Stat(filepath.Join(s.Home, "mailbox.db-wal"))
	if e != nil {
		t.Fatal(e)
	}
	if info.Size() != 0 {
		t.Fatal("WAL did not truncate", info.Size())
	}
}

func TestInflightHandoffPreservesAttemptOwnership(t *testing.T) {
	ctx := context.Background()
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if e = s.Update(ctx, func(d *Data) error {
		d.Messages["local/a"] = &Message{ID: "a", Origin: "local", Recipient: "r", State: states.Claimed, Attempts: []Attempt{{ID: "immutable"}}}
		d.Messages["local/b"] = &Message{ID: "b", Origin: "local", Recipient: "r", State: states.Queued}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e = s.Update(ctx, func(d *Data) error {
		d.Messages["local/a"].State = states.Queued
		d.Messages["local/b"].State = states.Claimed
		return nil
	}); e != nil {
		t.Fatal("atomic inflight handoff failed", e)
	}
	if e = s.Update(ctx, func(d *Data) error { d.Messages["local/b"].Attempts = []Attempt{{ID: "immutable"}}; return nil }); e == nil {
		t.Fatal("duplicate attempt ownership accepted")
	}
	var owner string
	if e = s.db.QueryRow("SELECT message_key FROM attempts WHERE id='immutable'").Scan(&owner); e != nil || owner != "local/a" {
		t.Fatal("immutable attempt ownership changed", owner, e)
	}
}
