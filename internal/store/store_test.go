package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/states"
)

func TestAtomicStoreAndSnapshots(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	s, e := Open(home)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	if other, e := Open(home); e == nil {
		other.Close()
		t.Fatal("second writer opened")
	}
	if e = s.Update(ctx, func(d *Data) error {
		d.Messages["local/test"] = &Message{ID: "test", Origin: "local", Recipient: "receiver", State: states.Queued, Sequence: d.NextSequence()}
		d.Sessions["receiver"] = Session{ID: "receiver"}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e = s.Update(ctx, func(d *Data) error { delete(d.Messages, "local/test"); return errors.New("rollback") }); e == nil {
		t.Fatal("expected rollback")
	}
	if e = s.Update(ctx, func(d *Data) error {
		m, _ := d.Find("test")
		if e := d.Move(m, states.Claimed, "broker", "claim", time.Now()); e != nil {
			return e
		}
		m.Attempts = append(m.Attempts, Attempt{ID: "attempt-a"})
		return d.Move(m, states.Injecting, "broker", "attempt committed", time.Now())
	}); e != nil {
		t.Fatal(e)
	}
	d, e := Snapshot(ctx, home)
	if e != nil {
		t.Fatal(e)
	}
	m, _ := d.Find("test")
	if m.State != states.Injecting || len(m.Attempts) != 1 || len(m.History) != 2 {
		t.Fatal("incoherent snapshot", m)
	}
	var count int
	if e = s.db.QueryRow("SELECT count(*) FROM attempts").Scan(&count); e != nil || count != 1 {
		t.Fatal(count, e)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, e := os.Stat(filepath.Join(home, "mailbox.db") + suffix)
		if e != nil {
			t.Fatal(e)
		}
		if info.Mode().Perm() != 0600 {
			t.Fatalf("%s mode %o", suffix, info.Mode().Perm())
		}
	}
	if e = s.Update(ctx, func(d *Data) error {
		d.Messages["local/other"] = &Message{ID: "other", Origin: "local", Recipient: "receiver", State: states.Claimed}
		return nil
	}); e == nil {
		t.Fatal("overlapping recipient claims accepted")
	}
	if e = s.Update(ctx, func(d *Data) error { delete(d.Messages, "local/test"); return nil }); e != nil {
		t.Fatal(e)
	}
	s.db.QueryRow("SELECT count(*) FROM attempts").Scan(&count)
	if count != 0 {
		t.Fatal("orphan attempt")
	}
}
func TestSnapshotMissingAndNewerSchema(t *testing.T) {
	home := filepath.Join(t.TempDir(), "absent")
	d, e := Snapshot(context.Background(), home)
	if e != nil || len(d.Messages) != 0 {
		t.Fatal(d, e)
	}
	if _, e = os.Stat(home); !os.IsNotExist(e) {
		t.Fatal("offline snapshot created home")
	}
	s, e := Open(home)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.db.Exec("UPDATE schema_version SET version=2"); e != nil {
		t.Fatal(e)
	}
	if _, e = Snapshot(context.Background(), home); e == nil {
		t.Fatal("newer schema accepted")
	}
	s.Close()
	if s, e = Open(home); e == nil {
		s.Close()
		t.Fatal("writer accepted newer schema")
	}
}

func TestCleanClosedReadOnlySnapshot(t *testing.T) {
	home := t.TempDir()
	s, e := Open(home)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Update(context.Background(), func(d *Data) error { d.Sessions["saved"] = Session{ID: "saved"}; return nil }); e != nil {
		t.Fatal(e)
	}
	s.Close()
	before, e := os.ReadFile(filepath.Join(home, "mailbox.db"))
	if e != nil {
		t.Fatal(e)
	}
	d, e := Snapshot(context.Background(), home)
	if e != nil || d.Sessions["saved"].ID != "saved" {
		t.Fatal(d, e)
	}
	after, _ := os.ReadFile(filepath.Join(home, "mailbox.db"))
	if string(before) != string(after) {
		t.Fatal("clean snapshot mutated database")
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		info, e := os.Stat(filepath.Join(home, "mailbox.db") + suffix)
		if e == nil && info.Mode().Perm() != 0600 {
			t.Fatalf("sidecar %s mode %o", suffix, info.Mode().Perm())
		}
		if e != nil && !os.IsNotExist(e) {
			t.Fatal(e)
		}
	}
}
