package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestCachedSnapshotsAreIndependentAndObserveCommits(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := context.Background()
	seed := func(d *Data) error {
		session := Session{ID: "receiver", Hints: map[string]string{"instance": "original"}, Outstanding: []string{"work"}}
		d.Sessions[session.ID] = session
		d.Messages["local/mail"] = &Message{ID: "mail", Origin: "local", Recipient: session.ID, State: "injected", Attempts: []Attempt{{ID: "attempt", Session: session}}, History: []History{{Reason: "original"}}}
		d.Links["peer"] = Link{Sessions: []Session{session}}
		d.Health["check"] = Health{Detail: "original"}
		d.Dedupe["local/mail"] = Dedupe{ID: "mail"}
		return nil
	}
	if err = s.Update(c, seed); err != nil {
		t.Fatal(err)
	}
	original, err := s.Read(c)
	if err != nil {
		t.Fatal(err)
	}
	cached := s.cached
	snapshot, err := s.Read(c)
	if err != nil {
		t.Fatal(err)
	}
	if s.cached != cached {
		t.Fatal("unchanged read decoded the mailbox again")
	}
	mutate := func(d *Data) {
		v := d.Sessions["receiver"]
		v.Hints["instance"] = "mutated"
		v.Outstanding[0] = "mutated"
		m := d.Messages["local/mail"]
		m.Body = "mutated"
		m.History[0].Reason = "mutated"
		m.Attempts[0].Session.Hints["instance"] = "mutated"
		m.Attempts[0].Session.Outstanding[0] = "mutated"
		d.Links["peer"].Sessions[0].Hints["instance"] = "mutated"
		d.Links["peer"].Sessions[0].Outstanding[0] = "mutated"
		d.Health["check"] = Health{Detail: "mutated"}
		delete(d.Dedupe, "local/mail")
	}
	mutate(snapshot)
	if err = s.Update(c, func(d *Data) error { mutate(d); return errors.New("rollback") }); err == nil {
		t.Fatal("expected rollback")
	}
	again, err := s.Read(c)
	if err != nil || !reflect.DeepEqual(original, again) {
		t.Fatalf("snapshot or rollback mutated cache: %v", err)
	}
	if err = s.Update(c, func(d *Data) error { d.Messages["local/mail"].Body = "committed"; return nil }); err != nil {
		t.Fatal(err)
	}
	again, err = s.Read(c)
	if err != nil || again.Messages["local/mail"].Body != "committed" {
		t.Fatalf("cache missed writer commit: %v", err)
	}
	// A different SQLite connection may write during offline repairs or tests.
	u := url.URL{Scheme: "file", Path: filepath.Join(s.Home, "mailbox.db")}
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	payload, _ := json.Marshal(Session{ID: "receiver", Name: "external"})
	if _, err = db.Exec("UPDATE sessions SET payload=? WHERE key='receiver'", payload); err != nil {
		t.Fatal(err)
	}
	if err = s.Update(c, func(d *Data) error {
		if d.Sessions["receiver"].Name != "external" {
			return errors.New("writer cache missed external commit")
		}
		d.Sessions["receiver"] = Session{ID: "receiver", Name: "external-updated"}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	again, err = s.Read(c)
	if err != nil || again.Sessions["receiver"].Name != "external-updated" {
		t.Fatalf("cache missed external commit: %v", err)
	}
}

func TestReceiverStateReadsWithoutWriterAndNoOpUpdateDoesNotWrite(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := context.Background()
	if err = s.Update(c, func(d *Data) error { d.Sessions["receiver"] = Session{ID: "receiver"}; return nil }); err != nil {
		t.Fatal(err)
	}
	var before, after int
	if err = s.db.QueryRow("SELECT total_changes()").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err = s.Update(c, func(*Data) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err = s.db.QueryRow("SELECT total_changes()").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("no-op update wrote %d rows", after-before)
	}
	// Hold both the in-process writer mutex and an immediate SQLite transaction.
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	deadline, cancel := context.WithTimeout(c, time.Second)
	defer cancel()
	for i := 0; i < 20; i++ {
		d, err := s.ReadSessions(deadline)
		if err != nil || d.Sessions["receiver"].ID != "receiver" {
			t.Fatalf("poll needed writer: %v", err)
		}
		if len(d.Messages) != 0 {
			t.Fatal("poll included mailbox payloads")
		}
	}
}

func TestCancelledCachedReadCanRecover(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := context.Background()
	if _, err = s.Read(c); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		expired, cancel := context.WithTimeout(c, time.Nanosecond)
		_, _ = s.Read(expired)
		cancel()
		if err = s.Update(c, func(d *Data) error { d.Sequence++; return nil }); err != nil {
			t.Fatal(err)
		}
		d, err := s.Read(c)
		if err != nil || d.Sequence != int64(i+1) {
			t.Fatalf("read did not recover: %v", err)
		}
	}
}

func TestWriteProbeCommitsRealChangesAndReportsWriteFailures(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c := context.Background()
	for _, want := range []int{1, 0, 1} {
		if err = s.WriteProbe(c); err != nil {
			t.Fatal(err)
		}
		var value int
		if err = s.db.QueryRow("SELECT value FROM write_probe WHERE id=1").Scan(&value); err != nil || value != want {
			t.Fatalf("probe did not change durable row: got %d want %d: %v", value, want, err)
		}
	}
	// Reject an actual SQL write after BEGIN IMMEDIATE has succeeded.
	if _, err = s.db.Exec("CREATE TEMP TRIGGER reject_probe BEFORE INSERT ON write_probe BEGIN SELECT RAISE(ABORT, 'forced write failure'); END"); err != nil {
		t.Fatal(err)
	}
	if err = s.Update(c, func(*Data) error { return nil }); err != nil {
		t.Fatalf("no-op should not need a write: %v", err)
	}
	if err = s.WriteProbe(c); err == nil {
		t.Fatal("probe reported healthy while real writes failed")
	}
	if _, err = s.db.Exec("DROP TRIGGER reject_probe"); err != nil {
		t.Fatal(err)
	}
	if err = s.WriteProbe(c); err != nil {
		t.Fatalf("probe did not recover: %v", err)
	}
	cancelled, cancel := context.WithCancel(c)
	cancel()
	if err = s.WriteProbe(cancelled); err == nil {
		t.Fatal("cancelled probe committed")
	}
	var value int
	if err = s.db.QueryRow("SELECT value FROM write_probe WHERE id=1").Scan(&value); err != nil || value != 0 {
		t.Fatal("failed probe changed row", value, err)
	}
}

func TestChangeNotificationsFollowCommittedChangesOnly(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	changed := s.Changes()
	session := s.SessionChanges("one")
	if e = s.Update(context.Background(), func(d *Data) error { return nil }); e != nil {
		t.Fatal(e)
	}
	select {
	case <-changed:
		t.Fatal("no-op notified")
	default:
	}
	if e = s.Update(context.Background(), func(d *Data) error { d.Sessions["one"] = Session{ID: "one"}; return nil }); e != nil {
		t.Fatal(e)
	}
	select {
	case <-changed:
	default:
		t.Fatal("commit did not notify")
	}
	select {
	case <-session:
	default:
		t.Fatal("session did not notify")
	}
}
