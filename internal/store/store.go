package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/states"

	_ "modernc.org/sqlite"
)

const Schema = 1

type Store struct {
	db             *sql.DB
	lock           *os.File
	Home           string
	mu             sync.Mutex
	readonly       bool
	reader         *sql.DB
	readConn       *sql.Conn
	readMu         sync.Mutex
	cached         *Data
	dataVersion    int64
	changeMu       sync.Mutex
	changed        chan struct{}
	sessionChanges map[string]chan struct{}
}

func Home(path string) (string, error) {
	if path == "" {
		path = os.Getenv("HERD_MSG_HOME")
	}
	if path == "" {
		h, e := os.UserHomeDir()
		if e != nil {
			return "", e
		}
		path = filepath.Join(h, ".local", "share", "herd-msg")
	}
	abs, e := filepath.Abs(path)
	if e != nil {
		return "", e
	}
	return abs, nil
}
func PrivateDir(path string) error {
	if e := os.MkdirAll(path, 0700); e != nil {
		return e
	}
	s, e := os.Stat(path)
	if e != nil {
		return e
	}
	if !s.IsDir() {
		return errors.New("mailbox path is not a directory")
	}
	if v, ok := s.Sys().(*syscall.Stat_t); !ok || v.Uid != uint32(os.Getuid()) {
		return errors.New("mailbox directory is not owned by this account")
	}
	return os.Chmod(path, 0700)
}
func privateFile(path string) error {
	info, e := os.Lstat(path)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s must be a regular file", filepath.Base(path))
	}
	if s := info.Sys().(*syscall.Stat_t); s.Uid != uint32(os.Getuid()) {
		return errors.New("mailbox file owned by another account")
	}
	return os.Chmod(path, 0600)
}
func Open(home string) (*Store, error) {
	h, e := Home(home)
	if e != nil {
		return nil, e
	}
	if e = PrivateDir(h); e != nil {
		return nil, e
	}
	h, e = filepath.EvalSymlinks(h)
	if e != nil {
		return nil, e
	}
	if e = privateFile(filepath.Join(h, "broker.lock")); e != nil {
		return nil, e
	}
	lock, e := os.OpenFile(filepath.Join(h, "broker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		lock.Close()
		return nil, errors.New("broker already running for this home")
	}
	s := &Store{Home: h, lock: lock}
	defer func() {
		if e != nil {
			s.Close()
		}
	}()
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if e = privateFile(filepath.Join(h, "mailbox.db") + suffix); e != nil {
			return nil, e
		}
	}
	// Create privately before SQLite can open the database or its sidecars.
	f, e := os.OpenFile(filepath.Join(h, "mailbox.db"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	f.Close()
	u := url.URL{Scheme: "file", Path: filepath.Join(h, "mailbox.db")}
	s.db, e = sql.Open("sqlite", u.String()+"?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=foreign_keys(ON)&_pragma=busy_timeout(3000)&_pragma=secure_delete(ON)&_pragma=wal_autocheckpoint(256)&_pragma=journal_size_limit(16777216)")
	if e != nil {
		return nil, e
	}
	s.db.SetMaxOpenConns(1)
	if e = s.migrate(); e != nil {
		return nil, e
	}
	var mode string
	var full int
	if e = s.db.QueryRow("PRAGMA journal_mode").Scan(&mode); e != nil {
		return nil, e
	}
	if e = s.db.QueryRow("PRAGMA synchronous").Scan(&full); e != nil {
		return nil, e
	}
	if mode != "wal" || full != 2 {
		e = errors.New("SQLite requires WAL and synchronous FULL")
		return nil, e
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if e = privateFile(filepath.Join(h, "mailbox.db") + suffix); e != nil {
			return nil, e
		}
	}
	s.reader, e = openReader(h)
	if e != nil {
		return nil, e
	}
	return s, nil
}
func openReader(home string) (*sql.DB, error) {
	u := url.URL{Scheme: "file", Path: filepath.Join(home, "mailbox.db")}
	db, err := sql.Open("sqlite", u.String()+"?mode=ro&_pragma=query_only(ON)&_pragma=busy_timeout(3000)")
	if err == nil {
		db.SetMaxOpenConns(1)
	}
	return db, err
}
func (s *Store) migrate() error {
	tx, e := s.db.Begin()
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if _, e = tx.Exec("CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL, sequence INTEGER NOT NULL DEFAULT 0, replay_floor TEXT NOT NULL DEFAULT '')"); e != nil {
		return e
	}
	var v int
	e = tx.QueryRow("SELECT version FROM schema_version").Scan(&v)
	if errors.Is(e, sql.ErrNoRows) {
		_, e = tx.Exec("INSERT INTO schema_version(version) VALUES (?)", Schema)
		v = Schema
	}
	if e != nil {
		return e
	}
	if v != Schema {
		return fmt.Errorf("unsupported database schema %d", v)
	}
	for _, stmt := range []string{
		"CREATE TABLE IF NOT EXISTS messages (key TEXT PRIMARY KEY, origin TEXT NOT NULL, id TEXT NOT NULL, recipient TEXT NOT NULL, state TEXT NOT NULL, sequence INTEGER NOT NULL, payload BLOB NOT NULL, UNIQUE(origin,id))",
		"CREATE UNIQUE INDEX IF NOT EXISTS one_inflight ON messages(recipient) WHERE state IN ('claimed','injecting')",
		"CREATE TABLE IF NOT EXISTS attempts (id TEXT PRIMARY KEY, message_key TEXT NOT NULL, payload BLOB NOT NULL, FOREIGN KEY(message_key) REFERENCES messages(key) ON DELETE CASCADE)",
		"CREATE TABLE IF NOT EXISTS state_history (message_key TEXT NOT NULL, ordinal INTEGER NOT NULL, payload BLOB NOT NULL, PRIMARY KEY(message_key,ordinal), FOREIGN KEY(message_key) REFERENCES messages(key) ON DELETE CASCADE)",
		"CREATE TABLE IF NOT EXISTS sessions (key TEXT PRIMARY KEY, payload BLOB NOT NULL)",
		"CREATE TABLE IF NOT EXISTS links (key TEXT PRIMARY KEY, payload BLOB NOT NULL)",
		"CREATE TABLE IF NOT EXISTS health (key TEXT PRIMARY KEY, payload BLOB NOT NULL)",
		"CREATE TABLE IF NOT EXISTS write_probe (id INTEGER PRIMARY KEY CHECK(id=1), value INTEGER NOT NULL)",
		"CREATE TABLE IF NOT EXISTS dedupe (key TEXT PRIMARY KEY, payload BLOB NOT NULL)",
	} {
		if _, e = tx.Exec(stmt); e != nil {
			return e
		}
	}
	return tx.Commit()
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readMu.Lock()
	defer s.readMu.Unlock()
	var e error
	if s.readConn != nil {
		_ = s.readConn.Close()
	}
	if s.reader != nil {
		_ = s.reader.Close()
	}
	if s.db != nil {
		e = s.db.Close()
	}
	if s.lock != nil {
		_ = syscall.Flock(int(s.lock.Fd()), syscall.LOCK_UN)
		s.lock.Close()
	}
	return e
}
func load(ctx context.Context, tx *sql.Tx) (*Data, error) {
	d := Empty()
	var v int
	var floor string
	if e := tx.QueryRowContext(ctx, "SELECT version,sequence,replay_floor FROM schema_version").Scan(&v, &d.Sequence, &floor); e != nil {
		return nil, e
	}
	if v != Schema {
		return nil, fmt.Errorf("unsupported database schema %d", v)
	}
	if floor != "" {
		var e error
		d.ReplayFloor, e = time.Parse(time.RFC3339Nano, floor)
		if e != nil {
			return nil, e
		}
	}
	for _, tab := range []string{"messages", "sessions", "dedupe", "links", "health"} {
		rows, e := tx.QueryContext(ctx, "SELECT key,payload FROM "+tab)
		if e != nil {
			return nil, e
		}
		for rows.Next() {
			var k string
			var b []byte
			if e = rows.Scan(&k, &b); e != nil {
				break
			}
			switch tab {
			case "messages":
				var val Message
				e = json.Unmarshal(b, &val)
				d.Messages[k] = &val
			case "sessions":
				var val Session
				e = json.Unmarshal(b, &val)
				d.Sessions[k] = val
			case "dedupe":
				var val Dedupe
				e = json.Unmarshal(b, &val)
				d.Dedupe[k] = val
			case "links":
				var val Link
				e = json.Unmarshal(b, &val)
				d.Links[k] = val
			case "health":
				var val Health
				e = json.Unmarshal(b, &val)
				d.Health[k] = val
			}
			if e != nil {
				break
			}
		}
		re := rows.Err()
		rows.Close()
		if e != nil {
			return nil, e
		}
		if re != nil {
			return nil, re
		}
	}
	return d, nil
}

// readSnapshot pins data_version and the decoded cache to one SQLite connection.
// The version is read inside the same transaction as the rows, so a concurrent
// writer cannot cause an old snapshot to be cached under a new version.
func (s *Store) readSnapshot(ctx context.Context) (*Data, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.readConn == nil {
		db := s.reader
		if s.readonly {
			db = s.db
		}
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, err
		}
		s.readConn = conn
		s.cached = nil
	}
	d, err := s.readCached(ctx)
	if err != nil {
		// A cancelled read can discard its driver connection. Never reuse a cached
		// version with a replacement connection, whose data_version can start over.
		s.cached = nil
		_ = s.readConn.Close()
		s.readConn = nil
	}
	return d, err
}
func (s *Store) readCached(ctx context.Context) (*Data, error) {
	tx, err := s.readConn.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var version int64
	if err = tx.QueryRowContext(ctx, "PRAGMA data_version").Scan(&version); err != nil {
		return nil, err
	}
	d := s.cached
	if d == nil || version != s.dataVersion {
		d, err = load(ctx, tx)
		if err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	s.cached, s.dataVersion = d, version
	// Internal cached snapshots are immutable. Only the methods below expose
	// copies to callers; Update keeps this snapshot only as its comparison baseline.
	return d, nil
}
func (s *Store) Read(ctx context.Context) (*Data, error) {
	d, err := s.readSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return d.clone(), nil
}

// ReadSessions copies the registration history without cloning mailbox payloads.
// It uses only the read connection, without the writer mutex or a write transaction.
func (s *Store) ReadSessions(ctx context.Context) (*Data, error) {
	d, err := s.readSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	out := Empty()
	for k, v := range d.Sessions {
		out.Sessions[k] = cloneSession(v)
	}
	return out, nil
}

// Update serializes writers and commits state, attempts, audit and dedupe together.
func (s *Store) Update(ctx context.Context, fn func(*Data) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readonly {
		return errors.New("read-only store")
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	// The caller's deadline must stay out of SQLite. Cancelling a write transaction
	// interrupts the writer connection, and that can strand the WAL write lock inside
	// this process until it exits, stopping all mail. busy_timeout bounds the wait, and
	// the deadline is honored at the checks here and before commit.
	caller := ctx
	ctx = context.Background()
	// The writer DSN begins an immediate transaction before reading any gate or row.
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	// BEGIN IMMEDIATE excludes other writers before consulting the reader cache.
	// No rows have been written yet, so its WAL snapshot is also this transaction's
	// starting state. Read without the caller deadline to preserve the wedge fix.
	before, e := s.readSnapshot(ctx)
	if e != nil {
		return e
	}
	after := before.clone()
	if e = fn(after); e != nil {
		return e
	}
	// Release changing inflight index entries without deleting their audit rows.
	for k, old := range before.Messages {
		m, exists := after.Messages[k]
		if !exists {
			if _, e = tx.ExecContext(ctx, "DELETE FROM messages WHERE key=?", k); e != nil {
				return e
			}
		} else if (old.State == states.Claimed || old.State == states.Injecting) && (old.State != m.State || old.Recipient != m.Recipient) {
			if _, e = tx.ExecContext(ctx, "UPDATE messages SET state='queued' WHERE key=?", k); e != nil {
				return e
			}
		}
	}
	for k, m := range after.Messages {
		if reflect.DeepEqual(before.Messages[k], m) {
			continue
		}
		b, e := json.Marshal(m)
		if e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO messages(key,origin,id,recipient,state,sequence,payload) VALUES(?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET origin=excluded.origin,id=excluded.id,recipient=excluded.recipient,state=excluded.state,sequence=excluded.sequence,payload=excluded.payload", k, m.Origin, m.ID, m.Recipient, m.State, m.Sequence, b); e != nil {
			return e
		}
		oldAttempts := map[string]Attempt{}
		if old := before.Messages[k]; old != nil {
			for _, a := range old.Attempts {
				oldAttempts[a.ID] = a
			}
		}
		for _, a := range m.Attempts {
			old, exists := oldAttempts[a.ID]
			delete(oldAttempts, a.ID)
			if exists && reflect.DeepEqual(old, a) {
				continue
			}
			ab, _ := json.Marshal(a)
			if exists {
				_, e = tx.ExecContext(ctx, "UPDATE attempts SET payload=? WHERE id=? AND message_key=?", ab, a.ID, k)
			} else {
				_, e = tx.ExecContext(ctx, "INSERT INTO attempts(id,message_key,payload) VALUES(?,?,?)", a.ID, k, ab)
			}
			if e != nil {
				return e
			}
		}
		for id := range oldAttempts {
			if _, e = tx.ExecContext(ctx, "DELETE FROM attempts WHERE id=? AND message_key=?", id, k); e != nil {
				return e
			}
		}
		for i, h := range m.History {
			if old := before.Messages[k]; old != nil && i < len(old.History) && reflect.DeepEqual(old.History[i], h) {
				continue
			}
			hb, _ := json.Marshal(h)
			if _, e = tx.ExecContext(ctx, "INSERT INTO state_history(message_key,ordinal,payload) VALUES(?,?,?) ON CONFLICT(message_key,ordinal) DO UPDATE SET payload=excluded.payload", k, i, hb); e != nil {
				return e
			}
		}
		if old := before.Messages[k]; old != nil && len(old.History) > len(m.History) {
			if _, e = tx.ExecContext(ctx, "DELETE FROM state_history WHERE message_key=? AND ordinal>=?", k, len(m.History)); e != nil {
				return e
			}
		}
	}
	if e = writeMap(ctx, tx, "sessions", before.Sessions, after.Sessions); e != nil {
		return e
	}
	if e = writeMap(ctx, tx, "dedupe", before.Dedupe, after.Dedupe); e != nil {
		return e
	}
	if e = writeMap(ctx, tx, "links", before.Links, after.Links); e != nil {
		return e
	}
	if e = writeMap(ctx, tx, "health", before.Health, after.Health); e != nil {
		return e
	}
	if before.Sequence != after.Sequence || !before.ReplayFloor.Equal(after.ReplayFloor) {
		floor := ""
		if !after.ReplayFloor.IsZero() {
			floor = after.ReplayFloor.UTC().Format(time.RFC3339Nano)
		}
		if _, e = tx.ExecContext(ctx, "UPDATE schema_version SET sequence=?,replay_floor=?", after.Sequence, floor); e != nil {
			return e
		}
	}
	if e = caller.Err(); e != nil {
		return e
	}
	if e = tx.Commit(); e != nil {
		return e
	}
	if !reflect.DeepEqual(before, after) {
		s.notifyChanged(before, after)
	}
	return nil
}

// Changes returns a broadcast closed after the next committed state change.
// Subscribe before reading to avoid losing a concurrent commit.
func (s *Store) Changes() <-chan struct{} {
	s.changeMu.Lock()
	defer s.changeMu.Unlock()
	if s.changed == nil {
		s.changed = make(chan struct{})
	}
	return s.changed
}
func (s *Store) notifyChanged(before, after *Data) {
	s.changeMu.Lock()
	defer s.changeMu.Unlock()
	if s.changed != nil {
		close(s.changed)
	}
	s.changed = make(chan struct{})
	for id, ch := range s.sessionChanges {
		if !reflect.DeepEqual(before.Sessions[id], after.Sessions[id]) {
			close(ch)
			delete(s.sessionChanges, id)
		}
	}
}

// SessionChanges isolates receiver waits from unrelated mail and peer updates.
func (s *Store) SessionChanges(id string) <-chan struct{} {
	s.changeMu.Lock()
	defer s.changeMu.Unlock()
	if s.sessionChanges == nil {
		s.sessionChanges = map[string]chan struct{}{}
	}
	ch := s.sessionChanges[id]
	if ch == nil {
		ch = make(chan struct{})
		s.sessionChanges[id] = ch
	}
	return ch
}

// writeMap compares typed values before encoding, avoiding full-map JSON work
// for unchanged sessions, links, health and dedupe entries.
func writeMap[T any](ctx context.Context, tx *sql.Tx, table string, before, after map[string]T) error {
	for k := range before {
		if _, ok := after[k]; !ok {
			if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE key=?", k); err != nil {
				return err
			}
		}
	}
	for k, v := range after {
		if old, ok := before[k]; ok && reflect.DeepEqual(old, v) {
			continue
		}
		payload, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO "+table+"(key,payload) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET payload=excluded.payload", k, payload); err != nil {
			return err
		}
	}
	return nil
}

func Snapshot(ctx context.Context, home string) (*Data, error) {
	h, e := Home(home)
	if e != nil {
		return nil, e
	}
	path := filepath.Join(h, "mailbox.db")
	if _, e = os.Stat(path); errors.Is(e, os.ErrNotExist) {
		return Empty(), nil
	} else if e != nil {
		return nil, e
	}
	u := url.URL{Scheme: "file", Path: path}
	db, e := sql.Open("sqlite", u.String()+"?mode=ro&_pragma=query_only(ON)&_pragma=busy_timeout(3000)")
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, readonly: true}
	defer s.Close()
	return s.Read(ctx)
}
func (s *Store) Checkpoint(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	u := url.URL{Scheme: "file", Path: filepath.Join(s.Home, "mailbox.db")}
	db, e := sql.Open("sqlite", u.String()+"?_pragma=busy_timeout(0)")
	if e != nil {
		return e
	}
	defer db.Close()
	var busy, log, done int
	// Never wait behind a reader or sender to shrink the WAL.
	if e := db.QueryRowContext(context.Background(), "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &log, &done); e != nil {
		return e
	}
	if busy != 0 {
		return errors.New("WAL checkpoint busy; payload erasure pending")
	}
	return nil
}

// WriteProbe changes one private row so the watchdog tests actual SQL writes and
// durable commits even when ordinary no-op updates have no dirty pages.
func (s *Store) WriteProbe(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readonly {
		return errors.New("read-only store")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// As in Update, never let cancellation interrupt the writer connection.
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(context.Background(), "INSERT INTO write_probe(id,value) VALUES(1,1) ON CONFLICT(id) DO UPDATE SET value=1-write_probe.value"); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return tx.Commit()
}
