// Package muse delivers through Muse's opt-in native session-message command.
package muse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/store"
	"github.com/google/uuid"
)

const version = "Muse Code 1.4.14 (1.4.14-R7309.1)"

// Driver inherits the operator's environment. It never enables experimental
// flags itself. Both broker and receiver need a per-process ingress opt-in.
type Driver struct {
	HerdrSession   string
	versionMu      sync.Mutex
	versionKey     string
	versionErr     error
	versionChecked bool
	lookupMu       sync.Mutex
	lookups        map[string]*transcriptCursor
}

type availabilityKey struct{}
type availabilityCache struct {
	driver       *Driver
	probeOnce    sync.Once
	probeErr     error
	discoverOnce sync.Once
	sessions     []store.Session
	discoverErr  error
}

// AvailabilityContext shares observations only within one broker refresh pass.
func (d *Driver) AvailabilityContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, availabilityKey{}, &availabilityCache{driver: d})
}

func (d *Driver) cache(ctx context.Context) *availabilityCache {
	c, _ := ctx.Value(availabilityKey{}).(*availabilityCache)
	if c != nil && c.driver == d {
		return c
	}
	return nil
}

func dataHome() string {
	root := os.Getenv("XDG_DATA_HOME")
	if root == "" {
		home, _ := os.UserHomeDir()
		root = filepath.Join(home, ".local", "share")
	}
	return filepath.Join(root, "muse")
}

// run calls the muse launcher. MUSE_NO_AUTO_UPDATE stops the broker's frequent
// probes from triggering the launcher's hourly self-update, which would replace
// the verified binary and park all Muse mail until the new version is verified.
func run(ctx context.Context, input string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "muse", args...)
	cmd.Env = append(os.Environ(), "MUSE_NO_AUTO_UPDATE=1")
	cmd.Stdin = strings.NewReader(input)
	return cmd.Output()
}

func (d *Driver) Probe(ctx context.Context) error {
	if os.Getenv("MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS") != "1" {
		return errors.New("Muse broker needs per-process MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=1")
	}
	if c := d.cache(ctx); c != nil {
		c.probeOnce.Do(func() { c.probeErr = d.probe(ctx) })
		return c.probeErr
	}
	return d.probe(ctx)
}

func (d *Driver) probe(ctx context.Context) error {
	key, e := versionKey()
	if e != nil {
		return e
	}
	d.versionMu.Lock()
	defer d.versionMu.Unlock()
	if d.versionChecked && key == d.versionKey {
		return d.versionErr
	}
	err := d.probeVersion(ctx)
	// Cancellation and execution errors are transient. Only cache completed checks.
	if ctx.Err() == nil && (err == nil || strings.HasPrefix(err.Error(), "unsupported/unverified")) {
		d.versionKey, d.versionErr, d.versionChecked = key, err, true
	}
	return err
}

// Include both the launcher and its installed payload. A launcher can remain
// unchanged while an update replaces the large versioned binary beside it.
func versionKey() (string, error) {
	path, e := exec.LookPath("muse")
	if e != nil {
		return "", e
	}
	path, e = filepath.EvalSymlinks(path)
	if e != nil {
		return "", e
	}
	paths := []string{path}
	if _, e := os.Stat(filepath.Join(filepath.Dir(path), ".muse-version")); e == nil {
		paths = append(paths, filepath.Join(filepath.Dir(path), ".muse-version"))
	}
	payloads, _ := filepath.Glob(filepath.Join(filepath.Dir(path), "muse-bin*"))
	paths = append(paths, payloads...)
	var key strings.Builder
	for _, p := range paths {
		resolved, e := filepath.EvalSymlinks(p)
		if e != nil {
			return "", e
		}
		info, e := os.Stat(resolved)
		if e != nil {
			return "", e
		}
		fmt.Fprintf(&key, "%s:%d:%s;", resolved, info.Size(), info.ModTime().Format(time.RFC3339Nano))
	}
	return key.String(), nil
}

func (d *Driver) probeVersion(ctx context.Context) error {
	b, e := run(ctx, "", "--version")
	if e != nil {
		return e
	}
	if strings.TrimSpace(string(b)) != version {
		return fmt.Errorf("unsupported/unverified Muse version: %s", strings.TrimSpace(string(b)))
	}
	return nil
}

type peer struct {
	ID   string `json:"session_id"`
	Name string `json:"session_name"`
}

func (d *Driver) peers(ctx context.Context) ([]peer, error) {
	if e := d.Probe(ctx); e != nil {
		return nil, e
	}
	b, e := run(ctx, "", "session-message", "list", "--json")
	var p struct {
		Schema   int    `json:"schema_version"`
		Sessions []peer `json:"sessions"`
		Error    string `json:"error_code"`
	}
	if json.Unmarshal(b, &p) != nil || p.Schema != 1 {
		return nil, errors.New("unrecognized Muse session list")
	}
	if p.Error != "" {
		return nil, fmt.Errorf("Muse discovery unavailable: %s", p.Error)
	}
	if e != nil || p.Sessions == nil {
		return nil, errors.New("Muse session list failed")
	}
	seen := map[string]bool{}
	for _, s := range p.Sessions {
		if _, e := uuid.Parse(s.ID); e != nil || seen[s.ID] {
			return nil, errors.New("invalid Muse session identity")
		}
		seen[s.ID] = true
	}
	return p.Sessions, nil
}

// logPath queries only a known session ID. The index is not a liveness source.
func logPath(ctx context.Context, id string) (string, error) {
	if _, e := uuid.Parse(id); e != nil {
		return "", e
	}
	u := url.URL{Scheme: "file", Path: filepath.Join(dataHome(), "session-index.db")}
	db, e := sql.Open("sqlite", u.String()+"?mode=ro")
	if e != nil {
		return "", e
	}
	defer db.Close()
	var path string
	if e = db.QueryRowContext(ctx, "SELECT session_log_path FROM sessions WHERE session_id = ?", id).Scan(&path); e != nil {
		return "", e
	}
	root := filepath.Join(dataHome(), "sessions")
	rel, e := filepath.Rel(root, path)
	if e != nil || !filepath.IsAbs(path) || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.Base(path) != "session.jsonl" || filepath.Base(filepath.Dir(path)) != id {
		return "", errors.New("Muse index returned an unexpected transcript path")
	}
	info, e := os.Stat(path)
	if e != nil {
		return "", e
	}
	if !info.Mode().IsRegular() {
		return "", errors.New("Muse transcript is not a regular file")
	}
	return path, nil
}

func registryPID(id string) (int, error) {
	b, e := os.ReadFile(filepath.Join(dataHome(), "runtime", "muse", "sessions", id+".json"))
	if e != nil {
		return 0, e
	}
	var r struct {
		Schema int    `json:"schema_version"`
		ID     string `json:"session_id"`
		PID    string `json:"process_generation_hint"`
	}
	if e = json.Unmarshal(b, &r); e != nil || r.Schema != 1 || r.ID != id || !strings.HasPrefix(r.PID, "pid=") {
		return 0, errors.New("unrecognized Muse registry record")
	}
	pid, e := strconv.Atoi(strings.TrimPrefix(r.PID, "pid="))
	if e != nil || pid <= 0 {
		return 0, errors.New("invalid Muse registry process")
	}
	return pid, nil
}

func (d *Driver) panePID(ctx context.Context, pane string) (int, error) {
	args := []string{}
	if d.HerdrSession != "" {
		args = append(args, "--session", d.HerdrSession)
	}
	args = append(args, "pane", "process-info", "--pane", pane)
	b, e := exec.CommandContext(ctx, "herdr", args...).Output()
	if e != nil {
		return 0, e
	}
	var r struct {
		Result struct {
			Process struct {
				Pane string `json:"pane_id"`
				PIDs []struct {
					PID int `json:"pid"`
				} `json:"foreground_processes"`
			} `json:"process_info"`
		} `json:"result"`
	}
	if e = json.Unmarshal(b, &r); e != nil || r.Result.Process.Pane != pane || len(r.Result.Process.PIDs) != 1 {
		return 0, errors.New("Muse pane has no unique foreground process")
	}
	return r.Result.Process.PIDs[0].PID, nil
}

func optedIn(pid int) bool {
	b, e := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "environ"))
	if e != nil {
		return false
	}
	for _, entry := range strings.Split(string(b), "\x00") {
		if entry == "MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=1" {
			return true
		}
	}
	return false
}

func (d *Driver) Discover(ctx context.Context) ([]store.Session, error) {
	if c := d.cache(ctx); c != nil {
		c.discoverOnce.Do(func() { c.sessions, c.discoverErr = d.discover(ctx) })
		return c.sessions, c.discoverErr
	}
	return d.discover(ctx)
}

func (d *Driver) discover(ctx context.Context) ([]store.Session, error) {
	agents, e := identity.Agents(ctx, d.HerdrSession)
	if e != nil {
		return nil, e
	}
	hasMuse := false
	for _, a := range agents {
		if a.Harness == "muse" {
			hasMuse = true
			break
		}
	}
	if !hasMuse {
		return []store.Session{}, nil
	}
	peers, e := d.peers(ctx)
	if e != nil {
		return nil, e
	}
	panes := map[int]identity.Agent{}
	for _, a := range agents {
		if a.Harness != "muse" {
			continue
		}
		pid, e := d.panePID(ctx, a.Pane)
		if e == nil {
			panes[pid] = a
		}
	}
	out := []store.Session{}
	for _, p := range peers {
		name := p.Name
		if policy.Identity(name) != nil {
			name = p.ID
		}
		s := store.Session{ID: p.ID, Name: name, Harness: "muse", State: "detached", Reason: "Muse receiver has no verified herdr pane; reattach before delivery"}
		pid, err := registryPID(p.ID)
		a, attached := panes[pid]
		if err == nil && attached {
			s.Pane = a.Pane
			s.State = "live"
			s.Blocked = a.State != "idle" && a.State != "done"
			s.Reason = a.State
			if !optedIn(pid) {
				s.Blocked = true
				s.Reason = "Muse receiver needs per-process MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS=1"
			}
		}
		if _, err := logPath(ctx, p.ID); err != nil {
			s.Blocked = true
			s.Reason = "Muse transcript unavailable; durable acceptance cannot be verified"
		}
		out = append(out, s)
	}
	return out, nil
}

func (d *Driver) SessionState(ctx context.Context, s store.Session) (broker.Availability, error) {
	peers, e := d.Discover(ctx)
	if e != nil {
		return broker.Availability{}, e
	}
	for _, p := range peers {
		if p.ID == s.ID {
			return broker.Availability{State: p.State, Blocked: p.Blocked, Reason: p.Reason}, nil
		}
	}
	return broker.Availability{State: "offline", Reason: "Muse session is not a registered receiver; resume it with local messaging enabled"}, nil
}

// Prepare runs before the broker commits the immutable attempt, so its receipt
// starting point is durable before any native input can be written.
func (d *Driver) Prepare(ctx context.Context, s store.Session, aid string) (string, error) {
	return d.seedLookup(ctx, s.ID, aid)
}

func (d *Driver) Inject(ctx context.Context, s store.Session, body, aid string) (broker.Result, error) {
	// Never reuse a background availability observation for a send preflight.
	ctx = d.AvailabilityContext(identity.CachedAgents(ctx))
	a, e := d.SessionState(ctx, s)
	if e != nil || a.State != "live" || a.Blocked || ctx.Err() != nil {
		return broker.Result{Outcome: broker.NotSent, Class: "precondition_race", Proof: "no Muse send started; receiver unavailable"}, nil
	}
	d.lookupMu.Lock()
	cached := d.lookups[s.ID+"/"+aid]
	proof := ""
	if cached != nil {
		proof = cached.Proof
	}
	d.lookupMu.Unlock()
	if proof == "" {
		proof, e = d.seedLookup(ctx, s.ID, aid)
	}
	if e != nil {
		return broker.Result{Outcome: broker.NotSent, Class: "precondition_race", Proof: "no Muse send started; transcript offset unavailable: " + e.Error()}, nil
	}
	_, e = run(ctx, body, "session-message", "send", "--target", s.ID, "--json")
	// Even a nonzero CLI exit can mean accepted. R7309.1 reports
	// unverified_target_receipt after durable target admission on Linux.
	var start *exec.Error
	if errors.As(e, &start) {
		return broker.Result{Outcome: broker.NotSent, Class: "transport", Proof: "Muse executable could not start"}, nil
	}
	found, err := d.Lookup(ctx, broker.LookupRequest{Attempt: store.Attempt{ID: aid, Session: s, Envelope: body, Proof: proof, Started: time.Now()}})
	if err == nil && found {
		return broker.Result{Outcome: broker.Accepted, Proof: "durable Muse target admission"}, nil
	}
	return broker.Result{Outcome: broker.Unconfirmed, Proof: proof}, nil
}
