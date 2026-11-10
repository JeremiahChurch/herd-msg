// Package broker coordinates durable admission and at-most-once automatic injection.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

type Availability struct {
	State   string            `json:"state"`
	Blocked bool              `json:"blocked"`
	Reason  string            `json:"reason"`
	Hints   map[string]string `json:"hints"`
}
type Result struct {
	Outcome string `json:"outcome"`
	Proof   string `json:"proof"`
	Class   string `json:"class"`
}

const (
	Accepted    = "accepted"
	NotSent     = "not_sent"
	Unconfirmed = "unconfirmed"
)

type LookupRequest struct {
	Attempt    store.Attempt
	Successors []store.Session
}
type Driver interface {
	Probe(context.Context) error
	SessionState(context.Context, store.Session) (Availability, error)
	Inject(context.Context, store.Session, string, string) (Result, error)
	Lookup(context.Context, LookupRequest) (bool, error)
}
type Caller struct {
	ID      string `json:"id"`
	Harness string `json:"harness"`
}

func (c Caller) Operator() bool { return c.ID == "operator" && c.Harness == "" }
func (c Caller) Validate() error {
	if c.Operator() {
		return nil
	}
	if c.Harness != "codex" && c.Harness != "claude" && c.Harness != "hermes" && c.Harness != "muse" {
		return errors.New("invalid caller harness")
	}
	return policy.Identity(c.ID)
}

type Error struct {
	Code      int    `json:"code"`
	Message   string `json:"error"`
	Retryable bool   `json:"retryable"`
}

// InvalidConfigError distinguishes invalid settings from temporary lock failures.
type InvalidConfigError struct{ Cause error }

func (e *InvalidConfigError) Error() string { return "invalid config: " + e.Cause.Error() }
func (e *InvalidConfigError) Unwrap() error {
	return &Error{Code: 2, Message: e.Error(), Retryable: true}
}
func ConfigRepair(path string) string { return "edit " + path + " to restore a valid configuration" }

// CheckMachine prevents hand-edited settings from changing retained mailbox identity.
func CheckMachine(d *store.Data, machine string) error {
	for _, s := range d.Sessions {
		if s.Machine != "" && s.Machine != machine {
			return fmt.Errorf("configured machine %q differs from recorded machine %q; restore the recorded machine in the configuration", machine, s.Machine)
		}
	}
	return nil
}

func (e *Error) Error() string                    { return e.Message }
func fail(code int, retry bool, msg string) error { return &Error{code, msg, retry} }
func operator(c Caller) error {
	if !c.Operator() {
		return fail(2, false, "this action requires operator identity")
	}
	return nil
}

type Broker struct {
	Store          *store.Store
	ConfigPath     string
	Epoch          string
	Drivers        map[string]Driver
	Now            func() time.Time
	Fault          func(string)
	gate           sync.Mutex
	configQueue    chan struct{}
	mu             sync.Mutex
	sweepMu        sync.Mutex
	nextSweep      time.Time
	locks          map[string]chan struct{}
	outstanding    map[string]string // attempt to recipient; remains until the call returns
	claimStarted   map[string]time.Time
	stopping       bool
	paused         map[string]string
	displayConfig  policy.Config
	configError    bool
	transfers      map[string]context.CancelFunc
	DiscoveryWake  chan struct{}
	TickWake       chan struct{}
	TransportWake  chan struct{}
	TransportReset chan struct{}
	Network        func(context.Context, string, PeerRequest) (any, error)
}

func New(home, configPath string, drivers map[string]Driver, faults ...func(string)) (*Broker, error) {
	return NewWithClock(home, configPath, drivers, time.Now, faults...)
}

// NewWithClock gives recovery and subsequent operations the same clock.
func NewWithClock(home, configPath string, drivers map[string]Driver, clock func() time.Time, faults ...func(string)) (*Broker, error) {
	if clock == nil {
		clock = time.Now
	}
	s, e := store.Open(home)
	if e != nil {
		return nil, e
	}
	if configPath == "" {
		configPath, e = policy.ConfigPath(s.Home)
		if e != nil {
			s.Close()
			return nil, e
		}
	}
	if e = store.PrivateDir(filepath.Dir(configPath)); e != nil {
		s.Close()
		return nil, e
	}
	b := &Broker{TickWake: make(chan struct{}, 1), DiscoveryWake: make(chan struct{}, 1), configQueue: make(chan struct{}, 1), Store: s, ConfigPath: configPath, Epoch: store.Random(), Drivers: drivers, Now: clock, locks: map[string]chan struct{}{}, outstanding: map[string]string{}, transfers: map[string]context.CancelFunc{}, TransportWake: make(chan struct{}, 1), TransportReset: make(chan struct{}, 1), claimStarted: map[string]time.Time{}}
	b.displayConfig = policy.Defaults()
	if len(faults) > 0 {
		b.Fault = faults[0]
	}
	b.hook("before_recovery")
	if e = b.withConfig(context.Background(), func(c policy.Config) error {
		return b.Store.Update(context.Background(), func(d *store.Data) error {
			now := b.now()
			for key := range d.Health {
				if strings.HasPrefix(key, "outstanding/") {
					delete(d.Health, key)
				}
			}
			for _, m := range d.Messages {
				switch m.State {
				case states.Claimed:
					if e := d.Move(m, states.Queued, "broker", "claim recovered", now); e != nil {
						return e
					}
				case states.Injecting:
					if e := d.Move(m, states.Indeterminate, "broker", "awaiting_confirmation", now); e != nil {
						return e
					}
				}
			}
			for _, m := range d.Messages {
				if e := settleStored(d, m, now); e != nil {
					return e
				}
			}
			for _, s := range d.Sessions {
				receiverNotice(d, s, now)
			}
			b.hook("before_recovery_commit")
			return b.reconcile(d, c, now)
		})
	}); e != nil {
		s.Close()
		return nil, e
	}
	b.hook("after_recovery_commit")
	if e = b.maintain(context.Background()); e != nil {
		s.Close()
		return nil, e
	}
	return b, nil
}
func (b *Broker) now() time.Time { return b.Now().UTC() }
func (b *Broker) hook(name string) {
	if b.Fault != nil {
		b.Fault(name)
	}
}
func (b *Broker) Close() error {
	b.Stop()
	if b.Outstanding() > 0 {
		return errors.New("driver calls still outstanding; home lock retained until process exit")
	}
	return b.Store.Close()
}
func (b *Broker) Stop() { b.mu.Lock(); b.stopping = true; b.mu.Unlock() }

// lockConfig queues broker callers in memory before competing with offline
// operators for the file lock. Local contention follows the caller's deadline
// instead of making every waiter poll flock and time out after one second.
func (b *Broker) lockConfig(ctx context.Context) (func(), error) {
	select {
	case b.configQueue <- struct{}{}:
	case <-ctx.Done():
		return nil, fail(2, true, "configuration wait cancelled: "+ctx.Err().Error())
	}
	if err := ctx.Err(); err != nil {
		<-b.configQueue
		return nil, fail(2, true, "configuration wait cancelled: "+err.Error())
	}
	lockctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	f, err := policy.Lock(lockctx, b.ConfigPath+".lock")
	if err != nil {
		<-b.configQueue
		return nil, fail(2, true, "configuration lock unavailable")
	}
	b.gate.Lock()
	return func() { b.gate.Unlock(); policy.Unlock(f); <-b.configQueue }, nil
}
func (b *Broker) withConfig(ctx context.Context, fn func(policy.Config) error) error {
	unlock, e := b.lockConfig(ctx)
	if e != nil {
		return e
	}
	defer unlock()
	c, e := policy.Read(b.ConfigPath)
	if e == nil {
		var peers map[string]PrivatePeer
		peers, e = b.readPeers()
		if e == nil {
			for name, p := range peers {
				if p.Revoked {
					delete(c.Links, name)
				}
			}
		}
	}
	if e == nil {
		d, readErr := b.Store.ReadSessions(ctx)
		if readErr != nil {
			return readErr
		}
		e = CheckMachine(d, c.Machine)
	}
	if e != nil {
		b.configError = true
		invalid := &InvalidConfigError{Cause: e}
		_ = b.Store.Update(context.Background(), func(d *store.Data) error {
			if h, exists := d.Health["config"]; !exists || h.Detail != invalid.Error() {
				d.Notice("config", invalid.Error(), ConfigRepairError(b.ConfigPath, b.Store.Home, invalid), b.now())
			}
			return nil
		})
		return invalid
	}

	if b.configError {
		if e = b.Store.Update(ctx, func(d *store.Data) error { delete(d.Health, "config"); return nil }); e != nil {
			return e
		}
		b.configError = false
	}
	encoded, _ := json.Marshal(c)
	b.displayConfig = policy.Config{}
	_ = json.Unmarshal(encoded, &b.displayConfig)
	return fn(c)
}
func (b *Broker) lock(ctx context.Context, id string) (func(), error) {
	b.mu.Lock()
	l := b.locks[id]
	if l == nil {
		l = make(chan struct{}, 1)
		b.locks[id] = l
	}
	b.mu.Unlock()
	select {
	case l <- struct{}{}:
		return func() { <-l }, nil
	case <-ctx.Done():
		return nil, fail(2, true, "recipient delivery busy")
	}
}
func (b *Broker) busy(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.outstanding {
		if r == id {
			return true
		}
	}
	return false
}
func (b *Broker) Outstanding() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.outstanding) + len(b.transfers)
}
func (b *Broker) Drain(ctx context.Context) int {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		n := b.Outstanding()
		if n == 0 {
			return 0
		}
		select {
		case <-ctx.Done():
			return n
		case <-t.C:
		}
	}
}
func (b *Broker) Register(ctx context.Context, s store.Session) error {
	return b.register(ctx, s, nil)
}
func (b *Broker) register(ctx context.Context, s store.Session, expected *string) error {
	if e := validateSession(s); e != nil {
		return e
	}
	unlock, e := b.lock(ctx, s.ID)
	if e != nil {
		return e
	}
	defer unlock()
	if b.busy(s.ID) {
		return fail(2, true, "recipient has an outstanding driver call")
	}
	return b.withConfig(ctx, func(c policy.Config) error {
		return b.Store.Update(ctx, func(d *store.Data) error {
			old := d.Sessions[s.ID]
			if expected != nil && old.Generation != *expected {
				return fail(2, true, "registration generation changed")
			}
			s.Generation = store.Random()
			if s.Harness == "claude" {
				if s.Hints == nil {
					s.Hints = map[string]string{}
				}
				s.Hints["broker_epoch"] = b.Epoch
			}
			s.Machine = c.Machine
			s.LastSeen = b.now()
			s.Tombstone = false
			s.PrunedAt = old.PrunedAt
			s.Successor = old.Successor
			s.ReboundAt = old.ReboundAt
			d.Sessions[s.ID] = s
			receiverNotice(d, s, b.now())
			return nil
		})
	})
}
func validateSession(s store.Session) error {
	for _, v := range []string{s.ID, s.Name} {
		if e := policy.Identity(v); e != nil {
			return e
		}
	}
	if s.Harness != "codex" && s.Harness != "claude" && s.Harness != "hermes" && s.Harness != "muse" {
		return errors.New("invalid session harness")
	}
	if s.State != "live" && s.State != "offline" && s.State != "detached" && s.State != "dead" {
		return errors.New("invalid session state")
	}
	return nil
}
func (b *Broker) Rebind(ctx context.Context, oldID string, next store.Session) error {
	return b.rebind(ctx, oldID, next, nil)
}
func (b *Broker) rebind(ctx context.Context, oldID string, next store.Session, expected *string) error {
	if oldID == next.ID {
		return errors.New("successor must be a different conversation")
	}
	if e := validateSession(next); e != nil {
		return e
	}
	ids := []string{oldID, next.ID}
	sort.Strings(ids)
	u, e := b.lock(ctx, ids[0])
	if e != nil {
		return e
	}
	defer u()
	v, e := b.lock(ctx, ids[1])
	if e != nil {
		return e
	}
	defer v()
	if b.busy(oldID) || b.busy(next.ID) {
		return fail(2, true, "conversation has outstanding work")
	}
	return b.withConfig(ctx, func(c policy.Config) error {
		return b.Store.Update(ctx, func(d *store.Data) error {
			old, ok := d.Sessions[oldID]
			if !ok {
				return errors.New("old conversation is not registered")
			}
			if expected != nil && old.Generation != *expected {
				return fail(2, true, "rebind generation changed")
			}
			if len(old.Outstanding) > 0 {
				return fail(2, true, "receiver has outstanding work")
			}
			old.Generation = ""
			old.State = "offline"
			old.Successor = next.ID
			old.ReboundAt = b.now()
			d.Sessions[oldID] = old
			for _, m := range d.Messages {
				if m.Recipient == oldID && m.State == states.Queued {
					m.Error = "conversation changed; resume the pinned conversation or prune its mailbox"
				}
			}
			next.PrunedAt = d.Sessions[next.ID].PrunedAt
			next.Successor = d.Sessions[next.ID].Successor
			next.ReboundAt = d.Sessions[next.ID].ReboundAt
			next.Tombstone = false
			next.Generation = store.Random()
			if next.Harness == "claude" {
				if next.Hints == nil {
					next.Hints = map[string]string{}
				}
				next.Hints["broker_epoch"] = b.Epoch
			}
			next.Machine = c.Machine
			next.LastSeen = b.now()
			d.Sessions[next.ID] = next
			receiverNotice(d, next, b.now())
			return nil
		})
	})
}
func gateFor(c policy.Config, m *store.Message, s store.Session, admission bool, count int) policy.Decision {
	g, linked := c.Links[m.Origin]
	return policy.Evaluate(c, policy.Input{Killed: c.Killed, OptOut: c.OptOut[m.Recipient], Hop: m.Hop, PairCount: count, Origin: m.Origin, Local: c.Machine, Linked: linked, AutoRun: g.AutoRun, Released: m.Released, Availability: s.State, Blocked: s.Blocked}, admission)
}
func (b *Broker) reconcile(d *store.Data, c policy.Config, now time.Time) error {
	delete(d.Health, "config")
	for _, m := range d.Messages {
		if m.State != states.Queued && m.State != states.Held && m.State != states.Claimed && m.State != states.Outbox {
			continue
		}
		if !now.Before(m.Expires) {
			if e := d.Move(m, states.Expired, "broker", "message TTL expired", now); e != nil {
				return e
			}
			continue
		}
		if m.State == states.Outbox {
			continue
		}
		dec := gateFor(c, m, d.Sessions[m.Recipient], false, 0)
		if dec.Refuse {
			if e := d.Move(m, states.Dead, "broker", dec.Reason, now); e != nil {
				return e
			}
			continue
		}
		if dec.Held && m.State != states.Held {
			if m.State == states.Claimed {
				if e := d.Move(m, states.Queued, "broker", "grant changed", now); e != nil {
					return e
				}
			}
			if e := d.Move(m, states.Held, "broker", dec.Reason, now); e != nil {
				return e
			}
		}
	}
	return nil
}
func (b *Broker) Config(ctx context.Context, patch map[string]json.RawMessage, caller Caller) (policy.Config, error) {
	var out policy.Config
	e := b.withConfig(ctx, func(current policy.Config) error {
		out = current
		if patch == nil {
			return nil
		}
		if e := operator(caller); e != nil {
			return e
		}
		next, e := policy.Patch(current, patch)
		if e != nil {
			return e
		}
		if next.Machine != current.Machine {
			return errors.New("machine identity cannot change while the broker is running")
		}
		if e = policy.Write(b.ConfigPath, next, b.hook); e != nil {
			return e
		}
		defer b.WakeTicks()
		out = next
		return b.Store.Update(ctx, func(d *store.Data) error { return b.reconcile(d, next, b.now()) })
	})
	return out, e
}
func (b *Broker) changeConfig(ctx context.Context, caller Caller, mutate func(*policy.Config) error) (policy.Config, error) {
	var out policy.Config
	e := b.withConfig(ctx, func(c policy.Config) error {
		if e := mutate(&c); e != nil {
			return e
		}
		if e := policy.Write(b.ConfigPath, c, b.hook); e != nil {
			return e
		}
		defer b.WakeTicks()
		out = c
		return b.Store.Update(ctx, func(d *store.Data) error { return b.reconcile(d, c, b.now()) })
	})
	return out, e
}
func (b *Broker) Kill(ctx context.Context, on bool, caller Caller) (int, error) {
	if e := operator(caller); e != nil {
		return 0, e
	}
	_, e := b.changeConfig(ctx, caller, func(c *policy.Config) error { c.Killed = on; return nil })
	if e != nil {
		return 0, e
	}
	if on {
		b.mu.Lock()
		for _, cancel := range b.transfers {
			cancel()
		}
		b.mu.Unlock()
	}
	if on {
		b.WakeTransport()
	} else {
		b.ResetTransport()
	}
	wait, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	return b.Drain(wait), nil
}
func (b *Broker) Allow(ctx context.Context, id string, on bool, caller Caller) error {
	if !caller.Operator() && caller.ID != id {
		return fail(2, false, "only operator can change another session")
	}
	_, e := b.changeConfig(ctx, caller, func(c *policy.Config) error { c.OptOut[id] = !on; return nil })
	return e
}
func (b *Broker) Data(ctx context.Context) (*store.Data, policy.Config, error) {
	var d *store.Data
	var c policy.Config
	e := b.withConfig(ctx, func(config policy.Config) error { c = config; var e error; d, e = b.Store.Read(ctx); return e })
	return d, c, e
}
func (b *Broker) String() string { return fmt.Sprintf("broker %s", b.Epoch) }

// Snapshot uses fallback settings only for display. Admission and dispatch always use Data.
func (b *Broker) Snapshot(ctx context.Context) (*store.Data, policy.Config, error) {
	d, c, e := b.Data(ctx)
	if e == nil {
		return d, c, nil
	}
	if ctx.Err() != nil {
		return nil, c, ctx.Err()
	}
	d, readErr := b.Store.Read(ctx)
	if readErr != nil {
		return nil, c, readErr
	}
	b.gate.Lock()
	encoded, _ := json.Marshal(b.displayConfig)
	b.gate.Unlock()
	_ = json.Unmarshal(encoded, &c)
	var invalid *InvalidConfigError
	if errors.As(e, &invalid) {
		c.Killed = true
		if h, exists := d.Health["config"]; !exists || h.Detail != e.Error() {
			d.Notice("config", e.Error(), ConfigRepairError(b.ConfigPath, b.Store.Home, e), b.now())
		}
	} else {
		d.Notice("config-lock", e.Error(), "retry after the config lock is released", b.now())
	}
	return d, c, nil
}

const receiverWaiting = "receiver has outstanding work; delivery paused"

func receiverNotice(d *store.Data, s store.Session, now time.Time) {
	key := "receiver/" + s.ID
	if len(s.Outstanding) > 0 {
		if _, exists := d.Health[key]; !exists {
			d.Notice(key, receiverWaiting, "reconnect or fence the receiver before clearing outstanding attempts", now)
		}
	} else {
		delete(d.Health, key)
	}
	for _, m := range d.Messages {
		if m.Recipient == s.ID && m.Destination == s.Machine && m.State == states.Queued {
			if len(s.Outstanding) > 0 {
				m.Error = receiverWaiting
			} else if m.Error == receiverWaiting {
				m.Error = ""
			}
		}
	}
}

// ConfigRepairError distinguishes private route failures from JSON settings errors.
func ConfigRepairError(configPath, home string, cause error) string {
	if strings.Contains(cause.Error(), "peers.json") {
		path := filepath.Join(home, "peers.json")
		quoted := "'" + strings.ReplaceAll(path, "'", "'\"'\"'") + "'"
		if info, e := os.Lstat(path); e == nil && info.Mode().IsRegular() && info.Mode().Perm() != 0600 {
			return "chmod 600 " + quoted
		}
		if strings.Contains(cause.Error(), "owned by this account") {
			return `sudo chown "$(id -u):$(id -g)" ` + quoted
		}
		return "ls -l " + quoted
	}
	return ConfigRepair(configPath)
}
