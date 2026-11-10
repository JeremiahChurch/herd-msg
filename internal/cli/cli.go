package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/drivers/claude"
	"github.com/JeremiahChurch/herd-msg/internal/drivers/codex"
	"github.com/JeremiahChurch/herd-msg/internal/drivers/muse"
	"github.com/JeremiahChurch/herd-msg/internal/health"
	harnessidentity "github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/notify"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/receiver"
	"github.com/JeremiahChurch/herd-msg/internal/setup"
	"github.com/JeremiahChurch/herd-msg/internal/store"
	"github.com/JeremiahChurch/herd-msg/internal/transport"
)

type options struct {
	json                          bool
	id, created, session, updated string
	hop, expect                   *int
	confirm                       bool
}

func parse(args []string) ([]string, options, error) {
	var rest []string
	var o options
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			rest = append(rest, args[i+1:]...)
			break
		}
		switch arg {
		case "--json":
			o.json = true
		case "--confirm":
			o.confirm = true
		case "--id", "--created", "--session", "--if-updated", "--hop", "--expect-unsent":
			if i+1 >= len(args) {
				return nil, o, fmt.Errorf("%s needs a value", arg)
			}
			i++
			v := args[i]
			switch arg {
			case "--id":
				o.id = v
			case "--created":
				o.created = v
			case "--session":
				o.session = v
			case "--if-updated":
				o.updated = v
			case "--hop", "--expect-unsent":
				n, e := strconv.Atoi(v)
				if e != nil {
					return nil, o, e
				}
				if arg == "--hop" {
					o.hop = &n
				} else {
					o.expect = &n
				}
			}
		default:
			rest = append(rest, arg)
		}
	}
	return rest, o, nil
}
func identity() (broker.Caller, error) {
	c := broker.Caller{ID: "operator"}
	for _, h := range []struct{ harness, variable string }{{"codex", "CODEX_THREAD_ID"}, {"claude", "CLAUDE_CODE_SESSION_ID"}, {"muse", "MUSE_SESSION_ID"}} {
		if id := os.Getenv(h.variable); id != "" {
			if !c.Operator() {
				return broker.Caller{}, errors.New("both harness identity variables are set; remove inherited identities from the launcher")
			}
			c = broker.Caller{ID: id, Harness: h.harness}
		}
	}
	return c, c.Validate()
}

type command struct {
	ctx                       context.Context
	out, err                  io.Writer
	version, home, configPath string
	caller                    broker.Caller
	opts                      options
	knownID, knownCreated     string
	exitCode                  int
}

func (c *command) print(v any) error {
	if c.opts.json {
		return json.NewEncoder(c.out).Encode(v)
	}
	switch v := v.(type) {
	case health.Report:
		for _, check := range v.Checks {
			state := "pass"
			if !check.OK {
				state = "fail"
			}
			fmt.Fprintf(c.out, "%s %s: %s\n", state, check.Check, check.Detail)
			if !check.OK {
				fmt.Fprintln(c.out, "  fix:", check.Fix)
			}
		}
		return nil
	case broker.Receipt:
		_, e := fmt.Fprintf(c.out, "%s %s\n", v.ID, v.State)
		if v.Notice != "" {
			fmt.Fprintln(c.out, v.Notice)
		}
		return e
	case json.RawMessage:
		var decoded any
		if e := json.Unmarshal(v, &decoded); e != nil {
			return e
		}
		b, e := json.MarshalIndent(decoded, "", "  ")
		if e == nil {
			_, e = fmt.Fprintln(c.out, string(b))
		}
		return e
	default:
		b, e := json.MarshalIndent(v, "", "  ")
		if e == nil {
			_, e = fmt.Fprintln(c.out, string(b))
		}
		return e
	}
}
func (c *command) failure(e error) int {
	code := 1
	retry := false
	var be *broker.Error
	var ambiguous *store.AmbiguousError
	if errors.As(e, &ambiguous) {
		code = 3
	}
	if errors.As(e, &be) {
		code = be.Code
		retry = be.Retryable
	}
	if c.opts.json {
		v := map[string]any{"error": e.Error(), "retryable": retry}
		if c.knownID != "" {
			v["id"] = c.knownID
			v["created"] = c.knownCreated
		}
		_ = json.NewEncoder(c.out).Encode(v)
	} else {
		fmt.Fprintln(c.err, e)
		// Definite non-retryable refusals (code 2) and ambiguous addresses (code 3)
		// were never recorded, and repeating them fails the same way. Internal
		// errors (code 1) and uncertain outcomes keep the hint.
		definite := ambiguous != nil || (be != nil && !be.Retryable && be.Code != 1)
		if c.knownID != "" && !definite {
			fmt.Fprintf(c.err, "Retry with --id %s --created %s\n", c.knownID, c.knownCreated)
		}
	}
	return code
}
func (c *command) rpc(r broker.Request) (json.RawMessage, error) {
	r.Caller = c.caller
	response, e := broker.Call(c.ctx, c.home, r)
	if e != nil {
		return nil, e
	}
	if response.Code != 0 {
		return nil, &broker.Error{Code: response.Code, Message: response.Error, Retryable: response.Retryable}
	}
	return response.Data, nil
}
func (c *command) snapshot() (broker.Snapshot, bool, error) {
	raw, e := c.rpc(broker.Request{Verb: "snapshot"})
	if e == nil {
		var snap broker.Snapshot
		e = json.Unmarshal(raw, &snap)
		return snap, false, e
	}
	var be *broker.Error
	if errors.As(e, &be) && be.Code != 4 {
		return broker.Snapshot{}, false, e
	}
	d, e := store.Snapshot(c.ctx, c.home)
	if e != nil {
		return broker.Snapshot{}, true, e
	}
	config, e := policy.Read(c.configPath)
	if e == nil {
		e = broker.CheckMachine(d, config.Machine)
	}
	validConfig := e == nil
	if e != nil {
		config = policy.Defaults()
		config.Killed = true
		for _, s := range d.Sessions {
			if s.Machine != "" {
				config.Machine = s.Machine
				break
			}
		}
		if len(d.Sessions) == 0 {
			for _, m := range d.Messages {
				config.Machine = m.Origin
				break
			}
		}
		since := time.Now()
		if info, statErr := os.Stat(c.configPath); statErr == nil {
			since = info.ModTime()
		}
		d.Notice("config", "invalid config: "+e.Error(), broker.ConfigRepairError(c.configPath, c.home, e), since)
		e = nil
	}
	if validConfig {
		delete(d.Health, "config")
	}
	return broker.Snapshot{Data: d, Config: config}, true, e
}
func (c *command) offlineConfig(r broker.Request) (any, error) {
	if r.Verb == "config" && r.Patch == nil {
		return policy.Read(c.configPath)
	}
	if !c.caller.Operator() {
		return nil, &broker.Error{Code: 2, Message: "this action requires operator identity"}
	}
	if e := store.PrivateDir(filepath.Dir(c.configPath)); e != nil {
		return nil, e
	}
	lockctx, cancel := context.WithTimeout(c.ctx, time.Second)
	defer cancel()
	lock, e := policy.Lock(lockctx, c.configPath+".lock")
	if e != nil {
		return nil, e
	}
	defer policy.Unlock(lock)
	config, e := policy.Read(c.configPath)
	if e != nil {
		return nil, e
	}
	if r.Verb == "kill" {
		config.Killed = r.On
	} else if r.Patch != nil {
		next, err := policy.Patch(config, r.Patch)
		if err != nil {
			return nil, err
		}
		if next.Machine != config.Machine {
			d, err := store.Snapshot(c.ctx, c.home)
			if err != nil {
				return nil, err
			}
			if len(d.Messages) > 0 || len(d.Dedupe) > 0 || len(d.Sessions) > 0 || len(d.Links) > 0 {
				return nil, &broker.Error{Code: 2, Message: "machine identity cannot change while mailbox records are retained"}
			}
		}
		config = next
	} else {
		return config, nil
	}
	if e = policy.Write(c.configPath, config, nil); e != nil {
		return nil, e
	}
	if r.Verb == "kill" {
		return map[string]int{"in_flight": 0}, nil
	}
	return config, nil
}
func Run(ctx context.Context, args []string, out, errout io.Writer, version string) int {
	rest, o, e := parse(args)
	c := &command{ctx: ctx, out: out, err: errout, version: version, opts: o}
	if e != nil {
		return c.failure(e)
	}
	if (len(rest) > 0 && (rest[0] == "help" || rest[0] == "--help")) || (len(rest) == 2 && rest[1] == "--help") {
		fmt.Fprintln(out, "herd-msg send <name[.harness][@machine]|session-id> <body> [--json]\nherd-msg reply <message-id> <body>\nherd-msg peers [--all] | status | config | inbox | log | show | read\nherd-msg link <saved-machine|linked-name> [--auto-run|--no-auto-run]\nherd-msg unlink <linked-name> [--keep|--drop --expect-unsent N]\nherd-msg release | resolve | prune | allow | kill | name | watch | broker\nherd-msg setup | doctor | uninstall [--purge]")
		return 0
	}

	if len(rest) == 0 || rest[0] == "version" || rest[0] == "--version" {
		fmt.Fprintln(out, version)
		return 0
	}
	c.caller, e = identity()
	if e != nil {
		return c.failure(e)
	}
	c.home, e = store.Home("")
	if e != nil {
		return c.failure(e)
	}
	c.configPath, e = policy.ConfigPath(c.home)
	if e != nil {
		return c.failure(e)
	}
	value, e := c.execute(rest)
	if e != nil {
		return c.failure(e)
	}
	if value != nil {
		if e = c.print(value); e != nil {
			return c.failure(e)
		}
	}
	return c.exitCode
}
func (c *command) execute(args []string) (any, error) {
	verb := args[0]
	args = args[1:]
	switch verb {
	case "deploy-target":
		if len(args) != 1 {
			return nil, errors.New("deploy-target requires a saved machine label")
		}
		machines, e := harnessidentity.Machines(c.ctx)
		if e != nil {
			return nil, e
		}
		m, e := harnessidentity.FindMachine(machines, args[0])
		if e != nil {
			return nil, e
		}
		if m.Target == "" || strings.HasPrefix(m.Target, "-") || strings.ContainsAny(m.Target, "\r\n\x00") {
			return nil, errors.New("invalid saved SSH target")
		}
		_, e = fmt.Fprintln(c.out, m.Target)
		return nil, e
	case "setup", "uninstall":
		if !c.caller.Operator() {
			return nil, &broker.Error{Code: 2, Message: "this action requires operator identity"}
		}
		if verb == "setup" {
			if len(args) != 0 {
				return nil, errors.New("setup takes no arguments")
			}
			source, e := os.Executable()
			if e != nil {
				return nil, e
			}
			return setup.Install(c.ctx, source)
		}
		if len(args) > 1 || len(args) == 1 && args[0] != "--purge" {
			return nil, errors.New("uninstall accepts only --purge")
		}
		return setup.Remove(c.ctx, len(args) == 1)
	case "doctor":
		if len(args) != 0 {
			return nil, errors.New("doctor takes no arguments")
		}
		snap, _, e := c.snapshot()
		if e != nil {
			return nil, e
		}
		report := health.Doctor(c.ctx, health.Options{Home: c.home, Config: c.configPath, Machine: snap.Config.Machine, Data: snap.Data})
		if e = c.print(report); e != nil {
			return nil, e
		}
		if !report.OK {
			c.exitCode = 1
		}
		return nil, nil
	case "accept", "drain", "ack", "peer-probe", "peer-info", "peer-route-proof", "peer-install", "peer-revoke", "peer-reverse":
		return nil, transport.Endpoint(c.ctx, c.home, verb, os.Stdin, c.out, c.caller)
	case "link", "unlink":
		return c.rpc(broker.Request{Verb: "network-" + verb, Peer: &broker.PeerRequest{Args: args, Expected: c.opts.expect}})
	case "receiver":
		return nil, receiver.Run(c.ctx, c.home, os.Stdin, c.out, c.err)
	case "receiver-hook":
		return nil, receiver.Hook(c.ctx, c.home, os.Stdin)
	case "name":
		if len(args) != 1 {
			return nil, errors.New("name requires one name")
		}
		return c.rpc(broker.Request{Verb: "name", Name: args[0]})
	case "peers":
		all := false
		for _, arg := range args {
			if arg != "--all" {
				return nil, errors.New("peers accepts only --all")
			}
			all = true
		}
		snap, offline, e := c.snapshot()
		if e != nil {
			return nil, e
		}
		peers := harnessidentity.Peers(snap.Data, snap.Config, time.Now(), offline)
		// With the broker down every peer reads offline, so filtering would show nothing.
		if all || offline {
			return peers, nil
		}
		reachable, hidden := harnessidentity.Reachable(peers)
		if hidden > 0 {
			fmt.Fprintf(c.err, "%d offline or ended peers hidden; use herd-msg peers --all\n", hidden)
		}
		return reachable, nil
	case "broker":
		return nil, c.runBroker()
	case "send", "reply":
		if len(args) < 2 {
			return nil, errors.New("send/reply requires a recipient/message ID and body")
		}
		now := time.Now().UTC()
		id := c.opts.id
		created := now
		var e error
		if id == "" {
			id = broker.NewID(now)
		} else if c.opts.created == "" {
			created, e = broker.CreatedFromID(id)
			if e != nil {
				return nil, e
			}
		}
		if c.opts.created != "" {
			created, e = time.Parse(time.RFC3339Nano, c.opts.created)
			if e != nil {
				return nil, e
			}
		}
		c.knownID = id
		c.knownCreated = stamp(created)
		r := broker.SendRequest{ID: id, Created: created, To: args[0], Body: strings.Join(args[1:], " "), Hop: c.opts.hop}
		if len(r.Body) > 65536 {
			return nil, &broker.Error{Code: 2, Message: "body exceeds 65536 bytes"}
		}
		if verb == "reply" {
			snap, _, e := c.snapshot()
			if e != nil {
				return nil, e
			}
			parent, e := snap.Data.Find(args[0])
			if e != nil {
				return nil, e
			}
			if parent.SenderID == "operator" {
				return nil, errors.New("operator messages have no reply session")
			}
			r.ParentID = parent.ID
			r.To = parent.SenderID + "@" + parent.Origin
		}
		// Retry only transport errors with the exact frozen request.
		raw, e := c.rpc(broker.Request{Verb: "send", Send: r})
		if e != nil {
			var be *broker.Error
			if !errors.As(e, &be) {
				raw, e = c.rpc(broker.Request{Verb: "send", Send: r})
			}
		}
		if e != nil {
			return nil, e
		}
		var receipt broker.Receipt
		e = json.Unmarshal(raw, &receipt)
		return receipt, e
	case "status", "watch", "inbox", "log", "read", "show":
		if verb == "watch" {
			return nil, c.watch()
		}
		snap, offline, e := c.snapshot()
		if e != nil {
			return nil, e
		}
		if verb == "status" {
			return BuildStatus(snap.Data, snap.Config, c.version, offline, time.Now()), nil
		}
		if verb == "read" || verb == "show" {
			if len(args) != 1 {
				return nil, errors.New("read/show requires a message ID")
			}
			return snap.Data.Find(args[0])
		}
		var messages []*store.Message
		for _, m := range snap.Data.Messages {
			if verb == "log" || (m.Recipient == c.caller.ID && m.Destination == snap.Config.Machine) {
				messages = append(messages, m)
			}
		}
		sort.Slice(messages, func(i, j int) bool { return messages[i].Sequence < messages[j].Sequence })
		rows := []MessageStatus{}
		for _, m := range messages {
			rows = append(rows, summary(m))
		}
		return rows, nil
	case "release", "resolve", "prune", "allow", "kill":
		r := broker.Request{Verb: verb, Confirm: c.opts.confirm, IfUpdated: c.opts.updated, ExpectUnsent: c.opts.expect}
		switch verb {
		case "release":
			if len(args) != 1 {
				return nil, errors.New("release requires a message ID")
			}
			r.ID = args[0]
		case "resolve":
			if len(args) != 2 {
				return nil, errors.New("resolve requires a message ID and resend|drop")
			}
			r.ID = args[0]
			r.Action = args[1]
		case "prune":
			if c.opts.session == "" || len(args) != 0 {
				return nil, errors.New("prune requires --session ID")
			}
			r.ID = c.opts.session
		case "allow":
			if c.opts.session == "" || len(args) != 1 {
				return nil, errors.New("allow requires --session ID on|off")
			}
			r.ID = c.opts.session
		case "kill":
			if len(args) != 1 {
				return nil, errors.New("kill requires on|off")
			}
		}
		if verb == "kill" || verb == "allow" {
			if args[0] != "on" && args[0] != "off" {
				return nil, errors.New("value must be on or off")
			}
			r.On = args[0] == "on"
		}
		raw, e := c.rpc(r)
		if e != nil && verb == "kill" {
			var be *broker.Error
			if errors.As(e, &be) && be.Code == 4 {
				return c.offlineConfig(r)
			}
		}
		return raw, e
	case "config":
		if len(args) == 0 || len(args) == 1 && args[0] == "get" {
			raw, e := c.rpc(broker.Request{Verb: "config"})
			if e != nil {
				var be *broker.Error
				if errors.As(e, &be) && be.Code == 4 {
					return c.offlineConfig(broker.Request{Verb: "config"})
				}
			}
			return raw, e
		}
		if !c.caller.Operator() {
			return nil, &broker.Error{Code: 2, Message: "this action requires operator identity"}
		}
		patch := map[string]json.RawMessage{}
		if len(args) == 3 && args[0] == "set" {
			patch[args[1]] = json.RawMessage(args[2])
		} else if len(args) == 1 {
			if e := json.Unmarshal([]byte(args[0]), &patch); e != nil {
				return nil, e
			}
			if patch == nil {
				return nil, errors.New("config patch must be an object")
			}
		} else {
			return nil, errors.New("config accepts get, set KEY JSON, or a JSON object")
		}
		r := broker.Request{Verb: "config", Patch: patch}
		raw, e := c.rpc(r)
		if e != nil {
			var be *broker.Error
			if errors.As(e, &be) && be.Code == 4 {
				return c.offlineConfig(r)
			}
		}
		return raw, e

	default:
		return nil, fmt.Errorf("unknown command %s", verb)
	}
}
func (c *command) watch() error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if c.ctx.Err() != nil {
			return nil
		}
		snap, offline, e := c.snapshot()
		if e != nil {
			if c.ctx.Err() != nil {
				return nil
			}
			return e
		}
		status := BuildStatus(snap.Data, snap.Config, c.version, offline, time.Now())
		if c.opts.json {
			e = c.print(status)
		} else {
			e = RenderWatch(c.out, status)
		}
		if e != nil {
			return e
		}
		select {
		case <-c.ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
func (c *command) runBroker() error {
	if !c.caller.Operator() {
		return &broker.Error{Code: 2, Message: "broker must be started by operator"}
	}
	ctx, cancel := signal.NotifyContext(c.ctx, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if e := store.PrivateDir(filepath.Dir(c.configPath)); e != nil {
		return e
	}
	if e := harnessidentity.SelectMachine(ctx, c.home, c.configPath); e != nil {
		return c.startupFailure(ctx, e)
	}
	cd := &codex.Driver{HerdrSession: os.Getenv("HERDR_SESSION")}
	md := &muse.Driver{HerdrSession: os.Getenv("HERDR_SESSION")}
	b, e := broker.New(c.home, c.configPath, map[string]broker.Driver{"codex": cd, "claude": &claude.Driver{}, "muse": md})
	if e != nil {
		if e.Error() == "broker already running for this home" {
			return e
		}
		return c.startupFailure(ctx, e)
	}
	defer b.Close()
	manager := &transport.Manager{Broker: b}
	b.Network = manager.Command
	transportDone := make(chan struct{})
	defer func() { cancel(); <-transportDone }()
	go func() { defer close(transportDone); manager.Run(ctx) }()
	server, e := b.Listen()
	if e != nil {
		return e
	}
	defer server.Close()
	healthDone := make(chan struct{})
	defer func() { cancel(); <-healthDone }()
	go func() { defer close(healthDone); c.monitor(ctx, b) }()
	var maintenance sync.WaitGroup
	startMaintenance := func(work func()) { maintenance.Add(1); go func() { defer maintenance.Done(); work() }() }
	defer func() { cancel(); maintenance.Wait() }()
	startMaintenance(func() {
		broker.WatchWrites(ctx, broker.WriteProbeInterval, broker.WriteStallLimit, b.WriteProbe, func(since time.Time, cause error) { c.writesStalled(since, cause) })
	})
	startMaintenance(func() { harnessidentity.WatchAgents(ctx, os.Getenv("HERDR_SESSION"), b.WakeDiscovery) })
	startMaintenance(func() {
		b.RunDiscovery(ctx, func(ctx context.Context) {
			pass := harnessidentity.CachedAgents(ctx)
			probe, stop := context.WithTimeout(pass, 2*time.Second)
			sessions, err := cd.Discover(probe)
			if err == nil {
				_ = b.Refresh(probe, sessions)
			} else {
				_ = b.DriverNotice(probe, "codex", err.Error())
			}
			stop()
			// Muse is optional and requires an explicit per-process opt-in. Do not
			// probe an absent or unopened installation every discovery pass.
			if os.Getenv("MUSE_EXPERIMENTAL_EXTERNAL_AGENT_INGRESS") == "1" {
				probe, stop = context.WithTimeout(pass, 2*time.Second)
				sessions, err = md.Discover(probe)
				if err == nil {
					_ = b.RefreshHarness(probe, "muse", sessions)
				} else {
					_ = b.DriverNotice(probe, "muse", err.Error())
				}
				stop()
			}
			probe, stop = context.WithTimeout(pass, 2*time.Second)
			_ = b.RefreshAvailability(probe)
			stop()
		})
	})
	startMaintenance(func() { b.RunTicks(ctx) })

	e = server.Serve(ctx)
	b.Stop()
	wait, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	b.Drain(wait)
	return e
}

// monitor runs a complete startup probe and repeats it every ten minutes.
// Alerts use the current mailbox between probes so stuck mail is reported promptly.
func (c *command) monitor(ctx context.Context, b *broker.Broker) {
	c.monitorInterval(ctx, b, 10*time.Second)
}
func (c *command) monitorInterval(ctx context.Context, b *broker.Broker, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	next := time.Time{}
	notifier := notify.Notifier{Path: filepath.Join(c.home, "notifications.json")}
	isolated := os.Getenv("HERD_MSG_HOME") != ""
	for {
		if !time.Now().Before(next) {
			d, config, e := b.Snapshot(ctx)
			if e == nil {
				var report health.Report
				if isolated {
					probe, stop := context.WithTimeout(ctx, 5*time.Second)
					e := (&codex.Driver{}).Probe(probe)
					stop()
					check := health.Check{Check: "codex-socket", OK: e == nil, Detail: "Codex socket ready", Fix: health.CodexSocketRepair}
					if e != nil {
						check.Detail = e.Error()
					}
					report = health.Report{OK: e == nil, Checks: []health.Check{check}}
				} else {
					report = health.Doctor(ctx, health.Options{Home: c.home, Config: c.configPath, Machine: config.Machine, Data: d, Background: true})
				}
				reasons := map[string]string{}
				for _, check := range report.Checks {
					if !check.OK {
						switch check.Check {
						case "codex-socket":
							reasons["codex"] += check.Detail + "; "
						case "claude-plugin":
							reasons["claude"] += check.Detail + "; "
						}
					}
				}
				b.PauseDriver("codex", reasons["codex"])
				b.PauseDriver("claude", reasons["claude"])
				_ = b.Store.Update(ctx, func(d *store.Data) error { health.Apply(d, report, time.Now()); return nil })
			}
			next = time.Now().Add(10 * time.Minute)
		}
		if d, config, e := b.Snapshot(ctx); e == nil && !isolated {
			notifier.ActiveLinks = map[string]bool{}
			for name := range config.Links {
				notifier.ActiveLinks[name] = true
			}
			if e = notifier.Sweep(ctx, d, time.Now()); e != nil && ctx.Err() == nil {
				fmt.Fprintln(c.err, "notification:", e)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// writesStalled ends the broker so its supervisor restarts it. Accepted mail is durable
// and a restart has cleared every stall seen so far, while staying up keeps refusing mail.
func (c *command) writesStalled(since time.Time, cause error) {
	detail := "mailbox writes stopped at " + since.Format("Jan 2 15:04:05")
	if cause != nil {
		detail += ": " + cause.Error()
	}
	fmt.Fprintln(c.err, "watchdog:", detail+"; restarting broker")
	_ = pprof.Lookup("goroutine").WriteTo(c.err, 2)
	if os.Getenv("HERD_MSG_HOME") == "" {
		done := make(chan struct{})
		go func() {
			defer close(done)
			wait, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			n := notify.Notifier{Path: filepath.Join(c.home, "notifications.json")}
			if e := n.Failure(wait, "watchdog", detail+"; the broker restarted itself", "herd-msg doctor; stack dump is in journalctl --user -u herd-msg"); e != nil {
				fmt.Fprintln(c.err, "watchdog notification:", e)
			}
		}()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
	}
	os.Exit(1)
}

func (c *command) startupFailure(ctx context.Context, cause error) error {
	if os.Getenv("HERD_MSG_HOME") != "" {
		return cause
	}
	detail := cause.Error()
	if !strings.HasPrefix(detail, "invalid config:") {
		detail = "invalid config: " + detail
	}
	n := notify.Notifier{Path: filepath.Join(c.home, "notifications.json")}
	if e := n.Failure(ctx, "config", detail, broker.ConfigRepairError(c.configPath, c.home, cause)); e != nil {
		fmt.Fprintln(c.err, "startup notification:", e)
	}
	return cause
}
