// Package health probes runtime prerequisites without sending agent input.
package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/drivers/codex"
	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/setup"
	"github.com/JeremiahChurch/herd-msg/internal/store"
	"github.com/JeremiahChurch/herd-msg/internal/transport"
)

const MinHerdr = "0.9.3"
const MinCodex = "0.157.1"

// A plain launch starts an absent daemon; explicit remote requires it already running.
const CodexSocketRepair = "bash -ic 'codex'"

type Check struct {
	Check  string `json:"check"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
	Fix    string `json:"fix"`
}
type Report struct {
	OK     bool    `json:"ok"`
	Checks []Check `json:"checks"`
}
type Options struct {
	Home, Config, Machine string
	Background            bool // Service probes must not open an interactive shell.
	Data                  *store.Data
	SocketProbe           func(context.Context) error
	LinkProbe             func(context.Context, broker.PrivatePeer) error
}

func AtLeast(raw, min string) bool {
	parse := func(s string) ([3]int, bool) {
		var n [3]int
		s = strings.TrimPrefix(s, "v")
		parts := strings.Split(s, ".")
		if len(parts) != 3 {
			return n, false
		}
		for i, p := range parts {
			if x := strings.IndexAny(p, "-+"); x >= 0 {
				if i != 2 || p[x] == '-' {
					return n, false
				}
				p = p[:x]
			}
			v, e := strconv.Atoi(p)
			if e != nil || v < 0 {
				return n, false
			}
			n[i] = v
		}
		return n, true
	}
	a, ok := parse(raw)
	if !ok {
		return false
	}
	b, _ := parse(min)
	for i := range a {
		if a[i] != b[i] {
			return a[i] > b[i]
		}
	}
	return true
}
func Version(raw string) string {
	for _, v := range strings.Fields(raw) {
		if AtLeast(v, "0.0.0") {
			return strings.TrimPrefix(v, "v")
		}
	}
	return ""
}
func ShellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
func Doctor(ctx context.Context, o Options) Report {
	r := Report{OK: true, Checks: []Check{}}
	add := func(name string, ok bool, detail, fix string) {
		r.Checks = append(r.Checks, Check{name, ok, detail, fix})
		if !ok {
			r.OK = false
		}
	}
	p, e := setup.Locations()
	if e != nil {
		add("home", false, e.Error(), "herd-msg setup")
		return r
	}
	run := func(name string, args ...string) (string, error) {
		probe, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		b, e := setup.Run(probe, name, args...)
		return strings.TrimSpace(string(b)), e
	}
	_, e = run("systemctl", "--user", "is-active", "--quiet", setup.Unit)
	add("unit-active", e == nil, describe(e == nil, "systemd user service active", "systemd user service is inactive"), "systemctl --user restart herd-msg.service")
	_, e = run("systemctl", "--user", "is-enabled", "--quiet", setup.Unit)
	add("unit-enabled", e == nil, describe(e == nil, "systemd user service enabled", "systemd user service is disabled"), "systemctl --user enable herd-msg.service")
	_, e = os.Stat(p.Unit)
	add("unit-file", e == nil, describe(e == nil, "persistent unit file present", "persistent unit file is missing"), "herd-msg setup")
	user := strconv.Itoa(os.Getuid())
	s, e := run("loginctl", "show-user", user, "--property=Linger", "--value")
	add("linger", e == nil && s == "yes", "linger: "+s, "loginctl enable-linger "+user)
	s, e = run("herdr", "--version")
	v := Version(s)
	add("herdr-version", e == nil && AtLeast(v, MinHerdr), "herdr "+v+"; minimum "+MinHerdr, "herdr update")
	var ce error
	var probe context.Context
	var cancel context.CancelFunc
	if !o.Background {
		// Codex is commonly present only on interactive PATH. Keep stdout separate
		// from interactive-shell job-control warnings.
		probe, cancel = context.WithTimeout(ctx, 5*time.Second)
		cmd := exec.CommandContext(probe, "bash", "-ic", "codex --version")
		cmd.WaitDelay = time.Second
		b, err := cmd.Output()
		cancel()
		v = Version(string(b))
		add("codex-version", (err == nil || errors.Is(err, exec.ErrWaitDelay) && cmd.ProcessState != nil && cmd.ProcessState.Success()) && AtLeast(v, MinCodex), "Codex "+v+"; minimum "+MinCodex, "bash -ic 'npm install -g @openai/codex@latest'")
	}
	socketProbe := o.SocketProbe
	if socketProbe == nil {
		socketProbe = (&codex.Driver{}).Probe
	}
	probe, cancel = context.WithTimeout(ctx, 5*time.Second)
	e = socketProbe(probe)
	cancel()
	detail := "Codex socket protocol ready"
	if e != nil {
		detail = "Codex paused: " + e.Error()
	}
	add("codex-socket", e == nil, detail, CodexSocketRepair)
	claude, ce := setup.Find(ctx, "claude")
	var plugins string
	if ce == nil {
		plugins, ce = run(claude, "plugin", "list", "--json")
	}
	var rows []struct {
		ID          string `json:"id"`
		Enabled     bool   `json:"enabled"`
		InstallPath string `json:"installPath"`
	}
	present := false
	if ce == nil && json.Unmarshal([]byte(plugins), &rows) == nil {
		for _, row := range rows {
			if row.ID == setup.Plugin && row.Enabled {
				_, e = os.Stat(filepath.Join(row.InstallPath, ".claude-plugin/plugin.json"))
				present = e == nil
			}
		}
	}
	add("claude-plugin", present, describe(present, "Claude receiver plugin installed and enabled", "Claude receiver plugin is missing, disabled or unreadable"), "herd-msg setup")
	_, e = os.Stat(p.Skill)
	add("codex-skill", e == nil, describe(e == nil, "Codex messaging skill present", "Codex messaging skill is missing"), "herd-msg setup")
	if o.Data != nil {
		ids := []string{}
		for id, s := range o.Data.Sessions {
			if s.State == "detached" {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		for _, id := range ids {
			add("session/"+id, false, "Codex session detached: "+id, "Exit the detached terminal first, then run bash -ic "+ShellQuote("codex --remote unix:// resume "+ShellQuote(id))+". New Codex sessions need --remote unix:// to receive mail; for herd-spawn, set HERD_CODEX_CMD='codex --yolo --remote unix://'")
		}
	}
	// Offline doctor reads private routes without exposing link keys. Each linked
	// machine is tested independently.
	mailbox := o.Home
	if mailbox == "" {
		mailbox, _ = store.Home("")
	}
	var peers map[string]broker.PrivatePeer
	routePath := filepath.Join(mailbox, "peers.json")
	data, readErr := os.ReadFile(routePath)
	if readErr == nil {
		info, statErr := os.Lstat(filepath.Join(mailbox, "peers.json"))
		if statErr != nil {
			readErr = statErr
		} else if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			readErr = fmt.Errorf("private routes must be a regular file with mode 0600")
		} else {
			if owner, ok := info.Sys().(*syscall.Stat_t); !ok || owner.Uid != uint32(os.Getuid()) {
				readErr = fmt.Errorf("private routes must be owned by this account")
			} else {
				readErr = json.Unmarshal(data, &peers)
				if readErr == nil && peers == nil {
					readErr = fmt.Errorf("private link routes must contain an object")
				}
			}
		}
	}
	if readErr != nil && !os.IsNotExist(readErr) {
		fix := "ls -l " + ShellQuote(routePath)
		if strings.Contains(readErr.Error(), "mode 0600") {
			fix = "chmod 600 " + ShellQuote(routePath)
		}
		if strings.Contains(readErr.Error(), "owned by this account") {
			fix = `sudo chown "$(id -u):$(id -g)" ` + ShellQuote(routePath)
		}
		add("links-config", false, "cannot read private link routes: "+readErr.Error(), fix)
	}
	configPath := o.Config
	if configPath == "" {
		configPath = p.Config
	}
	if config, ce := policy.Read(configPath); ce == nil {
		for name := range config.Links {
			if peer, exists := peers[name]; !exists || peer.Revoked {
				add("link/"+name, false, "private link route is missing or revoked", "herd-msg link "+ShellQuote(name))
			}
		}
	}
	sessions := []store.Session{}
	if o.Data != nil {
		for _, session := range o.Data.Sessions {
			if identity.Visible(session) {
				sessions = append(sessions, identity.Public(session))
			}
		}
	}
	names := []string{}
	for name, peer := range peers {
		if !peer.Revoked {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		peer := peers[name]
		probe, cancel = context.WithTimeout(ctx, 15*time.Second)
		if o.LinkProbe != nil {
			e = o.LinkProbe(probe, peer)
		} else if peer.Target != "" {
			var response broker.Response
			response, e = (transport.SSH{}).Call(probe, peer, "peer-probe", broker.PeerRequest{Machine: o.Machine, Key: peer.Key, Protocol: broker.Protocol, Sessions: sessions})
			if e == nil {
				var reply broker.PeerReply
				e = json.Unmarshal(response.Data, &reply)
				if e == nil && reply.Revoked {
					e = fmt.Errorf("peer revoked this link; relink before sending")
				}
			}
		} else {
			e = nil
			if o.Data == nil || time.Since(o.Data.Links[name].LastContact) > 10*time.Minute {
				e = fmt.Errorf("passive link has no recent incoming contact")
			}
		}
		cancel()
		if e == nil && o.Data != nil {
			link := o.Data.Links[name]
			if !link.Reachable {
				e = fmt.Errorf("broker route unavailable: %s", link.Error)
			}
		}
		fix := "herd-msg link " + ShellQuote(name)
		if peer.Target != "" {
			fix = "ssh -o BatchMode=yes -o StrictHostKeyChecking=yes -- " + ShellQuote(peer.Target) + " " + ShellQuote("test -x "+ShellQuote(peer.Path)) + "; herd-msg link " + ShellQuote(name)
			var sshError *transport.SSHError
			if errors.As(e, &sshError) && sshError.Kind == "host-key" {
				fix = "ssh-keygen -F " + ShellQuote(peer.Target) + "; ssh -G " + ShellQuote(peer.Target)
			}
			if errors.As(e, &sshError) && sshError.Kind == "install" && peer.Label != "" {
				fix = "scripts/deploy.sh " + ShellQuote(peer.Label)
			}
		}
		detail := "link reachable: " + name
		if e != nil {
			detail = "link unavailable: " + name + ": " + e.Error()
		}
		add("link/"+name, e == nil, detail, fix)
	}
	return r
}

// Apply retains the original failure time until the failure itself changes.
func Apply(d *store.Data, r Report, now time.Time) {
	for key := range d.Health {
		if strings.HasPrefix(key, "probe/") {
			deleteCheck := strings.TrimPrefix(key, "probe/")
			found := false
			for _, c := range r.Checks {
				if c.Check == deleteCheck {
					found = true
					break
				}
			}
			if !found {
				delete(d.Health, key)
			}
		}
	}
	for _, c := range r.Checks {
		key := "probe/" + c.Check
		if c.OK {
			delete(d.Health, key)
		} else {
			d.Notice(key, c.Detail, c.Fix, now)
		}
	}
}

func describe(ok bool, pass, fail string) string {
	if ok {
		return pass
	}
	return fail
}
