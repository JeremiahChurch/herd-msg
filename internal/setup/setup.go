// Package setup installs and removes only herd-msg's owned resources.
package setup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	assets "github.com/JeremiahChurch/herd-msg"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
)

const Plugin = "herd-msg@herd-msg-local"
const Unit = "herd-msg.service"

type Paths struct{ Home, Binary, Root, Unit, Skill, Claude, Config, Data string }

func Locations() (Paths, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Paths{}, err
	}
	conf := os.Getenv("XDG_CONFIG_HOME")
	if conf == "" {
		conf = filepath.Join(home, ".config")
	}
	data := os.Getenv("XDG_DATA_HOME")
	if data == "" {
		data = filepath.Join(home, ".local/share")
	}
	codex := os.Getenv("CODEX_HOME")
	if codex == "" {
		codex = filepath.Join(home, ".codex")
	}
	claude := os.Getenv("CLAUDE_CONFIG_DIR")
	if claude == "" {
		claude = filepath.Join(home, ".claude")
	}
	return Paths{Home: home, Binary: filepath.Join(home, ".local/bin/herd-msg"), Root: filepath.Join(data, "herd-msg/plugin"), Unit: filepath.Join(conf, "systemd/user", Unit), Skill: filepath.Join(codex, "skills/herd-msg/SKILL.md"), Claude: claude, Config: filepath.Join(conf, "herd-msg/config.json"), Data: filepath.Join(home, ".local/share/herd-msg")}, nil
}

type Result struct {
	Installed bool     `json:"installed"`
	Linger    bool     `json:"linger"`
	Notes     []string `json:"notes"`
}
type prior struct {
	path   string
	data   []byte
	mode   fs.FileMode
	exists bool
}
type transaction struct{ files []prior }

func (t *transaction) write(path string, data []byte, mode fs.FileMode) error {
	info, err := os.Lstat(path)
	p := prior{path: path}
	if err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("refusing to replace non-regular file %s", path)
		}
		p.data, err = os.ReadFile(path)
		if err != nil {
			return err
		}
		p.mode = info.Mode().Perm()
		p.exists = true
	} else if !os.IsNotExist(err) {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	t.files = append(t.files, p)
	return Atomic(path, data, mode)
}
func Atomic(path string, data []byte, mode fs.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".herd-msg-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if err = f.Chmod(mode); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err == nil {
		err = ce
	}
	if err != nil {
		return err
	}
	return os.Rename(name, path)
}
func (t *transaction) rollback() error {
	var errs []error
	for i := len(t.files) - 1; i >= 0; i-- {
		p := t.files[i]
		var err error
		if p.exists {
			err = Atomic(p.path, p.data, p.mode)
		} else {
			err = os.Remove(p.path)
			if os.IsNotExist(err) {
				err = nil
			}
			_ = os.Remove(filepath.Dir(p.path))
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
func listed(data []byte, id string) bool {
	var rows []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if json.Unmarshal(data, &rows) != nil {
		return false
	}
	for _, r := range rows {
		if r.ID == id || r.Name == id {
			return true
		}
	}
	return false
}
func unitQuote(s string) string { return strconv.Quote(strings.ReplaceAll(s, "%", "%%")) }
func quote(s string) string     { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }

func Install(ctx context.Context, source string) (Result, error) {
	return install(ctx, source, waitBroker)
}

func install(ctx context.Context, source string, ready func(context.Context, string, string) error) (res Result, err error) {
	res.Notes = []string{}
	if os.Getenv("HERD_MSG_HOME") != "" {
		return res, errors.New("setup uses the account mailbox; unset HERD_MSG_HOME first")
	}
	p, err := Locations()
	if err != nil {
		return res, err
	}
	if _, err = Run(ctx, "systemctl", "--user", "show-environment"); err != nil {
		return res, fmt.Errorf("systemd user manager is required; start it before setup: %w", err)
	}
	claude, err := Find(ctx, "claude")
	if err != nil {
		return res, err
	}
	herdr, findErr := Find(ctx, "herdr")
	if findErr != nil {
		return res, findErr
	}
	hv, versionErr := Run(ctx, herdr, "--version")
	if versionErr != nil {
		return res, versionErr
	}
	var major, minor, patch int
	if _, scanErr := fmt.Sscanf(strings.TrimSpace(string(hv)), "herdr %d.%d.%d", &major, &minor, &patch); scanErr != nil || major == 0 && (minor < 9 || minor == 9 && patch < 3) {
		return res, errors.New("herdr 0.9.3 or newer is required; run herdr update")
	}
	plugins, err := Run(ctx, claude, "plugin", "list", "--json")
	if err != nil {
		return res, err
	}
	markets, err := Run(ctx, claude, "plugin", "marketplace", "list", "--json")
	if err != nil {
		return res, err
	}
	if !validRegistry(plugins) || !validRegistry(markets) {
		return res, errors.New("invalid Claude plugin registry response")
	}
	hadPlugin, hadMarket := listed(plugins, Plugin), listed(markets, "herd-msg-local")
	registry, registryErr := Run(ctx, herdr, "plugin", "list", "--json")
	if registryErr != nil {
		return res, registryErr
	}
	previousRoot, hadLink, registryErr := herdrLink(registry)
	if registryErr != nil {
		return res, registryErr
	}
	_, activeErr := Run(ctx, "systemctl", "--user", "is-active", "--quiet", Unit)
	_, enabledErr := Run(ctx, "systemctl", "--user", "is-enabled", "--quiet", Unit)
	tx := &transaction{}
	touchedMarket, touchedPlugin, linked, service := false, false, false, false
	// A native registration command can mutate its registry before returning an error.
	defer func() {
		if err == nil {
			return
		}
		var failures []error
		undo := func(name string, args ...string) {
			data, e := Run(context.Background(), name, args...)
			if e != nil && (strings.Contains(string(data), `"not_installed"`) || strings.Contains(string(data), `"not_configured"`)) {
				e = nil
			}
			if e != nil {
				failures = append(failures, e)
			}
		}
		if service {
			undo("systemctl", "--user", "stop", Unit)
			if enabledErr != nil {
				undo("systemctl", "--user", "disable", Unit)
			}
		}
		if touchedPlugin && !hadPlugin {
			undo(claude, "plugin", "uninstall", Plugin, "--scope", "user", "--json")
		}
		if touchedMarket && !hadMarket {
			undo(claude, "plugin", "marketplace", "remove", "herd-msg-local", "--scope", "user", "--json")
		}
		if linked {
			undo(herdr, "plugin", "unlink", "herd-msg")
		}
		if e := tx.rollback(); e != nil {
			failures = append(failures, e)
		}
		if !hadPlugin {
			if e := os.RemoveAll(filepath.Join(p.Claude, "plugins/cache/herd-msg-local")); e != nil {
				failures = append(failures, e)
			}
		}
		if !hadMarket {
			if e := os.RemoveAll(filepath.Join(p.Claude, "plugins/marketplaces/herd-msg-local")); e != nil {
				failures = append(failures, e)
			}
		}
		pruneEmpty(p.Root)
		if hadLink && previousRoot != "" {
			undo(herdr, "plugin", "link", previousRoot)
		}
		_ = os.Remove(filepath.Dir(p.Skill))
		undo("systemctl", "--user", "daemon-reload")
		if service && activeErr == nil {
			undo("systemctl", "--user", "restart", Unit)
		}
		if len(failures) > 0 {
			err = fmt.Errorf("%w; rollback needs repair: %v; rerun herd-msg setup", err, errors.Join(failures...))
		}
	}()
	binary, err := os.ReadFile(source)
	if err != nil {
		return res, err
	}
	if err = tx.write(p.Binary, binary, 0755); err != nil {
		return res, err
	}
	err = fs.WalkDir(assets.Files, ".", func(path string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		data, e := assets.Files.ReadFile(path)
		if e != nil {
			return e
		}
		if path == "claude-plugin/.claude-plugin/plugin.json" {
			data = []byte(strings.ReplaceAll(string(data), `"command": "herd-msg"`, `"command": `+strconv.Quote(p.Binary)))
		}
		if path == "claude-plugin/hooks/hooks.json" {
			var v any
			if e = json.Unmarshal(data, &v); e != nil {
				return e
			}
			data = []byte(strings.ReplaceAll(string(data), "herd-msg receiver-hook", strings.Trim(strconv.Quote(quote(p.Binary)+" receiver-hook"), `"`)))
		}
		return tx.write(filepath.Join(p.Root, path), data, 0600)
	})
	if err != nil {
		return res, err
	}
	skill, err := assets.Files.ReadFile("skills/herd-msg/SKILL.md")
	if err != nil {
		return res, err
	}
	if err = tx.write(p.Skill, skill, 0600); err != nil {
		return res, err
	}
	unit := "[Unit]\nDescription=Durable agent mailbox\nAfter=default.target\n\n[Service]\nType=simple\nExecStart=" + unitQuote(p.Binary) + " broker\nRestart=on-failure\nRestartSec=5\nUMask=0077\nEnvironment=" + unitQuote("PATH="+strings.Join([]string{filepath.Dir(p.Binary), filepath.Dir(herdr), filepath.Dir(claude), "/usr/local/bin", "/usr/bin", "/bin"}, ":")) + "\nUnsetEnvironment=CODEX_THREAD_ID CLAUDE_CODE_SESSION_ID\n\n[Install]\nWantedBy=default.target\n"
	for _, key := range []string{"CODEX_HOME", "CLAUDE_CONFIG_DIR", "XDG_CONFIG_HOME", "XDG_DATA_HOME"} {
		if value := os.Getenv(key); value != "" {
			if !filepath.IsAbs(value) {
				return res, fmt.Errorf("%s must be an absolute path", key)
			}
			unit = strings.Replace(unit, "UnsetEnvironment=", "Environment="+unitQuote(key+"="+value)+"\nUnsetEnvironment=", 1)
		}
	}
	if err = tx.write(p.Unit, []byte(unit), 0600); err != nil {
		return res, err
	}
	if _, e := os.Stat(p.Config); os.IsNotExist(e) {
		data, _ := json.MarshalIndent(policy.Defaults(), "", "  ")
		if err = tx.write(p.Config, append(data, '\n'), 0600); err != nil {
			return res, err
		}
	} else if e != nil {
		return res, e
	}
	linked = !hadLink
	if _, err = Run(ctx, herdr, "plugin", "link", p.Root); err != nil {
		return res, err
	}
	if !hadMarket {
		touchedMarket = true
		if _, err = Run(ctx, claude, "plugin", "marketplace", "add", p.Root, "--scope", "user", "--json"); err != nil {
			return res, err
		}
	}
	touchedPlugin = true
	if _, err = Run(ctx, claude, "plugin", "install", Plugin, "--scope", "user", "--json"); err != nil {
		return res, err
	}
	if _, err = Run(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return res, err
	}
	oldEpoch := socketEpoch(p.Data)
	service = true
	if _, err = Run(ctx, "systemctl", "--user", "enable", Unit); err != nil {
		return res, err
	}
	if _, err = Run(ctx, "systemctl", "--user", "restart", Unit); err != nil {
		return res, err
	}
	if _, err = Run(ctx, "systemctl", "--user", "is-active", "--quiet", Unit); err != nil {
		return res, err
	}
	if err = ready(ctx, p.Data, oldEpoch); err != nil {
		return res, err
	}
	user := strconv.Itoa(os.Getuid())
	linger, _ := Run(ctx, "loginctl", "show-user", user, "--property=Linger", "--value")
	if strings.TrimSpace(string(linger)) != "yes" {
		_, _ = Run(ctx, "loginctl", "--no-ask-password", "enable-linger", user)
		linger, _ = Run(ctx, "loginctl", "show-user", user, "--property=Linger", "--value")
	}
	res.Linger = strings.TrimSpace(string(linger)) == "yes"
	if !res.Linger {
		res.Notes = append(res.Notes, "Linger is off; run: loginctl enable-linger "+user)
	}
	res.Installed = true
	res.Notes = append(res.Notes, "Restart Claude sessions to load the receiver; run herd-msg doctor. Linger is account-wide and is retained on uninstall.")
	return res, nil
}

// Remove keeps mail and link credentials unless purge is explicit. Errors keep the
// executable available so removal can be retried after repairing the failed step.
func Remove(ctx context.Context, purge bool) (Result, error) {
	res := Result{Notes: []string{}}
	if os.Getenv("HERD_MSG_HOME") != "" {
		return res, errors.New("uninstall uses the account mailbox; unset HERD_MSG_HOME first")
	}
	p, err := Locations()
	if err != nil {
		return res, err
	}
	var errs []error
	run := func(name string, args ...string) {
		if _, e := Run(ctx, name, args...); e != nil {
			errs = append(errs, e)
		}
	}
	claude, e := Find(ctx, "claude")
	if e != nil {
		return res, e
	}
	plugins, e := Run(ctx, claude, "plugin", "list", "--json")
	if e != nil {
		return res, e
	}
	if !validRegistry(plugins) {
		return res, errors.New("invalid Claude plugin registry response; repair claude plugin list --json before uninstall")
	}
	markets, e := Run(ctx, claude, "plugin", "marketplace", "list", "--json")
	if e != nil {
		return res, e
	}
	if !validRegistry(markets) {
		return res, errors.New("invalid Claude marketplace registry response; repair claude plugin marketplace list --json before uninstall")
	}
	registry, e := Run(ctx, "herdr", "plugin", "list", "--json")
	if e != nil {
		return res, e
	}
	_, exists, e := herdrLink(registry)
	if e != nil {
		return res, e
	}
	if _, e = os.Stat(p.Unit); e == nil {
		run("systemctl", "--user", "stop", Unit)
		run("systemctl", "--user", "disable", Unit)
	}
	if len(errs) > 0 {
		return res, errors.Join(errs...)
	}
	if listed(plugins, Plugin) {
		run(claude, "plugin", "uninstall", Plugin, "--scope", "user", "--json")
	}
	if listed(markets, "herd-msg-local") {
		run(claude, "plugin", "marketplace", "remove", "herd-msg-local", "--scope", "user", "--json")
	}
	if exists {
		run("herdr", "plugin", "unlink", "herd-msg")
	}
	if len(errs) > 0 {
		return res, errors.Join(errs...)
	}
	for _, path := range []string{p.Unit, p.Skill} {
		if e = os.Remove(path); e != nil && !os.IsNotExist(e) {
			errs = append(errs, e)
		}
	}
	_ = os.Remove(filepath.Dir(p.Skill))
	run("systemctl", "--user", "daemon-reload")
	// Only these two cache namespaces belong to this installer.
	for _, path := range []string{p.Root, filepath.Join(p.Claude, "plugins/cache/herd-msg-local"), filepath.Join(p.Claude, "plugins/marketplaces/herd-msg-local")} {
		if e = os.RemoveAll(path); e != nil {
			errs = append(errs, e)
		}
	}
	if purge {
		for _, path := range []string{p.Data, filepath.Dir(p.Config)} {
			if e = os.RemoveAll(path); e != nil {
				errs = append(errs, e)
			}
		}
	} else {
		res.Notes = append(res.Notes, "Mail, config and link credentials retained. Use uninstall --purge to remove them.")
	}
	if len(errs) > 0 {
		return res, errors.Join(errs...)
	}
	if e = os.Remove(p.Binary); e != nil && !os.IsNotExist(e) {
		return res, e
	}
	res.Notes = append(res.Notes, "Removed. Linger and shared agent settings/cache backups remain account-owned.")
	return res, nil
}

// pruneEmpty removes only empty owned directories after a failed first install.
func pruneEmpty(root string) {
	var dirs []string
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})
	for i := len(dirs) - 1; i >= 0; i-- {
		_ = os.Remove(dirs[i])
	}
}

func herdrLink(data []byte) (string, bool, error) {
	var envelope struct {
		Result struct {
			Plugins []struct {
				ID   string `json:"plugin_id"`
				Root string `json:"plugin_root"`
			} `json:"plugins"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return "", false, fmt.Errorf("invalid herdr plugin registry: %w", err)
	}
	for _, p := range envelope.Result.Plugins {
		if p.ID == "herd-msg" {
			return p.Root, true, nil
		}
	}
	return "", false, nil
}

func socketEpoch(home string) string {
	var info broker.SocketInfo
	data, _ := os.ReadFile(filepath.Join(home, "socket.json"))
	_ = json.Unmarshal(data, &info)
	return info.Epoch
}
func waitBroker(ctx context.Context, home, previous string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if epoch := socketEpoch(home); epoch != "" && epoch != previous {
			probe, stop := context.WithTimeout(ctx, 500*time.Millisecond)
			response, e := broker.Call(probe, home, broker.Request{Verb: "snapshot", Caller: broker.Caller{ID: "operator"}})
			stop()
			if e == nil && response.Code == 0 {
				var snap broker.Snapshot
				if json.Unmarshal(response.Data, &snap) == nil && snap.Data != nil {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return errors.New("broker did not become ready after restart; inspect journalctl --user -u herd-msg.service")
		case <-ticker.C:
		}
	}
}

func validRegistry(data []byte) bool {
	var rows []json.RawMessage
	return strings.HasPrefix(strings.TrimSpace(string(data)), "[") && json.Unmarshal(data, &rows) == nil
}
