// Package policy owns mailbox settings and ordered delivery gates.
package policy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
	"unicode"
)

type Grant struct {
	AutoRun bool `json:"auto_run"`
}
type Config struct {
	Machine          string           `json:"machine"`
	Killed           bool             `json:"killed"`
	OptOut           map[string]bool  `json:"opt_out"`
	Links            map[string]Grant `json:"links"`
	HopLimit         int              `json:"hop_limit"`
	PairLimit        int              `json:"pair_limit"`
	PairWindow       int              `json:"pair_window_seconds"`
	IncomingWindow   int              `json:"incoming_window_seconds"`
	PendingLimit     int              `json:"pending_limit"`
	ActiveBytes      int64            `json:"active_bytes"`
	InjectionLimit   int              `json:"injection_limit"`
	TTL              int              `json:"ttl_seconds"`
	OutboxTTL        int              `json:"outbox_ttl_seconds"`
	Retention        int              `json:"retention_seconds"`
	Dedupe           int              `json:"dedupe_seconds"`
	Skew             int              `json:"skew_seconds"`
	Claim            int              `json:"claim_seconds"`
	DriverTimeout    int              `json:"driver_timeout_seconds"`
	LookupInterval   int              `json:"lookup_interval_seconds"`
	LookupWindow     int              `json:"lookup_window_seconds"`
	SweepInterval    int              `json:"sweep_interval_seconds"`
	OfflineGrace     int              `json:"offline_grace_seconds"`
	SessionRetention int              `json:"session_retention_seconds"`
	NoticeGrace      int              `json:"notice_grace_seconds"`
}

func Defaults() Config {
	return Config{Machine: "local", OptOut: map[string]bool{}, Links: map[string]Grant{}, HopLimit: 6, PairLimit: 20, PairWindow: 600, IncomingWindow: 600, PendingLimit: 1000, ActiveBytes: 64 << 20, InjectionLimit: 20, TTL: 604800, OutboxTTL: 604800, Retention: 604800, Dedupe: 2592000, Skew: 300, Claim: 60, DriverTimeout: 5, LookupInterval: 10, NoticeGrace: 60, LookupWindow: 3600, SweepInterval: 600, OfflineGrace: 86400, SessionRetention: 604800}
}
func (c Config) Validate() error {
	if err := Identity(c.Machine); err != nil {
		return err
	}
	vals := []int{c.HopLimit, c.PairLimit, c.PairWindow, c.IncomingWindow, c.PendingLimit, c.InjectionLimit, c.TTL, c.OutboxTTL, c.Retention, c.Dedupe, c.Skew, c.Claim, c.DriverTimeout, c.LookupInterval, c.NoticeGrace, c.LookupWindow, c.SweepInterval, c.OfflineGrace, c.SessionRetention}
	for _, v := range vals {
		if v < 1 || v > 315360000 {
			return errors.New("settings must be positive and at most ten years")
		}
	}
	if c.ActiveBytes < 1 || c.ActiveBytes > 1<<40 || c.Retention <= c.PairWindow || c.Dedupe <= max(c.TTL, c.OutboxTTL)+c.Skew || c.DriverTimeout >= c.Claim {
		return errors.New("invalid capacity, retention, dedupe or timeout settings")
	}
	for id := range c.OptOut {
		if err := Identity(id); err != nil {
			return err
		}
	}
	for id := range c.Links {
		if err := Identity(id); err != nil {
			return err
		}
	}
	return nil
}
func Identity(s string) error { return identityLimit(s, 256) }

// SenderLabel contains a name, harness and machine, each validated independently.
func SenderLabel(s string) error { return identityLimit(s, 2*256+8) }
func identityLimit(s string, limit int) error {
	if s == "" || len(s) > limit {
		return fmt.Errorf("identity must contain 1 to %d bytes", limit)
	}
	for _, r := range s {
		if unicode.IsControl(r) || r == '|' {
			return errors.New("identity contains controls or delimiter")
		}
	}
	return nil
}
func Seconds(v int) time.Duration { return time.Duration(v) * time.Second }

// Lock also coordinates offline configuration changes with dispatch commits.
func Lock(ctx context.Context, path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return nil, err
	}
	timer := time.NewTicker(10 * time.Millisecond)
	defer timer.Stop()
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) {
			f.Close()
			return nil, err
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
func Unlock(f *os.File) {
	if f != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}
}
func Read(path string) (Config, error) {
	c := Defaults()
	b, e := os.ReadFile(path)
	if errors.Is(e, os.ErrNotExist) {
		return c, nil
	}
	if e != nil {
		return c, e
	}
	if e = json.Unmarshal(b, &c); e != nil {
		return c, e
	}
	if c.OptOut == nil {
		c.OptOut = map[string]bool{}
	}
	if c.Links == nil {
		c.Links = map[string]Grant{}
	}
	return c, c.Validate()
}

// Write requires the caller to hold the config lock.
func Write(path string, c Config, hook func(string)) error {
	if e := c.Validate(); e != nil {
		return e
	}
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	b, e := json.MarshalIndent(c, "", "  ")
	if e != nil {
		return e
	}
	b = append(b, '\n')
	f, e := os.CreateTemp(filepath.Dir(path), ".config-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if hook != nil {
		hook("before_config_rename")
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	if hook != nil {
		hook("after_config_rename")
	}
	dir, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
func ConfigPath(home string) (string, error) {
	if os.Getenv("HERD_MSG_HOME") != "" {
		return filepath.Join(home, "config.json"), nil
	}
	base, e := os.UserConfigDir()
	if e != nil {
		return "", e
	}
	return filepath.Join(base, "herd-msg", "config.json"), nil
}

type Input struct {
	Killed, OptOut            bool
	Hop, PairCount            int
	Origin, Local             string
	Linked, AutoRun, Released bool
	Availability              string
	Blocked                   bool
}
type Decision struct {
	Refuse, Retryable, Held, Wait bool
	Reason                        string
}

// Refusal reasons other packages match on to add detail.
const (
	HopLimitReason  = "hop limit exceeded"
	PairLimitReason = "pair rate limit exceeded"
)

func Evaluate(c Config, in Input, admission bool) Decision {
	if in.Killed {
		return Decision{Refuse: admission, Retryable: true, Wait: true, Reason: "traffic stopped"}
	}
	if in.OptOut {
		return Decision{Refuse: admission, Retryable: true, Wait: true, Reason: "recipient opted out"}
	}
	if in.Hop > c.HopLimit {
		return Decision{Refuse: true, Reason: HopLimitReason}
	}
	if admission && in.PairCount >= c.PairLimit {
		return Decision{Refuse: true, Retryable: true, Reason: PairLimitReason}
	}
	if in.Origin != in.Local {
		if !in.Linked {
			return Decision{Refuse: true, Reason: "sender machine is not linked"}
		}
		if !in.AutoRun && !in.Released {
			return Decision{Held: true, Reason: "waiting for operator release"}
		}
	}
	if in.Availability == "detached" {
		return Decision{Wait: true, Reason: "recipient detached; run herd-msg doctor for its pinned Codex resume command"}
	}
	if in.Availability != "live" {
		return Decision{Wait: true, Reason: fmt.Sprintf("recipient %s", in.Availability)}
	}
	if in.Blocked {
		return Decision{Wait: true, Reason: "recipient blocked"}
	}
	return Decision{}
}

// Patch replaces only named top-level settings, under the caller's config lock.
// Decode into a fresh value so removing a map entry really removes its grant.
func Patch(current Config, patch map[string]json.RawMessage) (Config, error) {
	b, e := json.Marshal(current)
	if e != nil {
		return Config{}, e
	}
	var object map[string]json.RawMessage
	if e = json.Unmarshal(b, &object); e != nil {
		return Config{}, e
	}
	for key, value := range patch {
		if _, ok := object[key]; !ok {
			return Config{}, fmt.Errorf("unknown config key %s", key)
		}
		if !json.Valid(value) {
			return Config{}, fmt.Errorf("invalid JSON for %s", key)
		}
		object[key] = value
	}
	b, e = json.Marshal(object)
	if e != nil {
		return Config{}, e
	}
	var next Config
	if e = json.Unmarshal(b, &next); e != nil {
		return Config{}, e
	}
	if next.OptOut == nil {
		next.OptOut = map[string]bool{}
	}
	if next.Links == nil {
		next.Links = map[string]Grant{}
	}
	return next, next.Validate()
}
