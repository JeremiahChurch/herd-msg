// Package notify sends durable, deduplicated operator alerts through herdr.
package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/setup"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

type State struct {
	Sent map[string]string    `json:"sent"`
	Down map[string]time.Time `json:"down"`
}

const ConfirmationNoticeDelay = 15 * time.Minute

type Notifier struct {
	ActiveLinks map[string]bool
	Path        string
	Send        func(context.Context, string, string) error
}

func (n Notifier) Sweep(ctx context.Context, d *store.Data, now time.Time) error {
	unlock, e := n.lock(ctx)
	if e != nil {
		return e
	}
	defer unlock()
	return n.sweep(ctx, d, now)
}
func (n Notifier) sweep(ctx context.Context, d *store.Data, now time.Time) error {
	state := State{Sent: map[string]string{}, Down: map[string]time.Time{}}
	data, e := os.ReadFile(n.Path)
	if e == nil {
		if e = json.Unmarshal(data, &state); e != nil {
			return fmt.Errorf("notification state: %w", e)
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	if state.Sent == nil {
		state.Sent = map[string]string{}
	}
	if state.Down == nil {
		state.Down = map[string]time.Time{}
	}
	failures := map[string]string{}
	for key, h := range d.Health {
		if (strings.HasPrefix(key, "receiver/") || strings.HasPrefix(key, "driver/")) && now.Sub(h.Since) < ConfirmationNoticeDelay {
			continue
		}
		if strings.HasPrefix(key, "driver/") {
			if _, exists := d.Health["probe/"+strings.TrimPrefix(key, "driver/")+"-socket"]; exists {
				continue
			}
		}
		if strings.HasPrefix(key, "probe/session/") {
			continue
		}
		if strings.HasPrefix(key, "driver/") || strings.HasPrefix(key, "probe/") && !strings.HasPrefix(key, "probe/link/") || strings.HasPrefix(key, "receiver/") || strings.HasPrefix(key, "outstanding/") || key == "config" {
			failures[key] = h.Detail + "; fix: " + h.Fix
		}
	}
	for _, m := range d.Messages {
		if m.State == states.Dead || m.State == states.Indeterminate && now.Sub(m.Updated) >= ConfirmationNoticeDelay {
			detail := fmt.Sprintf("mail %s is %s: %s; inspect: herd-msg show %s", m.ID, m.State, m.Error, m.ID)
			if m.State == states.Indeterminate {
				detail += "; input may still be pending; wait for the recipient turn boundary before any resend"
			}
			failures["message/"+store.Key(m.Origin, m.ID)] = detail
		}
	}
	for id, session := range d.Sessions {
		if session.Harness != "codex" || session.State != "detached" {
			continue
		}
		waiting := false
		for _, message := range d.Messages {
			if message.Recipient == id && message.State == states.Queued {
				waiting = true
				break
			}
		}
		if !waiting {
			continue
		}
		fix := d.Health["probe/session/"+id].Fix
		if fix == "" {
			fix = "bash -ic " + shellQuote("codex --remote unix:// resume "+shellQuote(id))
		}
		failures["detached/"+id] = "mail queued for detached Codex session " + id + "; fix: " + fix
	}
	for name, l := range d.Links {
		key := "link/" + name
		if n.ActiveLinks != nil && !n.ActiveLinks[name] {
			delete(state.Down, key)
			continue
		}
		if l.Reachable && !l.LastContact.IsZero() && now.Sub(l.LastContact) <= 10*time.Minute {
			delete(state.Down, key)
			continue
		}
		since, exists := state.Down[key]
		if !exists {
			since = now
			state.Down[key] = since
		}
		if now.Sub(since) >= time.Hour {
			reason := l.Error
			if reason == "" {
				reason = "no recent contact"
			}
			failures[key] = "link " + name + " down over one hour: " + reason + "; fix: herd-msg doctor"
		}
	}
	for key := range state.Down {
		if _, ok := d.Links[strings.TrimPrefix(key, "link/")]; !ok {
			delete(state.Down, key)
		}
	}
	for key := range state.Sent {
		if _, ok := failures[key]; !ok {
			delete(state.Sent, key)
		}
	}
	keys := []string{}
	for key := range failures {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	send := n.Send
	if send == nil {
		send = func(ctx context.Context, title, body string) error {
			_, e := setup.Run(ctx, "herdr", "notification", "show", title, "--body", body, "--sound", "none")
			return e
		}
	}
	var sendErr error
	for _, key := range keys {
		detail := failures[key]
		if state.Sent[key] == detail {
			continue
		}
		if e = send(ctx, "herd-msg needs attention", detail); e != nil {
			sendErr = e
			continue
		}
		state.Sent[key] = detail
	}
	if e = os.MkdirAll(filepath.Dir(n.Path), 0700); e != nil {
		return e
	}
	data, e = json.Marshal(state)
	if e != nil {
		return e
	}
	if e = setup.Atomic(n.Path, data, 0600); e != nil {
		return e
	}
	return sendErr
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }

// Failure changes only one alert record. Refused startup must preserve the running
// broker's unrelated link timers and dedupe records.
func (n Notifier) Failure(ctx context.Context, key, detail, fix string) error {
	unlock, e := n.lock(ctx)
	if e != nil {
		return e
	}
	defer unlock()
	state := State{Sent: map[string]string{}, Down: map[string]time.Time{}}
	data, e := os.ReadFile(n.Path)
	if e == nil {
		if e = json.Unmarshal(data, &state); e != nil {
			return e
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	if state.Sent == nil {
		state.Sent = map[string]string{}
	}
	body := detail + "; fix: " + fix
	if state.Sent[key] == body {
		return nil
	}
	send := n.Send
	if send == nil {
		send = func(ctx context.Context, title, body string) error {
			_, e := setup.Run(ctx, "herdr", "notification", "show", title, "--body", body, "--sound", "none")
			return e
		}
	}
	if e = send(ctx, "herd-msg needs attention", body); e != nil {
		return e
	}
	state.Sent[key] = body
	data, e = json.Marshal(state)
	if e != nil {
		return e
	}
	return setup.Atomic(n.Path, data, 0600)
}
func (n Notifier) lock(ctx context.Context) (func(), error) {
	if e := os.MkdirAll(filepath.Dir(n.Path), 0700); e != nil {
		return nil, e
	}
	wait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	lock, e := policy.Lock(wait, n.Path+".lock")
	if e != nil {
		return nil, e
	}
	return func() { policy.Unlock(lock) }, nil
}
