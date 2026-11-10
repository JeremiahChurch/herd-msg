// Package cli implements the mailbox command interface and its status contract.
package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

type MessageStatus struct {
	ID        string       `json:"id"`
	From      string       `json:"from"`
	To        string       `json:"to"`
	State     states.State `json:"state"`
	Created   string       `json:"created"`
	Updated   string       `json:"updated"`
	FirstLine string       `json:"first_line"`
	Attempts  int          `json:"attempts"`
	Error     string       `json:"error"`
}
type SessionStatus struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Harness  string `json:"harness"`
	State    string `json:"state"`
	Pane     string `json:"pane"`
	LastSeen string `json:"last_seen"`
	OptedOut bool   `json:"opted_out"`
	Unsent   int    `json:"unsent"`
	Blocked  bool   `json:"blocked"`
}
type LinkStatus struct {
	Machine     string `json:"machine"`
	AutoRun     bool   `json:"auto_run"`
	Mode        string `json:"mode"`
	Reachable   bool   `json:"reachable"`
	LastContact string `json:"last_contact"`
	Error       string `json:"error"`
}
type HealthStatus struct {
	Check  string `json:"check"`
	Detail string `json:"detail"`
	Since  string `json:"since"`
	Fix    string `json:"fix"`
}
type Status struct {
	Schema   int                  `json:"schema"`
	Machine  string               `json:"machine"`
	Version  string               `json:"version"`
	Killed   bool                 `json:"killed"`
	Counts   map[states.State]int `json:"counts"`
	Sessions []SessionStatus      `json:"sessions"`
	Links    []LinkStatus         `json:"links"`
	Pending  []MessageStatus      `json:"pending"`
	Recent   []MessageStatus      `json:"recent"`
	Health   []HealthStatus       `json:"health"`
}

func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}
func preview(s string) string {
	s, _, _ = strings.Cut(s, "\n")
	r := []rune(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s))
	if len(r) > 160 {
		r = r[:160]
	}
	return string(r)
}
func summary(m *store.Message) MessageStatus {
	return MessageStatus{m.ID, m.Sender, m.Recipient + "@" + m.Destination, m.State, stamp(m.Created), stamp(m.Updated), preview(m.Body), len(m.Attempts), m.Error}
}
func BuildStatus(d *store.Data, c policy.Config, version string, offline bool, now time.Time) Status {
	out := Status{Schema: 1, Machine: c.Machine, Version: version, Killed: c.Killed, Counts: map[states.State]int{}, Sessions: []SessionStatus{}, Links: []LinkStatus{}, Pending: []MessageStatus{}, Recent: []MessageStatus{}, Health: []HealthStatus{}}
	for _, s := range states.All {
		out.Counts[s] = 0
	}
	var pending, recent []*store.Message
	for _, m := range d.Messages {
		out.Counts[m.State]++
		if states.Terminal(m.State) {
			recent = append(recent, m)
		} else {
			pending = append(pending, m)
		}
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].Sequence == pending[j].Sequence {
			return pending[i].ID < pending[j].ID
		}
		return pending[i].Sequence < pending[j].Sequence
	})
	sort.Slice(recent, func(i, j int) bool {
		if recent[i].TerminalAt.Equal(recent[j].TerminalAt) {
			return recent[i].ID < recent[j].ID
		}
		return recent[i].TerminalAt.After(recent[j].TerminalAt)
	})
	for _, m := range pending {
		out.Pending = append(out.Pending, summary(m))
	}
	for i, m := range recent {
		if i >= 100 {
			break
		}
		out.Recent = append(out.Recent, summary(m))
	}
	for _, s := range d.Sessions {
		if s.Tombstone {
			continue
		}
		state := s.State
		if offline {
			state = "offline"
		} else if state == "offline" && !s.LastSeen.IsZero() && now.Sub(s.LastSeen) >= 24*time.Hour {
			state = "dead"
		}
		if state != "live" && state != "offline" && state != "dead" && state != "detached" {
			state = "offline"
		}
		n := 0
		for _, m := range d.Messages {
			if m.Recipient == s.ID && m.Destination == c.Machine && states.Unsent(m.State) {
				n++
			}
		}
		out.Sessions = append(out.Sessions, SessionStatus{s.ID, s.Name, s.Harness, state, s.Pane, stamp(s.LastSeen), c.OptOut[s.ID], n, s.Blocked && !offline})
	}
	for machine, g := range c.Links {
		l := d.Links[machine]
		out.Links = append(out.Links, LinkStatus{machine, g.AutoRun, l.Mode, l.Reachable && !offline, stamp(l.LastContact), l.Error})
	}
	for _, h := range d.Health {
		out.Health = append(out.Health, HealthStatus{h.Check, h.Detail, stamp(h.Since), h.Fix})
	}
	if offline {
		out.Health = append(out.Health, HealthStatus{"broker", "broker not running", stamp(now), "herd-msg broker"})
	}
	sort.Slice(out.Sessions, func(i, j int) bool { return out.Sessions[i].ID < out.Sessions[j].ID })
	sort.Slice(out.Links, func(i, j int) bool { return out.Links[i].Machine < out.Links[j].Machine })
	sort.Slice(out.Health, func(i, j int) bool { return out.Health[i].Check < out.Health[j].Check })
	return out
}
func DecodeStatus(b []byte) (Status, error) {
	var s Status
	e := json.Unmarshal(b, &s)
	if e == nil && s.Schema != 1 {
		e = fmt.Errorf("unsupported status schema %d; upgrade herd-remote", s.Schema)
	}
	return s, e
}

// RenderWatch keeps the pane bounded while JSON consumers retain the complete stream.
func RenderWatch(out io.Writer, s Status) error {
	var text strings.Builder
	text.WriteString("\x1b[H\x1b[2J")
	fmt.Fprintf(&text, "herd-msg  %s  %s\n", preview(s.Machine), preview(s.Version))
	if s.Killed {
		text.WriteString("Traffic stopped\n")
	}
	fmt.Fprintf(&text, "Queued %d  Held %d  Stuck %d  Outbox %d\n", s.Counts[states.Queued], s.Counts[states.Held], s.Counts[states.Indeterminate]+s.Counts[states.Dead], s.Counts[states.Outbox])
	text.WriteString("\nPending mail\n")
	if len(s.Pending) == 0 {
		text.WriteString("No messages\n")
	}
	for i, m := range s.Pending {
		if i == 8 {
			fmt.Fprintf(&text, "... %d more; herd-msg status --json\n", len(s.Pending)-i)
			break
		}
		fmt.Fprintf(&text, "%s %s %s: %s\n", m.State, m.ID, compact(m.To, 24), compact(m.FirstLine, 55))
	}
	fmt.Fprintf(&text, "\nSessions %d  Links %d\n", len(s.Sessions), len(s.Links))
	healthRows := append([]HealthStatus(nil), s.Health...)
	sort.SliceStable(healthRows, func(i, j int) bool {
		return !strings.HasPrefix(healthRows[i].Check, "probe/session/") && strings.HasPrefix(healthRows[j].Check, "probe/session/")
	})
	for i, h := range healthRows {
		if i == 4 {
			fmt.Fprintf(&text, "... %d more checks; herd-msg status --json\n", len(s.Health)-i)
			break
		}
		fmt.Fprintf(&text, "%s: %s\n", compact(h.Check, 28), compact(h.Detail, 70))
	}
	text.WriteString("\nherd-msg show <id> for details; Ctrl-C to close\n")
	_, e := io.WriteString(out, text.String())
	return e
}
func compact(s string, n int) string {
	r := []rune(preview(s))
	if len(r) > n {
		return string(r[:n]) + "..."
	}
	return string(r)
}
