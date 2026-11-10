// Package store persists mailbox snapshots in SQLite transactions.
package store

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/states"
)

type Session struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Machine     string            `json:"machine"`
	Harness     string            `json:"harness"`
	Generation  string            `json:"generation"`
	State       string            `json:"state"`
	Pane        string            `json:"pane"`
	LastSeen    time.Time         `json:"last_seen"`
	Blocked     bool              `json:"blocked"`
	Reason      string            `json:"reason"`
	Hints       map[string]string `json:"hints"`
	Tombstone   bool              `json:"tombstone"`
	PrunedAt    time.Time         `json:"pruned_at"`
	Successor   string            `json:"successor"`
	ReboundAt   time.Time         `json:"rebound_at"`
	Outstanding []string          `json:"outstanding"`
}
type Message struct {
	ID              string       `json:"id"`
	Origin          string       `json:"origin"`
	Sender          string       `json:"sender"`
	SenderID        string       `json:"sender_id"`
	Address         string       `json:"address"`
	Recipient       string       `json:"recipient"`
	Destination     string       `json:"destination"`
	Body            string       `json:"body"`
	ParentID        string       `json:"parent_id"`
	Hop             int          `json:"hop"`
	Created         time.Time    `json:"created"`
	Accepted        time.Time    `json:"accepted"`
	Updated         time.Time    `json:"updated"`
	Expires         time.Time    `json:"expires"`
	NextAttempt     time.Time    `json:"next_attempt_at"`
	LookupChecks    int          `json:"lookup_checks"`
	NextLookup      time.Time    `json:"next_lookup_at"`
	State           states.State `json:"state"`
	Sequence        int64        `json:"sequence"`
	Released        bool         `json:"operator_released"`
	BudgetEpoch     int          `json:"budget_epoch"`
	Failures        int          `json:"failures"`
	ClaimToken      string       `json:"claim_token"`
	BrokerEpoch     string       `json:"broker_epoch"`
	Lease           time.Time    `json:"lease"`
	ClaimGeneration string       `json:"claim_generation"`
	ActiveAttempt   string       `json:"active_attempt"`
	Error           string       `json:"error"`
	TerminalAt      time.Time    `json:"terminal_at"`
	Envelope        string       `json:"envelope"`
	Attempts        []Attempt    `json:"attempts"`
	History         []History    `json:"history"`
}
type Attempt struct {
	ID          string    `json:"id"`
	MessageID   string    `json:"message_id"`
	Origin      string    `json:"origin"`
	Session     Session   `json:"session"`
	BudgetEpoch int       `json:"budget_epoch"`
	Envelope    string    `json:"envelope"`
	Started     time.Time `json:"started"`
	Finished    time.Time `json:"finished"`
	Outcome     string    `json:"outcome"`
	Proof       string    `json:"proof"`
	Class       string    `json:"class"`
	Positive    bool      `json:"positive"`
}
type History struct {
	From   states.State `json:"from"`
	To     states.State `json:"to"`
	Actor  string       `json:"actor"`
	Reason string       `json:"reason"`
	At     time.Time    `json:"at"`
}
type Dedupe struct {
	Origin      string       `json:"origin"`
	ID          string       `json:"id"`
	RequestHash string       `json:"request_hash"`
	WireHash    string       `json:"wire_hash"`
	Created     time.Time    `json:"created"`
	TerminalAt  time.Time    `json:"terminal_at"`
	State       states.State `json:"state"`
}
type Link struct {
	Sessions    []Session `json:"sessions"`
	Machine     string    `json:"machine"`
	Mode        string    `json:"mode"`
	Reachable   bool      `json:"reachable"`
	LastContact time.Time `json:"last_contact"`
	Error       string    `json:"error"`
}
type Health struct {
	Check  string    `json:"check"`
	Detail string    `json:"detail"`
	Since  time.Time `json:"since"`
	Fix    string    `json:"fix"`
	Count  int64     `json:"count"`
}
type Data struct {
	Messages    map[string]*Message
	Sessions    map[string]Session
	Dedupe      map[string]Dedupe
	Links       map[string]Link
	Health      map[string]Health
	Sequence    int64
	ReplayFloor time.Time
}

func Empty() *Data {
	return &Data{Messages: map[string]*Message{}, Sessions: map[string]Session{}, Dedupe: map[string]Dedupe{}, Links: map[string]Link{}, Health: map[string]Health{}}
}
func Key(origin, id string) string { return url.PathEscape(origin) + "/" + url.PathEscape(id) }
func Random() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func (d *Data) NextSequence() int64 { d.Sequence++; return d.Sequence }

type AmbiguousError struct{ ID string }

func (e *AmbiguousError) Error() string { return fmt.Sprintf("ambiguous message ID %s", e.ID) }
func (d *Data) Find(id string) (*Message, error) {
	var found *Message
	for _, m := range d.Messages {
		if m.ID == id {
			if found != nil {
				return nil, &AmbiguousError{ID: id}
			}
			found = m
		}
	}
	if found == nil {
		return nil, fmt.Errorf("message %s is missing or expired", id)
	}
	return found, nil
}
func (d *Data) Move(m *Message, to states.State, actor, reason string, now time.Time) error {
	if e := states.Transition(m.State, to); e != nil {
		return e
	}
	if m.State == states.Dead || m.State == states.Indeterminate {
		delete(d.Health, "message/"+Key(m.Origin, m.ID))
	}
	m.History = append(m.History, History{m.State, to, actor, reason, now.UTC()})
	m.State = to
	m.Updated = now.UTC()
	m.Error = reason
	if to != states.Claimed {
		m.ClaimToken = ""
		m.BrokerEpoch = ""
		m.Lease = time.Time{}
		m.ClaimGeneration = ""
	}
	if to != states.Injecting {
		m.ActiveAttempt = ""
	}
	if states.Terminal(to) {
		m.TerminalAt = now.UTC()
		if to != states.Expired {
			m.Error = ""
		}
		delete(d.Health, "message/"+Key(m.Origin, m.ID))
	}
	k := Key(m.Origin, m.ID)
	dd := d.Dedupe[k]
	dd.State = to
	dd.TerminalAt = m.TerminalAt
	d.Dedupe[k] = dd
	// Keep operator audit forever within the retained payload, plus 200 automatic entries.
	n := 0
	for i := len(m.History) - 1; i >= 0; i-- {
		if m.History[i].Actor != "operator" {
			n++
			if n > 200 {
				m.History = append(m.History[:i], m.History[i+1:]...)
			}
		}
	}
	return nil
}
func (d *Data) Notice(check, detail, fix string, now time.Time) {
	h := d.Health[check]
	if h.Detail != detail {
		h.Since = now.UTC()
	}
	h.Check = check
	h.Detail = detail
	h.Fix = fix
	h.Count++
	d.Health[check] = h
}

// clone returns an independent snapshot, including all nested mutable values.
func (d *Data) clone() *Data {
	out := *d
	out.Messages = make(map[string]*Message, len(d.Messages))
	for k, m := range d.Messages {
		if m == nil {
			out.Messages[k] = nil
			continue
		}
		n := *m
		n.History = slices.Clone(m.History)
		n.Attempts = slices.Clone(m.Attempts)
		for i := range n.Attempts {
			n.Attempts[i].Session = cloneSession(n.Attempts[i].Session)
		}
		out.Messages[k] = &n
	}
	out.Sessions = maps.Clone(d.Sessions)
	for k, s := range out.Sessions {
		out.Sessions[k] = cloneSession(s)
	}
	out.Dedupe = maps.Clone(d.Dedupe)
	out.Health = maps.Clone(d.Health)
	out.Links = maps.Clone(d.Links)
	for k, l := range out.Links {
		l.Sessions = slices.Clone(l.Sessions)
		for i := range l.Sessions {
			l.Sessions[i] = cloneSession(l.Sessions[i])
		}
		out.Links[k] = l
	}
	return &out
}
func cloneSession(s Session) Session {
	s.Hints = maps.Clone(s.Hints)
	s.Outstanding = slices.Clone(s.Outstanding)
	return s
}
