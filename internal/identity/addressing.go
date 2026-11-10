package identity

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

type Ambiguous struct{ Candidates []string }

func (e *Ambiguous) Error() string {
	return "ambiguous address; candidates: " + strings.Join(e.Candidates, ", ")
}

func Address(address, local string) (name, machine string, err error) {
	name, machine, remote := strings.Cut(address, "@")
	if !remote {
		machine = local
	}
	if strings.Contains(machine, "@") {
		return "", "", fmt.Errorf("invalid address")
	}
	if err = policy.Identity(name); err != nil {
		return
	}
	err = policy.Identity(machine)
	return
}

// MinIDPrefix is the shortest session ID prefix accepted as an address.
const MinIDPrefix = 8

func Visible(s store.Session) bool { return !s.Tombstone && !(s.Generation == "" && s.Successor != "") }
func Public(s store.Session) store.Session {
	return store.Session{ID: s.ID, Name: s.Name, Machine: s.Machine, Harness: s.Harness, State: s.State, Pane: s.Pane, LastSeen: s.LastSeen, Blocked: s.Blocked, Reason: s.Reason}
}
func Resolve(d *store.Data, address, local string) (store.Session, string, error) {
	addr, machine, e := Address(address, local)
	if e != nil {
		return store.Session{}, "", e
	}
	var sessions []store.Session
	if machine == local {
		for _, s := range d.Sessions {
			sessions = append(sessions, s)
		}
	} else {
		sessions = d.Links[machine].Sessions
	}
	for _, s := range sessions {
		if s.ID == addr {
			return s, machine, nil
		}
	}
	name, harness := addr, ""
	for _, h := range []string{"claude", "codex", "hermes", "muse"} {
		if strings.HasSuffix(addr, "."+h) {
			name = strings.TrimSuffix(addr, "."+h)
			harness = h
			break
		}
	}
	var found []store.Session
	for _, s := range sessions {
		if Visible(s) && s.Name == name && (harness == "" || harness == s.Harness) {
			found = append(found, s)
		}
	}
	// A name always wins over an ID prefix, so a prefix cannot shadow a named peer.
	if len(found) == 0 && len(name) >= MinIDPrefix {
		for _, s := range sessions {
			if Visible(s) && strings.HasPrefix(s.ID, name) && (harness == "" || harness == s.Harness) {
				found = append(found, s)
			}
		}
	}
	if len(found) == 0 {
		return store.Session{}, "", fmt.Errorf("recipient is not registered; refresh peers and use its full ID or a unique prefix of at least %d characters", MinIDPrefix)
	}
	if len(found) > 1 {
		names := []string{}
		for _, s := range found {
			candidate := s.ID
			if machine != local {
				candidate += "@" + machine
			}
			names = append(names, candidate)
		}
		sort.Strings(names)
		return store.Session{}, "", &Ambiguous{names}
	}
	return found[0], machine, nil
}

// Reachable keeps peers that can receive now or after a reattach, and counts the rest.
func Reachable(peers []Peer) ([]Peer, int) {
	out := []Peer{}
	hidden := 0
	for _, p := range peers {
		if p.State == "offline" || p.State == "dead" {
			hidden++
			continue
		}
		out = append(out, p)
	}
	return out, hidden
}

type Peer struct {
	Blocked   bool      `json:"blocked"`
	Reason    string    `json:"reason"`
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Machine   string    `json:"machine"`
	Harness   string    `json:"harness"`
	State     string    `json:"state"`
	Pane      string    `json:"pane"`
	LastSeen  time.Time `json:"last_seen"`
	CachedAge int64     `json:"cached_age_seconds"`
}

func Peers(d *store.Data, c policy.Config, now time.Time, offline bool) []Peer {
	out := []Peer{}
	add := func(s store.Session, age int64, down bool) {
		if !Visible(s) {
			return
		}
		state := s.State
		if down {
			state = "offline"
		}
		out = append(out, Peer{Blocked: s.Blocked && !down, Reason: s.Reason, ID: s.ID, Name: s.Name, Machine: s.Machine, Harness: s.Harness, State: state, Pane: s.Pane, LastSeen: s.LastSeen, CachedAge: age})
	}
	for _, s := range d.Sessions {
		add(s, 0, offline)
	}
	for machine := range c.Links {
		l := d.Links[machine]
		age := int64(now.Sub(l.LastContact).Seconds())
		if age < 0 {
			age = 0
		}
		for _, s := range l.Sessions {
			add(s, age, offline || !l.Reachable || age > 600)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Machine != out[j].Machine {
			return out[i].Machine < out[j].Machine
		}
		return out[i].ID < out[j].ID
	})
	return out
}
