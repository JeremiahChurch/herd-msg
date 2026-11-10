package identity

import (
	"errors"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func TestAddressGrammarAndRemoteAmbiguity(t *testing.T) {
	d := store.Empty()
	d.Sessions["local-id"] = store.Session{ID: "local-id", Name: "builder", Machine: "node-a", Harness: "codex"}
	d.Links["node-b"] = store.Link{Sessions: []store.Session{{ID: "first-id", Name: "review", Machine: "node-b", Harness: "claude"}, {ID: "second-id", Name: "review", Machine: "node-b", Harness: "codex"}}}
	for _, tc := range []struct{ address, id string }{{"builder", "local-id"}, {"builder.codex", "local-id"}, {"local-id", "local-id"}, {"review.claude@node-b", "first-id"}, {"review.codex@node-b", "second-id"}, {"first-id@node-b", "first-id"}} {
		s, _, e := Resolve(d, tc.address, "node-a")
		if e != nil || s.ID != tc.id {
			t.Fatal(tc, s, e)
		}
	}
	_, _, e := Resolve(d, "review@node-b", "node-a")
	var ambiguous *Ambiguous
	if !errors.As(e, &ambiguous) || len(ambiguous.Candidates) != 2 {
		t.Fatal(e)
	}
	if _, _, e = Resolve(d, "unknown@node-b", "node-a"); e == nil {
		t.Fatal("deferred name resolution accepted")
	}
	if _, _, e = Resolve(d, "review@node-b@node-c", "node-a"); e == nil {
		t.Fatal("invalid machine grammar accepted")
	}
	// Equal IDs on different machines remain distinct.
	d.Links["node-b"] = store.Link{Sessions: []store.Session{{ID: "local-id", Name: "remote", Machine: "node-b", Harness: "claude"}}}
	s, m, e := Resolve(d, "local-id@node-b", "node-a")
	if e != nil || s.Name != "remote" || m != "node-b" {
		t.Fatal(s, m, e)
	}
}

func TestMuseHarnessSuffix(t *testing.T) {
	d := store.Empty()
	d.Sessions["muse-session"] = store.Session{ID: "muse-session", Name: "builder", Harness: "muse"}
	d.Sessions["codex-session"] = store.Session{ID: "codex-session", Name: "builder", Harness: "codex"}
	s, _, e := Resolve(d, "builder.muse", "local")
	if e != nil || s.ID != "muse-session" {
		t.Fatal(s, e)
	}
}
func TestPassivePeerCacheExpiresOnRead(t *testing.T) {
	now := time.Now()
	d := store.Empty()
	c := policy.Defaults()
	c.Links["node-b"] = policy.Grant{}
	d.Links["node-b"] = store.Link{Machine: "node-b", Reachable: true, LastContact: now.Add(-11 * time.Minute), Sessions: []store.Session{{ID: "peer-id", Name: "peer", Machine: "node-b", Harness: "claude", State: "live"}}}
	peers := Peers(d, c, now, false)
	if len(peers) != 1 || peers[0].State != "offline" || peers[0].CachedAge != 660 {
		t.Fatal(peers)
	}
	l := d.Links["node-b"]
	l.LastContact = now
	d.Links["node-b"] = l
	peers = Peers(d, c, now, false)
	if peers[0].State != "live" {
		t.Fatal(peers)
	}
}

func TestPeersPreserveBlockedAndRepairReason(t *testing.T) {
	d := store.Empty()
	c := policy.Defaults()
	d.Sessions["blocked"] = store.Session{ID: "blocked", Name: "blocked", Machine: c.Machine, State: "live", Blocked: true, Reason: "relaunch with codex --remote unix://"}
	peers := Peers(d, c, time.Now(), false)
	if len(peers) != 1 || !peers[0].Blocked || peers[0].Reason != "relaunch with codex --remote unix://" {
		t.Fatal(peers)
	}
}

func TestUniqueIDPrefixResolves(t *testing.T) {
	d := store.Empty()
	d.Sessions["01a1a812-aaaa"] = store.Session{ID: "01a1a812-aaaa", Name: "builder", Machine: "node-a", Harness: "codex"}
	d.Sessions["01a1a812-bbbb"] = store.Session{ID: "01a1a812-bbbb", Name: "checker", Machine: "node-a", Harness: "claude"}
	d.Sessions["7f3c9e21-cccc"] = store.Session{ID: "7f3c9e21-cccc", Name: "01a1a812", Machine: "node-a", Harness: "codex"}
	d.Sessions["5e5e5e5e-dddd"] = store.Session{ID: "5e5e5e5e-dddd", Name: "gone", Machine: "node-a", Tombstone: true}
	d.Links["node-b"] = store.Link{Sessions: []store.Session{{ID: "aaaabbbb-eeee", Name: "remote", Machine: "node-b", Harness: "claude"}}}

	if s, _, e := Resolve(d, "7f3c9e21", "node-a"); e != nil || s.ID != "7f3c9e21-cccc" {
		t.Fatal("unique prefix", s, e)
	}
	if s, m, e := Resolve(d, "aaaabbbb@node-b", "node-a"); e != nil || s.ID != "aaaabbbb-eeee" || m != "node-b" {
		t.Fatal("remote prefix", s, m, e)
	}
	// A name beats a prefix, so a peer named like an ID stays reachable by name.
	if s, _, e := Resolve(d, "01a1a812", "node-a"); e != nil || s.ID != "7f3c9e21-cccc" {
		t.Fatal("name should win over prefix", s, e)
	}
	var ambiguous *Ambiguous
	if _, _, e := Resolve(d, "01a1a812-", "node-a"); !errors.As(e, &ambiguous) || len(ambiguous.Candidates) != 2 {
		t.Fatal("shared prefix must be ambiguous", e)
	}
	if s, _, e := Resolve(d, "01a1a812-.claude", "node-a"); e != nil || s.ID != "01a1a812-bbbb" {
		t.Fatal("harness narrows a shared prefix", s, e)
	}
	if _, _, e := Resolve(d, "7f3c9e2", "node-a"); e == nil {
		t.Fatal("prefix under the minimum accepted")
	}
	if _, _, e := Resolve(d, "5e5e5e5e", "node-a"); e == nil {
		t.Fatal("pruned session matched by prefix")
	}
	if _, _, e := Resolve(d, "aaaabbbb", "node-a"); e == nil {
		t.Fatal("prefix crossed machines")
	}
}

func TestReachableHidesOfflineAndEndedPeers(t *testing.T) {
	kept, hidden := Reachable([]Peer{{ID: "a", State: "live"}, {ID: "b", State: "detached"}, {ID: "c", State: "offline"}, {ID: "d", State: "dead"}})
	if hidden != 2 || len(kept) != 2 || kept[0].ID != "a" || kept[1].ID != "b" {
		t.Fatal(kept, hidden)
	}
	if kept, hidden = Reachable(nil); kept == nil || hidden != 0 {
		t.Fatal("empty result must stay a JSON array", kept, hidden)
	}
}
