package broker

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

// PrivatePeer is credential material. It must never enter a public snapshot.
type PrivatePeer struct {
	Machine     string    `json:"machine"`
	Label       string    `json:"label"`
	Target      string    `json:"target"`
	Path        string    `json:"path"`
	Home        string    `json:"home"`
	Key         string    `json:"key"`
	Mode        string    `json:"mode"`
	Revoked     bool      `json:"revoked"`
	Received    bool      `json:"received"`
	RevokeUntil time.Time `json:"revoke_until"`
}
type PeerRequest struct {
	Protocol int             `json:"protocol"`
	Machine  string          `json:"machine"`
	Key      string          `json:"key"`
	Nonce    string          `json:"nonce,omitempty"`
	Sessions []store.Session `json:"sessions,omitempty"`
	Message  *store.Message  `json:"message,omitempty"`
	Proof    *TransferProof  `json:"proof,omitempty"`
	Peer     *PrivatePeer    `json:"peer,omitempty"`
	OldKey   string          `json:"old_key,omitempty"`
	AutoRun  *bool           `json:"auto_run,omitempty"`
	Drop     bool            `json:"drop"`
	Expected *int            `json:"expected,omitempty"`
	Received bool            `json:"received"`
	Args     []string        `json:"args,omitempty"`
}
type TransferProof struct {
	ID        string `json:"id"`
	Hash      string `json:"hash"`
	Committed bool   `json:"committed"`
	Refused   bool   `json:"refused"`
	Retryable bool   `json:"retryable"`
	Reason    string `json:"reason,omitempty"`
}
type PeerInfo struct {
	Protocol int    `json:"protocol"`
	Machine  string `json:"machine"`
	Path     string `json:"path"`
	Home     string `json:"home"`
}
type PeerReply struct {
	Revoked  bool            `json:"revoked"`
	Sessions []store.Session `json:"sessions"`
	Messages []store.Message `json:"messages"`
	Proof    *TransferProof  `json:"proof,omitempty"`
}
type UnlinkCount struct {
	Outbound int    `json:"outbound"`
	Inbound  int    `json:"inbound"`
	Total    int    `json:"total"`
	Notice   string `json:"notice"`
}

type AuthenticationError struct{ Cause error }

func (e *AuthenticationError) Error() string { return e.Cause.Error() }
func (e *AuthenticationError) Unwrap() error { return e.Cause }

var errRevoked = errors.New("link revoked")

func validKey(key string) bool { b, e := hex.DecodeString(key); return e == nil && len(b) == 32 }
func sameKey(a, b string) bool {
	return validKey(a) && validKey(b) && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
func (b *Broker) peerPath() string { return filepath.Join(b.Store.Home, "peers.json") }
func (b *Broker) readPeers() (map[string]PrivatePeer, error) {
	out := map[string]PrivatePeer{}
	info, e := os.Lstat(b.peerPath())
	if errors.Is(e, os.ErrNotExist) {
		return out, nil
	}
	if e != nil {
		return nil, e
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return nil, errors.New("peers.json must be a regular file with permissions 0600")
	}
	if owner, ok := info.Sys().(*syscall.Stat_t); !ok || owner.Uid != uint32(os.Getuid()) {
		return nil, errors.New("peers.json must be owned by this account")
	}
	data, e := os.ReadFile(b.peerPath())
	if e != nil {
		return nil, e
	}
	if e = json.Unmarshal(data, &out); e != nil {
		return nil, errors.New("invalid peers.json; restore link credentials")
	}
	if out == nil {
		return nil, errors.New("invalid peers.json")
	}
	return out, nil
}
func (b *Broker) writePeers(peers map[string]PrivatePeer) error {
	data, e := json.Marshal(peers)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(b.Store.Home, ".peers-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(append(data, '\n'))
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
	if e = os.Rename(f.Name(), b.peerPath()); e != nil {
		return e
	}
	dir, e := os.Open(b.Store.Home)
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}
func (b *Broker) PrivatePeers(ctx context.Context) (map[string]PrivatePeer, error) {
	var peers map[string]PrivatePeer
	e := b.withConfig(ctx, func(c policy.Config) error { var e error; peers, e = b.readPeers(); return e })
	return peers, e
}
func (b *Broker) authenticate(c policy.Config, r PeerRequest) (PrivatePeer, error) {
	peers, e := b.readPeers()
	if e != nil {
		return PrivatePeer{}, e
	}
	p, ok := peers[r.Machine]
	if !ok || !sameKey(p.Key, r.Key) {
		return p, fail(2, false, "link authentication failed; run herd-msg link to repair")
	}
	if p.Revoked {
		return p, errRevoked
	}
	if _, ok = c.Links[r.Machine]; !ok {
		return p, fail(2, false, "sender machine is not linked; run herd-msg link")
	}
	return p, nil
}
func (b *Broker) PeerInfo(ctx context.Context) (PeerInfo, error) {
	var info PeerInfo
	e := b.withConfig(ctx, func(c policy.Config) error {
		path, e := os.Executable()
		if e != nil {
			return e
		}
		path, e = filepath.Abs(path)
		if e != nil {
			return e
		}
		home := ""
		if os.Getenv("HERD_MSG_HOME") != "" {
			home = b.Store.Home
		}
		info = PeerInfo{Protocol, c.Machine, path, home}
		return nil
	})
	return info, e
}
func publicSessions(d *store.Data) []store.Session {
	out := []store.Session{}
	for _, s := range d.Sessions {
		if identity.Visible(s) {
			out = append(out, identity.Public(s))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (b *Broker) PublicSessions(ctx context.Context) ([]store.Session, error) {
	d, _, e := b.Data(ctx)
	if e != nil {
		return nil, e
	}
	return publicSessions(d), nil
}
func (b *Broker) cachePeer(d *store.Data, machine string, sessions []store.Session, now time.Time) error {
	if len(sessions) > 10000 {
		return fail(2, false, "too many peer sessions")
	}
	clean := make([]store.Session, 0, len(sessions))
	seen := map[string]bool{}
	for _, s := range sessions {
		if s.Machine != machine || policy.Identity(s.ID) != nil || policy.Identity(s.Name) != nil || seen[s.ID] {
			return fail(2, false, "invalid peer session identity")
		}
		seen[s.ID] = true
		if identity.Visible(s) {
			clean = append(clean, identity.Public(s))
		}
	}
	l := d.Links[machine]
	l.Machine = machine
	l.Sessions = clean
	l.Reachable = true
	l.Error = ""
	if l.LastContact.IsZero() || now.Sub(l.LastContact) >= time.Minute {
		l.LastContact = now
	}
	d.Links[machine] = l
	return nil
}
func canonical(m store.Message) store.Message {
	return store.Message{ID: m.ID, Origin: m.Origin, Sender: m.Sender, SenderID: m.SenderID, Address: m.Address, Recipient: m.Recipient, Destination: m.Destination, Body: m.Body, ParentID: m.ParentID, Hop: m.Hop, Created: m.Created}
}
func WireHash(m store.Message) string { return wireHash(m) }
func outbound(d *store.Data, machine, local string, now time.Time) []store.Message {
	rows := []*store.Message{}
	for _, m := range d.Messages {
		if m.Origin == local && m.Destination == machine && m.State == states.Outbox && (m.Expires.IsZero() || now.Before(m.Expires)) {
			rows = append(rows, m)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Sequence < rows[j].Sequence })
	out := []store.Message{}
	blocked := map[string]bool{}
	size := 0
	for _, m := range rows {
		if blocked[m.Recipient] {
			continue
		}
		if now.Before(m.NextAttempt) {
			blocked[m.Recipient] = true
			continue
		}
		wire := canonical(*m)
		data, _ := json.Marshal(wire)
		if len(out) > 0 && (size+len(data) > 512<<10 || len(out) >= 8) {
			break
		}
		out = append(out, wire)
		size += len(data)
	}
	return out
}
func (b *Broker) Outbound(ctx context.Context, machine string) ([]store.Message, error) {
	var out []store.Message
	e := b.withConfig(ctx, func(c policy.Config) error {
		if c.Killed {
			return fail(2, true, "traffic stopped")
		}
		peers, e := b.readPeers()
		if e != nil {
			return e
		}
		p, ok := peers[machine]
		if !ok || p.Revoked || !validKey(p.Key) {
			return fail(2, false, "link is incomplete; run herd-msg link")
		}
		if _, ok = c.Links[machine]; !ok {
			return fail(2, false, "destination machine is not linked")
		}
		d, e := b.Store.Read(ctx)
		if e != nil {
			return e
		}
		out = outbound(d, machine, c.Machine, b.now())
		return nil
	})
	return out, e
}
func settle(d *store.Data, c policy.Config, machine string, p TransferProof, now time.Time) error {
	m := d.Messages[store.Key(c.Machine, p.ID)]
	if m == nil {
		return errors.New("outbound message is missing or expired")
	}
	if m.Destination != machine || wireHash(*m) != p.Hash {
		return fail(2, false, "acknowledgement does not match outbound payload")
	}
	if p.Committed {
		if m.State == states.Sent {
			return nil
		}
		if states.Terminal(m.State) {
			for _, h := range m.History {
				if h.Reason == "late durable acknowledgement" {
					return nil
				}
			}
			m.History = append(m.History, store.History{From: m.State, To: m.State, Actor: "transport", Reason: "late durable acknowledgement", At: now})
			return nil
		}
		return d.Move(m, states.Sent, "transport", "peer durably committed payload", now)
	}
	if !p.Refused {
		return errors.New("acknowledgement has no outcome")
	}
	if states.Terminal(m.State) || m.State == states.Dead {
		return nil
	}
	if p.Retryable {
		m.Error = p.Reason
		m.Updated = now
		m.NextAttempt = now.Add(30 * time.Second)
		return nil
	}
	return d.Move(m, states.Dead, "transport", p.Reason, now)
}
func (b *Broker) Settle(ctx context.Context, machine string, p TransferProof) error {
	return b.withConfig(ctx, func(c policy.Config) error {
		return b.Store.Update(ctx, func(d *store.Data) error { return settle(d, c, machine, p, b.now()) })
	})
}
func (b *Broker) PeerCall(ctx context.Context, verb string, r PeerRequest) (PeerReply, error) {
	var out PeerReply
	if r.Protocol != Protocol {
		return out, fail(2, false, "peer protocol mismatch; install matching herd-msg binaries")
	}
	// Authenticate inside Accept's transaction gate as well as here.
	e := b.withConfig(ctx, func(c policy.Config) error {
		_, e := b.authenticate(c, r)
		if e != nil {
			return e
		}
		if verb == "drain" && c.Killed {
			return fail(2, true, "traffic stopped")
		}
		return b.Store.Update(ctx, func(d *store.Data) error {
			if e := b.cachePeer(d, r.Machine, r.Sessions, b.now()); e != nil {
				return e
			}
			out.Sessions = publicSessions(d)
			switch verb {
			case "drain":
				out.Messages = outbound(d, r.Machine, c.Machine, b.now())
				if len(out.Messages) > 1 {
					out.Messages = out.Messages[:1]
				}
			case "ack":
				if r.Proof == nil {
					return errors.New("acknowledgement required")
				}
				return settle(d, c, r.Machine, *r.Proof, b.now())
			case "accept", "probe":
			default:
				return errors.New("unsupported peer operation")
			}
			return nil
		})
	})
	if errors.Is(e, errRevoked) {
		out.Revoked = true
		return out, nil
	}
	if e != nil {
		return out, e
	}
	if verb == "accept" {
		if r.Message == nil {
			return out, errors.New("message required")
		}
		b.hook("after_peer_authentication")
		_, e = b.accept(ctx, *r.Message, &r)
		proof := TransferProof{ID: r.Message.ID, Hash: wireHash(*r.Message)}
		if e == nil {
			proof.Committed = true
		} else {
			var authentication *AuthenticationError
			if errors.As(e, &authentication) {
				return out, e
			}
			var be *Error
			if errors.As(e, &be) && be.Code == 2 {
				proof.Refused = true
				proof.Retryable = be.Retryable
				proof.Reason = be.Message
			} else {
				return out, e
			}
		}
		out.Proof = &proof
	}
	return out, nil
}
func (b *Broker) InstallPeer(ctx context.Context, p PrivatePeer, auto *bool, oldKey string) error {
	if policy.Identity(p.Machine) != nil || !validKey(p.Key) || !filepath.IsAbs(p.Path) || (p.Home != "" && !filepath.IsAbs(p.Home)) {
		return fail(2, false, "invalid link identity, credential or absolute path")
	}
	e := b.withConfig(ctx, func(c policy.Config) error {
		if p.Machine == c.Machine {
			return fail(2, false, "peer declares the local machine name; rename it with herd-msg config")
		}
		peers, e := b.readPeers()
		if e != nil {
			return e
		}
		old, exists := peers[p.Machine]
		proven := sameKey(old.Key, oldKey) || sameKey(old.Key, p.Key)
		if p.Label == "" {
			p.Label = old.Label
		}
		if p.Target == "" && proven && !old.Revoked {
			p.Target = old.Target
			p.Mode = old.Mode
		}
		if exists && !old.Revoked && old.Target != "" && p.Target != "" && old.Target != p.Target && !proven {
			return fail(2, false, "machine name already belongs to another saved target; unlink or rename first")
		}
		grant := c.Links[p.Machine]
		if !exists || old.Revoked || !proven {
			grant.AutoRun = false
		}
		if auto != nil {
			grant.AutoRun = *auto
		}
		p.Revoked = false
		p.Received = false
		p.RevokeUntil = time.Time{}
		peers[p.Machine] = p
		if e = b.writePeers(peers); e != nil {
			return e
		}
		c.Links[p.Machine] = grant
		if e = policy.Write(b.ConfigPath, c, b.hook); e != nil {
			return e
		}
		return b.Store.Update(ctx, func(d *store.Data) error {
			l := d.Links[p.Machine]
			l.Machine = p.Machine
			l.Mode = p.Mode
			l.Error = ""
			d.Links[p.Machine] = l
			delete(d.Health, "link/"+p.Machine)
			return b.reconcile(d, c, b.now())
		})
	})
	if e == nil {
		b.ResetTransport()
	}
	return e
}
func (b *Broker) AutoRun(ctx context.Context, machine string, on bool) error {
	_, e := b.changeConfig(ctx, Caller{ID: "operator"}, func(c *policy.Config) error {
		peers, e := b.readPeers()
		if e != nil {
			return e
		}
		p, ok := peers[machine]
		if !ok || p.Revoked || !validKey(p.Key) {
			return fail(2, false, "active link is required")
		}
		if _, ok = c.Links[machine]; !ok {
			return fail(2, false, "active link is required")
		}
		c.Links[machine] = policy.Grant{AutoRun: on}
		return nil
	})
	return e
}
func (b *Broker) CachePeer(ctx context.Context, machine string, sessions []store.Session, detail string) error {
	return b.withConfig(ctx, func(c policy.Config) error {
		peers, e := b.readPeers()
		if e != nil {
			return e
		}
		p, ok := peers[machine]
		if !ok || p.Revoked {
			return nil
		}
		return b.Store.Update(ctx, func(d *store.Data) error {
			l := d.Links[machine]
			l.Machine = machine
			l.Mode = p.Mode
			if detail != "" {
				l.Reachable = false
				l.Error = detail
				d.Links[machine] = l
				return nil
			}
			d.Links[machine] = l
			return b.cachePeer(d, machine, sessions, b.now())
		})
	})
}
func affected(d *store.Data, c policy.Config, machine string) UnlinkCount {
	n := UnlinkCount{Notice: "Keep retains outbound mail blocked. Drop cannot recall copies already committed. Removing trust makes inbound pending mail dead here and at the peer; relink then resolve resend to recover it."}
	for _, m := range d.Messages {
		if m.Origin == c.Machine && m.Destination == machine && (m.State == states.Outbox || m.State == states.Dead) {
			n.Outbound++
		}
		if m.Origin == machine && m.Destination == c.Machine && (m.State == states.Queued || m.State == states.Held || m.State == states.Claimed || m.State == states.Dead) {
			n.Inbound++
		}
	}
	n.Total = n.Inbound + n.Outbound
	return n
}
func (b *Broker) UnlinkPeer(ctx context.Context, machine string, drop bool, expected *int, received bool) (UnlinkCount, error) {
	return b.unlinkPeer(ctx, machine, drop, expected, received, nil)
}
func (b *Broker) unlinkPeer(ctx context.Context, machine string, drop bool, expected *int, received bool, auth *PeerRequest) (UnlinkCount, error) {
	var count UnlinkCount
	e := b.withConfig(ctx, func(c policy.Config) error {
		if auth != nil {
			_, e := b.authenticate(c, *auth)
			if e != nil && !errors.Is(e, errRevoked) {
				return e
			}
		}
		peers, e := b.readPeers()
		if e != nil {
			return e
		}
		p, exists := peers[machine]
		if !exists {
			return fail(2, false, "machine is not linked")
		}
		d, e := b.Store.Read(ctx)
		if e != nil {
			return e
		}
		count = affected(d, c, machine)
		if expected == nil && !received {
			return nil
		}
		if expected != nil && *expected != count.Total {
			return fail(2, true, fmt.Sprintf("mail count changed to %d; preview unlink again", count.Total))
		}
		if !p.Revoked {
			p.RevokeUntil = b.now().Add(policy.Seconds(c.OutboxTTL))
		}
		p.Revoked = true
		p.Received = p.Received || received
		peers[machine] = p
		if e = b.writePeers(peers); e != nil {
			return e
		}
		b.hook("after_peer_revoke")
		delete(c.Links, machine)
		if e = policy.Write(b.ConfigPath, c, b.hook); e != nil {
			return e
		}
		return b.Store.Update(ctx, func(d *store.Data) error {
			// Check again at the disposition commit, sharing the config lock with imports.
			current := affected(d, c, machine)
			if expected != nil && current.Total != *expected {
				return fail(2, true, "mail count changed; preview unlink again")
			}
			for _, m := range d.Messages {
				if m.Origin == c.Machine && m.Destination == machine && (m.State == states.Outbox || m.State == states.Dead) {
					if drop {
						if e := d.Move(m, states.Dropped, "operator", "link removed; outbound mail dropped", b.now()); e != nil {
							return e
						}
					} else {
						m.Error = "link removed; outbound mail retained"
					}
				}
			}
			if e := b.reconcile(d, c, b.now()); e != nil {
				return e
			}
			l := d.Links[machine]
			l.Reachable = false
			l.Error = "link revoked; outbound mail retained unless dropped"
			d.Links[machine] = l
			d.Notice("link/"+machine, fmt.Sprintf("link revoked; %d inbound pending messages need attention", count.Inbound), "relink the machine, then use herd-msg resolve <id> resend", b.now())
			return nil
		})
	})
	if e == nil && (expected != nil || received) {
		b.WakeTransport()
	}
	return count, e
}
func (b *Broker) ReceiveRevocation(ctx context.Context, r PeerRequest) error {
	_, e := b.unlinkPeer(ctx, r.Machine, false, nil, true, &r)
	return e
}
func (b *Broker) FinishRevocation(ctx context.Context, machine, key string) error {
	e := b.withConfig(ctx, func(c policy.Config) error {
		peers, e := b.readPeers()
		if e != nil {
			return e
		}
		p := peers[machine]
		if !p.Revoked || !sameKey(p.Key, key) {
			return nil
		}
		p.Received = true
		peers[machine] = p
		return b.writePeers(peers)
	})
	if e == nil {
		b.WakeTransport()
	}
	return e
}
func (b *Broker) BeginTransfer(ctx context.Context) (context.Context, func(), error) {
	var out context.Context
	var finish func()
	e := b.withConfig(ctx, func(c policy.Config) error {
		if c.Killed {
			return fail(2, true, "traffic stopped")
		}
		var cancel context.CancelFunc
		out, cancel = context.WithCancel(ctx)
		id := store.Random()
		b.mu.Lock()
		b.transfers[id] = cancel
		b.mu.Unlock()
		finish = func() { cancel(); b.mu.Lock(); delete(b.transfers, id); b.mu.Unlock() }
		return nil
	})
	if e != nil {
		return nil, nil, e
	}
	return out, finish, nil
}
func (b *Broker) WakeTransport() {
	if b.TransportWake != nil {
		select {
		case b.TransportWake <- struct{}{}:
		default:
		}
	}
}

// TransferReady fences a worker that captured a route before a relink or unlink.
func (b *Broker) TransferReady(ctx context.Context, p PrivatePeer) error {
	return b.withConfig(ctx, func(c policy.Config) error {
		if c.Killed {
			return fail(2, true, "traffic stopped")
		}
		_, e := b.authenticate(c, PeerRequest{Machine: p.Machine, Key: p.Key})
		return e
	})
}
func (b *Broker) ObservePeer(ctx context.Context, p PrivatePeer, sessions []store.Session, detail string) error {
	return b.withConfig(ctx, func(c policy.Config) error {
		if _, e := b.authenticate(c, PeerRequest{Machine: p.Machine, Key: p.Key}); e != nil {
			return e
		}
		return b.Store.Update(ctx, func(d *store.Data) error {
			l := d.Links[p.Machine]
			l.Machine = p.Machine
			l.Mode = p.Mode
			if detail != "" {
				l.Reachable = false
				l.Error = detail
				d.Links[p.Machine] = l
				return nil
			}
			d.Links[p.Machine] = l
			return b.cachePeer(d, p.Machine, sessions, b.now())
		})
	})
}

// ResetTransport is an operator repair signal; ordinary sends use WakeTransport.
func (b *Broker) ResetTransport() {
	if b.TransportReset != nil {
		select {
		case b.TransportReset <- struct{}{}:
		default:
		}
	}
	b.WakeTransport()
}

// OutboundMessage rechecks an individual captured row just before network I/O.
// An operator drop or expiry during an earlier transfer must prevent this send.
func (b *Broker) OutboundMessage(ctx context.Context, p PrivatePeer, id string) (store.Message, bool, error) {
	var out store.Message
	ready := false
	e := b.withConfig(ctx, func(c policy.Config) error {
		if c.Killed {
			return fail(2, true, "traffic stopped")
		}
		if _, e := b.authenticate(c, PeerRequest{Machine: p.Machine, Key: p.Key}); e != nil {
			return e
		}
		d, e := b.Store.Read(ctx)
		if e != nil {
			return e
		}
		row := d.Messages[store.Key(c.Machine, id)]
		if row == nil || row.State != states.Outbox || row.Destination != p.Machine || b.now().Before(row.NextAttempt) || !row.Expires.IsZero() && !b.now().Before(row.Expires) {
			return nil
		}
		out = canonical(*row)
		ready = true
		return nil
	})
	return out, ready, e
}
