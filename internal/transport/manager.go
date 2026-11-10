package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

type Manager struct {
	Broker *broker.Broker
	SSH    SSH
	Fault  func(string)
}
type LinkResult struct {
	Machine      string `json:"machine"`
	Mode         string `json:"mode"`
	AutoRun      bool   `json:"auto_run"`
	ReverseError string `json:"reverse_error,omitempty"`
	Notice       string `json:"notice"`
}

func find(peers map[string]broker.PrivatePeer, name string) (broker.PrivatePeer, bool, error) {
	var found []broker.PrivatePeer
	for _, p := range peers {
		if p.Machine == name || p.Label == name || p.Target == name {
			found = append(found, p)
		}
	}
	if len(found) > 1 {
		return broker.PrivatePeer{}, false, errors.New("ambiguous linked machine; use its declared name")
	}
	if len(found) == 1 {
		return found[0], true, nil
	}
	return broker.PrivatePeer{}, false, nil
}
func (m *Manager) request(ctx context.Context, p broker.PrivatePeer) (broker.PeerRequest, error) {
	info, e := m.Broker.PeerInfo(ctx)
	if e != nil {
		return broker.PeerRequest{}, e
	}
	sessions, e := m.Broker.PublicSessions(ctx)
	return broker.PeerRequest{Machine: info.Machine, Key: p.Key, Sessions: sessions, Protocol: broker.Protocol}, e
}
func (m *Manager) Command(ctx context.Context, verb string, r broker.PeerRequest) (any, error) {
	ctx, cancel := context.WithTimeout(ctx, 24*time.Second)
	defer cancel()
	if verb == "network-reverse" {
		reverseCtx, stop := context.WithTimeout(ctx, 10*time.Second)
		defer stop()
		return m.reverse(reverseCtx, r)
	}
	if len(r.Args) == 0 {
		return nil, errors.New("machine name required")
	}
	name := r.Args[0]
	peers, e := m.Broker.PrivatePeers(ctx)
	if e != nil {
		return nil, e
	}
	existing, found, e := find(peers, name)
	if e != nil {
		return nil, e
	}
	if verb == "network-unlink" {
		if !found {
			return nil, errors.New("machine is not linked")
		}
		choice := ""
		for _, a := range r.Args[1:] {
			if a != "--keep" && a != "--drop" {
				return nil, errors.New("unlink accepts --keep or --drop")
			}
			if choice != "" {
				return nil, errors.New("choose only one unlink disposition")
			}
			choice = a
		}
		expected := r.Expected
		if choice == "" {
			expected = nil
		} else if expected == nil {
			return nil, errors.New("preview unlink, then provide --expect-unsent with its total")
		}
		return m.Broker.UnlinkPeer(ctx, existing.Machine, choice == "--drop", expected, false)
	}
	var auto *bool
	path, home := "", ""
	for i := 1; i < len(r.Args); i++ {
		a := r.Args[i]
		switch a {
		case "--auto-run", "--no-auto-run":
			if auto != nil {
				return nil, errors.New("choose one auto-run value")
			}
			v := a == "--auto-run"
			auto = &v
		case "--remote-path", "--remote-home":
			if i+1 >= len(r.Args) {
				return nil, errors.New("bootstrap option needs an absolute path")
			}
			i++
			if a == "--remote-path" {
				path = r.Args[i]
			} else {
				home = r.Args[i]
			}
		default:
			return nil, fmt.Errorf("unknown link option %s", a)
		}
	}
	if found && !existing.Revoked && auto != nil {
		if e = m.Broker.AutoRun(ctx, existing.Machine, *auto); e != nil {
			return nil, e
		}
		return LinkResult{Machine: existing.Machine, Mode: existing.Mode, AutoRun: *auto, Notice: "receiving permission updated locally; held mail needs explicit release"}, nil
	}
	if auto != nil && !*auto {
		return nil, errors.New("--no-auto-run requires an existing link; use a declared linked name or saved label")
	}
	target, label := "", name
	if found {
		target, label = existing.Target, existing.Label
		if path == "" {
			path = existing.Path
		}
		if home == "" {
			home = existing.Home
		}
	}
	if target == "" {
		machines, e := identity.Machines(ctx)
		if e != nil {
			return nil, e
		}
		saved, e := identity.FindMachine(machines, name)
		if e != nil {
			return nil, e
		}
		target, label = saved.Target, saved.Label
	}
	if path == "" {
		path, e = m.SSH.Discover(ctx, target)
		if e != nil {
			return nil, errors.New(Repair(target, e))
		}
	}
	remote := broker.PrivatePeer{Target: target, Path: path, Home: home, Label: label, Mode: "push-poll"}
	resp, e := m.SSH.Call(ctx, remote, "peer-info", broker.PeerRequest{})
	if e != nil {
		return nil, errors.New(Repair(target, e))
	}
	info, e := decode[broker.PeerInfo](resp)
	if e != nil || info.Protocol != broker.Protocol {
		return nil, errors.New("peer protocol mismatch; install matching herd-msg binaries")
	}
	local, e := m.Broker.PeerInfo(ctx)
	if e != nil {
		return nil, e
	}
	if info.Machine == local.Machine {
		return nil, errors.New("peer declares the local machine name; rename it with herd-msg config")
	}
	remote.Machine = info.Machine
	old, known := peers[info.Machine]
	oldKey := ""
	key := store.Random() + store.Random()
	if known && !old.Revoked {
		oldKey = old.Key
		key = old.Key
	}
	remote.Key = key
	offered := broker.PrivatePeer{Machine: local.Machine, Path: local.Path, Home: local.Home, Key: key, Mode: "poll"}
	resp, e = m.SSH.Call(ctx, remote, "peer-install", broker.PeerRequest{Peer: &offered, OldKey: oldKey, Protocol: broker.Protocol})
	if e != nil {
		return nil, e
	}
	if e = m.Broker.InstallPeer(ctx, remote, auto, oldKey); e != nil {
		return nil, e
	}
	req, e := m.request(ctx, remote)
	if e != nil {
		return nil, e
	}
	resp, e = m.SSH.Call(ctx, remote, "peer-reverse", req)
	reverseError := ""
	if e != nil {
		reverseError = "reverse route unavailable; initiator will poll"
	} else {
		result, e := decode[LinkResult](resp)
		if e != nil {
			return nil, e
		}
		reverseError = result.ReverseError
	}
	req, e = m.request(ctx, remote)
	if e != nil {
		return nil, e
	}
	resp, e = m.SSH.Call(ctx, remote, "peer-probe", req)
	if e != nil {
		return nil, e
	}
	reply, e := decode[broker.PeerReply](resp)
	if e != nil {
		return nil, e
	}
	if reply.Revoked {
		return nil, errors.New("peer revoked link during setup")
	}
	if e = m.Broker.ObservePeer(ctx, remote, reply.Sessions, ""); e != nil {
		return nil, e
	}
	_, config, e := m.Broker.Data(ctx)
	if e != nil {
		return nil, e
	}
	return LinkResult{remote.Machine, remote.Mode, config.Links[remote.Machine].AutoRun, reverseError, "addresses use the peer's declared machine name; auto-run applies only to this receiving machine"}, nil
}
func (m *Manager) reverse(ctx context.Context, r broker.PeerRequest) (any, error) {
	if _, e := m.Broker.PeerCall(ctx, "probe", r); e != nil {
		return nil, e
	}
	peers, e := m.Broker.PrivatePeers(ctx)
	if e != nil {
		return nil, e
	}
	p, ok := peers[r.Machine]
	if !ok || p.Revoked {
		return nil, errors.New("active link required")
	}
	detail := "no enabled saved reverse route; receiving side is passive"
	if p.Target != "" {
		req, e := m.request(ctx, p)
		if e == nil {
			reply, e := m.SSH.Call(ctx, p, "peer-probe", req)
			if e != nil {
				detail = "reverse route failed; receiving side is passive: " + e.Error()
			} else {
				v, e := decode[broker.PeerReply](reply)
				if e == nil && !v.Revoked {
					return LinkResult{Machine: p.Machine, Mode: p.Mode}, nil
				}
			}
		}
	}
	machines, e := identity.Machines(ctx)
	if e != nil {
		return LinkResult{Machine: p.Machine, Mode: "poll", ReverseError: "reverse saved-machine discovery failed; receiving side is passive"}, nil
	}
	for _, machine := range machines {
		if ctx.Err() != nil {
			return LinkResult{Machine: p.Machine, Mode: "poll", ReverseError: detail}, nil
		}
		if !machine.Enabled {
			continue
		}
		candidate := p
		candidate.Target = machine.Target
		candidate.Label = machine.Label
		req, e := m.request(ctx, candidate)
		if e != nil {
			if ctx.Err() != nil {
				return LinkResult{Machine: p.Machine, Mode: "poll", ReverseError: detail}, nil
			}
			return nil, e
		}
		resp, e := m.SSH.Call(ctx, candidate, "peer-probe", req)
		if e != nil {
			if ctx.Err() != nil {
				return LinkResult{Machine: p.Machine, Mode: "poll", ReverseError: detail}, nil
			}
			detail = "reverse route failed; receiving side is passive: " + e.Error()
			continue
		}
		reply, e := decode[broker.PeerReply](resp)
		if e != nil || reply.Revoked {
			continue
		}
		candidate.Mode = "push-poll"
		// SSH verifies a fresh route proof before transmitting the keyed probe.
		if e = m.Broker.InstallPeer(ctx, candidate, nil, p.Key); e != nil {
			return nil, e
		}
		if e = m.Broker.ObservePeer(ctx, candidate, reply.Sessions, ""); e != nil {
			return nil, e
		}
		return LinkResult{Machine: p.Machine, Mode: candidate.Mode}, nil
	}
	return LinkResult{Machine: p.Machine, Mode: "poll", ReverseError: detail}, nil
}
func (m *Manager) applyReply(ctx context.Context, p broker.PrivatePeer, r broker.PeerReply) error {
	if r.Revoked {
		if e := m.Broker.ReceiveRevocation(ctx, broker.PeerRequest{Machine: p.Machine, Key: p.Key}); e != nil {
			return e
		}
		return errors.New("peer revoked the link; relink before sending")
	}
	return m.Broker.ObservePeer(ctx, p, r.Sessions, "")
}
func proofFor(message store.Message, r broker.PeerReply) (broker.TransferProof, error) {
	if r.Proof == nil || r.Proof.ID != message.ID || r.Proof.Hash != broker.WireHash(message) || (!r.Proof.Committed && !r.Proof.Refused) {
		return broker.TransferProof{}, errors.New("peer did not return a matching durable receipt")
	}
	return *r.Proof, nil
}
func (m *Manager) Exchange(ctx context.Context, p broker.PrivatePeer) error {
	transfer, done, e := m.Broker.BeginTransfer(ctx)
	if e != nil {
		return e
	}
	defer done()
	ctx = transfer
	if e := m.Broker.TransferReady(ctx, p); e != nil {
		return e
	}
	messages, e := m.Broker.Outbound(ctx, p.Machine)
	if e != nil {
		return e
	}
	blocked := map[string]bool{}
	notReady := errors.New("outbound message is no longer ready")
	transferReady := func() error { return m.Broker.TransferReady(ctx, p) }
	for _, message := range messages {
		if blocked[message.Recipient] {
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		req, e := m.request(ctx, p)
		if e != nil {
			return e
		}
		req.Message = &message
		resp, e := m.SSH.callChecked(ctx, p, "accept", req, func() error {
			current, ready, e := m.Broker.OutboundMessage(ctx, p, message.ID)
			if e != nil {
				return e
			}
			if !ready {
				return notReady
			}
			message = current
			return nil
		})
		if errors.Is(e, notReady) {
			continue
		}
		if e != nil {
			return e
		}
		reply, e := decode[broker.PeerReply](resp)
		if e != nil {
			return e
		}
		if e = m.applyReply(ctx, p, reply); e != nil {
			return e
		}
		proof, e := proofFor(message, reply)
		if e != nil {
			return e
		}
		if e = m.Broker.Settle(ctx, p.Machine, proof); e != nil {
			return e
		}
		if proof.Refused && proof.Retryable {
			blocked[message.Recipient] = true
		}
	}
	bytesRead := 0
	for count := 0; count < 8; count++ {
		req, e := m.request(ctx, p)
		if e != nil {
			return e
		}
		resp, e := m.SSH.callChecked(ctx, p, "drain", req, transferReady)
		if e != nil {
			return e
		}
		reply, e := decode[broker.PeerReply](resp)
		if e != nil {
			return e
		}
		if e = m.applyReply(ctx, p, reply); e != nil {
			return e
		}
		if len(reply.Messages) == 0 {
			return nil
		}
		message := reply.Messages[0]
		wire, e := json.Marshal(message)
		if e != nil {
			return e
		}
		if count > 0 && bytesRead+len(wire) > 512<<10 {
			return nil
		}
		bytesRead += len(wire)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Reuse the authenticated import path, including dedupe and receiving policy.
		incoming, e := m.Broker.PeerCall(ctx, "accept", broker.PeerRequest{Machine: p.Machine, Key: p.Key, Sessions: reply.Sessions, Message: &message, Protocol: broker.Protocol})
		if e != nil {
			return e
		}
		if incoming.Revoked {
			return errors.New("link revoked during polling")
		}
		if m.Fault != nil {
			m.Fault("after_poll_commit")
		}
		proof, e := proofFor(message, incoming)
		if e != nil {
			return e
		}
		req, e = m.request(ctx, p)
		if e != nil {
			return e
		}
		req.Proof = &proof
		ack, e := m.SSH.callChecked(ctx, p, "ack", req, transferReady)
		if e != nil {
			return e
		}
		ackReply, e := decode[broker.PeerReply](ack)
		if e != nil {
			return e
		}
		if e = m.applyReply(ctx, p, ackReply); e != nil {
			return e
		}
		if proof.Refused && proof.Retryable {
			blocked[message.Recipient] = true
		}
	}
	return nil
}
func Backoff(previous time.Duration, e error) time.Duration {
	var se *SSHError
	if errors.As(e, &se) && (se.Kind == "auth" || se.Kind == "host-key") {
		return 5 * time.Minute
	}
	if previous < 30*time.Second {
		return 30 * time.Second
	}
	return min(previous*2, 5*time.Minute)
}
func publicError(e error) string {
	var se *SSHError
	if errors.As(e, &se) {
		return se.Error()
	}
	var be *broker.Error
	if errors.As(e, &be) {
		return be.Message
	}
	if errors.Is(e, context.Canceled) {
		return "traffic stopped or broker shutting down"
	}
	if errors.Is(e, context.DeadlineExceeded) {
		return (&SSHError{Kind: "timeout"}).Error()
	}
	return "peer operation failed; verify link and matching herd-msg versions"
}
func (m *Manager) revoke(ctx context.Context, p broker.PrivatePeer) error {
	req, e := m.request(ctx, p)
	if e != nil {
		return e
	}
	resp, e := m.SSH.Call(ctx, p, "peer-revoke", req)
	if e != nil {
		return e
	}
	reply, e := decode[broker.PeerReply](resp)
	if e != nil || !reply.Revoked {
		return errors.New("revocation was not acknowledged")
	}
	return m.Broker.FinishRevocation(ctx, p.Machine, p.Key)
}
func (m *Manager) Run(ctx context.Context) {
	type progress struct {
		next       time.Time
		delay      time.Duration
		running    bool
		generation uint64
		wake       bool
	}
	work := map[string]*progress{}
	var peers map[string]broker.PrivatePeer
	refresh := true
	var mu sync.Mutex
	var wg sync.WaitGroup
	defer wg.Wait()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		var e error
		if refresh {
			peers, e = m.Broker.PrivatePeers(ctx)
			if e == nil {
				refresh = false
			}
		}
		if e == nil {
			names := []string{}
			for name := range peers {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				p := peers[name]
				if p.Target == "" {
					continue
				}
				if p.Revoked && (p.Received || !time.Now().Before(p.RevokeUntil)) {
					continue
				}
				mu.Lock()
				state := work[name]
				if state == nil {
					state = &progress{}
					work[name] = state
				}
				if state.running || time.Now().Before(state.next) {
					mu.Unlock()
					continue
				}
				state.running = true
				generation := state.generation
				mu.Unlock()
				wg.Add(1)
				go func(p broker.PrivatePeer, state *progress) {
					defer wg.Done()
					operation, cancel := context.WithTimeout(ctx, 24*time.Second)
					defer cancel()
					var e error
					if p.Revoked {
						e = m.revoke(operation, p)
					} else {
						e = m.Exchange(operation, p)
					}
					mu.Lock()
					defer mu.Unlock()
					state.running = false
					woke := state.wake
					state.wake = false
					if state.generation != generation {
						state.delay = 0
						state.next = time.Time{}
						return
					}
					var stopped *broker.Error
					if errors.Is(e, context.Canceled) || (errors.As(e, &stopped) && stopped.Message == "traffic stopped") {
						state.delay = 0
						state.next = time.Now().Add(30 * time.Second)
						return
					}
					if e != nil {
						state.delay = Backoff(state.delay, e)
						state.next = time.Now().Add(state.delay)
						_ = m.Broker.ObservePeer(ctx, p, nil, publicError(e))
					} else {
						state.delay = 0
						if woke {
							state.next = time.Time{}
						} else {
							state.next = time.Now().Add(30 * time.Second)
						}
					}
				}(p, state)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-m.Broker.TransportReset:
			refresh = true
			mu.Lock()
			for _, state := range work {
				state.generation++
				state.delay = 0
				state.next = time.Time{}
			}
			mu.Unlock()
		case <-m.Broker.TransportWake:
			refresh = true
			// Eager sends may advance ordinary idle polling, but never failure backoff.
			mu.Lock()
			for _, state := range work {
				state.wake = state.running
				if state.delay == 0 {
					state.next = time.Time{}
				}
			}
			mu.Unlock()
		}
	}
}
