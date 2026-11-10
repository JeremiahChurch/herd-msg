// Package claude hands committed attempts to the recipient's trusted MCP child.
package claude

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"syscall"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/receiver"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

type Driver struct{}

func (d *Driver) Probe(context.Context) error { return nil }
func (d *Driver) SessionState(ctx context.Context, s store.Session) (broker.Availability, error) {
	if s.Generation == "" {
		return broker.Availability{State: "offline", Reason: "conversation is fenced; resume it or prune its mailbox"}, nil
	}
	path := s.Hints["endpoint"]
	if path == "" {
		return broker.Availability{State: "offline", Reason: "Claude receiver or SessionStart hook is missing"}, nil
	}
	r, e := receiver.Call(ctx, path, receiver.Control{Verb: "status"})
	if e != nil {
		var op *net.OpError
		if errors.As(e, &op) && op.Op == "dial" && (errors.Is(e, syscall.ENOENT) || errors.Is(e, syscall.ECONNREFUSED)) {
			return broker.Availability{State: "offline", Reason: "Claude receiver is offline"}, nil
		}
		return broker.Availability{}, e
	}
	if r.Status.Instance != s.Hints["instance"] || r.Status.Session != s.ID || r.Status.Generation != s.Generation {
		return broker.Availability{State: "unavailable", Reason: "receiver is registering or rebinding"}, nil
	}
	a, e := identity.Pane(ctx, s)
	if e != nil {
		return broker.Availability{}, e
	}
	return broker.Availability{State: "live", Blocked: a.State == "working" || a.State == "blocked" || a.State == "unknown", Reason: a.State}, nil
}
func (d *Driver) Inject(ctx context.Context, s store.Session, body, aid string) (broker.Result, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	deadline = deadline.Add(-50 * time.Millisecond)
	dial, cancel := context.WithTimeout(ctx, time.Until(deadline)/2)
	defer cancel()
	conn, e := (&net.Dialer{}).DialContext(dial, "unix", s.Hints["endpoint"])
	if e != nil {
		return broker.Result{Outcome: broker.NotSent, Class: "transport", Proof: "receiver connection failed before request"}, nil
	}
	defer conn.Close()
	conn.SetDeadline(deadline)
	if ctx.Err() != nil || !time.Now().Before(deadline) {
		return broker.Result{Outcome: broker.NotSent, Class: "transport", Proof: "deadline expired before request"}, nil
	}
	q := receiver.Control{Verb: "inject", Work: receiver.Work{Session: s.ID, Generation: s.Generation, Instance: s.Hints["instance"], Epoch: s.Hints["broker_epoch"], Attempt: aid, Deadline: deadline}}
	if e = json.NewEncoder(conn).Encode(q); e != nil {
		return broker.Result{Outcome: broker.Unconfirmed}, nil
	}
	var reply receiver.Reply
	if e = json.NewDecoder(conn).Decode(&reply); e != nil {
		return broker.Result{Outcome: broker.Unconfirmed}, nil
	}
	return reply.Result, nil
}
func (d *Driver) Lookup(ctx context.Context, r broker.LookupRequest) (bool, error) {
	return receiver.Lookup(ctx, r)
}
