package broker

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

const Protocol = 1
const MaxFrame = 1 << 20

type Request struct {
	Version            int                        `json:"version"`
	Verb               string                     `json:"verb"`
	Caller             Caller                     `json:"caller"`
	Send               SendRequest                `json:"send"`
	ID                 string                     `json:"id"`
	Action             string                     `json:"action"`
	Confirm            bool                       `json:"confirm"`
	IfUpdated          string                     `json:"if_updated"`
	ExpectUnsent       *int                       `json:"expect_unsent,omitempty"`
	On                 bool                       `json:"on"`
	Patch              map[string]json.RawMessage `json:"patch,omitempty"`
	Session            *store.Session             `json:"session,omitempty"`
	ExpectedGeneration *string                    `json:"expected_generation,omitempty"`
	AttemptID          string                     `json:"attempt_id,omitempty"`
	Generation         string                     `json:"generation,omitempty"`
	Epoch              string                     `json:"epoch,omitempty"`
	Name               string                     `json:"name,omitempty"`
	Peer               *PeerRequest               `json:"peer,omitempty"`
	Wait               bool                       `json:"wait,omitempty"`
	OldID              string                     `json:"old_id"`
}
type Response struct {
	Code      int             `json:"code"`
	Error     string          `json:"error,omitempty"`
	Retryable bool            `json:"retryable"`
	Data      json.RawMessage `json:"data,omitempty"`
}
type Snapshot struct {
	Data   *store.Data   `json:"data"`
	Config policy.Config `json:"config"`
}
type SocketInfo struct {
	Path  string `json:"path"`
	Epoch string `json:"epoch"`
}

func response(v any, e error) Response {
	if e != nil {
		var be *Error
		if errors.As(e, &be) {
			return Response{Code: be.Code, Error: be.Message, Retryable: be.Retryable}
		}
		var ambiguous *store.AmbiguousError
		if errors.As(e, &ambiguous) {
			return Response{Code: 3, Error: e.Error()}
		}
		return Response{Code: 1, Error: e.Error()}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return Response{Code: 1, Error: err.Error()}
	}
	return Response{Data: b}
}
func (b *Broker) Handle(ctx context.Context, r Request) Response {
	if r.Version != Protocol {
		return response(nil, errors.New("unsupported RPC version"))
	}
	if e := r.Caller.Validate(); e != nil {
		return response(nil, e)
	}
	b.hook("before_rpc_" + r.Verb)
	switch r.Verb {
	case "peer-info":
		v, e := b.PeerInfo(ctx)
		return response(v, e)
	case "peer-route-proof":
		if r.Peer == nil {
			return response(nil, errors.New("peer request required"))
		}
		v, e := b.ProveRoute(ctx, *r.Peer)
		return response(v, e)
	case "peer-accept", "peer-drain", "peer-ack", "peer-probe":
		if r.Peer == nil {
			return response(nil, errors.New("peer request required"))
		}
		v, e := b.PeerCall(ctx, r.Verb[5:], *r.Peer)
		return response(v, e)
	case "peer-revoke":
		if r.Peer == nil {
			return response(nil, errors.New("peer request required"))
		}
		e := b.ReceiveRevocation(ctx, *r.Peer)
		return response(PeerReply{Revoked: e == nil}, e)
	case "network-link", "network-unlink", "network-reverse", "peer-install":
		if e := operator(r.Caller); e != nil {
			return response(nil, e)
		}
		if r.Peer == nil {
			return response(nil, errors.New("peer request required"))
		}
		if r.Verb == "peer-install" {
			if r.Peer.Peer == nil {
				return response(nil, errors.New("peer required"))
			}
			return response(map[string]bool{"ok": true}, b.InstallPeer(ctx, *r.Peer.Peer, r.Peer.AutoRun, r.Peer.OldKey))
		}
		if b.Network == nil {
			return response(nil, errors.New("transport unavailable"))
		}
		v, e := b.Network(ctx, r.Verb, *r.Peer)
		return response(v, e)

	case "send":
		v, e := b.Send(ctx, r.Send, r.Caller)
		return response(v, e)
	case "snapshot":
		d, c, e := b.Snapshot(ctx)
		return response(Snapshot{d, c}, e)
	case "release":
		return response(map[string]bool{"ok": true}, b.Release(ctx, r.ID, r.Caller))
	case "resolve":
		e := b.Resolve(ctx, r.ID, r.Action, r.Confirm, r.IfUpdated, r.Caller)
		if e != nil {
			return response(nil, e)
		}
		d, e := b.Store.Read(ctx)
		if e != nil {
			return response(nil, e)
		}
		m, e := d.Find(r.ID)
		if e != nil {
			return response(nil, e)
		}
		return response(map[string]any{"ok": true, "state": m.State, "already_injected": m.State == "injected"}, nil)
	case "prune":
		v, e := b.Prune(ctx, r.ID, r.Confirm, r.ExpectUnsent, r.Caller)
		return response(map[string]int{"dropped": v}, e)
	case "allow":
		return response(map[string]bool{"ok": true}, b.Allow(ctx, r.ID, r.On, r.Caller))
	case "kill":
		v, e := b.Kill(ctx, r.On, r.Caller)
		return response(map[string]int{"in_flight": v}, e)
	case "config":
		v, e := b.Config(ctx, r.Patch, r.Caller)
		return response(v, e)
	case "receiver-state":
		return b.receiverState(ctx, r)
	case "validate-attempt":
		if r.Caller.Harness != "claude" || r.Epoch != b.Epoch {
			return response(nil, fail(2, false, "receiver epoch or harness changed"))
		}
		d, e := b.Store.Read(ctx)
		if e != nil {
			return response(nil, e)
		}
		s := d.Sessions[r.Caller.ID]
		if s.Generation != r.Generation || s.Generation == "" {
			return response(nil, fail(2, false, "receiver generation changed"))
		}
		for _, m := range d.Messages {
			if m.State == "injecting" && m.ActiveAttempt == r.AttemptID && m.Recipient == s.ID {
				for _, a := range m.Attempts {
					if a.ID == r.AttemptID && a.Session.Generation == s.Generation {
						return response(map[string]any{"attempt": a}, nil)
					}
				}
			}
		}
		return response(nil, fail(2, false, "attempt is no longer injecting"))
	case "name":
		if r.Caller.Harness != "codex" {
			return response(nil, fail(2, false, "name requires a Codex caller"))
		}
		driver, ok := b.Drivers["codex"].(interface {
			Name(context.Context, string, string) error
		})
		if !ok {
			return response(nil, errors.New("Codex driver unavailable"))
		}
		if e := driver.Name(ctx, r.Caller.ID, r.Name); e != nil {
			return response(nil, e)
		}
		e := b.Store.Update(ctx, func(d *store.Data) error {
			s, ok := d.Sessions[r.Caller.ID]
			if !ok {
				return errors.New("caller is not registered")
			}
			s.Name = r.Name
			d.Sessions[s.ID] = s
			return nil
		})
		return response(map[string]bool{"ok": true}, e)
	case "register":
		if r.Session == nil {
			return response(nil, errors.New("session required"))
		}
		if !r.Caller.Operator() && (r.Caller.ID != r.Session.ID || r.Caller.Harness != r.Session.Harness) {
			return response(nil, fail(2, false, "registration identity mismatch"))
		}
		bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		e := b.register(bounded, *r.Session, r.ExpectedGeneration)
		if e != nil {
			return response(nil, e)
		}
		d, e := b.Store.Read(ctx)
		if e != nil {
			return response(nil, e)
		}
		return response(map[string]any{"session": d.Sessions[r.Session.ID], "epoch": b.Epoch}, nil)
	case "rebind":
		if r.Session == nil {
			return response(nil, errors.New("session required"))
		}
		if !r.Caller.Operator() && r.Caller.ID != r.OldID {
			return response(nil, fail(2, false, "rebind identity mismatch"))
		}
		bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		e := b.rebind(bounded, r.OldID, *r.Session, r.ExpectedGeneration)
		if e != nil {
			return response(nil, e)
		}
		d, e := b.Store.Read(ctx)
		if e != nil {
			return response(nil, e)
		}
		return response(map[string]any{"session": d.Sessions[r.Session.ID], "epoch": b.Epoch}, nil)
	default:
		return response(nil, errors.New("unknown RPC verb"))
	}
}
func SocketPath(home string) (string, error) {
	runtimeDir := os.Getenv("XDG_RUNTIME_DIR")
	if runtimeDir == "" {
		runtimeDir = filepath.Join(home, "runtime")
	}
	if e := store.PrivateDir(runtimeDir); e != nil {
		return "", e
	}
	digest := sha256.Sum256([]byte(home))
	path := filepath.Join(runtimeDir, "herd-msg-"+hex.EncodeToString(digest[:10])+".sock")
	if len(path) >= 108 {
		return "", errors.New("Unix socket path exceeds 107 bytes; set a shorter XDG_RUNTIME_DIR")
	}
	return path, nil
}
func saveSocket(home string, info SocketInfo) error {
	b, e := json.Marshal(info)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(home, ".socket-*")
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
	if e = os.Rename(f.Name(), filepath.Join(home, "socket.json")); e != nil {
		return e
	}
	d, e := os.Open(home)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}

type Server struct {
	listener net.Listener
	lock     *os.File
	path     string
	b        *Broker
	mu       sync.Mutex
	conns    map[net.Conn]bool
	wg       sync.WaitGroup
}

func (b *Broker) Listen() (*Server, error) {
	path, e := SocketPath(b.Store.Home)
	if e != nil {
		return nil, e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	lock, e := policy.Lock(ctx, path+".lock")
	if e != nil {
		return nil, e
	}
	if e = os.Remove(path); e != nil && !errors.Is(e, os.ErrNotExist) {
		policy.Unlock(lock)
		return nil, e
	}
	// The parent directory is private while bind creates the socket; chmod before serving.
	listener, e := net.Listen("unix", path)
	if e != nil {
		policy.Unlock(lock)
		return nil, e
	}
	if e = os.Chmod(path, 0600); e != nil {
		listener.Close()
		policy.Unlock(lock)
		return nil, e
	}
	if e = saveSocket(b.Store.Home, SocketInfo{path, b.Epoch}); e != nil {
		listener.Close()
		policy.Unlock(lock)
		return nil, e
	}
	return &Server{listener: listener, lock: lock, path: path, b: b, conns: map[net.Conn]bool{}}, nil
}
func readFrame(r io.Reader) ([]byte, error) {
	br := bufio.NewReader(io.LimitReader(r, MaxFrame+1))
	line, e := br.ReadBytes('\n')
	if len(line) > MaxFrame {
		return nil, errors.New("RPC frame exceeds 1 MiB")
	}
	if e != nil {
		return nil, e
	}
	return line, nil
}
func (s *Server) Serve(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		s.listener.Close()
		s.mu.Lock()
		for c := range s.conns {
			c.Close()
		}
		s.mu.Unlock()
	}()
	for {
		conn, e := s.listener.Accept()
		if e != nil {
			if ctx.Err() != nil {
				return nil
			}
			return e
		}
		s.mu.Lock()
		s.conns[conn] = true
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer conn.Close()
			defer func() { s.mu.Lock(); delete(s.conns, conn); s.mu.Unlock() }()
			_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
			frame, e := readFrame(conn)
			if e != nil {
				_ = json.NewEncoder(conn).Encode(response(nil, e))
				return
			}
			var r Request
			if e = json.Unmarshal(frame, &r); e != nil {
				_ = json.NewEncoder(conn).Encode(response(nil, e))
				return
			}
			call, cancel := context.WithCancel(ctx)
			defer cancel()
			go func() { var extra [1]byte; _, _ = conn.Read(extra[:]); cancel() }()
			_ = json.NewEncoder(conn).Encode(s.b.Handle(call, r))
		}()
	}
}
func (s *Server) Close() error {
	e := s.listener.Close()
	s.mu.Lock()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	policy.Unlock(s.lock)
	_ = os.Remove(s.path)
	return e
}
func Call(ctx context.Context, home string, r Request) (Response, error) {
	var out Response
	h, e := store.Home(home)
	if e != nil {
		return out, e
	}
	if canonical, e := filepath.EvalSymlinks(h); e == nil {
		h = canonical
	}
	infoBytes, e := os.ReadFile(filepath.Join(h, "socket.json"))
	if e != nil {
		return out, fail(4, true, "broker not running")
	}
	var info SocketInfo
	if e = json.Unmarshal(infoBytes, &info); e != nil {
		return out, e
	}
	conn, e := (&net.Dialer{}).DialContext(ctx, "unix", info.Path)
	if e != nil {
		return out, fail(4, true, "broker not running")
	}
	defer conn.Close()
	deadline := time.Now().Add(30 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()
	r.Version = Protocol
	b, e := json.Marshal(r)
	if e != nil {
		return out, e
	}
	if len(b)+1 > MaxFrame {
		return out, errors.New("RPC frame exceeds 1 MiB")
	}
	if _, e = conn.Write(append(b, '\n')); e != nil {
		return out, fmt.Errorf("broker reply lost: %w", e)
	}
	// Status snapshots are responses and may contain every pending body.
	dec := json.NewDecoder(conn)
	if e = dec.Decode(&out); e != nil {
		return out, fmt.Errorf("broker reply lost: %w", e)
	}
	return out, nil
}
