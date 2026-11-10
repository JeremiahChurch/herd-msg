// Package codex delivers through the attached app-server protocol.
package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/store"
	"github.com/JeremiahChurch/herd-msg/internal/wsunix"
)

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("app-server %d: %s", e.Code, e.Message) }

type Client struct {
	ws      *wsunix.Conn
	id      int
	Version string
}

func Open(ctx context.Context, path string) (*Client, error) {
	w, e := wsunix.Dial(ctx, path)
	if e != nil {
		return nil, e
	}
	c := &Client{ws: w}
	var r struct {
		UserAgent string `json:"userAgent"`
	}
	if e = c.Call("initialize", map[string]any{"clientInfo": map[string]string{"name": "herd-msg", "version": "1"}, "capabilities": map[string]bool{"experimentalApi": false}}, &r); e != nil {
		w.Close()
		return nil, e
	}
	_, v, ok := strings.Cut(r.UserAgent, "/")
	if !ok || len(strings.Fields(v)) == 0 {
		w.Close()
		return nil, errors.New("unsupported/unverified app-server version")
	}
	c.Version = strings.Fields(v)[0]
	if !supported(c.Version) {
		w.Close()
		return nil, fmt.Errorf("unsupported/unverified Codex %s; minimum 0.157.1", c.Version)
	}
	if e = w.WriteMessage([]byte(`{"method":"initialized"}`)); e != nil {
		w.Close()
		return nil, e
	}
	return c, nil
}
func (c *Client) Close() error { return c.ws.Close() }
func supported(s string) bool {
	p := strings.Split(s, ".")
	if len(p) != 3 {
		return false
	}
	n := make([]int, 3)
	for i, v := range p {
		x, e := strconv.Atoi(v)
		if e != nil || x < 0 {
			return false
		}
		n[i] = x
	}
	return n[0] > 0 || n[1] > 157 || (n[1] == 157 && n[2] >= 1)
}
func (c *Client) Call(method string, params, out any) error {
	c.id++
	b, e := json.Marshal(map[string]any{"id": c.id, "method": method, "params": params})
	if e != nil {
		return e
	}
	if e = c.ws.WriteMessage(b); e != nil {
		return e
	}
	for {
		b, e = c.ws.ReadMessage()
		if e != nil {
			return e
		}
		var r struct {
			ID     *int            `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *RPCError       `json:"error"`
		}
		if e = json.Unmarshal(b, &r); e != nil {
			return e
		}
		if r.ID == nil || *r.ID != c.id {
			continue
		}
		if r.Error != nil {
			return r.Error
		}
		if len(r.Result) == 0 {
			return errors.New("missing RPC result")
		}
		return json.Unmarshal(r.Result, out)
	}
}

type Thread struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Cwd       string `json:"cwd"`
	Parent    string `json:"parentThreadId"`
	Ephemeral bool   `json:"ephemeral"`
	Status    struct {
		Type  string   `json:"type"`
		Flags []string `json:"activeFlags"`
	} `json:"status"`
	Turns []struct {
		ID     string `json:"id"`
		Status string `json:"status"`
		Items  []struct {
			Type     string `json:"type"`
			ClientID string `json:"clientId"`
		} `json:"items"`
	} `json:"turns"`
}

func (c *Client) Read(id string, turns bool) (Thread, error) {
	var r struct {
		Thread Thread `json:"thread"`
	}
	e := c.Call("thread/read", map[string]any{"threadId": id, "includeTurns": turns}, &r)
	if e == nil && (r.Thread.ID != id || r.Thread.Status.Type == "") {
		e = errors.New("unknown thread/read shape")
	}
	return r.Thread, e
}
func (t Thread) availability() broker.Availability {
	a := broker.Availability{State: "live", Reason: "reachable daemon thread; terminal observation unavailable"}
	switch t.Status.Type {
	case "idle":
	case "active":
		a.Blocked = true
		a.Reason = "recipient turn is active; waiting for idle"
		for _, f := range t.Status.Flags {
			if f == "waitingOnApproval" || f == "waitingOnUserInput" {
				a.Blocked = true
				a.Reason = f
			}
		}
	case "notLoaded":
		a.State = "offline"
		a.Reason = "thread is not loaded"
	default:
		a.State = "unavailable"
		a.Reason = "unknown thread status"
	}
	return a
}
func (t Thread) turn() string {
	for _, v := range t.Turns {
		if v.Status == "inProgress" {
			return v.ID
		}
	}
	return ""
}

type Driver struct {
	Socket       string
	HerdrSession string
	Agents       func(context.Context, string) ([]identity.Agent, error)
	lookupMu     sync.Mutex
	lookups      map[string]*historyCursor
}

func DefaultSocket() string {
	home := os.Getenv("CODEX_HOME")
	if home == "" {
		home, _ = os.UserHomeDir()
		home = filepath.Join(home, ".codex")
	}
	return filepath.Join(home, "app-server-control", "app-server-control.sock")
}
func (d *Driver) open(ctx context.Context) (*Client, error) {
	path := d.Socket
	if path == "" {
		path = DefaultSocket()
	}
	return Open(ctx, path)
}
func (d *Driver) Probe(ctx context.Context) error {
	c, e := d.open(ctx)
	if e != nil {
		return e
	}
	return c.Close()
}
func (c *Client) state(id string) (Thread, error) { return c.Read(id, false) }
func (d *Driver) SessionState(ctx context.Context, s store.Session) (broker.Availability, error) {
	c, e := d.open(ctx)
	if e != nil {
		return broker.Availability{}, e
	}
	defer c.Close()
	t, e := c.state(s.ID)
	if e != nil {
		var rpc *RPCError
		if errors.As(e, &rpc) && rpc.Code == -32600 && rpc.Message == "thread not loaded: "+s.ID {
			t = Thread{ID: s.ID}
			t.Status.Type = "notLoaded"
		} else {
			return broker.Availability{}, e
		}
	}
	a := t.availability()
	if t.Ephemeral {
		return broker.Availability{State: "unavailable", Reason: "ephemeral Codex thread has no verifiable acceptance history"}, nil
	}
	if a.State == "live" && t.Status.Type == "active" && !a.Blocked && t.turn() == "" {
		a.State = "unavailable"
		a.Reason = "active thread has no current turn"
	}
	if t.Status.Type != "notLoaded" {
		return a, nil
	}
	agents := d.Agents
	if agents == nil {
		agents = identity.Agents
	}
	as, e := agents(ctx, d.HerdrSession)
	if e != nil {
		return broker.Availability{}, e
	}
	for _, p := range as {
		if p.Harness == "codex" && p.Session.Value == s.ID {
			a.State = "detached"
			a.Reason = "detached; run herd-msg doctor for the pinned Codex resume command"
			break
		}
	}
	return a, nil
}
func noSend(class string, e error) (broker.Result, error) {
	return broker.Result{Outcome: broker.NotSent, Class: class, Proof: "no input bytes written: " + e.Error()}, nil
}
func (d *Driver) Inject(ctx context.Context, s store.Session, body, aid string) (broker.Result, error) {
	// Reserve time for a proven pre-send result before the broker's deadline.
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	pre, stop := context.WithDeadline(ctx, time.Now().Add(time.Until(deadline)/2))
	defer stop()
	c, e := d.open(pre)
	if e != nil {
		return noSend("transport", e)
	}
	defer c.Close()
	t, e := c.state(s.ID)
	if e != nil {
		return noSend("transport", e)
	}
	a := t.availability()
	if t.Ephemeral {
		return noSend("precondition_race", errors.New("ephemeral thread has no verifiable acceptance history"))
	}
	if a.State != "live" || a.Blocked {
		return noSend("precondition_race", errors.New(a.Reason))
	}
	t, e = c.state(s.ID)
	if e != nil {
		return noSend("transport", e)
	}
	a = t.availability()
	if t.Ephemeral {
		return noSend("precondition_race", errors.New("ephemeral thread has no verifiable acceptance history"))
	}
	if a.State != "live" || a.Blocked {
		return noSend("precondition_race", errors.New(a.Reason))
	}
	method := "turn/start"
	params := map[string]any{"threadId": s.ID, "clientUserMessageId": aid, "input": []any{map[string]string{"type": "text", "text": body}}}

	if pre.Err() != nil || ctx.Err() != nil {
		return noSend("precondition_race", errors.New("pre-send deadline elapsed"))
	}
	// The connection retains the shorter deadline; no write can begin after it.
	var result json.RawMessage
	if e = c.Call(method, params, &result); e != nil {
		var rpc *RPCError
		if errors.As(e, &rpc) {
			return broker.Result{Outcome: broker.NotSent, Class: "rpc_rejected", Proof: rpc.Error()}, nil
		}
		return broker.Result{Outcome: broker.Unconfirmed, Proof: e.Error()}, nil
	}
	var found bool
	if c.Version == "0.167.0" {
		found, e = c.lookup(s.ID, aid)
	} else {
		found, e = d.lookupClient(c, s.ID, aid)
	}
	if e == nil && found {
		return broker.Result{Outcome: broker.Accepted}, nil
	}
	return broker.Result{Outcome: broker.Unconfirmed}, nil
}

type historyCursor struct {
	LegacyChecked bool
	LegacyFound   bool
	Newest        bool
	Cursor        string
	Checked       time.Time
}

// Continue from the last page examined. Recheck only the final page at EOF so
// asynchronous acceptance can be found without replaying the whole thread.
// A call reads at most two pages, including on old or malformed protocols.
func (c *Client) lookupPages(id, aid string, scan *historyCursor) (bool, error) {
	for page := 0; page < 2; page++ {
		params := map[string]any{"threadId": id, "limit": 100}
		if scan.Newest {
			params["sortDirection"] = "desc"
		}
		if scan.Cursor != "" {
			params["cursor"] = scan.Cursor
		}
		var p struct {
			Data *[]struct {
				Item *struct {
					Type     string `json:"type"`
					ClientID string `json:"clientId"`
				} `json:"item"`
			} `json:"data"`
			Next *string `json:"nextCursor"`
		}
		if e := c.Call("thread/items/list", params, &p); e != nil {
			var rpc *RPCError
			if errors.As(e, &rpc) && rpc.Code == -32601 && (c.Version == "0.157.1" || c.Version == "0.166.0") {
				return c.lookupLegacy(id, aid, scan)
			}
			return false, e
		}
		if p.Data == nil {
			return false, errors.New("missing items page")
		}
		for _, v := range *p.Data {
			if v.Item == nil || v.Item.Type == "" {
				return false, errors.New("unknown items page shape")
			}
			if v.Item.Type == "userMessage" && v.Item.ClientID == aid {
				return true, nil
			}
		}
		if p.Next == nil || *p.Next == "" {
			return false, nil
		}
		if *p.Next == scan.Cursor {
			return false, errors.New("repeated items cursor")
		}
		scan.Cursor = *p.Next
	}
	return false, nil
}

// Known older protocols may lack item pagination. Permit one bounded-frame
// history read per attempt per process; never retry full history in a poll loop.
func (c *Client) lookupLegacy(id, aid string, scan *historyCursor) (bool, error) {
	if scan.LegacyChecked {
		return scan.LegacyFound, nil
	}
	scan.LegacyChecked = true
	t, e := c.Read(id, true)
	if e != nil {
		return false, e
	}
	if t.Turns == nil {
		return false, errors.New("missing legacy history")
	}
	for _, turn := range t.Turns {
		for _, item := range turn.Items {
			if item.Type == "userMessage" && item.ClientID == aid {
				scan.LegacyFound = true
				return true, nil
			}
		}
	}
	return false, nil
}
func (c *Client) lookup(id, aid string) (bool, error) {
	return c.lookupPages(id, aid, &historyCursor{Newest: c.Version == "0.167.0"})
}
func (d *Driver) Lookup(ctx context.Context, r broker.LookupRequest) (bool, error) {
	c, e := d.open(ctx)
	if e != nil {
		return false, e
	}
	defer c.Close()
	t, e := c.Read(r.Attempt.Session.ID, false)
	if e != nil {
		var rpc *RPCError
		if errors.As(e, &rpc) && rpc.Code == -32600 && rpc.Message == "thread not loaded: "+r.Attempt.Session.ID {
			return false, broker.ErrRecipientGone
		}
		return false, e
	}
	if t.Status.Type == "notLoaded" {
		return false, broker.ErrRecipientGone
	}
	return d.lookupClient(c, r.Attempt.Session.ID, r.Attempt.ID)
}

func (d *Driver) lookupClient(c *Client, id, aid string) (bool, error) {
	d.lookupMu.Lock()
	if d.lookups == nil {
		d.lookups = map[string]*historyCursor{}
	}
	for key, scan := range d.lookups {
		if time.Since(scan.Checked) > time.Hour {
			delete(d.lookups, key)
		}
	}
	key := id + "/" + aid
	scan := d.lookups[key]
	if scan == nil {
		if len(d.lookups) >= 1024 {
			for key := range d.lookups {
				delete(d.lookups, key)
				break
			}
		}
		scan = &historyCursor{}
		d.lookups[key] = scan
	}
	scan.Checked = time.Now()
	local := *scan
	d.lookupMu.Unlock()
	found, e := c.lookupPages(id, aid, &local)
	d.lookupMu.Lock()
	if current := d.lookups[key]; current != nil && current.Checked.Equal(local.Checked) {
		*current = local
	}
	d.lookupMu.Unlock()
	return found, e
}
func (d *Driver) Name(ctx context.Context, id, name string) error {
	if e := policy.Identity(name); e != nil {
		return e
	}
	c, e := d.open(ctx)
	if e != nil {
		return e
	}
	defer c.Close()
	var r json.RawMessage
	return c.Call("thread/name/set", map[string]string{"threadId": id, "name": name}, &r)
}
func (d *Driver) Discover(ctx context.Context) ([]store.Session, error) {
	c, e := d.open(ctx)
	if e != nil {
		return nil, e
	}
	defer c.Close()
	out := []store.Session{}
	loaded := map[string]bool{}
	cursor := ""
	seen := map[string]bool{}
	for {
		params := map[string]any{"limit": 100}
		if cursor != "" {
			params["cursor"] = cursor
		}
		var p struct {
			Data *[]string `json:"data"`
			Next *string   `json:"nextCursor"`
		}
		if e = c.Call("thread/loaded/list", params, &p); e != nil {
			return nil, e
		}
		if p.Data == nil {
			return nil, errors.New("unknown loaded-list shape")
		}
		for _, id := range *p.Data {
			loaded[id] = true
			t, e := c.Read(id, false)
			if e != nil {
				return nil, e
			}
			a := t.availability()
			name := t.Name
			if t.Parent != "" || t.Ephemeral {
				name = id
			}
			if policy.Identity(name) != nil {
				name = filepath.Base(t.Cwd)
			}
			if policy.Identity(name) != nil {
				name = id
			}
			out = append(out, store.Session{ID: id, Name: name, Harness: "codex", State: a.State, Blocked: a.Blocked, Reason: a.Reason, Hints: map[string]string{"cwd": t.Cwd}})
		}
		if p.Next == nil || *p.Next == "" {
			agents := d.Agents
			if agents == nil {
				agents = identity.Agents
			}
			peers, probeErr := agents(ctx, d.HerdrSession)
			if probeErr == nil {
				for _, a := range peers {
					if a.Harness != "codex" || a.Session.Value == "" || loaded[a.Session.Value] {
						continue
					}
					t, e := c.Read(a.Session.Value, false)
					if e != nil || t.Status.Type != "notLoaded" {
						continue
					}
					name := t.Name
					if policy.Identity(name) != nil {
						name = filepath.Base(t.Cwd)
					}
					if policy.Identity(name) != nil {
						name = t.ID
					}
					out = append(out, store.Session{ID: t.ID, Name: name, Harness: "codex", Pane: a.Pane, State: "detached", Reason: "detached; run herd-msg doctor for the pinned Codex resume command", Hints: map[string]string{"cwd": t.Cwd}})
				}
			}
			return out, nil
		}
		cursor = *p.Next
		if seen[cursor] {
			return nil, errors.New("repeated loaded cursor")
		}
		seen[cursor] = true
	}
}
