package codex

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

type request struct {
	ID     *int                       `json:"id"`
	Method string                     `json:"method"`
	Params map[string]json.RawMessage `json:"params"`
}

func serve(t *testing.T, fn func(request) (any, *RPCError)) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "api.sock")
	l, e := net.Listen("unix", p)
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	t.Cleanup(func() { l.Close(); wg.Wait() })
	go func() {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				c.SetDeadline(time.Now().Add(3 * time.Second))
				br := bufio.NewReader(c)
				r, e := http.ReadRequest(br)
				if e != nil {
					return
				}
				sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
				fmt.Fprintf(c, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(sum[:]))
				for {
					b, e := readClient(br)
					if e != nil {
						return
					}
					var q request
					if json.Unmarshal(b, &q) != nil {
						return
					}
					if q.ID == nil {
						continue
					}
					_, _ = c.Write(append([]byte{0x81, byte(len(`{"method":"thread/status/changed"}`))}, []byte(`{"method":"thread/status/changed"}`)...))
					v, re := fn(q)
					reply := map[string]any{"id": *q.ID}
					if re != nil {
						reply["error"] = re
					} else {
						reply["result"] = v
					}
					b, _ = json.Marshal(reply)
					h := []byte{0x81}
					if len(b) < 126 {
						h = append(h, byte(len(b)))
					} else if len(b) <= 65535 {
						h = append(h, 126, byte(len(b)>>8), byte(len(b)))
					} else {
						var size [8]byte
						binary.BigEndian.PutUint64(size[:], uint64(len(b)))
						h = append(append(h, 127), size[:]...)
					}
					if _, e = c.Write(append(h, b...)); e != nil {
						return
					}
				}
			}()
		}
	}()
	return p
}
func readClient(r io.Reader) ([]byte, error) {
	var h [2]byte
	if _, e := io.ReadFull(r, h[:]); e != nil {
		return nil, e
	}
	if h[1]&128 == 0 {
		return nil, fmt.Errorf("unmasked")
	}
	n := int(h[1] & 127)
	if n == 126 {
		var b [2]byte
		io.ReadFull(r, b[:])
		n = int(binary.BigEndian.Uint16(b[:]))
	}
	if n == 127 {
		var b [8]byte
		io.ReadFull(r, b[:])
		n = int(binary.BigEndian.Uint64(b[:]))
	}
	var m [4]byte
	io.ReadFull(r, m[:])
	b := make([]byte, n)
	_, e := io.ReadFull(r, b)
	for i := range b {
		b[i] ^= m[i%4]
	}
	return b, e
}
func thread(state string, flags []string) map[string]any {
	return map[string]any{"thread": map[string]any{"id": "recipient", "cwd": "/tmp/project", "status": map[string]any{"type": state, "activeFlags": flags}, "turns": []any{map[string]any{"id": "turn-current", "status": "inProgress", "items": []any{}}}}}
}
func TestDeliveryRacesRequirePositiveProof(t *testing.T) {
	for _, tc := range []struct {
		name, state     string
		conflict, found bool
		want            string
	}{{"idle", "idle", false, true, broker.Accepted}, {"active waits for idle", "active", false, true, broker.NotSent}, {"RPC rejection", "idle", true, false, broker.NotSent}, {"missing receipt", "idle", false, false, broker.Unconfirmed}} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			calls := []string{}
			p := serve(t, func(q request) (any, *RPCError) {
				mu.Lock()
				defer mu.Unlock()
				calls = append(calls, q.Method)
				switch q.Method {
				case "initialize":
					return map[string]any{"userAgent": "test/0.157.1 machine"}, nil
				case "thread/read":
					return thread(tc.state, nil), nil
				case "thread/resume":
					return map[string]any{}, nil
				case "turn/start", "turn/steer":
					var id string
					json.Unmarshal(q.Params["clientUserMessageId"], &id)
					if id != "immutable-attempt" {
						t.Error("wrong client id", id)
					}
					if q.Method == "turn/steer" {
						var turn string
						json.Unmarshal(q.Params["expectedTurnId"], &turn)
						if turn != "turn-current" {
							t.Error("steer precondition missing")
						}
					}
					if tc.conflict {
						return nil, &RPCError{-32600, "expected active turn id changed"}
					}
					return map[string]any{"turnId": "turn-current"}, nil
				case "thread/items/list":
					data := []any{}
					if tc.found {
						data = append(data, map[string]any{"item": map[string]string{"type": "userMessage", "clientId": "immutable-attempt"}})
					}
					return map[string]any{"data": data, "nextCursor": nil}, nil
				}
				return nil, &RPCError{-32601, "unknown"}
			})
			d := &Driver{Socket: p}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			r, e := d.Inject(ctx, store.Session{ID: "recipient"}, "body", "immutable-attempt")
			if e != nil || r.Outcome != tc.want {
				t.Fatalf("%+v %v", r, e)
			}
			mu.Lock()
			defer mu.Unlock()
			starts, steers := 0, 0
			for _, c := range calls {
				if c == "turn/start" {
					starts++
				}
				if c == "turn/steer" {
					steers++
				}
			}
			if tc.state == "active" && (starts != 0 || steers != 0) {
				t.Fatal("steer fell back", calls)
			}
			if tc.state == "idle" && (starts != 1 || steers != 0) {
				t.Fatal(calls)
			}
		})
	}
}
func TestBlockedDetachedAndUnsupported(t *testing.T) {
	for _, tc := range []struct {
		name, version, state string
		flags                []string
		live                 bool
		want                 string
	}{{"approval", "0.166.0", "active", []string{"waitingOnApproval"}, false, "live"}, {"question", "0.166.0", "active", []string{"waitingOnUserInput"}, false, "live"}, {"detached", "0.166.0", "notLoaded", nil, true, "detached"}, {"offline", "0.166.0", "notLoaded", nil, false, "offline"}, {"old", "0.156.0", "idle", nil, false, "error"}, {"unknown version", "", "idle", nil, false, "error"}} {
		t.Run(tc.name, func(t *testing.T) {
			p := serve(t, func(q request) (any, *RPCError) {
				if q.Method == "initialize" {
					return map[string]string{"userAgent": "test/" + tc.version}, nil
				}
				if q.Method == "thread/read" {
					return thread(tc.state, tc.flags), nil
				}
				t.Error("unexpected write", q.Method)
				return map[string]any{}, nil
			})
			d := &Driver{Socket: p, Agents: func(context.Context, string) ([]identity.Agent, error) {
				if tc.live {
					a := identity.Agent{Harness: "codex"}
					a.Session.Value = "recipient"
					return []identity.Agent{a}, nil
				}
				return []identity.Agent{}, nil
			}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			a, e := d.SessionState(ctx, store.Session{ID: "recipient"})
			if tc.want == "error" {
				if e == nil {
					t.Fatal("version accepted")
				}
				return
			}
			if e != nil || a.State != tc.want || a.Blocked != (len(tc.flags) > 0) {
				t.Fatalf("%+v %v", a, e)
			}
			if tc.want == "detached" && !strings.Contains(a.Reason, "herd-msg doctor") {
				t.Fatal("detached repair must preserve the pinned thread", a.Reason)
			}
		})
	}
}
func TestLookupPaginationFallbackAndNoResume(t *testing.T) {
	for _, mode := range []string{"pages", "old", "shape", "malformed"} {
		t.Run(mode, func(t *testing.T) {
			p := serve(t, func(q request) (any, *RPCError) {
				switch q.Method {
				case "initialize":
					return map[string]string{"userAgent": "test/0.157.1"}, nil
				case "thread/items/list":
					if mode == "old" {
						return nil, &RPCError{-32601, "not found"}
					}
					if mode == "shape" || mode == "malformed" {
						return map[string]any{"data": []string{"changed"}}, nil
					}
					if len(q.Params["cursor"]) == 0 {
						return map[string]any{"data": []any{map[string]any{"item": map[string]string{"type": "agentMessage", "clientId": "aid"}}}, "nextCursor": "next"}, nil
					}
					return map[string]any{"data": []any{map[string]any{"item": map[string]string{"type": "userMessage", "clientId": "aid"}}}, "nextCursor": nil}, nil
				case "thread/read":
					var turns bool
					json.Unmarshal(q.Params["includeTurns"], &turns)
					if turns && mode != "old" {
						t.Error("lookup fetched full history")
					}
					result := thread("idle", nil)
					if turns {
						result["thread"].(map[string]any)["turns"] = []any{map[string]any{"items": []any{map[string]string{"type": "userMessage", "clientId": "aid"}}}}
					}
					return result, nil
				default:
					t.Error("lookup mutated thread", q.Method)
					return nil, &RPCError{-32601, "unknown"}
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			d := &Driver{Socket: p}
			found, e := d.Lookup(ctx, broker.LookupRequest{Attempt: store.Attempt{ID: "aid", Session: store.Session{ID: "recipient"}}})
			if mode != "pages" && mode != "old" {
				if e == nil || found {
					t.Fatal(found, e)
				}
			} else if e != nil || !found {
				t.Fatal(found, e)
			}
		})
	}
}

func TestDiscoveryUsesMetadataAndStableNames(t *testing.T) {
	p := serve(t, func(q request) (any, *RPCError) {
		switch q.Method {
		case "initialize":
			return map[string]string{"userAgent": "custom-client/0.166.0"}, nil
		case "thread/loaded/list":
			return map[string]any{"data": []string{"parent", "child"}, "nextCursor": nil}, nil
		case "thread/read":
			var id string
			json.Unmarshal(q.Params["threadId"], &id)
			var turns bool
			json.Unmarshal(q.Params["includeTurns"], &turns)
			if turns {
				t.Error("discovery requested ephemeral history")
				return nil, &RPCError{-32600, "ephemeral threads do not support includeTurns"}
			}
			thread := map[string]any{"id": id, "name": "invalid|title", "cwd": "/tmp/project", "status": map[string]any{"type": "idle"}}
			if id == "child" {
				thread["ephemeral"] = true
				thread["parentThreadId"] = "parent"
			}
			return map[string]any{"thread": thread}, nil
		default:
			return nil, &RPCError{-32601, "unexpected"}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sessions, e := (&Driver{Socket: p, Agents: func(context.Context, string) ([]identity.Agent, error) { return []identity.Agent{}, nil }}).Discover(ctx)
	if e != nil || len(sessions) != 2 {
		t.Fatal(sessions, e)
	}
	if sessions[0].Name != "project" || sessions[1].Name != "child" {
		t.Fatal("title fallback or child collision", sessions)
	}
}

func TestBlankIdleThreadDoesNotRequireHistory(t *testing.T) {
	p := serve(t, func(q request) (any, *RPCError) {
		switch q.Method {
		case "initialize":
			return map[string]string{"userAgent": "test/0.166.0"}, nil
		case "thread/read":
			var turns bool
			json.Unmarshal(q.Params["includeTurns"], &turns)
			if turns {
				return nil, &RPCError{-32601, "list_turns is not supported yet"}
			}
			r := thread("idle", nil)
			r["thread"].(map[string]any)["turns"] = nil
			return r, nil
		case "thread/resume":
			t.Error("delivery must not resume a blank loaded thread")
			return nil, &RPCError{-32600, "no rollout found"}
		case "turn/start":
			return map[string]any{}, nil
		case "thread/items/list":
			return map[string]any{"data": []any{map[string]any{"item": map[string]string{"type": "userMessage", "clientId": "first"}}}, "nextCursor": nil}, nil
		default:
			return nil, &RPCError{-32601, "unexpected"}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	d := &Driver{Socket: p}
	a, e := d.SessionState(ctx, store.Session{ID: "recipient"})
	if e != nil || a.State != "live" {
		t.Fatal(a, e)
	}
	r, e := d.Inject(ctx, store.Session{ID: "recipient"}, "first input", "first")
	if e != nil || r.Outcome != broker.Accepted {
		t.Fatal(r, e)
	}
}

func TestDiscoveryRegistersNewDetachedThreadAndRefusesEphemeral(t *testing.T) {
	p := serve(t, func(q request) (any, *RPCError) {
		switch q.Method {
		case "initialize":
			return map[string]string{"userAgent": "test/0.166.0"}, nil
		case "thread/loaded/list":
			return map[string]any{"data": []string{}, "nextCursor": nil}, nil
		case "thread/read":
			r := thread("notLoaded", nil)
			r["thread"].(map[string]any)["name"] = "detached"
			return r, nil
		default:
			t.Error(q.Method)
			return nil, &RPCError{-32601, "unexpected"}
		}
	})
	d := &Driver{Socket: p, Agents: func(context.Context, string) ([]identity.Agent, error) {
		a := identity.Agent{Harness: "codex", Pane: "test-pane"}
		a.Session.Value = "recipient"
		return []identity.Agent{a}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sessions, e := d.Discover(ctx)
	if e != nil || len(sessions) != 1 || sessions[0].State != "detached" || sessions[0].ID != "recipient" {
		t.Fatal(sessions, e)
	}
	if !strings.Contains(sessions[0].Reason, "herd-msg doctor") {
		t.Fatal("discovered detached thread omitted pinned repair", sessions[0].Reason)
	}
	p2 := serve(t, func(q request) (any, *RPCError) {
		if q.Method == "initialize" {
			return map[string]string{"userAgent": "test/0.166.0"}, nil
		}
		if q.Method == "thread/read" {
			r := thread("idle", nil)
			r["thread"].(map[string]any)["ephemeral"] = true
			return r, nil
		}
		t.Error("ephemeral mutation", q.Method)
		return nil, &RPCError{-32601, "unexpected"}
	})
	d = &Driver{Socket: p2}
	a, e := d.SessionState(ctx, store.Session{ID: "recipient"})
	if e != nil || a.State != "unavailable" {
		t.Fatal(a, e)
	}
	r, e := d.Inject(ctx, store.Session{ID: "recipient"}, "body", "attempt")
	if e != nil || r.Outcome != broker.NotSent {
		t.Fatal(r, e)
	}
}

func TestGoneThreadUsesFreshHerdrBeforePrune(t *testing.T) {
	for _, tc := range []struct {
		name, message     string
		live, herdrFailed bool
		state             string
		wantErr           bool
	}{{"gone", "thread not loaded: recipient", false, false, "offline", false}, {"detached", "thread not loaded: recipient", true, false, "detached", false}, {"herdr unavailable", "thread not loaded: recipient", false, true, "", true}, {"other error", "database unavailable", false, false, "", true}} {
		t.Run(tc.name, func(t *testing.T) {
			p := serve(t, func(q request) (any, *RPCError) {
				if q.Method == "initialize" {
					return map[string]string{"userAgent": "test/0.166.0"}, nil
				}
				return nil, &RPCError{-32600, tc.message}
			})
			d := &Driver{Socket: p, Agents: func(context.Context, string) ([]identity.Agent, error) {
				if tc.herdrFailed {
					return nil, fmt.Errorf("herdr unavailable")
				}
				if tc.live {
					a := identity.Agent{Harness: "codex"}
					a.Session.Value = "recipient"
					return []identity.Agent{a}, nil
				}
				return []identity.Agent{}, nil
			}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			a, e := d.SessionState(ctx, store.Session{ID: "recipient"})
			if (e != nil) != tc.wantErr || a.State != tc.state {
				t.Fatal(a, e)
			}
		})
	}
}

func TestLookupContinuesFromCursorWithoutFullHistory(t *testing.T) {
	var mu sync.Mutex
	var cursors []string
	p := serve(t, func(q request) (any, *RPCError) {
		switch q.Method {
		case "initialize":
			return map[string]string{"userAgent": "test/0.167.0"}, nil
		case "thread/read":
			var turns bool
			json.Unmarshal(q.Params["includeTurns"], &turns)
			if turns {
				t.Error("full history read")
			}
			return thread("idle", nil), nil
		case "thread/items/list":
			var cursor string
			json.Unmarshal(q.Params["cursor"], &cursor)
			mu.Lock()
			cursors = append(cursors, cursor)
			mu.Unlock()
			next := "one"
			if cursor == "one" {
				next = "two"
			}
			if cursor == "two" {
				next = ""
			}
			data := []any{map[string]any{"item": map[string]string{"type": "agentMessage"}}}
			if cursor == "two" {
				data = []any{map[string]any{"item": map[string]string{"type": "userMessage", "clientId": "aid"}}}
			}
			return map[string]any{"data": data, "nextCursor": next}, nil
		}
		t.Error("unexpected method", q.Method)
		return nil, &RPCError{-32601, "unknown"}
	})
	d := &Driver{Socket: p}
	r := broker.LookupRequest{Attempt: store.Attempt{ID: "aid", Session: store.Session{ID: "recipient"}}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if found, e := d.Lookup(ctx, r); found || e != nil {
		t.Fatal(found, e)
	}
	if found, e := d.Lookup(ctx, r); !found || e != nil {
		t.Fatal(found, e)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(cursors, ",") != ",one,two" {
		t.Fatal("lookup replayed pages", cursors)
	}
}

func TestRPCRejectionKeepsProofAndActiveProbeUsesMetadata(t *testing.T) {
	p := serve(t, func(q request) (any, *RPCError) {
		switch q.Method {
		case "initialize":
			return map[string]string{"userAgent": "test/0.167.0"}, nil
		case "thread/read":
			var turns bool
			json.Unmarshal(q.Params["includeTurns"], &turns)
			if turns {
				t.Error("full history fetched")
			}
			return thread("idle", nil), nil
		case "turn/start":
			return nil, &RPCError{-32600, "thread changed before delivery"}
		}
		t.Error("unexpected method", q.Method)
		return nil, &RPCError{-32601, "unknown"}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, e := (&Driver{Socket: p}).Inject(ctx, store.Session{ID: "recipient"}, "tiny", "aid")
	if e != nil || r.Outcome != broker.NotSent || !strings.Contains(r.Proof, "thread changed before delivery") {
		t.Fatal(r, e)
	}
}

func TestLegacyFallbackDoesNotRepeatFullHistory(t *testing.T) {
	var mu sync.Mutex
	reads := 0
	p := serve(t, func(q request) (any, *RPCError) {
		switch q.Method {
		case "initialize":
			return map[string]string{"userAgent": "test/0.157.1"}, nil
		case "thread/items/list":
			return nil, &RPCError{-32601, "unsupported"}
		case "thread/read":
			var turns bool
			json.Unmarshal(q.Params["includeTurns"], &turns)
			if turns {
				mu.Lock()
				reads++
				mu.Unlock()
			}
			r := thread("idle", nil)
			return r, nil
		}
		return nil, &RPCError{-32601, "unknown"}
	})
	d := &Driver{Socket: p}
	r := broker.LookupRequest{Attempt: store.Attempt{ID: "missing", Session: store.Session{ID: "recipient"}}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		if found, e := d.Lookup(ctx, r); found || e != nil {
			t.Fatal(found, e)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if reads != 1 {
		t.Fatal("legacy history repeatedly read", reads)
	}
}
