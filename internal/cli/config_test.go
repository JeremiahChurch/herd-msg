package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func TestConfigReplacement(t *testing.T) {
	for _, online := range []bool{false, true} {
		t.Run(map[bool]string{true: "online", false: "offline"}[online], func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HERD_MSG_HOME", home)
			t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
			t.Setenv("CODEX_THREAD_ID", "")
			t.Setenv("CLAUDE_CODE_SESSION_ID", "")
			var b *broker.Broker
			if online {
				var e error
				b, e = broker.New(home, "", nil)
				if e != nil {
					t.Fatal(e)
				}
				defer b.Close()
				server, e := b.Listen()
				if e != nil {
					t.Fatal(e)
				}
				defer server.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				go server.Serve(ctx)
				if e = b.Register(ctx, store.Session{ID: "receiver", Name: "reviewer", Harness: "claude", State: "offline"}); e != nil {
					t.Fatal(e)
				}
			}
			for i, form := range [][]string{{"set", "links", "{}"}, {`{"links":{}}`}} {
				if code, text := cli(t, "config", "set", "links", `{"peer":{"auto_run":true}}`, "--json"); code != 0 {
					t.Fatal(code, text)
				}
				id := []string{"remove-set", "remove-object"}[i]
				if online {
					_, e := b.Accept(context.Background(), store.Message{ID: id, Origin: "peer", Sender: "writer.codex@peer", SenderID: "writer", Recipient: "receiver", Destination: "local", Created: time.Now(), Body: "grant must be revoked"})
					if e != nil {
						t.Fatal(e)
					}
				}
				args := append([]string{"config"}, form...)
				args = append(args, "--json")
				if code, text := cli(t, args...); code != 0 {
					t.Fatal(code, text)
				}
				c, e := policy.Read(filepath.Join(home, "config.json"))
				if e != nil || len(c.Links) != 0 {
					t.Fatal("link removal did nothing", c, e)
				}
				if online {
					d, _ := b.Store.Read(context.Background())
					m, _ := d.Find(id)
					if m.State != states.Dead {
						t.Fatal("removed grant was not reconciled", m)
					}
				}
			}
			if code, text := cli(t, "kill", "on", "--json"); code != 0 {
				t.Fatal(code, text)
			}
			if code, text := cli(t, "config", "set", "hop_limit", "5", "--json"); code != 0 {
				t.Fatal(code, text)
			}
			c, _ := policy.Read(filepath.Join(home, "config.json"))
			if !c.Killed {
				t.Fatal("patch reset unrelated kill")
			}
		})
	}
}
func TestLostReplyAndInvalidConfigRemainVisible(t *testing.T) {
	home := t.TempDir()
	runtimeDir := t.TempDir()
	t.Setenv("HERD_MSG_HOME", home)
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	listener, e := net.Listen("unix", filepath.Join(runtimeDir, "gone.sock"))
	if e != nil {
		t.Fatal(e)
	}
	defer listener.Close()
	info, _ := json.Marshal(broker.SocketInfo{Path: listener.Addr().String()})
	if e = os.WriteFile(filepath.Join(home, "socket.json"), info, 0600); e != nil {
		t.Fatal(e)
	}
	go func() {
		for {
			conn, e := listener.Accept()
			if e != nil {
				return
			}
			buf := make([]byte, 4096)
			_, _ = conn.Read(buf)
			conn.Close()
		}
	}()
	if code, text := cli(t, "status", "--json"); code != 0 || !strings.Contains(text, `"check":"broker"`) {
		t.Fatal(code, text)
	}
	if e = os.WriteFile(filepath.Join(home, "config.json"), []byte(`{broken`), 0600); e != nil {
		t.Fatal(e)
	}
	if code, text := cli(t, "status", "--json"); code != 0 || !strings.Contains(text, `"check":"config"`) {
		t.Fatal(code, text)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 1100*time.Millisecond)
	defer cancel()
	var out, err bytes.Buffer
	if code := Run(ctx, []string{"watch", "--json"}, &out, &err, "test"); code != 0 {
		t.Fatal(code, err.String())
	}
	if strings.Count(out.String(), `"schema":1`) < 2 {
		t.Fatal("watch exited after lost reply", out.String())
	}
}
func TestReadAmbiguity(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HERD_MSG_HOME", home)
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	s, e := store.Open(home)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Update(context.Background(), func(d *store.Data) error {
		for _, origin := range []string{"local", "peer"} {
			d.Messages[store.Key(origin, "same")] = &store.Message{ID: "same", Origin: origin, State: states.Held}
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	s.Close()
	if code, text := cli(t, "read", "same", "--json"); code != 3 {
		t.Fatal(code, text)
	}
}
func TestOfflineRenameCannotStrandRetainedMail(t *testing.T) {
	for _, records := range []string{"mail", "sessions"} {
		t.Run(records, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("HERD_MSG_HOME", home)
			t.Setenv("CODEX_THREAD_ID", "")
			t.Setenv("CLAUDE_CODE_SESSION_ID", "")
			s, e := store.Open(home)
			if e != nil {
				t.Fatal(e)
			}
			if e = s.Update(context.Background(), func(d *store.Data) error {
				if records == "mail" {
					d.Messages["local/test"] = &store.Message{ID: "test", Origin: "local", Recipient: "receiver", Destination: "local", State: states.Queued}
				} else {
					d.Sessions["receiver"] = store.Session{ID: "receiver", Machine: "local"}
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			s.Close()
			if code, text := cli(t, "config", "set", "machine", `"renamed"`, "--json"); code != 2 {
				t.Fatal(code, text)
			}
		})
	}
}

func TestAmbiguousAddressExit(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HERD_MSG_HOME", home)
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	b, e := broker.New(home, "", nil)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	for _, id := range []string{"one", "two"} {
		if e = b.Register(context.Background(), store.Session{ID: id, Name: "duplicate", Harness: "claude", State: "offline"}); e != nil {
			t.Fatal(e)
		}
	}
	server, e := b.Listen()
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.Serve(ctx)
	if code, text := cli(t, "send", "duplicate", "body", "--json"); code != 3 || !strings.Contains(text, "one, two") {
		t.Fatal(code, text)
	}
}

func TestOfflineBrokenConfigKeepsRecordedMachine(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HERD_MSG_HOME", home)
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	cfg := policy.Defaults()
	cfg.Machine = "box"
	path := filepath.Join(home, "config.json")
	if e := policy.Write(path, cfg, nil); e != nil {
		t.Fatal(e)
	}
	b, e := broker.New(home, "", nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = b.Register(context.Background(), store.Session{ID: "receiver", Name: "receiver", Harness: "claude", State: "offline"}); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Send(context.Background(), broker.SendRequest{ID: "stored-mail", Created: time.Now(), To: "receiver", Body: "retained body"}, broker.Caller{ID: "operator"}); e != nil {
		t.Fatal(e)
	}
	if e = b.Store.Update(context.Background(), func(d *store.Data) error {
		d.Notice("config", "previous invalid config", "repair", time.Now())
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	b.Close()
	if e = os.WriteFile(path, []byte(`{broken`), 0600); e != nil {
		t.Fatal(e)
	}
	t.Setenv("CLAUDE_CODE_SESSION_ID", "receiver")
	if code, text := cli(t, "inbox", "--json"); code != 0 || !strings.Contains(text, "stored-mail") {
		t.Fatal("false empty inbox", code, text)
	}
	code, text := cli(t, "status", "--json")
	if code != 0 {
		t.Fatal(code, text)
	}
	var status Status
	if e = json.Unmarshal([]byte(text), &status); e != nil {
		t.Fatal(e)
	}
	if status.Machine != "box" || status.Sessions[0].Unsent != 1 {
		t.Fatal(status)
	}
	cfg.Killed = true
	if e = policy.Write(path, cfg, nil); e != nil {
		t.Fatal(e)
	}
	if code, text = cli(t, "status", "--json"); code != 0 || strings.Contains(text, `"check":"config"`) {
		t.Fatal("fixed offline config still had a notice", code, text)
	}
	d, e := store.Snapshot(context.Background(), home)
	if e != nil || d.Health["config"].Check == "" {
		t.Fatal("offline display modified durable content", e)
	}
}
func TestReadBrokerWorker(t *testing.T) {
	if os.Getenv("MAILBOX_READ_WORKER") != "1" {
		return
	}
	home := os.Getenv("HERD_MSG_HOME")
	b, e := broker.New(home, "", nil, func(point string) {
		if point == "before_rpc_snapshot" {
			if err := os.WriteFile(filepath.Join(home, "request-seen"), []byte("ready"), 0600); err != nil {
				panic(err)
			}
			select {}
		}
	})
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	if e = b.Register(context.Background(), store.Session{ID: "receiver", Name: "receiver", Harness: "claude", State: "offline"}); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Send(context.Background(), broker.SendRequest{ID: "read-crash", Created: time.Now(), To: "receiver", Body: "durable"}, broker.Caller{ID: "operator"}); e != nil {
		t.Fatal(e)
	}
	server, e := b.Listen()
	if e != nil {
		t.Fatal(e)
	}
	defer server.Close()
	if e = server.Serve(context.Background()); e != nil {
		t.Fatal(e)
	}
}
func TestReadCrash(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HERD_MSG_HOME", home)
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	cmd := exec.Command(os.Args[0], "-test.run=^TestReadBrokerWorker$")
	cmd.Env = append(os.Environ(), "MAILBOX_READ_WORKER=1")
	if e := cmd.Start(); e != nil {
		t.Fatal(e)
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	awaitFile := func(name string) {
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if _, e := os.Stat(filepath.Join(home, name)); e == nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatal("broker did not reach", name)
	}
	awaitFile("socket.json")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var out, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() { done <- Run(ctx, []string{"status", "--json"}, &out, &stderr, "test") }()
	awaitFile("request-seen")
	if e := cmd.Process.Kill(); e != nil {
		t.Fatal(e)
	}
	if e := cmd.Wait(); e == nil {
		t.Fatal("broker survived SIGKILL")
	}
	if code := <-done; code != 0 || !strings.Contains(out.String(), "read-crash") || !strings.Contains(out.String(), `"check":"broker"`) {
		t.Fatal("interrupted real broker read did not fall back", code, out.String(), stderr.String())
	}
}

func TestConfigRepairKeepsIdentity(t *testing.T) {
	for _, repair := range []string{"empty", "missing", "renamed"} {
		for _, online := range []bool{false, true} {
			t.Run(repair+map[bool]string{false: "/offline", true: "/online"}[online], func(t *testing.T) {
				home := t.TempDir()
				t.Setenv("HERD_MSG_HOME", home)
				t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
				t.Setenv("CODEX_THREAD_ID", "")
				t.Setenv("CLAUDE_CODE_SESSION_ID", "")
				path := filepath.Join(home, "config.json")
				cfg := policy.Defaults()
				cfg.Machine = "box"
				if e := policy.Write(path, cfg, nil); e != nil {
					t.Fatal(e)
				}
				b, e := broker.New(home, "", nil)
				if e != nil {
					t.Fatal(e)
				}
				defer b.Close()
				if e = b.Register(context.Background(), store.Session{ID: "receiver", Name: "receiver", Harness: "claude", State: "offline"}); e != nil {
					t.Fatal(e)
				}
				if _, e = b.Send(context.Background(), broker.SendRequest{ID: "retained", Created: time.Now(), To: "receiver", Body: "keep queued"}, broker.Caller{ID: "operator"}); e != nil {
					t.Fatal(e)
				}
				if online {
					server, e := b.Listen()
					if e != nil {
						t.Fatal(e)
					}
					defer server.Close()
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					go server.Serve(ctx)
				} else if e = b.Close(); e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(path, []byte(`{broken`), 0600); e != nil {
					t.Fatal(e)
				}
				t.Setenv("CLAUDE_CODE_SESSION_ID", "receiver")
				if code, text := cli(t, "inbox", "--json"); code != 0 || !strings.Contains(text, "retained") {
					t.Fatal(code, text)
				}
				switch repair {
				case "missing":
					e = os.Rename(path, path+".broken")
				case "empty":
					e = os.WriteFile(path, []byte(`{}`), 0600)
				case "renamed":
					e = os.WriteFile(path, []byte(`{"machine":"other"}`), 0600)
				}
				if e != nil {
					t.Fatal(e)
				}
				if code, text := cli(t, "inbox", "--json"); code != 0 || !strings.Contains(text, "retained") {
					t.Fatal("repair hid inbox", code, text)
				}
				code, text := cli(t, "status", "--json")
				var status Status
				if e = json.Unmarshal([]byte(text), &status); e != nil || code != 0 || status.Machine != "box" || status.Sessions[0].Unsent != 1 || !status.Killed || !strings.Contains(text, "recorded machine") {
					t.Fatal("repair hid identity warning", code, text, e)
				}
				if online {
					var invalid *broker.InvalidConfigError
					if e = b.Tick(context.Background()); !errors.As(e, &invalid) {
						t.Fatal("edited machine did not stop dispatch", e)
					}
				} else {
					unexpected, e := broker.New(home, "", nil)
					if unexpected != nil {
						unexpected.Close()
					}
					var invalid *broker.InvalidConfigError
					if !errors.As(e, &invalid) {
						t.Fatal("startup accepted changed identity", e)
					}
				}
				d, e := store.Snapshot(context.Background(), home)
				if e != nil || d.Messages["box/retained"].State != states.Queued {
					t.Fatal("identity repair changed queued mail", e)
				}
				if e = policy.Write(path, cfg, nil); e != nil {
					t.Fatal(e)
				}
				if code, text := cli(t, "status", "--json"); code != 0 || strings.Contains(text, `"check":"config"`) {
					t.Fatal("restored identity retained warning", code, text)
				}
			})
		}
	}
}

func TestOfflineConfigNoticeUsesEditTime(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HERD_MSG_HOME", home)
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	path := filepath.Join(home, "config.json")
	if e := os.WriteFile(path, []byte(`{"hop_limit":0}`), 0600); e != nil {
		t.Fatal(e)
	}
	edited := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	if e := os.Chtimes(path, edited, edited); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		code, text := cli(t, "status", "--json")
		var status Status
		if e := json.Unmarshal([]byte(text), &status); e != nil || code != 0 {
			t.Fatal(code, text, e)
		}
		found := false
		for _, h := range status.Health {
			if h.Check == "config" {
				found = true
				if h.Since != stamp(edited) || !strings.Contains(h.Fix, "restore a valid configuration") {
					t.Fatal("invalid settings need a stable notice and an accurate repair", h)
				}
			}
		}
		if !found {
			t.Fatal("invalid settings notice missing", text)
		}
	}
	if _, e := os.Stat(filepath.Join(home, "mailbox.db")); !os.IsNotExist(e) {
		t.Fatal("offline notice created a database", e)
	}
}
