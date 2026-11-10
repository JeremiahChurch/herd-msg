package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/drivers/claude"
	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/receiver"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

// The fake app-server is a separate process: its response construction and history
// encoding must not be attributed to broker CPU in the parent process's profile.
func TestProfileAppServer(t *testing.T) {
	if os.Getenv("HERD_MSG_PROFILE_APP_SERVER") != "1" {
		t.Skip("profiling helper process")
	}
	kb, _ := strconv.Atoi(os.Getenv("HERD_MSG_PROFILE_HISTORY_KB"))
	if kb <= 0 {
		kb = 1024
	}
	ids := []string{}
	full := map[string]json.RawMessage{}
	accepted := map[string][]string{}
	var acceptedMu sync.Mutex
	delivery := os.Getenv("HERD_MSG_PROFILE_DELIVERY") == "1"
	summary := map[string]any{}
	for i := 0; i < 9; i++ {
		id := fmt.Sprintf("codex-%d", i)
		ids = append(ids, id)
		turns := []any{}
		for j := 0; j < 20; j++ {
			status := "completed"
			if j == 19 && !delivery {
				status = "inProgress"
			}
			items := []any{}
			for k := 0; k < 20; k++ {
				items = append(items, map[string]any{"type": "agentMessage", "text": strings.Repeat("x", kb*1024/400), "clientId": ""})
			}
			turns = append(turns, map[string]any{"id": fmt.Sprintf("turn-%d", j), "status": status, "items": items})
		}
		thread := map[string]any{"id": id, "name": id, "cwd": "/tmp/profile", "status": map[string]string{"type": "active"}, "turns": turns}
		if delivery {
			thread["status"] = map[string]string{"type": "idle"}
		}
		encoded, _ := json.Marshal(map[string]any{"thread": thread})
		full[id] = encoded
		delete(thread, "turns")
		summary[id] = map[string]any{"thread": thread}
	}
	socket := serve(t, func(q request) (any, *RPCError) {
		switch q.Method {
		case "initialize":
			return map[string]string{"userAgent": "profile/0.166.0"}, nil
		case "thread/loaded/list":
			return map[string]any{"data": ids, "nextCursor": nil}, nil
		case "turn/start", "turn/steer":
			var id, aid string
			json.Unmarshal(q.Params["threadId"], &id)
			json.Unmarshal(q.Params["clientUserMessageId"], &aid)
			var input []struct {
				Text string `json:"text"`
			}
			json.Unmarshal(q.Params["input"], &input)
			if len(input) > 0 {
				recordProfileInjection(os.Getenv("HERD_MSG_PROFILE_INJECTION_DIR"), input[0].Text)
			}
			acceptedMu.Lock()
			accepted[id] = append(accepted[id], aid)
			acceptedMu.Unlock()
			return map[string]any{}, nil
		case "thread/items/list":
			var id string
			json.Unmarshal(q.Params["threadId"], &id)
			acceptedMu.Lock()
			defer acceptedMu.Unlock()
			items := []any{}
			for _, aid := range accepted[id] {
				items = append(items, map[string]any{"item": map[string]string{"type": "userMessage", "clientId": aid}})
			}
			if os.Getenv("HERD_MSG_PROFILE_HISTORY_KB") == "9216" {
				items = append(items, map[string]any{"item": map[string]string{"type": "agentMessage", "text": strings.Repeat("x", 9*1024*1024)}})
			}
			return map[string]any{"data": items, "nextCursor": nil}, nil
		case "thread/read":
			var id string
			var turns bool
			json.Unmarshal(q.Params["threadId"], &id)
			json.Unmarshal(q.Params["includeTurns"], &turns)
			if turns {
				return full[id], nil
			}
			if v, ok := summary[id]; ok {
				return v, nil
			}
			return map[string]any{"thread": map[string]any{"id": id, "status": map[string]string{"type": "idle"}}}, nil
		}
		return nil, &RPCError{Code: -32601, Message: "unexpected profiling request"}
	})
	fmt.Println("HERD_MSG_PROFILE_SOCKET=" + socket)
	io.Copy(io.Discard, os.Stdin)
}

func profileAppServer(t *testing.T) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(exe, "-test.run=^TestProfileAppServer$")
	cmd.Env = append(os.Environ(), "HERD_MSG_PROFILE_APP_SERVER=1")
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		input.Close()
		if err := cmd.Wait(); err != nil {
			t.Error(err)
		}
	})
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			if s, ok := strings.CutPrefix(scanner.Text(), "HERD_MSG_PROFILE_SOCKET="); ok {
				ready <- s
			}
		}
		close(ready)
	}()
	select {
	case socket := <-ready:
		if socket == "" {
			t.Fatal("app-server did not start")
		}
		return socket
	case <-time.After(10 * time.Second):
		t.Fatal("app-server startup timed out")
	}
	return ""
}

func profileReceiver(t *testing.T, path string, s store.Session) {
	t.Helper()
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	t.Cleanup(func() { l.Close(); wg.Wait() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(time.Second))
				var q receiver.Control
				if json.NewDecoder(conn).Decode(&q) == nil {
					json.NewEncoder(conn).Encode(receiver.Reply{Status: receiver.Status{Session: s.ID, Generation: s.Generation, Instance: "profile"}})
				}
			}()
		}
	}()
}

// Opt in with HERD_MSG_SYSTEM_LOAD_SECONDS=30 and HERD_MSG_PROFILE_RECEIVERS=0,
// 5 or 20. HERD_MSG_PROFILE_IDLE=1 disables sends;
// HERD_MSG_PROFILE_REAL_RECEIVERS=1 measures real receiver processes separately;
// HERD_MSG_PROFILE_DELIVERY=1 sends through synthetic Claude and Codex harnesses
// and times input arrival independently of transcript confirmation. All sockets, transcripts and mail are temporary.
// This runs the real Codex and Claude drivers, including full active-turn
// histories, Tick, discovery/availability refresh, watchdog writes and peer RPCs.
// Only the app-server and herdr responses and remote transport are synthetic.
func TestBrokerSystemLoad(t *testing.T) {
	seconds, _ := strconv.Atoi(os.Getenv("HERD_MSG_SYSTEM_LOAD_SECONDS"))
	if seconds <= 0 {
		t.Skip("set HERD_MSG_SYSTEM_LOAD_SECONDS")
	}
	idle := os.Getenv("HERD_MSG_PROFILE_IDLE") == "1"
	realReceivers := os.Getenv("HERD_MSG_PROFILE_REAL_RECEIVERS") == "1"
	delivery := os.Getenv("HERD_MSG_PROFILE_DELIVERY") == "1"
	count, _ := strconv.Atoi(os.Getenv("HERD_MSG_PROFILE_RECEIVERS"))
	if count < 0 || count > 20 {
		t.Fatal("receiver count must be between 0 and 20")
	}
	root, err := os.MkdirTemp("", "hm-profile-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	t.Setenv("HERD_MSG_PROFILE_INJECTION_DIR", root)
	cd := &Driver{Socket: profileAppServer(t)}
	var polls atomic.Int64
	b, err := broker.New(filepath.Join(root, "main"), filepath.Join(root, "main/config.json"), map[string]broker.Driver{"codex": cd, "claude": &claude.Driver{}}, func(point string) {
		if point == "before_rpc_receiver-state" {
			polls.Add(1)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	sessions := make([]store.Session, 100)
	agents := []identity.Agent{}
	for i := range sessions {
		s := store.Session{ID: fmt.Sprintf("claude-%d", i), Name: fmt.Sprintf("claude-%d", i), Harness: "claude", State: "offline", Reason: "Claude receiver or SessionStart hook is missing", Machine: "local", Generation: store.Random(), LastSeen: time.Now().UTC(), Hints: map[string]string{}}
		if i < count {
			s.State = "live"
			s.Pane = fmt.Sprintf("w1:p%d", i)
			s.Reason = "idle"
			s.Hints = map[string]string{"endpoint": filepath.Join(root, fmt.Sprintf("r%d.sock", i)), "instance": "profile", "broker_epoch": b.Epoch}
			if !realReceivers {
				profileReceiver(t, s.Hints["endpoint"], s)
			}
			agents = append(agents, identity.Agent{Pane: s.Pane, Name: s.Name, Harness: "claude", State: "idle"})
		}
		if i >= 20 && i < 29 {
			s.ID = fmt.Sprintf("codex-%d", i-20)
			s.Name = s.ID
			s.Harness = "codex"
			s.State = "live"
			s.Reason = "reachable daemon thread; terminal observation unavailable"
			s.Hints = map[string]string{"cwd": "/tmp/profile"}
		}
		sessions[i] = s
	}
	bin := filepath.Join(root, "bin")
	os.MkdirAll(bin, 0700)
	encoded, _ := json.Marshal(map[string]any{"result": map[string]any{"agents": agents}})
	os.WriteFile(filepath.Join(root, "agents.json"), encoded, 0600)
	os.WriteFile(filepath.Join(bin, "herdr"), []byte("#!/bin/sh\necho launch >> "+filepath.Join(root, "launches")+"\ncat "+filepath.Join(root, "agents.json")+"\n"), 0700)
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	err = b.Store.Update(context.Background(), func(d *store.Data) error {
		for _, s := range sessions {
			d.Sessions[s.ID] = s
		}
		now := time.Now().UTC()
		for i := 0; i < 300; i++ {
			id := fmt.Sprintf("retained-%d", i)
			s := sessions[i%100]
			k := store.Key("local", id)
			d.Messages[k] = &store.Message{ID: id, Origin: "local", SenderID: "sender", Sender: "sender", Recipient: s.ID, Destination: "local", State: "injected", Body: strings.Repeat("x", 1024), Sequence: d.NextSequence(), Created: now, Accepted: now, Updated: now, TerminalAt: now, Expires: now.Add(24 * time.Hour), Attempts: []store.Attempt{{ID: fmt.Sprintf("attempt-%d", i), Session: s, Started: now, Finished: now, Outcome: broker.Accepted, Positive: true}}, History: []store.History{{From: "queued", To: "claimed", Actor: "broker", At: now}, {From: "claimed", To: "injecting", Actor: "broker", At: now}, {From: "injecting", To: "injected", Actor: "broker", At: now}}}
			d.Dedupe[k] = store.Dedupe{Origin: "local", ID: id, Created: now, TerminalAt: now, State: "injected"}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	type peer struct {
		b *broker.Broker
		p broker.PrivatePeer
	}
	peers := []peer{}
	for _, name := range []string{"peer-a", "peer-b"} {
		home := filepath.Join(root, name)
		cfg := policy.Defaults()
		cfg.Machine = name
		os.MkdirAll(home, 0700)
		if err = policy.Write(filepath.Join(home, "config.json"), cfg, nil); err != nil {
			t.Fatal(err)
		}
		remote, err := broker.New(home, filepath.Join(home, "config.json"), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { remote.Close() })
		key := store.Random() + store.Random()
		on := true
		p := broker.PrivatePeer{Machine: name, Key: key, Mode: "push-poll", Path: "/tmp/profile-herd-msg", Home: home}
		if err = b.InstallPeer(context.Background(), p, &on, ""); err != nil {
			t.Fatal(err)
		}
		if err = remote.InstallPeer(context.Background(), broker.PrivatePeer{Machine: "local", Key: key, Mode: "poll", Path: "/tmp/profile-herd-msg", Home: b.Store.Home}, &on, ""); err != nil {
			t.Fatal(err)
		}
		if err = remote.Store.Update(context.Background(), func(d *store.Data) error {
			for i := 0; i < 30; i++ {
				s := store.Session{ID: fmt.Sprintf("%s-session-%d", name, i), Name: fmt.Sprintf("remote-%d", i), Machine: name, Harness: "codex", State: "live", Generation: store.Random(), LastSeen: time.Now().UTC()}
				d.Sessions[s.ID] = s
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		peers = append(peers, peer{remote, p})
	}
	server, err := b.Listen()
	if err != nil {
		t.Fatal(err)
	}
	serverCtx, serverStop := context.WithCancel(context.Background())
	defer serverStop()
	defer server.Close()
	go server.Serve(serverCtx)
	type receiverProcess struct {
		cmd   *exec.Cmd
		input io.WriteCloser
	}
	var processes []receiverProcess
	if realReceivers {
		exe, _ := os.Executable()
		for i, s := range sessions[:count] {
			socket, token := filepath.Join(root, fmt.Sprintf("parent%d.sock", i)), store.Random()
			t.Setenv("CLAUDE_CODE_MESSAGING_SOCKET", socket)
			t.Setenv("CLAUDE_CODE_MESSAGING_TOKEN", token)
			transcript := filepath.Join(root, s.ID+".jsonl")
			if err := os.WriteFile(transcript, nil, 0600); err != nil {
				t.Fatal(err)
			}
			if delivery {
				profileParent(t, socket, transcript, s.ID, root)
			}
			binding, _ := json.Marshal(receiver.Binding{ID: s.ID, Transcript: transcript, Cwd: root})
			if err := receiver.Hook(context.Background(), b.Store.Home, strings.NewReader(string(binding))); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(exe, "-test.run=^TestProfileReceiverClient$")
			cmd.Env = append(os.Environ(), "HERD_MSG_PROFILE_RECEIVER_CLIENT=1", "HERD_MSG_PROFILE_HOME="+b.Store.Home, "HERDR_PANE_ID="+s.Pane)
			cmd.Stderr = os.Stderr
			input, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = cmd.Start(); err != nil {
				t.Fatal(err)
			}
			processes = append(processes, receiverProcess{cmd, input})
		}
		defer func() {
			for _, p := range processes {
				p.input.Close()
				p.cmd.Wait()
			}
		}()
		limit := time.Now().Add(15 * time.Second)
		for {
			data, err := b.Store.ReadSessions(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ready := 0
			for _, s := range sessions[:count] {
				current := data.Sessions[s.ID]
				if current.Hints["instance"] != "profile" {
					call, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
					reply, e := receiver.Call(call, current.Hints["endpoint"], receiver.Control{Verb: "status"})
					stop()
					if e == nil && reply.Status.Generation == current.Generation {
						ready++
					}
				}
			}
			if ready == count {
				break
			}
			if time.Now().After(limit) {
				t.Fatal("receivers did not register", ready)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	// Establish steady availability before timing; cold registration churn is a
	// separate cost from the parent's steady-state observation.
	warm, stop := context.WithTimeout(context.Background(), 30*time.Second)
	discovered, err := cd.Discover(warm)
	if err == nil {
		err = b.Refresh(warm, discovered)
	}
	if err == nil {
		err = b.RefreshAvailability(warm)
	}
	stop()
	if err != nil {
		t.Fatal("warmup:", err)
	}
	c, cancel := context.WithTimeout(context.Background(), time.Duration(seconds)*time.Second)
	defer cancel()
	deadline, _ := c.Deadline()
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	var refreshes, peerCalls atomic.Int64
	run := func(interval time.Duration, work func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-c.Done():
					return
				case <-ticker.C:
					if time.Until(deadline) < 10*time.Millisecond {
						return
					}
					if err := work(c); err != nil && c.Err() == nil && time.Until(deadline) > 10*time.Millisecond {
						select {
						case errs <- err:
						default:
						}
					}
				}
			}
		}()
	}
	launchesAtStart, _ := os.ReadFile(filepath.Join(root, "launches"))
	var before, after syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	polls.Store(0)
	started := time.Now()
	t.Logf("measurement_start_unix_ns=%d", started.UnixNano())
	if !realReceivers {
		for _, s := range sessions[:count] {
			run(300*time.Millisecond, func(c context.Context) error {
				r := b.Handle(c, broker.Request{Version: broker.Protocol, Verb: "receiver-state", Caller: broker.Caller{ID: s.ID, Harness: "claude"}})
				if r.Code != 0 {
					return fmt.Errorf("poll: %s", r.Error)
				}
				return json.NewEncoder(io.Discard).Encode(r)
			})
		}
	}
	wg.Add(1)
	go func() { defer wg.Done(); b.RunTicks(c) }()
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.RunDiscovery(c, func(c context.Context) {
			if err := func(c context.Context) error {
				refreshes.Add(1)
				c = identity.CachedAgents(c)
				discovery, stop := context.WithTimeout(c, 2*time.Second)
				ss, err := cd.Discover(discovery)
				if err == nil {
					err = b.Refresh(discovery, ss)
				}
				stop()
				if err != nil {
					return err
				}
				availability, stop := context.WithTimeout(c, 2*time.Second)
				defer stop()
				return b.RefreshAvailability(availability)
			}(c); err != nil && c.Err() == nil {
				select {
				case errs <- err:
				default:
				}
			}
		})
	}()
	run(broker.WriteProbeInterval, b.WriteProbe)
	// Poll the same public-session, outbound, drain and observation paths as an idle
	// transport exchange. A one-second cadence is stricter than its 30-second idle poll.
	for _, peer := range peers {
		run(time.Second, func(c context.Context) error {
			if _, err := b.Outbound(c, peer.p.Machine); err != nil {
				return err
			}
			ss, err := b.PublicSessions(c)
			if err != nil {
				return err
			}
			reply, err := peer.b.PeerCall(c, "drain", broker.PeerRequest{Machine: "local", Key: peer.p.Key, Sessions: ss, Protocol: broker.Protocol})
			if err != nil {
				return err
			}
			peerCalls.Add(1)
			return b.ObservePeer(c, peer.p, reply.Sessions, "")
		})
	}
	var latencies []time.Duration
	delivered := map[string][]time.Duration{}
	n := 0
	if !idle {
		run(time.Second, func(c context.Context) error {
			n++
			start := time.Now()
			target := sessions[30+n%70].ID
			if delivery {
				if n%2 == 0 {
					target = "codex-0"
				} else {
					target = "claude-0"
				}
			}
			_, err := b.Send(c, broker.SendRequest{ID: fmt.Sprintf("load-%d", n), Created: time.Now().UTC(), To: target, Body: "latency probe"}, broker.Caller{ID: "operator"})
			if !delivery {
				latencies = append(latencies, time.Since(start))
			}
			if err != nil {
				return err
			}
			if delivery {
				timeout := time.NewTimer(3 * time.Second)
				defer timeout.Stop()
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				for {
					data, e := b.Store.Read(c)
					if e != nil {
						return e
					}
					m, e := data.Find(fmt.Sprintf("load-%d", n))
					if e != nil {
						return e
					}
					marker, err := os.ReadFile(filepath.Join(root, "inject-"+fmt.Sprintf("load-%d", n)))
					if err == nil && len(strings.Fields(string(marker))) > 0 {
						lines := strings.Fields(string(marker))
						if len(lines) != 1 {
							return fmt.Errorf("duplicate injection: %s", marker)
						}
						stamp, e := strconv.ParseInt(lines[0], 10, 64)
						if e != nil {
							return e
						}
						latency := time.Unix(0, stamp).Sub(start)
						latencies = append(latencies, latency)
						delivered[target] = append(delivered[target], latency)
						if latency > time.Second {
							return fmt.Errorf("input arrival took %s for %s", latency, target)
						}
						return nil
					}
					select {
					case <-c.Done():
						return c.Err()
					case <-timeout.C:
						return fmt.Errorf("delivery timed out: target=%s state=%s error=%s", target, m.State, m.Error)
					case <-ticker.C:
					}
				}
			}
			return b.Store.Update(c, func(d *store.Data) error {
				m, e := d.Find(fmt.Sprintf("load-%d", n))
				if e != nil {
					return e
				}
				return d.Move(m, "dead", "broker", "load probe complete", time.Now().UTC())
			})
		})
	}
	wg.Wait()
	elapsed := time.Since(started)
	syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	if delivery {
		settle, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		for {
			_ = b.Tick(settle)
			data, e := b.Store.Read(settle)
			if e != nil {
				t.Fatal(e)
			}
			waiting := 0
			for i := 1; i <= n; i++ {
				m, e := data.Find(fmt.Sprintf("load-%d", i))
				if e != nil {
					t.Fatal(e)
				}
				if m.State != "injected" {
					waiting++
				}
				marker, _ := os.ReadFile(filepath.Join(root, fmt.Sprintf("inject-load-%d", i)))
				if len(strings.Fields(string(marker))) > 1 {
					t.Fatal("duplicate input", i)
				}
			}
			if waiting == 0 {
				break
			}
			select {
			case <-settle.Done():
				t.Fatal("delivery did not settle", waiting)
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if realReceivers {
		var total, maximum float64
		for _, p := range processes {
			p.input.Close()
			if e := p.cmd.Wait(); e != nil {
				t.Error(e)
			}
			core := 100 * (p.cmd.ProcessState.UserTime() + p.cmd.ProcessState.SystemTime()).Seconds() / elapsed.Seconds()
			total += core
			if core > maximum {
				maximum = core
			}
		}
		t.Logf("receiver_core_mean=%.3f%% receiver_core_max=%.3f%% (includes startup)", total/float64(count), maximum)
		processes = nil
	}
	for target, samples := range delivered {
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		t.Logf("delivery_target=%s samples=%d p50=%s p95=%s", target, len(samples), samples[len(samples)/2], samples[(len(samples)-1)*95/100])
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if idle && realReceivers && polls.Load() > int64(count*(seconds/25+2)) {
		t.Errorf("idle receiver polling regressed: %d calls", polls.Load())
	}
	launches, _ := os.ReadFile(filepath.Join(root, "launches"))
	t.Logf("idle=%v herdr_launches=%d rate=%.3f/s", idle, (strings.Count(string(launches), "launch") - strings.Count(string(launchesAtStart), "launch")), float64((strings.Count(string(launches), "launch")-strings.Count(string(launchesAtStart), "launch")))/elapsed.Seconds())
	if len(latencies) == 0 {
		latencies = append(latencies, 0)
	}
	t.Logf("receivers=%d codex=9 sessions=100 retained=300 links=2 elapsed=%s cpu=%.3fs core=%.1f%% send_p50=%s send_p95=%s polls=%d refreshes=%d peer_calls=%d", count, elapsed, cpu(after)-cpu(before), 100*(cpu(after)-cpu(before))/elapsed.Seconds(), latencies[len(latencies)/2], latencies[(len(latencies)-1)*95/100], polls.Load(), refreshes.Load(), peerCalls.Load())
}

// Separate real receiver processes keep their CPU out of the broker profile.
func TestProfileReceiverClient(t *testing.T) {
	if os.Getenv("HERD_MSG_PROFILE_RECEIVER_CLIENT") != "1" {
		t.Skip("profiling helper process")
	}
	c, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { io.Copy(io.Discard, os.Stdin); cancel() }()
	if e := receiver.Run(c, os.Getenv("HERD_MSG_PROFILE_HOME"), &blockingReader{ctx: c}, io.Discard, os.Stderr); e != nil {
		t.Fatal(e)
	}
}

type blockingReader struct{ ctx context.Context }

func (r *blockingReader) Read(p []byte) (int, error) { <-r.ctx.Done(); return 0, io.EOF }

// profileParent accepts only synthetic receiver input and records the same peer
// transcript wrapper used by the real Claude harness.
func profileParent(t *testing.T, path, transcript, id, root string) {
	t.Helper()
	l, e := net.Listen("unix", path)
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	t.Cleanup(func() { l.Close(); wg.Wait() })
	go func() {
		for {
			conn, e := l.Accept()
			if e != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(2 * time.Second))
				dec := json.NewDecoder(conn)
				var auth map[string]any
				if dec.Decode(&auth) != nil {
					return
				}
				var q struct {
					Message struct {
						Content string `json:"content"`
					} `json:"message"`
				}
				if dec.Decode(&q) != nil {
					return
				}
				recordProfileInjection(root, q.Message.Content)
				f, e := os.OpenFile(transcript, os.O_WRONLY|os.O_APPEND, 0600)
				if e != nil {
					return
				}
				defer f.Close()
				json.NewEncoder(f).Encode(map[string]any{"type": "user", "sessionId": id, "origin": map[string]string{"kind": "peer"}, "message": map[string]string{"content": "Another Claude session sent a message:\n" + q.Message.Content}})
				f.Sync()
			}()
		}
	}()
}

func recordProfileInjection(root, body string) {
	_, rest, ok := strings.Cut(body, " | id ")
	if !ok {
		return
	}
	id, _, ok := strings.Cut(rest, " | attempt ")
	if !ok || !strings.HasPrefix(id, "load-") || strings.ContainsAny(id, "/\\") {
		return
	}
	f, e := os.OpenFile(filepath.Join(root, "inject-"+id), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		return
	}
	defer f.Close()
	fmt.Fprintln(f, time.Now().UnixNano())
}
