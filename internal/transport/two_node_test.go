package transport

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/identity"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

var binaryOnce sync.Once
var testBinary string
var buildError error

func binary(t *testing.T) string {
	t.Helper()
	binaryOnce.Do(func() {
		dir, e := os.MkdirTemp("", "hm-wire-binary-")
		if e != nil {
			buildError = e
			return
		}
		testBinary = filepath.Join(dir, "herd-msg")
		cmd := exec.Command(filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-o", testBinary, "../../cmd/herd-msg")
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		out, e := cmd.CombinedOutput()
		if e != nil {
			buildError = &buildFailure{e, string(out)}
		}
	})
	if buildError != nil {
		t.Fatal(buildError)
	}
	return testBinary
}

type buildFailure struct {
	err    error
	output string
}

func (e *buildFailure) Error() string { return e.err.Error() + e.output }
func TestMain(m *testing.M) {
	code := m.Run()
	if testBinary != "" {
		os.RemoveAll(filepath.Dir(testBinary))
	}
	os.Exit(code)
}

type nodes struct {
	t                     *testing.T
	ctx                   context.Context
	a, b                  *broker.Broker
	ma, mb                *Manager
	pa, pb                broker.PrivatePeer
	root, partition, loss string
}

func rig(t *testing.T, auto bool) *nodes {
	t.Helper()
	root := t.TempDir()
	runtimeDir, e := os.MkdirTemp("", "hmrt-")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { os.RemoveAll(runtimeDir) })
	t.Setenv("XDG_RUNTIME_DIR", runtimeDir)
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	n := &nodes{t: t, ctx: context.Background(), root: root, partition: filepath.Join(root, "partition"), loss: filepath.Join(root, "loss")}
	for _, machine := range []string{"node-a", "node-b"} {
		home := filepath.Join(root, machine+" space ' quote")
		if e := os.MkdirAll(home, 0700); e != nil {
			t.Fatal(e)
		}
		c := policy.Defaults()
		c.Machine = machine
		if e := policy.Write(filepath.Join(home, "config.json"), c, nil); e != nil {
			t.Fatal(e)
		}
		b, e := broker.New(home, filepath.Join(home, "config.json"), nil)
		if e != nil {
			t.Fatal(e)
		}
		server, e := b.Listen()
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithCancel(n.ctx)
		go server.Serve(ctx)
		t.Cleanup(func() { cancel(); server.Close(); b.Close() })
		if e = b.Register(n.ctx, store.Session{ID: machine + "-session", Name: "agent", Harness: "codex", State: "live"}); e != nil {
			t.Fatal(e)
		}
		if machine == "node-a" {
			n.a = b
		} else {
			n.b = b
		}
	}
	fake := filepath.Join(root, "ssh")
	script := `#!/bin/sh
set -eu
while [ "$1" = '-o' ]; do
 case "$2" in BatchMode=yes|StrictHostKeyChecking=yes|ConnectTimeout=8|ServerAliveInterval=5|ServerAliveCountMax=1) ;; *) exit 91;; esac
 shift 2
done
[ "$1" = '--' ] && shift
target="$1";shift
if [ -e ` + Quote(n.partition) + ` ]; then echo 'connection refused' >&2; exit 255; fi
unset CODEX_THREAD_ID CLAUDE_CODE_SESSION_ID
if [ -e ` + Quote(n.loss) + ` ] && echo "$*" | /bin/grep -q "'accept'"; then
 /bin/sh -c "$*" >/dev/null
 echo 'connection lost' >&2
 exit 255
fi
exec /bin/sh -c "$*"
`
	if e := os.WriteFile(fake, []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	key := store.Random() + store.Random()
	n.pa = broker.PrivatePeer{Machine: "node-b", Label: "Saved B", Target: "fake-b", Path: binary(t), Home: n.b.Store.Home, Key: key, Mode: "push-poll"}
	n.pb = broker.PrivatePeer{Machine: "node-a", Label: "Saved A", Target: "fake-a", Path: binary(t), Home: n.a.Store.Home, Key: key, Mode: "push-poll"}
	on := auto
	if e := n.a.InstallPeer(n.ctx, n.pa, &on, ""); e != nil {
		t.Fatal(e)
	}
	if e := n.b.InstallPeer(n.ctx, n.pb, &on, ""); e != nil {
		t.Fatal(e)
	}
	n.ma = &Manager{Broker: n.a, SSH: SSH{Binary: fake}}
	n.mb = &Manager{Broker: n.b, SSH: SSH{Binary: fake}}
	n.a.Network = n.ma.Command
	n.b.Network = n.mb.Command
	for _, x := range []struct {
		local, remote *broker.Broker
		machine       string
	}{{n.a, n.b, "node-b"}, {n.b, n.a, "node-a"}} {
		sessions, e := x.remote.PublicSessions(n.ctx)
		if e != nil {
			t.Fatal(e)
		}
		if e = x.local.CachePeer(n.ctx, x.machine, sessions, ""); e != nil {
			t.Fatal(e)
		}
	}
	return n
}
func (n *nodes) send(b *broker.Broker, to, body string) string {
	n.t.Helper()
	id := broker.NewID(time.Now())
	if _, e := b.Send(n.ctx, broker.SendRequest{ID: id, Created: time.Now(), To: to, Body: body}, broker.Caller{ID: "operator"}); e != nil {
		n.t.Fatal(e)
	}
	return id
}
func state(t *testing.T, b *broker.Broker, id string) states.State {
	t.Helper()
	d, e := b.Store.Read(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	m, e := d.Find(id)
	if e != nil {
		t.Fatal(e)
	}
	return m.State
}
func TestTwoWayTransfersAndReceivingPolicy(t *testing.T) {
	for _, auto := range []bool{false, true} {
		t.Run(map[bool]string{false: "held", true: "auto-run"}[auto], func(t *testing.T) {
			n := rig(t, auto)
			a := n.send(n.a, "agent@node-b", "forward")
			b := n.send(n.b, "agent@node-a", "reverse")
			if e := n.ma.Exchange(n.ctx, n.pa); e != nil {
				t.Fatal(e)
			}
			want := states.Held
			if auto {
				want = states.Queued
			}
			if state(t, n.a, a) != states.Sent || state(t, n.b, b) != states.Sent || state(t, n.b, a) != want || state(t, n.a, b) != want {
				t.Fatal("durable transfer or receiving policy failed")
			}
			if e := n.mb.Exchange(n.ctx, n.pb); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestOneWayPollLearnsPeersAndRepliesWithoutCache(t *testing.T) {
	n := rig(t, true)
	if e := n.b.Store.Update(n.ctx, func(d *store.Data) error { l := d.Links["node-a"]; l.Sessions = nil; d.Links["node-a"] = l; return nil }); e != nil {
		t.Fatal(e)
	}
	if e := n.ma.Exchange(n.ctx, n.pa); e != nil {
		t.Fatal(e)
	}
	d, c, e := n.b.Data(n.ctx)
	if e != nil {
		t.Fatal(e)
	}
	peers := identity.Peers(d, c, time.Now(), false)
	seen := false
	for _, p := range peers {
		if p.Machine == "node-a" {
			seen = true
		}
	}
	if !seen {
		t.Fatal("passive node did not learn caller sessions")
	}
	id := n.send(n.b, "agent@node-a", "one-way return")
	if e = n.ma.Exchange(n.ctx, n.pa); e != nil {
		t.Fatal(e)
	}
	if state(t, n.a, id) != states.Queued {
		t.Fatal("poll return missing")
	}
	// A verified parent permits replies even when the sender's cache entry is absent.
	parent := broker.NewID(time.Now())
	if _, e = n.a.Send(n.ctx, broker.SendRequest{ID: parent, Created: time.Now(), To: "agent@node-b", Body: "reply parent"}, broker.Caller{ID: "node-a-session", Harness: "codex"}); e != nil {
		t.Fatal(e)
	}
	if e = n.ma.Exchange(n.ctx, n.pa); e != nil {
		t.Fatal(e)
	}
	n.b.Store.Update(n.ctx, func(d *store.Data) error { l := d.Links["node-a"]; l.Sessions = nil; d.Links["node-a"] = l; return nil })
	reply := broker.NewID(time.Now())
	if _, e = n.b.Send(n.ctx, broker.SendRequest{ID: reply, Created: time.Now(), To: "node-a-session@node-a", ParentID: parent, Body: "reply"}, broker.Caller{ID: "node-b-session", Harness: "codex"}); e != nil {
		t.Fatal(e)
	}
}
func TestPartitionHealAndLostCommitReply(t *testing.T) {
	n := rig(t, true)
	id := n.send(n.a, "agent@node-b", "retry once")
	os.WriteFile(n.partition, nil, 0600)
	if e := n.ma.Exchange(n.ctx, n.pa); e == nil {
		t.Fatal("partition succeeded")
	}
	if state(t, n.a, id) != states.Outbox {
		t.Fatal("partition lost outbox")
	}
	os.Remove(n.partition)
	os.WriteFile(n.loss, nil, 0600)
	if e := n.ma.Exchange(n.ctx, n.pa); e == nil {
		t.Fatal("lost reply succeeded")
	}
	if state(t, n.a, id) != states.Outbox || state(t, n.b, id) != states.Queued {
		t.Fatal("commit ordering violated")
	}
	os.Remove(n.loss)
	if e := n.ma.Exchange(n.ctx, n.pa); e != nil {
		t.Fatal(e)
	}
	if state(t, n.a, id) != states.Sent {
		t.Fatal("retry did not settle")
	}
	d, _ := n.b.Store.Read(n.ctx)
	if len(d.Messages) != 1 {
		t.Fatal("duplicate mailbox insertion")
	}
}
func TestPollCrashAfterCommitBeforeAck(t *testing.T) {
	n := rig(t, true)
	id := n.send(n.b, "agent@node-a", "poll durable")
	r, _ := n.ma.request(n.ctx, n.pa)
	reply, e := n.b.PeerCall(n.ctx, "drain", r)
	if e != nil || len(reply.Messages) != 1 {
		t.Fatal(reply, e)
	}
	message := reply.Messages[0]
	incoming, e := n.a.PeerCall(n.ctx, "accept", broker.PeerRequest{Protocol: broker.Protocol, Machine: "node-b", Key: n.pa.Key, Sessions: reply.Sessions, Message: &message})
	if e != nil || !incoming.Proof.Committed {
		t.Fatal(e)
	}
	if state(t, n.b, id) != states.Outbox {
		t.Fatal("drain deleted before ack")
	}
	// A replacement poller has no in-memory state from the import; it redrains durable data.
	replacement := &Manager{Broker: n.a, SSH: n.ma.SSH}
	if e = replacement.Exchange(n.ctx, n.pa); e != nil {
		t.Fatal(e)
	}
	if state(t, n.b, id) != states.Sent {
		t.Fatal("replacement poll did not ack")
	}
	d, _ := n.a.Store.Read(n.ctx)
	if len(d.Messages) != 1 {
		t.Fatal("repeated poll inserted duplicate")
	}
}
func TestUnlinkedAndEmptyKeyRefused(t *testing.T) {
	n := rig(t, true)
	for _, key := range []string{"", "wrong", strings.Repeat("0", 64)} {
		if _, e := n.b.PeerCall(n.ctx, "probe", broker.PeerRequest{Protocol: broker.Protocol, Machine: "node-a", Key: key}); e == nil {
			t.Fatal("invalid key accepted")
		}
	}
	count, e := n.b.UnlinkPeer(n.ctx, "node-a", false, nil, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = n.b.UnlinkPeer(n.ctx, "node-a", false, &count.Total, false); e != nil {
		t.Fatal(e)
	}
	r, _ := n.ma.request(n.ctx, n.pa)
	reply, e := n.b.PeerCall(n.ctx, "probe", r)
	if e != nil || !reply.Revoked {
		t.Fatal(reply, e)
	}
	r.Key = ""
	if _, e = n.b.PeerCall(n.ctx, "probe", r); e == nil {
		t.Fatal("unauthenticated revocation revealed")
	}
}
func TestPermanentAndTemporaryRefusalsOnPushAndPoll(t *testing.T) {
	for _, poll := range []bool{false, true} {
		t.Run(map[bool]string{false: "push", true: "poll"}[poll], func(t *testing.T) {
			n := rig(t, true)
			sender, receiver, manager, p := n.a, n.b, n.ma, n.pa
			if poll {
				sender, receiver = n.b, n.a
			}
			id := n.send(sender, "agent@"+map[bool]string{false: "node-b", true: "node-a"}[poll], "refused")
			if e := receiver.Store.Update(n.ctx, func(d *store.Data) error {
				for id, s := range d.Sessions {
					s.Tombstone = true
					d.Sessions[id] = s
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			if e := manager.Exchange(n.ctx, p); e != nil {
				t.Fatal(e)
			}
			if state(t, sender, id) != states.Dead {
				t.Fatal("permanent refusal not returned")
			}
			if e := receiver.Store.Update(n.ctx, func(d *store.Data) error {
				for id, s := range d.Sessions {
					s.Tombstone = false
					d.Sessions[id] = s
				}
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			if e := manager.Exchange(n.ctx, p); e != nil {
				t.Fatal(e)
			}
			id = n.send(sender, "agent@"+map[bool]string{false: "node-b", true: "node-a"}[poll], "temporary")
			if _, e := receiver.Kill(n.ctx, true, broker.Caller{ID: "operator"}); e != nil {
				t.Fatal(e)
			}
			e := manager.Exchange(n.ctx, p)
			if poll && e == nil {
				t.Fatal("killed local worker did not stop")
			}
			if state(t, sender, id) != states.Outbox {
				t.Fatal("temporary refusal killed mail")
			}
		})
	}
}
func TestSecretsAbsentFromPublicOutputs(t *testing.T) {
	n := rig(t, true)
	d, c, e := n.a.Data(n.ctx)
	if e != nil {
		t.Fatal(e)
	}
	snapshot, _ := json.Marshal(broker.Snapshot{Data: d, Config: c})
	if strings.Contains(string(snapshot), n.pa.Key) || strings.Contains(string(snapshot), n.pa.Home) || strings.Contains(string(snapshot), n.pa.Target) {
		t.Fatal("private credential or route leaked")
	}
	info, e := os.Stat(filepath.Join(n.a.Store.Home, "peers.json"))
	if e != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, e)
	}
}

func TestPollCommitCrashProcess(t *testing.T) {
	if os.Getenv("HERD_MSG_POLL_CRASH_HELPER") != "1" {
		return
	}
	home := os.Getenv("HERD_MSG_CRASH_HOME")
	b, e := broker.New(home, filepath.Join(home, "config.json"), nil)
	if e != nil {
		os.Exit(81)
	}
	peers, e := b.PrivatePeers(context.Background())
	if e != nil {
		os.Exit(82)
	}
	m := &Manager{Broker: b, SSH: SSH{Binary: os.Getenv("HERD_MSG_CRASH_SSH")}, Fault: func(point string) {
		if point == "after_poll_commit" {
			os.Exit(73)
		}
	}}
	if e = m.Exchange(context.Background(), peers["node-b"]); e != nil {
		os.Exit(83)
	}
	os.Exit(84)
}
func TestActualProcessDeathBetweenCommitAndAck(t *testing.T) {
	n := rig(t, true)
	id := n.send(n.b, "agent@node-a", "survive process death")
	// The child owns a third isolated mailbox with the same receiving identity.
	home := filepath.Join(n.root, "crash-home")
	os.MkdirAll(home, 0700)
	c := policy.Defaults()
	c.Machine = "node-a"
	policy.Write(filepath.Join(home, "config.json"), c, nil)
	b, e := broker.New(home, filepath.Join(home, "config.json"), nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = b.Register(n.ctx, store.Session{ID: "node-a-session", Name: "agent", Harness: "codex", State: "live"}); e != nil {
		t.Fatal(e)
	}
	on := true
	if e = b.InstallPeer(n.ctx, n.pa, &on, ""); e != nil {
		t.Fatal(e)
	}
	b.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPollCommitCrashProcess$")
	cmd.Env = append(os.Environ(), "HERD_MSG_POLL_CRASH_HELPER=1", "HERD_MSG_CRASH_HOME="+home, "HERD_MSG_CRASH_SSH="+n.ma.SSH.Binary)
	output, e := cmd.CombinedOutput()
	exit, ok := e.(*exec.ExitError)
	if !ok || exit.ExitCode() != 73 {
		t.Fatal("poller did not die at committed boundary", e, string(output))
	}
	if state(t, n.b, id) != states.Outbox {
		t.Fatal("crashed poller acknowledged uncommitted state")
	}
	reopened, e := broker.New(home, filepath.Join(home, "config.json"), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	if state(t, reopened, id) != states.Queued {
		t.Fatal("committed import lost in process death")
	}
	replacement := &Manager{Broker: reopened, SSH: n.ma.SSH}
	if e = replacement.Exchange(n.ctx, n.pa); e != nil {
		t.Fatal(e)
	}
	if state(t, n.b, id) != states.Sent {
		t.Fatal("durable replacement did not settle")
	}
	d, _ := reopened.Store.Read(n.ctx)
	if len(d.Messages) != 1 {
		t.Fatal("replacement inserted duplicate")
	}
}
