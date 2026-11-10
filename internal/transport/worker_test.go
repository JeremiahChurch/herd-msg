package transport

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func waitFor(t *testing.T, limit time.Duration, fn func() bool) {
	t.Helper()
	end := time.Now().Add(limit)
	for time.Now().Before(end) {
		if fn() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("timed out waiting for worker")
}
func startWorker(t *testing.T, n *nodes) {
	t.Helper()
	clearSignals(n.a)
	ctx, cancel := context.WithCancel(n.ctx)
	done := make(chan struct{})
	go func() { defer close(done); n.ma.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("worker did not stop")
		}
	})
}
func clearSignals(b *broker.Broker) {
	for {
		select {
		case <-b.TransportWake:
		case <-b.TransportReset:
		default:
			return
		}
	}
}
func TestWorkerRecoversAfterReadRepairWithoutLocalSend(t *testing.T) {
	n := rig(t, true)
	clearSignals(n.a)
	raw, e := os.ReadFile(n.a.ConfigPath)
	if e != nil {
		t.Fatal(e)
	}
	os.WriteFile(n.a.ConfigPath, []byte("broken"), 0600)
	startWorker(t, n)
	time.Sleep(1200 * time.Millisecond)
	os.WriteFile(n.a.ConfigPath, raw, 0600)
	id := n.send(n.b, "agent@node-a", "passive recovery")
	waitFor(t, 5*time.Second, func() bool { d, _ := n.a.Store.Read(n.ctx); _, e := d.Find(id); return e == nil })
}
func TestWorkerKillOffAndRelinkResetFailureDelay(t *testing.T) {
	n := rig(t, true)
	os.WriteFile(n.partition, nil, 0600)
	startWorker(t, n)
	waitFor(t, 3*time.Second, func() bool { d, _, e := n.a.Data(n.ctx); return e == nil && d.Links["node-b"].Error != "" })
	os.Remove(n.partition)
	if _, e := n.a.Kill(n.ctx, true, broker.Caller{ID: "operator"}); e != nil {
		t.Fatal(e)
	}
	id := n.send(n.b, "agent@node-a", "resume promptly")
	if _, e := n.a.Kill(n.ctx, false, broker.Caller{ID: "operator"}); e != nil {
		t.Fatal(e)
	}
	waitFor(t, 3*time.Second, func() bool { d, _ := n.a.Store.Read(n.ctx); _, e := d.Find(id); return e == nil })
	os.WriteFile(n.partition, nil, 0600)
	n.a.WakeTransport()
	waitFor(t, 3*time.Second, func() bool { d, _, e := n.a.Data(n.ctx); return e == nil && d.Links["node-b"].Error != "" })
	os.Remove(n.partition)
	on := true
	if e := n.a.InstallPeer(n.ctx, n.pa, &on, n.pa.Key); e != nil {
		t.Fatal(e)
	}
	id = n.send(n.b, "agent@node-a", "repair promptly")
	waitFor(t, 3*time.Second, func() bool { d, _ := n.a.Store.Read(n.ctx); _, e := d.Find(id); return e == nil })
}
func TestWorkerPollsAfterThirtySecondsAndLinksAreIndependent(t *testing.T) {
	n := rig(t, true)
	slow := n.pa
	slow.Machine = "node-c"
	slow.Target = "slow"
	slow.Key = store.Random() + store.Random()
	on := false
	if e := n.a.InstallPeer(n.ctx, slow, &on, ""); e != nil {
		t.Fatal(e)
	}
	wrapper := filepath.Join(n.root, "slow-ssh")
	script := "#!/bin/sh\ncase \"$*\" in *' slow '*) exec sleep 10;; esac\nexec " + Quote(n.ma.SSH.Binary) + " \"$@\"\n"
	os.WriteFile(wrapper, []byte(script), 0700)
	n.ma.SSH.Binary = wrapper
	id := n.send(n.a, "agent@node-b", "healthy link stays prompt")
	startWorker(t, n)
	waitFor(t, 3*time.Second, func() bool { return state(t, n.a, id) == states.Sent })
	// Queue return mail only after the initial exchange has finished. The sender's
	// wake channel is a different broker, so the local poll must use its timer.
	waitFor(t, 3*time.Second, func() bool { return n.a.Outstanding() == 1 })
	id = n.send(n.b, "agent@node-a", "periodic poll")
	start := time.Now()
	waitFor(t, 36*time.Second, func() bool { d, _ := n.a.Store.Read(n.ctx); _, e := d.Find(id); return e == nil })
	if time.Since(start) < 25*time.Second {
		t.Fatal("remote send unexpectedly bypassed ordinary poll interval")
	}
	_, c, e := n.a.Data(n.ctx)
	if e != nil || c.Links["node-b"] != (policy.Grant{AutoRun: true}) {
		t.Fatal(c, e)
	}
}

func TestSendWakeDuringExchangeSchedulesImmediateNextPass(t *testing.T) {
	n := rig(t, true)
	marker := filepath.Join(n.root, "draining")
	wrapper := filepath.Join(n.root, "delayed-ssh")
	script := "#!/bin/sh\ncase \"$*\" in *\"'drain'\"*) : > " + Quote(marker) + ";; esac\nsleep 0.4\nexec " + Quote(n.ma.SSH.Binary) + " \"$@\"\n"
	os.WriteFile(wrapper, []byte(script), 0700)
	n.ma.SSH.Binary = wrapper
	startWorker(t, n)
	waitFor(t, 3*time.Second, func() bool { _, e := os.Stat(marker); return e == nil })
	id := n.send(n.a, "agent@node-b", "wake after outbox snapshot")
	waitFor(t, 4*time.Second, func() bool { return state(t, n.a, id) == states.Sent })
}
func TestOperationBudgetUsesTimeoutRepair(t *testing.T) {
	if got := publicError(context.DeadlineExceeded); got != (&SSHError{Kind: "timeout"}).Error() {
		t.Fatal(got)
	}
}

func TestWakeDuringSuccessfulRetryDoesNotWaitForIdlePoll(t *testing.T) {
	n := rig(t, true)
	marker := filepath.Join(n.root, "retry-drain")
	wrapper := filepath.Join(n.root, "retry-ssh")
	script := "#!/bin/sh\ncase \"$*\" in *\"'drain'\"*) : > " + Quote(marker) + ";; esac\nsleep 0.4\nexec " + Quote(n.ma.SSH.Binary) + " \"$@\"\n"
	os.WriteFile(wrapper, []byte(script), 0700)
	n.ma.SSH.Binary = wrapper
	os.WriteFile(n.partition, nil, 0600)
	startWorker(t, n)
	waitFor(t, 3*time.Second, func() bool { d, _, e := n.a.Data(n.ctx); return e == nil && d.Links["node-b"].Error != "" })
	os.Remove(marker)
	os.Remove(n.partition)
	waitFor(t, 34*time.Second, func() bool { _, e := os.Stat(marker); return e == nil })
	id := n.send(n.a, "agent@node-b", "send while recovering drain is running")
	waitFor(t, 4*time.Second, func() bool { return state(t, n.a, id) == states.Sent })
}
