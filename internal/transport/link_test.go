package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/states"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func TestLinkNegotiationAndPassivePermissionBySavedLabel(t *testing.T) {
	n := rig(t, false)
	// Relink with a fresh key after local unlink, rather than sending an
	// established credential to a peer that has lost its matching record.
	count, e := n.a.UnlinkPeer(n.ctx, "node-b", false, nil, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = n.a.UnlinkPeer(n.ctx, "node-b", false, &count.Total, false); e != nil {
		t.Fatal(e)
	}
	if e = n.b.Store.Update(n.ctx, func(d *store.Data) error { delete(d.Links, "node-a"); return nil }); e != nil {
		t.Fatal(e)
	}
	if e = os.Remove(filepath.Join(n.b.Store.Home, "peers.json")); e != nil {
		t.Fatal(e)
	}
	raw, e := policy.Read(n.b.ConfigPath)
	if e != nil {
		t.Fatal(e)
	}
	delete(raw.Links, "node-a")
	if e = policy.Write(n.b.ConfigPath, raw, nil); e != nil {
		t.Fatal(e)
	}
	tools := filepath.Join(n.root, "tools")
	os.MkdirAll(tools, 0700)
	os.WriteFile(filepath.Join(tools, "herdr"), []byte("#!/bin/sh\nprintf '%s\\n' '[]'\n"), 0700)
	t.Setenv("PATH", tools+":"+os.Getenv("PATH"))
	value, e := n.ma.Command(n.ctx, "network-link", broker.PeerRequest{Args: []string{"Saved B"}})
	if e != nil {
		t.Fatal(e)
	}
	result := value.(LinkResult)
	if result.Machine != "node-b" || result.Mode != "push-poll" {
		t.Fatal(result)
	}
	os.WriteFile(n.partition, nil, 0600)
	value, e = n.mb.Command(n.ctx, "network-link", broker.PeerRequest{Args: []string{"node-a", "--auto-run"}})
	if e != nil {
		t.Fatal(e)
	}
	if !value.(LinkResult).AutoRun {
		t.Fatal(value)
	}
	if _, e = n.mb.Command(n.ctx, "network-link", broker.PeerRequest{Args: []string{"node-a", "--no-auto-run"}}); e != nil {
		t.Fatal(e)
	}
	_, c, e := n.b.Data(n.ctx)
	if e != nil || c.Links["node-a"].AutoRun {
		t.Fatal(c, e)
	}
	if _, e = n.mb.Command(n.ctx, "network-link", broker.PeerRequest{Args: []string{"unknown", "--no-auto-run"}}); e == nil {
		t.Fatal("unknown permission toggle started a link")
	}
}
func TestIncomingKeyReplacementAndRevokedRelinkResetPermission(t *testing.T) {
	n := rig(t, true)
	replacement := n.pb
	replacement.Key = store.Random() + store.Random()
	if e := n.b.InstallPeer(n.ctx, replacement, nil, ""); e != nil {
		t.Fatal(e)
	}
	_, c, e := n.b.Data(n.ctx)
	if e != nil || c.Links["node-a"].AutoRun {
		t.Fatal("unproven key inherited receiving authority", e)
	}
	if e = n.b.AutoRun(n.ctx, "node-a", true); e != nil {
		t.Fatal(e)
	}
	count, e := n.b.UnlinkPeer(n.ctx, "node-a", false, nil, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = n.b.UnlinkPeer(n.ctx, "node-a", false, &count.Total, false); e != nil {
		t.Fatal(e)
	}
	// Restore a stale public grant, as could happen after interruption or manual config.
	c.Links["node-a"] = policy.Grant{AutoRun: true}
	if e = policy.Write(n.b.ConfigPath, c, nil); e != nil {
		t.Fatal(e)
	}
	if e = n.b.InstallPeer(n.ctx, replacement, nil, replacement.Key); e != nil {
		t.Fatal(e)
	}
	_, c, e = n.b.Data(n.ctx)
	if e != nil || c.Links["node-a"].AutoRun {
		t.Fatal("revoked relink restored authority", e)
	}
}
func TestUnlinkKeepDropAndIdempotentRevocation(t *testing.T) {
	n := rig(t, true)
	id := n.send(n.a, "agent@node-b", "retain outbound")
	count, e := n.a.UnlinkPeer(n.ctx, "node-b", false, nil, false)
	if e != nil || count.Outbound != 1 {
		t.Fatal(count, e)
	}
	wrong := count.Total + 1
	if _, e = n.a.UnlinkPeer(n.ctx, "node-b", false, &wrong, false); e == nil {
		t.Fatal("stale count accepted")
	}
	if _, e = n.a.UnlinkPeer(n.ctx, "node-b", false, &count.Total, false); e != nil {
		t.Fatal(e)
	}
	if state(t, n.a, id) != states.Outbox {
		t.Fatal("keep dropped outgoing mail")
	}
	if e = n.ma.revoke(n.ctx, broker.PrivatePeer{Machine: n.pa.Machine, Target: n.pa.Target, Path: n.pa.Path, Home: n.pa.Home, Key: n.pa.Key, Revoked: true}); e != nil {
		t.Fatal(e)
	}
	if e = n.ma.revoke(n.ctx, n.pa); e != nil {
		t.Fatal("repeated revocation failed", e)
	}
	peers, e := n.a.PrivatePeers(n.ctx)
	if e != nil || !peers["node-b"].Received {
		t.Fatal(peers, e)
	}
	peers, e = n.b.PrivatePeers(n.ctx)
	if e != nil || !peers["node-a"].Received {
		t.Fatal(peers, e)
	}
	count, e = n.a.UnlinkPeer(n.ctx, "node-b", false, nil, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = n.a.UnlinkPeer(n.ctx, "node-b", true, &count.Total, false); e != nil {
		t.Fatal(e)
	}
	if state(t, n.a, id) != states.Dropped {
		t.Fatal("drop did not dispose retained mail")
	}
}
func TestPeerOperatorGuardsAndExactImportPin(t *testing.T) {
	n := rig(t, true)
	for _, verb := range []string{"peer-install", "network-link", "network-unlink", "network-reverse"} {
		r := n.a.Handle(n.ctx, broker.Request{Version: broker.Protocol, Verb: verb, Caller: broker.Caller{ID: "node-a-session", Harness: "codex"}, Peer: &broker.PeerRequest{}})
		if r.Code != 2 {
			t.Fatal(verb, r)
		}
	}
	wire := store.Message{ID: broker.NewID(time.Now()), Origin: "node-a", Sender: "agent.codex@node-a", SenderID: "node-a-session", Recipient: "agent", Destination: "node-b", Body: "exact IDs only", Created: time.Now()}
	reply, e := n.b.PeerCall(n.ctx, "accept", broker.PeerRequest{Protocol: broker.Protocol, Machine: "node-a", Key: n.pb.Key, Message: &wire})
	if e != nil || reply.Proof == nil || !reply.Proof.Refused {
		t.Fatal(reply, e)
	}
}
func TestKillCancelsTransferAndPreventsNextFrame(t *testing.T) {
	n := rig(t, true)
	transfer, done, e := n.a.BeginTransfer(n.ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer done()
	if n.a.Outstanding() != 1 {
		t.Fatal("transfer missing from in-flight count")
	}
	// Keep the transfer open while kill waits, then confirm its cancellation.
	finished := make(chan struct{})
	go func() { defer close(finished); n.a.Kill(n.ctx, true, broker.Caller{ID: "operator"}) }()
	select {
	case <-transfer.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("kill did not cancel SSH context")
	}
	done()
	<-finished
	if _, e = n.a.Outbound(n.ctx, "node-b"); e == nil {
		t.Fatal("next frame passed kill gate")
	}
	r, _ := n.mb.request(n.ctx, n.pb)
	if _, e = n.a.PeerCall(n.ctx, "drain", r); e == nil {
		t.Fatal("killed exporter allowed drain")
	}
}
func TestLateRefusalCannotUndoPositiveCommit(t *testing.T) {
	n := rig(t, true)
	id := n.send(n.a, "agent@node-b", "positive wins")
	d, _, _ := n.a.Data(n.ctx)
	message, _ := d.Find(id)
	proof := broker.TransferProof{ID: id, Hash: broker.WireHash(*message), Committed: true}
	if e := n.a.Settle(n.ctx, "node-b", proof); e != nil {
		t.Fatal(e)
	}
	proof.Committed = false
	proof.Refused = true
	proof.Reason = "stale refusal"
	if e := n.a.Settle(n.ctx, "node-b", proof); e != nil {
		t.Fatal(e)
	}
	if state(t, n.a, id) != states.Sent {
		t.Fatal("late refusal undid positive commit")
	}
}
func TestBackoffAndStrictQuotedRemoteCommand(t *testing.T) {
	if Backoff(0, &SSHError{Kind: "auth"}) != 5*time.Minute || Backoff(0, &SSHError{Kind: "connection"}) != 30*time.Second || Backoff(30*time.Second, errors.New("network")) != time.Minute {
		t.Fatal("incorrect backoff")
	}
	command, e := RemoteCommand("/tmp/space ' quote/bin", "/tmp/space ' home", "accept")
	if e != nil || !strings.HasPrefix(command, "'env' '-u'") {
		t.Fatal(command, e)
	}
	if _, e = RemoteCommand("~/bin", "", "accept"); e == nil {
		t.Fatal("relative remote path accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = (SSH{}).raw(ctx, "-oBad", "true", nil); e == nil {
		t.Fatal("option target accepted")
	}
}
func TestPolicyPatchCannotErasePrivateCredential(t *testing.T) {
	n := rig(t, true)
	_, c, e := n.a.Data(n.ctx)
	if e != nil {
		t.Fatal(e)
	}
	patch := map[string]json.RawMessage{"links": json.RawMessage(`{"node-b":{"auto_run":false}}`)}
	if _, e = n.a.Config(n.ctx, patch, broker.Caller{ID: "operator"}); e != nil {
		t.Fatal(e)
	}
	peers, e := n.a.PrivatePeers(n.ctx)
	if e != nil || peers["node-b"].Key != n.pa.Key {
		t.Fatal("config patch overwrote credentials", e)
	}
	_ = c
}

func TestEndpointPreservesHarnessCallerThroughBinary(t *testing.T) {
	n := rig(t, true)
	for _, harness := range []string{"CODEX_THREAD_ID", "CLAUDE_CODE_SESSION_ID"} {
		for _, verb := range []string{"peer-install", "peer-reverse"} {
			invented := broker.PrivatePeer{Machine: "invented", Path: binary(t), Key: store.Random() + store.Random()}
			on := true
			frame, _ := json.Marshal(broker.PeerRequest{Protocol: broker.Protocol, Peer: &invented, AutoRun: &on})
			cmd := exec.Command(binary(t), verb)
			cmd.Env = append(os.Environ(), "HERD_MSG_HOME="+n.a.Store.Home, harness+"=node-a-session")
			cmd.Stdin = bytes.NewReader(append(frame, '\n'))
			data, e := cmd.CombinedOutput()
			if e != nil {
				t.Fatal(e, string(data))
			}
			var response broker.Response
			if json.Unmarshal(data, &response) != nil || response.Code != 2 {
				t.Fatal("endpoint discarded agent identity", verb, string(data))
			}
		}
	}
	peers, e := n.a.PrivatePeers(n.ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, exists := peers["invented"]; exists {
		t.Fatal("agent endpoint installed receiving authority")
	}
}
func TestSSHFailureClassesAndCancellation(t *testing.T) {
	n := rig(t, true)
	for _, tc := range []struct{ stderr, kind string }{{"Host key verification failed.", "host-key"}, {"Permission denied (publickey).", "auth"}, {"binary: not found", "install"}, {"connection refused", "connection"}} {
		path := filepath.Join(n.root, "error-ssh")
		os.WriteFile(path, []byte("#!/bin/sh\nprintf '%s\\n' "+Quote(tc.stderr)+" >&2\nexit 255\n"), 0700)
		_, e := (SSH{Binary: path}).raw(n.ctx, "fake", "true", nil)
		var se *SSHError
		if !errors.As(e, &se) || se.Kind != tc.kind {
			t.Fatal(tc, e)
		}
	}
	path := filepath.Join(n.root, "stall-ssh")
	os.WriteFile(path, []byte("#!/bin/sh\nexec sleep 10\n"), 0700)
	ctx, cancel := context.WithCancel(n.ctx)
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	_, e := (SSH{Binary: path}).raw(ctx, "fake", "true", nil)
	if !errors.Is(e, context.Canceled) {
		t.Fatal("operator cancellation classified as SSH failure", e)
	}
	if Backoff(0, &SSHError{Kind: "timeout"}) != 30*time.Second {
		t.Fatal("connectivity timeout jumped to auth backoff")
	}
}
func TestPublicCLIOutputsAndProtocolMismatch(t *testing.T) {
	n := rig(t, true)
	for _, verb := range []string{"config", "status", "peers"} {
		cmd := exec.Command(binary(t), verb, "--json")
		cmd.Env = append(os.Environ(), "HERD_MSG_HOME="+n.a.Store.Home)
		data, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatal(verb, e, string(data))
		}
		for _, secret := range []string{n.pa.Key, n.pa.Target, n.pa.Home} {
			if bytes.Contains(data, []byte(secret)) {
				t.Fatal("private credential or route leaked", verb)
			}
		}
	}
	r, _ := n.ma.request(n.ctx, n.pa)
	r.Protocol = broker.Protocol + 1
	if _, e := n.b.PeerCall(n.ctx, "probe", r); e == nil || !strings.Contains(e.Error(), "protocol mismatch") {
		t.Fatal(e)
	}
}
func TestCrossMachineAcceptedNameStaysPinnedAfterRename(t *testing.T) {
	n := rig(t, true)
	id := n.send(n.a, "agent@node-b", "pinned before rename")
	if e := n.b.Store.Update(n.ctx, func(d *store.Data) error {
		s := d.Sessions["node-b-session"]
		s.Name = "renamed"
		d.Sessions[s.ID] = s
		d.Sessions["replacement"] = store.Session{ID: "replacement", Name: "agent", Machine: "node-b", Harness: "claude", State: "live"}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e := n.ma.Exchange(n.ctx, n.pa); e != nil {
		t.Fatal(e)
	}
	d, _, _ := n.b.Data(n.ctx)
	m, _ := d.Find(id)
	if m.Recipient != "node-b-session" {
		t.Fatal("accepted mail followed a name", m)
	}
}

func TestRelinkBetweenImportAuthenticationChecksKeepsOutbox(t *testing.T) {
	n := rig(t, true)
	id := n.send(n.a, "agent@node-b", "uncertain authentication race")
	changed := false
	n.b.Fault = func(point string) {
		if point == "after_peer_authentication" && !changed {
			changed = true
			p := n.pb
			p.Key = store.Random() + store.Random()
			if e := n.b.InstallPeer(n.ctx, p, nil, ""); e != nil {
				t.Fatal(e)
			}
		}
	}
	if e := n.ma.Exchange(n.ctx, n.pa); e == nil {
		t.Fatal("relink race unexpectedly returned a receipt")
	}
	if state(t, n.a, id) != states.Outbox {
		t.Fatal("authentication race became permanent message refusal")
	}
}

func TestLongValidSenderNameCrossesMachineBoundary(t *testing.T) {
	n := rig(t, true)
	if e := n.a.Store.Update(n.ctx, func(d *store.Data) error {
		s := d.Sessions["node-a-session"]
		s.Name = strings.Repeat("x", 250)
		d.Sessions[s.ID] = s
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	id := broker.NewID(time.Now())
	if _, e := n.a.Send(n.ctx, broker.SendRequest{ID: id, Created: time.Now(), To: "agent@node-b", Body: "long sender label"}, broker.Caller{ID: "node-a-session", Harness: "codex"}); e != nil {
		t.Fatal(e)
	}
	if e := n.ma.Exchange(n.ctx, n.pa); e != nil {
		t.Fatal(e)
	}
	if state(t, n.a, id) != states.Sent || state(t, n.b, id) != states.Queued {
		t.Fatal("valid composed sender label was refused")
	}
	max := strings.Repeat("n", 256) + ".claude@" + strings.Repeat("m", 256)
	if e := policy.SenderLabel(max); e != nil {
		t.Fatal(e)
	}
	if e := policy.SenderLabel(max + "x"); e == nil {
		t.Fatal("unbounded sender label accepted")
	}
	if e := policy.SenderLabel("name|other"); e == nil {
		t.Fatal("delimiter validation weakened")
	}
}
func TestReverseBudgetPreservesEarlierRouteFailure(t *testing.T) {
	n := rig(t, true)
	tools := filepath.Join(n.root, "route-tools")
	os.MkdirAll(tools, 0700)
	os.WriteFile(filepath.Join(tools, "herdr"), []byte("#!/bin/sh\nprintf '%s\\n' '[{\"label\":\"First\",\"target\":\"first\",\"enabled\":true},{\"label\":\"Last\",\"target\":\"last\",\"enabled\":true}]'\n"), 0700)
	t.Setenv("PATH", tools+":"+os.Getenv("PATH"))
	wrapper := filepath.Join(n.root, "reverse-ssh")
	script := "#!/bin/sh\ncase \"$*\" in *' last '*) exec sleep 3;; esac\necho 'connection refused' >&2\nexit 255\n"
	os.WriteFile(wrapper, []byte(script), 0700)
	n.ma.SSH.Binary = wrapper
	r, _ := n.mb.request(n.ctx, n.pb)
	ctx, cancel := context.WithTimeout(n.ctx, 400*time.Millisecond)
	defer cancel()
	value, e := n.ma.reverse(ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	detail := value.(LinkResult).ReverseError
	if !strings.Contains(detail, "SSH connection failed") || strings.Contains(detail, "deadline exceeded") {
		t.Fatal(detail)
	}
}

func TestDroppedBatchMessageIsNotSentAfterEarlierSlowTransfer(t *testing.T) {
	n := rig(t, true)
	first := n.send(n.a, "agent@node-b", "first")
	later := n.send(n.a, "agent@node-b", "drop before its transfer")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	n.b.Fault = func(name string) {
		if name == "before_rpc_peer-accept" {
			once.Do(func() { close(entered); <-release })
		}
	}
	result := make(chan error, 1)
	go func() { result <- n.ma.Exchange(n.ctx, n.pa) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("first transfer never blocked")
	}
	e := n.a.Resolve(n.ctx, later, "drop", false, "", broker.Caller{ID: "operator"})
	close(release)
	if e != nil {
		t.Fatal(e)
	}
	if e = <-result; e != nil {
		t.Fatal(e)
	}
	if state(t, n.a, first) != states.Sent || state(t, n.a, later) != states.Dropped {
		t.Fatal("batch states changed")
	}
	d, e := n.b.Store.Read(n.ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = d.Find(later); e == nil {
		t.Fatal("dropped message was imported")
	}
}

func TestPolledRowsRecheckDropAndKillBetweenAcknowledgements(t *testing.T) {
	for _, action := range []string{"drop", "kill"} {
		t.Run(action, func(t *testing.T) {
			n := rig(t, true)
			n.send(n.b, "agent@node-a", "first")
			later := n.send(n.b, "agent@node-a", "later")
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			n.b.Fault = func(name string) {
				if name == "before_rpc_peer-ack" {
					once.Do(func() { close(entered); <-release })
				}
			}
			result := make(chan error, 1)
			go func() { result <- n.ma.Exchange(n.ctx, n.pa) }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				close(release)
				t.Fatal("first poll ack never blocked")
			}
			var e error
			if action == "drop" {
				e = n.b.Resolve(n.ctx, later, "drop", false, "", broker.Caller{ID: "operator"})
			} else {
				_, e = n.b.Kill(n.ctx, true, broker.Caller{ID: "operator"})
			}
			close(release)
			if e != nil {
				t.Fatal(e)
			}
			exchangeErr := <-result
			if action == "drop" && exchangeErr != nil {
				t.Fatal(exchangeErr)
			}
			if action == "kill" && exchangeErr == nil {
				t.Fatal("next drain ignored remote kill")
			}
			d, e := n.a.Store.Read(n.ctx)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = d.Find(later); e == nil {
				t.Fatal("cancelled later poll row was imported")
			}
		})
	}
}
