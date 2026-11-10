package transport

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func savedRoutes(t *testing.T, n *nodes, targets ...string) {
	t.Helper()
	machines := []map[string]any{}
	for _, target := range targets {
		machines = append(machines, map[string]any{"label": target, "target": target, "enabled": true})
	}
	data, e := json.Marshal(machines)
	if e != nil {
		t.Fatal(e)
	}
	dir := filepath.Join(n.root, "saved-routes")
	if e = os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(dir, "herdr"), []byte("#!/bin/sh\nprintf '%s\\n' "+Quote(string(data))+"\n"), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
}

func TestOperatorChangesDuringRouteProofPreventPayloadSend(t *testing.T) {
	for _, action := range []string{"drop", "unlink", "relink"} {
		t.Run(action, func(t *testing.T) {
			n := rig(t, true)
			id := n.send(n.a, "agent@node-b", "do not send after operator change")
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			n.b.Fault = func(point string) {
				if point == "before_rpc_peer-route-proof" {
					once.Do(func() { close(entered); <-release })
				}
			}
			result := make(chan error, 1)
			go func() { result <- n.ma.Exchange(n.ctx, n.pa) }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				close(release)
				t.Fatal("route proof never started")
			}
			var e error
			switch action {
			case "drop":
				e = n.a.Resolve(n.ctx, id, "drop", false, "", broker.Caller{ID: "operator"})
			case "unlink":
				var count broker.UnlinkCount
				count, e = n.a.UnlinkPeer(n.ctx, "node-b", false, nil, false)
				if e == nil {
					_, e = n.a.UnlinkPeer(n.ctx, "node-b", false, &count.Total, false)
				}
			case "relink":
				p := n.pa
				p.Key = store.Random() + store.Random()
				e = n.a.InstallPeer(n.ctx, p, nil, n.pa.Key)
			}
			close(release)
			if e != nil {
				t.Fatal(e)
			}
			e = <-result
			if action == "drop" && e != nil || action != "drop" && e == nil {
				t.Fatal("unexpected exchange result", e)
			}
			data, e := n.b.Store.Read(n.ctx)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = data.Find(id); e == nil {
				t.Fatal("payload sent after operator change during proof")
			}
		})
	}
}

// Capture every argument and input byte delivered to an unproven SSH route.
// The trusted route still uses the rig's real endpoint and disposable broker.
func unprovenSSH(t *testing.T, n *nodes, reply string) string {
	t.Helper()
	log := filepath.Join(n.root, "unproven-input")
	path := filepath.Join(n.root, "proof-ssh")
	script := `#!/bin/sh
set -eu
for arg do
 if [ "$arg" = 'unproven' ]; then
  printf '%s\n' "$@" >> ` + Quote(log) + `
  cat >> ` + Quote(log) + `
  case "$*" in *"'peer-info'")
   printf '%s\n' '{"data":{"protocol":1,"machine":"node-b"}}'
   exit 0;;
  esac
  printf '%s\n' ` + Quote(reply) + `
  exit 0
 fi
done
exec ` + Quote(n.mb.SSH.Binary) + ` "$@"
`
	if e := os.WriteFile(path, []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	n.ma.SSH.Binary = path
	n.mb.SSH.Binary = path
	return log
}

func assertNoCredential(t *testing.T, n *nodes, log string) {
	t.Helper()
	data, e := os.ReadFile(log)
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(data, []byte(n.pa.Key)) || bytes.Contains(data, []byte(n.pb.Key)) {
		t.Fatal("unproven route received a link credential")
	}
	// Only public information requests and challenges are permitted. No
	// sessions, install offers, messages, or earlier proof may accompany it.
	nonces := map[string]bool{}
	for _, line := range bytes.Split(data, []byte("\n")) {
		if len(line) == 0 || line[0] != '{' {
			continue
		}
		var r broker.PeerRequest
		if e = json.Unmarshal(line, &r); e != nil {
			t.Fatal(e)
		}
		if r.Key != "" || r.OldKey != "" || r.Peer != nil || r.Sessions != nil || r.Message != nil || r.Proof != nil {
			t.Fatal("unproven route received more than a challenge")
		}
		if r.Nonce != "" {
			if len(r.Nonce) != 64 || nonces[r.Nonce] {
				t.Fatal("route challenge did not use a fresh 32-byte nonce")
			}
			nonces[r.Nonce] = true
		}
	}
}

func TestRelinkDoesNotSendExistingKeyToUnprovenRoute(t *testing.T) {
	for _, sameTarget := range []bool{false, true} {
		t.Run(map[bool]string{false: "new alias", true: "same target"}[sameTarget], func(t *testing.T) {
			n := rig(t, true)
			log := unprovenSSH(t, n, `{"data":{"machine":"node-b","mac":"`+strings.Repeat("00", 32)+`"}}`)
			if sameTarget {
				p := n.pa
				p.Target = "unproven"
				if e := n.a.InstallPeer(n.ctx, p, nil, p.Key); e != nil {
					t.Fatal(e)
				}
			}
			savedRoutes(t, n, "unproven")
			_, e := n.ma.Command(n.ctx, "network-link", broker.PeerRequest{Args: []string{"unproven", "--remote-path", binary(t)}})
			if e == nil || !strings.Contains(e.Error(), "route proof failed") {
				t.Fatal("relink accepted an unproven route", e)
			}
			assertNoCredential(t, n, log)
		})
	}
}

func TestReverseDiscoveryProvesRoutesBeforeSendingKeys(t *testing.T) {
	n := rig(t, true)
	log := unprovenSSH(t, n, `{"data":{"machine":"node-a","mac":"`+strings.Repeat("00", 32)+`"}}`)
	// The initial saved target is also unproven, even if previously installed.
	p := n.pb
	p.Target, p.Mode = "unproven", "poll"
	if e := n.b.InstallPeer(n.ctx, p, nil, p.Key); e != nil {
		t.Fatal(e)
	}
	savedRoutes(t, n, "unproven", "fake-a")
	r, e := n.ma.request(n.ctx, n.pa)
	if e != nil {
		t.Fatal(e)
	}
	value, e := n.mb.reverse(n.ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	result := value.(LinkResult)
	peers, e := n.b.PrivatePeers(n.ctx)
	if e != nil || result.Mode != "push-poll" || result.ReverseError != "" || peers["node-a"].Target != "fake-a" {
		t.Fatal("true peer was not promoted", result, e)
	}
	assertNoCredential(t, n, log)
}

func TestReverseDiscoveryFailsClosed(t *testing.T) {
	for _, tc := range []struct{ name, reply string }{
		{"old binary", `{"code":1,"error":"unknown peer endpoint"}`},
		{"forged HMAC", `{"data":{"machine":"node-a","mac":"` + strings.Repeat("00", 32) + `"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := rig(t, false)
			log := unprovenSSH(t, n, tc.reply)
			p := n.pb
			p.Target, p.Mode = "unproven", "poll"
			if e := n.b.InstallPeer(n.ctx, p, nil, p.Key); e != nil {
				t.Fatal(e)
			}
			savedRoutes(t, n, "unproven")
			r, _ := n.ma.request(n.ctx, n.pa)
			value, e := n.mb.reverse(n.ctx, r)
			if e != nil {
				t.Fatal(e)
			}
			result := value.(LinkResult)
			peers, e := n.b.PrivatePeers(n.ctx)
			if e != nil || result.Mode != "poll" || !strings.Contains(result.ReverseError, "route proof failed") || peers["node-a"].Mode != "poll" {
				t.Fatal("unproven route was promoted", result, e)
			}
			assertNoCredential(t, n, log)
		})
	}
}

func TestEstablishedCredentialCallsRequireFreshRouteProof(t *testing.T) {
	n := rig(t, true)
	log := unprovenSSH(t, n, `{"code":1,"error":"unknown peer endpoint"}`)
	p := n.pa
	p.Target = "unproven"
	for _, verb := range []string{"peer-probe", "peer-reverse", "accept", "drain", "ack", "peer-revoke", "peer-install"} {
		r, _ := n.ma.request(n.ctx, p)
		if verb == "peer-install" {
			r = broker.PeerRequest{Protocol: broker.Protocol, OldKey: p.Key, Peer: &broker.PrivatePeer{Machine: "node-a", Key: p.Key}}
		}
		if _, e := n.ma.SSH.Call(n.ctx, p, verb, r); e == nil {
			t.Fatal("unproven credential call succeeded", verb)
		}
	}
	assertNoCredential(t, n, log)
}

func TestRouteProofBindsNamesAndFreshNonce(t *testing.T) {
	n := rig(t, true)
	r := broker.PeerRequest{Protocol: broker.Protocol, Machine: "node-a", Nonce: strings.Repeat("ab", 32)}
	proof, e := n.b.ProveRoute(n.ctx, r)
	if e != nil || !broker.VerifyRouteProof(n.pa.Key, "node-a", "node-b", r.Nonce, proof) {
		t.Fatal("valid proof rejected", e)
	}
	for _, values := range [][3]string{{"other", "node-b", r.Nonce}, {"node-a", "other", r.Nonce}, {"node-a", "node-b", strings.Repeat("cd", 32)}} {
		if broker.VerifyRouteProof(n.pa.Key, values[0], values[1], values[2], proof) {
			t.Fatal("proof accepted for a different transcript")
		}
	}
}
