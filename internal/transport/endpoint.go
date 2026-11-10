package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
)

// Endpoint keeps peer protocol records on stdout and never prints credentials.
func Endpoint(ctx context.Context, home, verb string, in io.Reader, out io.Writer, caller broker.Caller) error {
	verbs := map[string]string{"accept": "peer-accept", "drain": "peer-drain", "ack": "peer-ack", "peer-probe": "peer-probe", "peer-info": "peer-info", "peer-route-proof": "peer-route-proof", "peer-install": "peer-install", "peer-revoke": "peer-revoke", "peer-reverse": "network-reverse"}
	rpc, ok := verbs[verb]
	if !ok {
		return errors.New("unknown peer endpoint")
	}
	scan := bufio.NewScanner(in)
	scan.Buffer(make([]byte, 4096), broker.MaxFrame)
	for scan.Scan() {
		if strings.TrimSpace(scan.Text()) == "" {
			continue
		}
		var r broker.PeerRequest
		if e := json.Unmarshal(scan.Bytes(), &r); e != nil {
			return errors.New("invalid peer frame")
		}
		result, e := broker.Call(ctx, home, broker.Request{Verb: rpc, Caller: caller, Peer: &r})
		if e != nil {
			result = broker.Response{Code: 1, Error: e.Error(), Retryable: true}
		}
		if e = json.NewEncoder(out).Encode(result); e != nil {
			return e
		}
	}
	return scan.Err()
}
