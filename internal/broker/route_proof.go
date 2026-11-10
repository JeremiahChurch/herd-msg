package broker

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
)

type RouteProof struct {
	Machine string `json:"machine"`
	MAC     string `json:"mac"`
}

func routeMAC(key, requester, responder, nonce string) ([]byte, error) {
	secret, e := hex.DecodeString(key)
	if e != nil || len(secret) != 32 {
		return nil, errors.New("invalid route proof key")
	}
	// An encoded array makes machine names and the purpose unambiguous.
	transcript, _ := json.Marshal([]string{"herd-msg", "route-proof-v1", requester, responder, nonce})
	mac := hmac.New(sha256.New, secret)
	mac.Write(transcript)
	return mac.Sum(nil), nil
}

func VerifyRouteProof(key, requester, responder, nonce string, proof RouteProof) bool {
	if proof.Machine != responder {
		return false
	}
	want, e := routeMAC(key, requester, responder, nonce)
	got, decodeErr := hex.DecodeString(proof.MAC)
	return e == nil && decodeErr == nil && hmac.Equal(want, got)
}

// ProveRoute proves possession of a link key without accepting a credential or
// returning sessions. Revoked records can still prove identity for revocation
// retries, but this proof grants no receiving or transfer authority.
func (b *Broker) ProveRoute(ctx context.Context, r PeerRequest) (RouteProof, error) {
	var proof RouteProof
	if r.Protocol != Protocol || !validKey(r.Nonce) || r.Machine == "" {
		return proof, errors.New("invalid route challenge; install matching herd-msg binaries")
	}
	e := b.withConfig(ctx, func(c policy.Config) error {
		peers, e := b.readPeers()
		if e != nil {
			return e
		}
		p, ok := peers[r.Machine]
		if !ok {
			return errors.New("route proof requires an existing link")
		}
		if _, linked := c.Links[r.Machine]; !linked && !p.Revoked {
			return errors.New("route proof requires an existing link")
		}
		mac, e := routeMAC(p.Key, r.Machine, c.Machine, r.Nonce)
		if e != nil {
			return e
		}
		proof = RouteProof{Machine: c.Machine, MAC: hex.EncodeToString(mac)}
		return nil
	})
	return proof, e
}
