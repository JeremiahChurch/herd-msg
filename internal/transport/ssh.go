// Package transport transfers committed mailbox payloads over existing SSH routes.
package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
)

func Quote(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
func RemoteCommand(path, home, verb string) (string, error) {
	if !filepath.IsAbs(path) || strings.ContainsAny(path, "\x00\r\n") || (home != "" && (!filepath.IsAbs(home) || strings.ContainsAny(home, "\x00\r\n"))) {
		return "", errors.New("remote binary and home must be absolute paths")
	}
	words := []string{"env", "-u", "CODEX_THREAD_ID", "-u", "CLAUDE_CODE_SESSION_ID"}
	if home != "" {
		words = append(words, "HERD_MSG_HOME="+home)
	}
	words = append(words, path, verb)
	for i, w := range words {
		words[i] = Quote(w)
	}
	return strings.Join(words, " "), nil
}

type SSHError struct{ Kind string }

func (e *SSHError) Error() string {
	switch e.Kind {
	case "host-key":
		return "SSH host key check failed; verify the saved target fingerprint with ssh-keygen -F before repairing known_hosts; shared connections may delay detection"
	case "auth":
		return "SSH authentication failed; authorize the saved target with ssh -o BatchMode=yes, and make its SSH agent available to the broker"
	case "timeout":
		return "SSH operation timed out; check the saved route and SSH agent availability"
	case "install":
		return "remote herd-msg is missing; run scripts/deploy.sh for the saved machine"
	default:
		return "SSH connection failed; check the saved route with ssh -o BatchMode=yes"
	}
}

type limitedBuffer struct {
	bytes.Buffer
	Limit int
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.Limit-b.Len() {
		return 0, errors.New("SSH output exceeds limit")
	}
	return b.Buffer.Write(p)
}

type SSH struct{ Binary string }

func (s SSH) raw(ctx context.Context, target, command string, input []byte) ([]byte, error) {
	if target == "" || strings.HasPrefix(target, "-") || strings.IndexFunc(target, unicode.IsControl) >= 0 {
		return nil, errors.New("invalid saved SSH target")
	}
	parent := ctx
	limit := 12 * time.Second
	if strings.HasSuffix(command, "'peer-reverse'") {
		limit = 20 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	binary := s.Binary
	if binary == "" {
		binary = "ssh"
	}
	cmd := exec.CommandContext(ctx, binary, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=8", "-o", "ServerAliveInterval=5", "-o", "ServerAliveCountMax=1", "--", target, command)
	cmd.WaitDelay = 500 * time.Millisecond
	cmd.Stdin = bytes.NewReader(input)
	out := &limitedBuffer{Limit: 8 << 20}
	stderr := &limitedBuffer{Limit: 64 << 10}
	cmd.Stdout = out
	cmd.Stderr = stderr
	if e := cmd.Run(); e != nil {
		if parent.Err() != nil {
			return nil, parent.Err()
		}
		text := strings.ToLower(stderr.String())
		kind := "connection"
		switch {
		case strings.Contains(text, "host key") || strings.Contains(text, "host identification"):
			kind = "host-key"
		case strings.Contains(text, "permission denied") || strings.Contains(text, "authentication") || strings.Contains(text, "sign_and_send_pubkey"):
			kind = "auth"
		case ctx.Err() != nil:
			kind = "timeout"
		case strings.Contains(text, "not found") || strings.Contains(text, "no such file"):
			kind = "install"
		}
		return nil, &SSHError{kind}
	}
	return out.Bytes(), nil
}
func (s SSH) Discover(ctx context.Context, target string) (string, error) {
	data, e := s.raw(ctx, target, `command -v herd-msg || printf '%s\n' "$HOME/.local/bin/herd-msg"`, nil)
	if e != nil {
		return "", e
	}
	path := strings.TrimSpace(string(data))
	if !filepath.IsAbs(path) || strings.ContainsAny(path, "\r\n") {
		return "", errors.New("remote binary discovery did not return one absolute path")
	}
	return path, nil
}
func (s SSH) Call(ctx context.Context, p broker.PrivatePeer, verb string, r broker.PeerRequest) (broker.Response, error) {
	return s.callChecked(ctx, p, verb, r, nil)
}

// callChecked runs the caller's final state check after the proof's network wait
// and before serializing or transmitting the credential-bearing frame.
func (s SSH) callChecked(ctx context.Context, p broker.PrivatePeer, verb string, r broker.PeerRequest, beforeSend func() error) (broker.Response, error) {
	// Every transmission of an established credential needs a fresh proof,
	// including retries against a previously working target and peer-install.
	key, machine := r.Key, r.Machine
	if r.OldKey != "" {
		key = r.OldKey
		if r.Peer == nil {
			return broker.Response{}, errors.New("peer required for relink")
		}
		machine = r.Peer.Machine
	}
	if key != "" {
		nonce := make([]byte, 32)
		if _, e := rand.Read(nonce); e != nil {
			return broker.Response{}, e
		}
		challenge := broker.PeerRequest{Protocol: broker.Protocol, Machine: machine, Nonce: hex.EncodeToString(nonce)}
		resp, e := s.call(ctx, p, "peer-route-proof", challenge)
		if e != nil {
			return broker.Response{}, fmt.Errorf("route proof failed; peer-route-proof support required; install matching herd-msg binaries: %w", e)
		}
		proof, e := decode[broker.RouteProof](resp)
		if e != nil || !broker.VerifyRouteProof(key, machine, p.Machine, challenge.Nonce, proof) {
			return broker.Response{}, errors.New("route proof failed; peer identity was not verified")
		}
	}
	if beforeSend != nil {
		if e := beforeSend(); e != nil {
			return broker.Response{}, e
		}
	}
	return s.call(ctx, p, verb, r)
}

func (s SSH) call(ctx context.Context, p broker.PrivatePeer, verb string, r broker.PeerRequest) (broker.Response, error) {
	command, e := RemoteCommand(p.Path, p.Home, verb)
	if e != nil {
		return broker.Response{}, e
	}
	data, e := json.Marshal(r)
	if e != nil {
		return broker.Response{}, e
	}
	if len(data)+1 > broker.MaxFrame {
		return broker.Response{}, errors.New("peer frame exceeds 1 MiB")
	}
	data, e = s.raw(ctx, p.Target, command, append(data, '\n'))
	if e != nil {
		return broker.Response{}, e
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	var result broker.Response
	if e = dec.Decode(&result); e != nil {
		return result, errors.New("invalid peer response; check herd-msg protocol versions")
	}
	var extra any
	if e = dec.Decode(&extra); e != io.EOF {
		return result, errors.New("unexpected extra peer response")
	}
	if result.Code != 0 {
		return result, &broker.Error{Code: result.Code, Retryable: result.Retryable, Message: result.Error}
	}
	return result, nil
}
func decode[T any](r broker.Response) (T, error) {
	var out T
	e := json.Unmarshal(r.Data, &out)
	return out, e
}
func Repair(target string, e error) string {
	var se *SSHError
	if errors.As(e, &se) && se.Kind == "host-key" {
		return "Check: ssh-keygen -F " + Quote(target) + ". Verify the fingerprint before changing known_hosts."
	}
	return fmt.Sprintf("Check: ssh -o BatchMode=yes -o StrictHostKeyChecking=yes %s true. %s", Quote(target), e.Error())
}
