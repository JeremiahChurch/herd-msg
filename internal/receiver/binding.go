// Package receiver runs as a trusted stdio MCP child of its Claude session.
package receiver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

type Binding struct {
	ID         string `json:"session_id"`
	Transcript string `json:"transcript_path"`
	Cwd        string `json:"cwd"`
}
type Bindings struct {
	Fingerprint string    `json:"fingerprint"`
	History     []Binding `json:"history"`
}

func fingerprint(token string) string {
	s := sha256.Sum256([]byte(token))
	return hex.EncodeToString(s[:])
}
func bindingPath(home, socket string) string {
	return filepath.Join(home, "receivers", fingerprint(socket)[:24]+".json")
}
func atomicJSON(path string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".binding-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func readBindings(home, socket, token string) (Bindings, error) {
	var b Bindings
	data, e := os.ReadFile(bindingPath(home, socket))
	if e != nil {
		return b, e
	}
	if e = json.Unmarshal(data, &b); e != nil {
		return b, e
	}
	if b.Fingerprint != fingerprint(token) || len(b.History) == 0 {
		return Bindings{}, errors.New("waiting for matching SessionStart hook")
	}
	return b, nil
}
func Hook(ctx context.Context, home string, in io.Reader) error {
	socket, token := os.Getenv("CLAUDE_CODE_MESSAGING_SOCKET"), os.Getenv("CLAUDE_CODE_MESSAGING_TOKEN")
	if socket == "" || token == "" {
		return errors.New("SessionStart hook requires Claude inbox coordinates")
	}
	var next Binding
	if e := json.NewDecoder(io.LimitReader(in, 1<<20)).Decode(&next); e != nil {
		return e
	}
	if e := policy.Identity(next.ID); e != nil {
		return e
	}
	if !filepath.IsAbs(next.Transcript) {
		return errors.New("hook requires absolute transcript path")
	}
	if e := store.PrivateDir(filepath.Join(home, "receivers")); e != nil {
		return e
	}
	path := bindingPath(home, socket)
	bounded, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	lock, e := policy.Lock(bounded, path+".hook.lock")
	if e != nil {
		return e
	}
	defer policy.Unlock(lock)
	b, e := readBindings(home, socket, token)
	if e != nil {
		b = Bindings{Fingerprint: fingerprint(token)}
	}
	if len(b.History) == 0 || b.History[len(b.History)-1].ID != next.ID {
		b.History = append(b.History, next)
	} else {
		b.History[len(b.History)-1] = next
	}
	return atomicJSON(path, b)
}
