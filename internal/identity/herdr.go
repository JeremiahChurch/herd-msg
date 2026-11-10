// Package identity reads current harness and machine identity without owning work status.
package identity

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/store"
	"os"
	"os/exec"
	"strings"
	"sync"
)

type Agent struct {
	Pane      string `json:"pane_id"`
	Name      string `json:"name"`
	Workspace string `json:"workspace_id"`
	Harness   string `json:"agent"`
	State     string `json:"agent_status"`
	Session   struct {
		Value string `json:"value"`
	} `json:"agent_session"`
}

type cacheKey struct{}
type agentResult struct {
	agents []Agent
	err    error
}
type agentCache struct {
	mu     sync.Mutex
	values map[string]agentResult
}

// CachedAgents shares one fresh herdr snapshot per named server within a refresh pass.
func CachedAgents(ctx context.Context) context.Context {
	return context.WithValue(ctx, cacheKey{}, &agentCache{values: map[string]agentResult{}})
}

// SharedAgents reuses a background discovery pass. Send preflights deliberately
// use CachedAgents instead, which always starts a fresh observation.
func SharedAgents(ctx context.Context) context.Context {
	if _, ok := ctx.Value(cacheKey{}).(*agentCache); ok {
		return ctx
	}
	return CachedAgents(ctx)
}
func Agents(ctx context.Context, session string) ([]Agent, error) {
	if cache, ok := ctx.Value(cacheKey{}).(*agentCache); ok {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		if r, exists := cache.values[session]; exists {
			return r.agents, r.err
		}
		agents, e := readAgents(ctx, session)
		cache.values[session] = agentResult{agents, e}
		return agents, e
	}
	return readAgents(ctx, session)
}
func readAgents(ctx context.Context, session string) ([]Agent, error) {
	args := []string{}
	if session != "" {
		args = append(args, "--session", session)
	}
	args = append(args, "agent", "list")
	cmd := exec.CommandContext(ctx, "herdr", args...)
	b, e := cmd.Output()
	if e != nil {
		return nil, e
	}
	var r struct {
		Result struct {
			Agents []Agent `json:"agents"`
		} `json:"result"`
	}
	if e = json.Unmarshal(b, &r); e != nil {
		return nil, e
	}
	if r.Result.Agents == nil {
		return nil, errors.New("missing herdr agents")
	}
	return r.Result.Agents, nil
}
func Pane(ctx context.Context, s store.Session) (Agent, error) {
	agents, e := Agents(ctx, s.Hints["herdr_session"])
	if e != nil {
		return Agent{}, e
	}
	for _, a := range agents {
		if a.Pane == s.Pane && a.Harness == "claude" {
			return a, nil
		}
	}
	return Agent{}, errors.New("recipient pane is not observed; waiting for herdr")
}
func Name(ctx context.Context, a Agent, session string) string {
	if policy.Identity(a.Name) == nil {
		return a.Name
	}
	args := []string{}
	if session != "" {
		args = append(args, "--session", session)
	}
	args = append(args, "workspace", "list")
	b, e := exec.CommandContext(ctx, "herdr", args...).Output()
	if e == nil {
		var r struct {
			Result struct {
				Workspaces []struct {
					ID          string `json:"id"`
					WorkspaceID string `json:"workspace_id"`
					Label       string `json:"label"`
				} `json:"workspaces"`
			} `json:"result"`
		}
		if json.Unmarshal(b, &r) == nil {
			for _, w := range r.Result.Workspaces {
				if (w.ID == a.Workspace || w.WorkspaceID == a.Workspace) && policy.Identity(w.Label) == nil {
					return w.Label
				}
			}
		}
	}
	return a.Pane
}

// SelectMachine must run before discovery can pin addresses to a machine.
func SelectMachine(ctx context.Context, home, configPath string) error {
	if e := store.PrivateDir(home); e != nil {
		return e
	}
	lock, e := policy.Lock(ctx, configPath+".lock")
	if e != nil {
		return e
	}
	defer policy.Unlock(lock)
	c, e := policy.Read(configPath)
	if e != nil {
		return e
	}
	d, e := store.Snapshot(ctx, home)
	if e != nil {
		return e
	}
	if len(d.Sessions)+len(d.Messages)+len(d.Dedupe) > 0 || c.Machine != "local" {
		return nil
	}
	host, e := os.Hostname()
	if e != nil {
		return e
	}
	host = strings.ToLower(host)
	var clean strings.Builder
	for _, r := range host {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			clean.WriteRune(r)
		} else {
			clean.WriteByte('-')
		}
	}
	c.Machine = strings.Trim(clean.String(), "-")
	if c.Machine == "" {
		return errors.New("cannot derive machine identity; set machine with herd-msg config")
	}
	return policy.Write(configPath, c, nil)
}
