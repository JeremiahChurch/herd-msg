package identity

import (
	"context"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/store"
	"os"
	"path/filepath"
	"testing"
)

func TestFirstMachineSelectionAfterOfflineKill(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(home, "config.json")
	c := policy.Defaults()
	c.Killed = true
	if e := policy.Write(path, c, nil); e != nil {
		t.Fatal(e)
	}
	if e := SelectMachine(context.Background(), home, path); e != nil {
		t.Fatal(e)
	}
	c, e := policy.Read(path)
	if e != nil || !c.Killed || c.Machine == "local" || c.Machine == "" {
		t.Fatal(c, e)
	}
	original := c.Machine
	s, e := store.Open(home)
	if e != nil {
		t.Fatal(e)
	}
	e = s.Update(context.Background(), func(d *store.Data) error {
		d.Sessions["existing"] = store.Session{ID: "existing", Machine: original}
		return nil
	})
	s.Close()
	if e != nil {
		t.Fatal(e)
	}
	c.Machine = "local"
	if e = policy.Write(path, c, nil); e != nil {
		t.Fatal(e)
	}
	if e = SelectMachine(context.Background(), home, path); e != nil {
		t.Fatal(e)
	}
	c, e = policy.Read(path)
	if e != nil || c.Machine != "local" {
		t.Fatal("changed retained identity", c, e)
	}
}
func TestExplicitMachineAndPrivateConfig(t *testing.T) {
	home := t.TempDir()
	path := filepath.Join(home, "config.json")
	c := policy.Defaults()
	c.Machine = "test-node"
	if e := policy.Write(path, c, nil); e != nil {
		t.Fatal(e)
	}
	if e := SelectMachine(context.Background(), home, path); e != nil {
		t.Fatal(e)
	}
	c, e := policy.Read(path)
	if e != nil || c.Machine != "test-node" {
		t.Fatal(c, e)
	}
	info, e := os.Stat(path)
	if e != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, e)
	}
}

func TestRefreshSharesHerdrSnapshotOnlyWithinItsPass(t *testing.T) {
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	script := "#!/bin/sh\nprintf 'call\\n' >> '" + calls + "'\nprintf '%s\\n' '{\"result\":{\"agents\":[]}}'\n"
	if e := os.WriteFile(filepath.Join(dir, "herdr"), []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	ctx := CachedAgents(context.Background())
	for range 3 {
		if _, e := Agents(ctx, "test"); e != nil {
			t.Fatal(e)
		}
	}
	b, _ := os.ReadFile(calls)
	if string(b) != "call\n" {
		t.Fatal("repeated snapshot in one pass", string(b))
	}
	if _, e := Agents(CachedAgents(context.Background()), "test"); e != nil {
		t.Fatal(e)
	}
	b, _ = os.ReadFile(calls)
	if string(b) != "call\ncall\n" {
		t.Fatal("snapshot survived refresh pass", string(b))
	}
}

func TestSharedDiscoverySnapshotDoesNotReplaceFreshSendSnapshot(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "agents.json")
	write := func(state string) {
		t.Helper()
		if e := os.WriteFile(path, []byte(`{"result":{"agents":[{"pane_id":"w1:p1","agent":"claude","agent_status":"`+state+`"}]}}`), 0600); e != nil {
			t.Fatal(e)
		}
	}
	if e := os.WriteFile(filepath.Join(root, "herdr"), []byte("#!/bin/sh\ncat '"+path+"'\n"), 0700); e != nil {
		t.Fatal(e)
	}
	t.Setenv("PATH", root+":"+os.Getenv("PATH"))
	write("idle")
	pass := CachedAgents(context.Background())
	agents, e := Agents(pass, "")
	if e != nil || agents[0].State != "idle" {
		t.Fatal(agents, e)
	}
	write("blocked")
	agents, e = Agents(SharedAgents(pass), "")
	if e != nil || agents[0].State != "idle" {
		t.Fatal("background pass was not shared", agents, e)
	}
	agents, e = Agents(CachedAgents(pass), "")
	if e != nil || agents[0].State != "blocked" {
		t.Fatal("send preflight reused stale panes", agents, e)
	}
}
