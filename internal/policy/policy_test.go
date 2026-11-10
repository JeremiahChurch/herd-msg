package policy

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGateOrder(t *testing.T) {
	c := Defaults()
	in := Input{Killed: true, OptOut: true, Hop: 7, PairCount: 20, Origin: "remote", Local: "local", Availability: "offline", Blocked: true}
	reasons := []string{"traffic stopped", "recipient opted out", "hop limit exceeded", "pair rate limit exceeded", "sender machine is not linked", "waiting for operator release", "recipient offline", "recipient blocked", ""}
	for i, want := range reasons {
		if got := Evaluate(c, in, true).Reason; got != want {
			t.Fatalf("gate %d = %q want %q", i, got, want)
		}
		switch i {
		case 0:
			in.Killed = false
		case 1:
			in.OptOut = false
		case 2:
			in.Hop = 6
		case 3:
			in.PairCount = 19
		case 4:
			in.Linked = true
		case 5:
			in.Released = true
		case 6:
			in.Availability = "live"
		case 7:
			in.Blocked = false
		}
	}
	in.PairCount = 999
	in.Origin = "local"
	if got := Evaluate(c, in, false); got.Refuse || got.Wait {
		t.Fatal("delivery charged admission rate", got)
	}
	in.Origin = "remote"
	in.Linked = false
	if !Evaluate(c, in, false).Refuse {
		t.Fatal("release bypassed removed link")
	}
}
func TestConfigAtomicValidationAndLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	c := Defaults()
	if e := Write(path, c, nil); e != nil {
		t.Fatal(e)
	}
	before, _ := os.ReadFile(path)
	c.Dedupe = 1
	if Write(path, c, nil) == nil {
		t.Fatal("invalid config written")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("validation changed config")
	}
	lock, e := Lock(context.Background(), path+".lock")
	if e != nil {
		t.Fatal(e)
	}
	defer Unlock(lock)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if f, e := Lock(ctx, path+".lock"); e == nil {
		Unlock(f)
		t.Fatal("lock wait unbounded")
	}
	target := filepath.Join(t.TempDir(), "target")
	if e = os.WriteFile(target, []byte("untouched"), 0600); e != nil {
		t.Fatal(e)
	}
	os.Remove(path)
	os.Symlink(target, path)
	if e = Write(path, Defaults(), nil); e != nil {
		t.Fatal(e)
	}
	b, _ := os.ReadFile(target)
	if string(b) != "untouched" {
		t.Fatal("followed config symlink")
	}
}
