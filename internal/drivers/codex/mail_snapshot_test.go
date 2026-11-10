package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/broker"
	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

// The fixture is deliberately separate from broker CPU. It models a large
// transcript with no matching receipt, never contacts an installed app-server,
// and rejects all writes. The evidence database is copied before opening.
func TestMailSnapshotIdle(t *testing.T) {
	source := os.Getenv("HERD_MSG_MAIL_SNAPSHOT")
	if source == "" {
		t.Skip("set HERD_MSG_MAIL_SNAPSHOT to a disposable evidence copy")
	}
	seconds, _ := strconv.Atoi(os.Getenv("HERD_MSG_MAIL_SECONDS"))
	if seconds == 0 {
		seconds = 20
	}
	root := t.TempDir()
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "mailbox.db"), data, 0600); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(root)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := st.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	cfg := policy.Defaults()
	for _, s := range snapshot.Sessions {
		cfg.Machine = s.Machine
		break
	}
	for _, m := range snapshot.Messages {
		if m.Origin != cfg.Machine {
			cfg.Links[m.Origin] = policy.Grant{AutoRun: true}
		}
	}
	if err = policy.Write(filepath.Join(root, "config.json"), cfg, nil); err != nil {
		t.Fatal(err)
	}
	// Reuse the external synthetic app-server, with 9 MB histories. Its IDs need
	// not match: lookups have no receipt and thread/read is never authoritative.
	t.Setenv("HERD_MSG_PROFILE_HISTORY_KB", "9216")
	socket := profileAppServer(t)
	driver := &Driver{Socket: socket}
	b, err := broker.New(root, filepath.Join(root, "config.json"), map[string]broker.Driver{"codex": driver})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	// Prevent injection while retaining every pending row and scheduling decision.
	b.PauseDriver("codex", "snapshot test prohibits delivery")
	b.PauseDriver("claude", "snapshot test prohibits delivery")
	var before, after syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(seconds)*time.Second)
	defer cancel()
	b.RunTicks(ctx)
	b.Drain(context.Background())
	syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	final, _ := b.Store.Read(context.Background())
	counts := map[string]int{}
	for _, m := range final.Messages {
		counts[string(m.State)]++
	}
	encoded, _ := json.Marshal(counts)
	fmt.Printf("snapshot wall=%.2fs cpu=%.3fs core=%.2f%% messages=%s sessions=%d\n", time.Since(start).Seconds(), cpu(after)-cpu(before), 100*(cpu(after)-cpu(before))/time.Since(start).Seconds(), encoded, len(final.Sessions))
}
