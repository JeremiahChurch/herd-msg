package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/store"
)

type loadDriver struct{ *FakeDriver }

func (d loadDriver) SessionState(_ context.Context, s store.Session) (Availability, error) {
	return Availability{State: s.State, Blocked: s.Blocked, Reason: s.Reason}, nil
}

// loadBroker models retained mail plus idle live receivers. It never discovers or
// contacts real harnesses, peers or the installed broker.
func loadBroker(t testing.TB) (*Broker, []store.Session) {
	t.Helper()
	home := t.TempDir()
	b, err := New(home, home+"/config.json", map[string]Driver{"claude": loadDriver{fake()}, "codex": loadDriver{fake()}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	sessions := make([]store.Session, 100)
	err = b.Store.Update(context.Background(), func(d *store.Data) error {
		for i := range sessions {
			s := store.Session{ID: fmt.Sprintf("session-%d", i), Name: fmt.Sprintf("receiver-%d", i), Machine: "local", Harness: "claude", State: "live", Generation: store.Random(), LastSeen: b.now(), Hints: map[string]string{"instance": "test", "broker_epoch": b.Epoch, "endpoint": "/tmp/disposable.sock"}}
			if i >= 20 {
				s.State = "offline"
			}
			sessions[i] = s
			d.Sessions[s.ID] = s
		}
		for i := 0; i < 300; i++ {
			id := fmt.Sprintf("retained-%d", i)
			s := sessions[i%100]
			m := &store.Message{ID: id, Origin: "local", SenderID: "sender", Sender: "sender", Recipient: s.ID, Destination: "local", State: "injected", Body: strings.Repeat("x", 1024), Sequence: d.NextSequence(), Created: b.now(), Accepted: b.now(), Updated: b.now(), TerminalAt: b.now(), Expires: b.now().Add(24 * time.Hour), Attempts: []store.Attempt{{ID: fmt.Sprintf("attempt-%d", i), Session: s, Started: b.now(), Finished: b.now(), Outcome: Accepted, Positive: true}}, History: []store.History{{From: "queued", To: "claimed", Actor: "broker", At: b.now()}, {From: "claimed", To: "injecting", Actor: "broker", At: b.now()}, {From: "injecting", To: "injected", Actor: "broker", At: b.now()}}}
			k := store.Key(m.Origin, id)
			d.Messages[k] = m
			d.Dedupe[k] = store.Dedupe{Origin: "local", ID: id, Created: b.now(), TerminalAt: b.now(), State: "injected"}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return b, sessions
}

func receiverPoll(ctx context.Context, b *Broker, s store.Session) error {
	r := b.Handle(ctx, Request{Version: Protocol, Verb: "receiver-state", Caller: Caller{ID: s.ID, Harness: "claude"}})
	if r.Code != 0 {
		return fmt.Errorf("receiver-state: %s", r.Error)
	}
	return json.NewEncoder(io.Discard).Encode(r)
}

func BenchmarkReceiverState(b *testing.B) {
	broker, sessions := loadBroker(b)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := receiverPoll(context.Background(), broker, sessions[i%20]); err != nil {
			b.Fatal(err)
		}
	}
}

// Opt in with HERD_MSG_LOAD_SECONDS=30. CPU includes the broker RPC handler and
// response encoding, but excludes receiver processes and real harness discovery.
func TestBrokerReceiverLoad(t *testing.T) {
	seconds, _ := strconv.Atoi(os.Getenv("HERD_MSG_LOAD_SECONDS"))
	if seconds <= 0 {
		t.Skip("set HERD_MSG_LOAD_SECONDS to run paced profiling load")
	}
	b, sessions := loadBroker(t)
	c, cancel := context.WithTimeout(context.Background(), time.Duration(seconds)*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	run := func(interval time.Duration, work func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-c.Done():
					return
				case <-ticker.C:
					if err := work(c); err != nil && c.Err() == nil {
						select {
						case errs <- err:
						default:
						}
					}
				}
			}
		}()
	}
	var before, after syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &before)
	started := time.Now()
	for _, s := range sessions[:20] {
		run(300*time.Millisecond, func(c context.Context) error { return receiverPoll(c, b, s) })
	}
	run(200*time.Millisecond, b.Tick)
	run(2*time.Second, func(c context.Context) error {
		if err := b.Refresh(c, nil); err != nil {
			return err
		}
		return b.RefreshAvailability(c)
	})
	var latencies []time.Duration
	n := 0
	run(time.Second, func(c context.Context) error {
		n++
		start := time.Now()
		_, err := b.Send(c, SendRequest{ID: fmt.Sprintf("load-%d", n), Created: b.now(), To: sessions[20+n%80].ID, Body: "latency probe"}, op)
		latencies = append(latencies, time.Since(start))
		if err != nil {
			return err
		}
		return b.Store.Update(c, func(d *store.Data) error {
			m, e := d.Find(fmt.Sprintf("load-%d", n))
			if e != nil {
				return e
			}
			return d.Move(m, "dead", "broker", "load probe complete", b.now())
		})
	})
	wg.Wait()
	elapsed := time.Since(started)
	syscall.Getrusage(syscall.RUSAGE_SELF, &after)
	cpu := func(r syscall.Rusage) float64 {
		return float64(r.Utime.Sec+r.Stime.Sec) + float64(r.Utime.Usec+r.Stime.Usec)/1e6
	}
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	if len(latencies) == 0 {
		t.Fatal("no send latency samples")
	}
	t.Logf("20 receivers, 100 sessions, 300 retained messages: elapsed=%s cpu=%.3fs core=%.1f%% send_p50=%s send_p95=%s sends=%d", elapsed, cpu(after)-cpu(before), 100*(cpu(after)-cpu(before))/elapsed.Seconds(), latencies[len(latencies)/2], latencies[(len(latencies)-1)*95/100], len(latencies))
}
