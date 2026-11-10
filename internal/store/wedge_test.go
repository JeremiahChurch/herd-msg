package store

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A caller's deadline expiring inside a write transaction used to strand the WAL write
// lock in this process, and every later write failed until the process exited. It took
// under a minute to hit with this mix of expiring writers and readers.
func TestExpiredWritesDoNotStrandTheWriteLock(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	filler := strings.Repeat("x", 2000)
	e = s.Update(context.Background(), func(d *Data) error {
		for i := 0; i < 70; i++ {
			id := fmt.Sprintf("session-%d", i)
			d.Sessions[id] = Session{ID: id, Name: filler[:100], Harness: "claude", State: "live"}
		}
		for i := 0; i < 80; i++ {
			k := fmt.Sprintf("message-%d", i)
			d.Messages[k] = &Message{Origin: "o", ID: k, Recipient: fmt.Sprintf("session-%d", i), State: "injected", Error: filler}
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	began := time.Now()
	if e = s.Update(context.Background(), func(*Data) error { return nil }); e != nil {
		t.Fatal(e)
	}
	span := int64(4*time.Since(began)) + 1
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var locked atomic.Int64
	run := func(seed int64, work func(context.Context, *rand.Rand)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				c, cancel := context.WithTimeout(context.Background(), time.Duration(r.Int63n(span)))
				work(c, r)
				cancel()
			}
		}()
	}
	for i := int64(0); i < 4; i++ {
		run(i, func(c context.Context, r *rand.Rand) {
			e := s.Update(c, func(d *Data) error {
				k := fmt.Sprintf("session-%d", r.Intn(70))
				v := d.Sessions[k]
				v.LastSeen = time.Now()
				d.Sessions[k] = v
				return nil
			})
			if e != nil && strings.Contains(e.Error(), "locked") {
				locked.Add(1)
			}
		})
		run(100+i, func(c context.Context, _ *rand.Rand) { _, _ = s.Read(c) })
	}
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	<-timer.C
	close(stop)
	wg.Wait()
	if n := locked.Load(); n != 0 {
		t.Errorf("%d writes found the database locked with only serialized writers", n)
	}
	if e = s.Update(context.Background(), func(*Data) error { return nil }); e != nil {
		t.Fatalf("write failed after all workers stopped: %v", e)
	}
}

func TestExpiredUpdateDoesNotCommit(t *testing.T) {
	s, e := Open(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	c, cancel := context.WithCancel(context.Background())
	e = s.Update(c, func(d *Data) error {
		d.Sessions["late"] = Session{ID: "late", State: "live"}
		cancel()
		return nil
	})
	if e == nil {
		t.Fatal("expected the expired deadline to be reported")
	}
	d, e := s.Read(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if _, ok := d.Sessions["late"]; ok {
		t.Fatal("expired update committed")
	}
	if e = s.Update(context.Background(), func(*Data) error { return nil }); e != nil {
		t.Fatal(e)
	}
}
