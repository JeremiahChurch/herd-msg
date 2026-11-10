package broker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/JeremiahChurch/herd-msg/internal/policy"
	"github.com/JeremiahChurch/herd-msg/internal/store"
)

func TestSendWaitsForLocalConfigContentionAndHonorsCancellation(t *testing.T) {
	b, _ := setup(t)
	held := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- b.withConfig(ctx, func(policy.Config) error { close(held); <-release; return nil }) }()
	<-held
	released := false
	defer func() {
		if !released {
			close(release)
			<-done
		}
	}()
	short, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := b.Send(short, request(b, "cancelled"), agent); err == nil {
		t.Fatal("cancelled send ignored its deadline")
	}
	sent := make(chan error, 1)
	call, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	go func() { _, err := b.Send(call, request(b, "waited"), agent); sent <- err }()
	// The former one-second file-lock limit must not reject an in-process wait.
	select {
	case err := <-sent:
		t.Fatalf("send stopped while local config was busy: %v", err)
	case <-time.After(1200 * time.Millisecond):
	}
	close(release)
	<-done
	released = true
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	d, err := b.Store.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.Find("cancelled"); err == nil {
		t.Fatal("cancelled send committed")
	}
	if _, err = d.Find("waited"); err != nil {
		t.Fatal(err)
	}
}

func TestReceiverStateIgnoresConfigLockAndOmitsMessages(t *testing.T) {
	b, _ := setup(t)
	send(t, b, "private-body")
	lock, err := policy.Lock(ctx, b.ConfigPath+".lock")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { policy.Unlock(lock) }()
	call, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	r := b.Handle(call, Request{Version: Protocol, Verb: "receiver-state", Caller: Caller{ID: "receiver", Harness: "claude"}})
	if r.Code != 0 {
		t.Fatal(r)
	}
	var snap struct {
		Data  *store.Data
		Epoch string
	}
	if err = json.Unmarshal(r.Data, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.Data == nil || snap.Data.Sessions["receiver"].Generation == "" || snap.Epoch != b.Epoch {
		t.Fatal("missing receiver registration")
	}
	if len(snap.Data.Messages) != 0 {
		t.Fatal("receiver poll leaked retained mail")
	}
}

func TestSendWaitsForExternalConfigLockBeyondOneSecond(t *testing.T) {
	b, _ := setup(t)
	lock, err := policy.Lock(ctx, b.ConfigPath+".lock")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { policy.Unlock(lock) }()
	call, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	sent := make(chan error, 1)
	go func() { _, err := b.Send(call, request(b, "external-wait"), agent); sent <- err }()
	select {
	case err := <-sent:
		t.Fatalf("send stopped before operator released lock: %v", err)
	case <-time.After(1200 * time.Millisecond):
	}
	policy.Unlock(lock)
	lock = nil
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
}
