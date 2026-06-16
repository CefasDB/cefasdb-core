package raft_test

import (
	"context"
	"sync"
	"testing"
	"time"

	pebbledb "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"

	craft "github.com/osvaldoandrade/cefas/internal/raft"
	"github.com/osvaldoandrade/cefas/internal/testutil/wait"
)

// openMemDB returns a fresh in-memory Pebble store so the test can
// exercise the publisher without touching disk.
func openMemDB(t *testing.T) *pebbledb.DB {
	t.Helper()
	opts := &pebbledb.Options{FS: vfs.NewMem()}
	db, err := pebbledb.Open("/test", opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestPublisherDeliversEvents(t *testing.T) {
	pub := craft.NewPublisher(16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, _ := pub.Subscribe(ctx)

	if got := pub.SubscriberCount(); got != 1 {
		t.Fatalf("expected 1 subscriber, got %d", got)
	}

	db := openMemDB(t)
	batch := db.NewBatch()
	_ = batch.Set([]byte("cefas/k1"), []byte("v1"), nil)
	_ = batch.Set([]byte("cefas/k2"), []byte("v2"), nil)
	_ = batch.Delete([]byte("cefas/k3"), nil)
	// raft/ keys must NOT be published.
	_ = batch.Set([]byte("raft/log/0001"), []byte("internal"), nil)
	repr := append([]byte(nil), batch.Repr()...)
	_ = batch.Close()

	if err := craft.ApplyAndPublishForTest(db, repr, 42, pub); err != nil {
		t.Fatalf("apply: %v", err)
	}

	var got []craft.ChangeEvent
	deadline := time.After(2 * time.Second)
	for len(got) < 3 {
		select {
		case ev := <-events:
			got = append(got, ev)
		case <-deadline:
			t.Fatalf("only %d events arrived: %v", len(got), got)
		}
	}

	if got[0].RaftIndex != 42 {
		t.Errorf("raft index = %d", got[0].RaftIndex)
	}
	keys := map[string]craft.ChangeOp{}
	for _, ev := range got {
		keys[string(ev.Key)] = ev.Op
	}
	if keys["cefas/k1"] != craft.OpPut || keys["cefas/k2"] != craft.OpPut {
		t.Errorf("missing put events: %+v", keys)
	}
	if keys["cefas/k3"] != craft.OpDelete {
		t.Errorf("missing delete event: %+v", keys)
	}
	if _, leaked := keys["raft/log/0001"]; leaked {
		t.Errorf("internal raft key leaked into CDC: %+v", keys)
	}
}

func TestPublisherCancellationFreesSlot(t *testing.T) {
	pub := craft.NewPublisher(4)
	ctx, cancel := context.WithCancel(context.Background())
	_, _ = pub.Subscribe(ctx)
	if got := pub.SubscriberCount(); got != 1 {
		t.Fatalf("subscribers = %d", got)
	}
	cancel()
	// Cancellation is processed by the watcher goroutine.
	wait.Eventually(t, func() bool {
		return pub.SubscriberCount() == 0
	}, 500*time.Millisecond, 5*time.Millisecond, "subscriber slot leaked after ctx cancel")
}

func TestPublisherFanOutNonBlocking(t *testing.T) {
	pub := craft.NewPublisher(2) // tiny ring
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Slow consumer — never drains the channel.
	_, _ = pub.Subscribe(ctx)

	db := openMemDB(t)
	batch := db.NewBatch()
	for i := 0; i < 10; i++ {
		_ = batch.Set([]byte("cefas/k"), []byte("v"), nil)
	}
	repr := append([]byte(nil), batch.Repr()...)
	_ = batch.Close()

	done := make(chan struct{})
	go func() {
		_ = craft.ApplyAndPublishForTest(db, repr, 1, pub)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("publish blocked behind a slow consumer")
	}
}

// Race-condition smoke: many concurrent subscribers + simultaneous
// publishes. We're not measuring anything specific, just exercising
// the lock paths so -race catches misuse.
func TestPublisherConcurrentSubscribers(t *testing.T) {
	pub := craft.NewPublisher(64)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub, c := pub.Subscribe(ctx)
			defer c()
			deadline := time.After(200 * time.Millisecond)
			for {
				select {
				case <-sub:
				case <-deadline:
					return
				}
			}
		}()
	}

	db := openMemDB(t)
	for i := 0; i < 100; i++ {
		b := db.NewBatch()
		_ = b.Set([]byte("cefas/x"), []byte("y"), nil)
		repr := append([]byte(nil), b.Repr()...)
		_ = b.Close()
		_ = craft.ApplyAndPublishForTest(db, repr, uint64(i), pub)
	}
	wg.Wait()
}
