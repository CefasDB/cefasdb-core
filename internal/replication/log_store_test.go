package replication

import (
	"bytes"
	"errors"
	"testing"
	"time"

	pebbledb "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	hraft "github.com/hashicorp/raft"
)

func newTestLogStore(t *testing.T) (*pebbledb.DB, *logStore) {
	t.Helper()
	db, err := pebbledb.Open("/test", &pebbledb.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	store := newLogStore(db)
	t.Cleanup(func() {
		store.Close()
		_ = db.Close()
	})
	return db, store
}

func TestLogStoreDeleteRangeRemovesInclusiveRange(t *testing.T) {
	_, store := newTestLogStore(t)
	store.compactRange = nil

	for i := uint64(1); i <= 5; i++ {
		if err := store.StoreLog(&hraft.Log{Index: i, Term: 1, Data: []byte{byte(i)}}); err != nil {
			t.Fatalf("store log %d: %v", i, err)
		}
	}

	if err := store.DeleteRange(2, 4); err != nil {
		t.Fatalf("delete range: %v", err)
	}

	for _, idx := range []uint64{1, 5} {
		var got hraft.Log
		if err := store.GetLog(idx, &got); err != nil {
			t.Fatalf("get kept log %d: %v", idx, err)
		}
		if got.Index != idx {
			t.Fatalf("kept log index = %d, want %d", got.Index, idx)
		}
	}
	for _, idx := range []uint64{2, 3, 4} {
		var got hraft.Log
		if err := store.GetLog(idx, &got); !errors.Is(err, hraft.ErrLogNotFound) {
			t.Fatalf("get deleted log %d error = %v, want ErrLogNotFound", idx, err)
		}
	}
}

func TestLogStoreDeleteRangeSchedulesCompactionForLargeDeletes(t *testing.T) {
	_, store := newTestLogStore(t)
	store.compactMinDeleted = 3
	store.compactCooldown = 0

	called := make(chan [2][]byte, 1)
	store.compactRange = func(start, end []byte) error {
		called <- [2][]byte{append([]byte(nil), start...), append([]byte(nil), end...)}
		return nil
	}

	if err := store.DeleteRange(10, 12); err != nil {
		t.Fatalf("delete range: %v", err)
	}

	select {
	case got := <-called:
		if !bytes.Equal(got[0], logKey(10)) {
			t.Fatalf("compact start = %x, want %x", got[0], logKey(10))
		}
		if !bytes.Equal(got[1], logKey(13)) {
			t.Fatalf("compact end = %x, want %x", got[1], logKey(13))
		}
	case <-time.After(time.Second):
		t.Fatalf("compaction was not scheduled")
	}
}

func TestLogStoreDeleteRangeSkipsSmallCompaction(t *testing.T) {
	_, store := newTestLogStore(t)
	store.compactMinDeleted = 4
	store.compactCooldown = 0

	called := make(chan struct{}, 1)
	store.compactRange = func(start, end []byte) error {
		called <- struct{}{}
		return nil
	}

	if err := store.DeleteRange(10, 12); err != nil {
		t.Fatalf("delete range: %v", err)
	}

	select {
	case <-called:
		t.Fatalf("small delete range scheduled compaction")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestLogStoreDeleteRangeMaxUint64UsesPrefixEnd(t *testing.T) {
	_, store := newTestLogStore(t)
	store.compactMinDeleted = 1
	store.compactCooldown = 0

	called := make(chan [2][]byte, 1)
	store.compactRange = func(start, end []byte) error {
		called <- [2][]byte{append([]byte(nil), start...), append([]byte(nil), end...)}
		return nil
	}

	max := ^uint64(0)
	if err := store.DeleteRange(max, max); err != nil {
		t.Fatalf("delete range: %v", err)
	}

	select {
	case got := <-called:
		if !bytes.Equal(got[0], logKey(max)) {
			t.Fatalf("compact start = %x, want %x", got[0], logKey(max))
		}
		if !bytes.Equal(got[1], logKeyPrefixEnd) {
			t.Fatalf("compact end = %x, want %x", got[1], logKeyPrefixEnd)
		}
	case <-time.After(time.Second):
		t.Fatalf("compaction was not scheduled")
	}
}

func TestLogStoreCloseWaitsForCompaction(t *testing.T) {
	_, store := newTestLogStore(t)
	store.compactMinDeleted = 1
	store.compactCooldown = 0

	started := make(chan struct{})
	release := make(chan struct{})
	store.compactRange = func(start, end []byte) error {
		close(started)
		<-release
		return nil
	}

	if err := store.DeleteRange(1, 1); err != nil {
		t.Fatalf("delete range: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatalf("compaction was not started")
	}

	done := make(chan struct{})
	go func() {
		store.Close()
		close(done)
	}()

	select {
	case <-done:
		t.Fatalf("close returned before compaction finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("close did not return after compaction finished")
	}
}
