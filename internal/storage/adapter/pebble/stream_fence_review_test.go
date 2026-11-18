package pebble

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// TestReseedChangeIndexRaisesAllocatorToInstalledChangelog covers the
// snapshot-install reseed: the allocator is raised to the installed
// changelog (so the watermark covers it and nothing is reused) and is never
// lowered.
func TestReseedChangeIndexRaisesAllocatorToInstalledChangelog(t *testing.T) {
	db, td := nativeStreamDB(t)
	expiry := time.Now().Add(time.Hour).Unix()
	for i := 0; i < 5; i++ {
		if err := db.PutItemWith(td, textEvent("a", fmt.Sprint(i), "m", expiry), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	// The store was repopulated underneath a stale allocator.
	db.changeIndex.Store(1)
	if err := db.ReseedChangeIndex(); err != nil {
		t.Fatal(err)
	}
	if got := db.changeIndex.Load(); got != 5 {
		t.Fatalf("reseeded allocator = %d, want 5", got)
	}
	if s := db.StreamFenceStats(); s.Watermark != 5 {
		t.Fatalf("watermark after reseed = %+v", s)
	}
	// A changelog key above the persisted counter (counter written out of
	// order) is covered by the scan.
	b := db.Batch()
	rec := newChangeRecord(td, ChangePut, types.Item{"tenantId": streamS("a"), "orderKey": streamS("x")}, nil, textEvent("a", "x", "m", expiry))
	rec.Index, rec.UnixNano = 9, time.Now().UnixNano()
	raw, err := encodeChangeRecord(nil, rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Set(storage.KeyChangeLog(9), raw, nil); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(pebbledb.Sync); err != nil {
		t.Fatal(err)
	}
	_ = b.Close()
	if err := db.ReseedChangeIndex(); err != nil {
		t.Fatal(err)
	}
	if got := db.changeIndex.Load(); got != 9 {
		t.Fatalf("reseeded allocator = %d, want 9 (changelog scan)", got)
	}
	// Raise-only: a live allocator ahead of the persisted state stays.
	db.changeIndex.Store(50)
	if err := db.ReseedChangeIndex(); err != nil {
		t.Fatal(err)
	}
	if got := db.changeIndex.Load(); got != 50 {
		t.Fatalf("reseed lowered the allocator to %d", got)
	}
	if err := db.PutItemWith(td, textEvent("a", "after", "m", expiry), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if got := db.changeIndex.Load(); got != 51 {
		t.Fatalf("allocation after reseed = %d, want 51", got)
	}
}

// TestPITRReplayWaitsForInFlightNativeWrite: a point-in-time restore whose
// target index lies between the watermark and the allocator waits for the
// in-flight native write instead of silently replaying short of it, and a
// bounded wait that expires is an explicit error.
func TestPITRReplayWaitsForInFlightNativeWrite(t *testing.T) {
	db, td := nativeStreamDB(t)
	expiry := time.Now().Add(time.Hour).Unix()
	if err := db.PutItemWith(td, textEvent("a", "base", "base", expiry), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateBackup("snap", []string{td.Name}); err != nil {
		t.Fatal(err)
	}
	r := &blockedNativeCommit{db: db, first: make(chan struct{}), release: make(chan struct{})}
	db.AttachReplicator(r)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(r.release) }) }
	slow := textEvent("a", "slow", "slow", expiry)
	slowDone := make(chan error, 1)
	slowExited := make(chan struct{})
	go func() {
		defer close(slowExited)
		slowDone <- db.PutItemWith(td, slow, PutOptions{})
	}()
	// Runs before nativeStreamDB's Close: a failed assertion cannot leave
	// the writer blocked or racing Close.
	t.Cleanup(func() { release(); <-slowExited })
	<-r.first
	later := otherShardItem(t, td, slow, "later", expiry)
	if err := db.PutItemWith(td, later, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	target, _ := db.CurrentChangeIndex()
	if s := db.StreamFenceStats(); s.Watermark >= target {
		t.Fatalf("fixture: watermark %d already covers target %d", s.Watermark, target)
	}

	// Bounded: an expired wait or a cancelled context is an error, never a
	// short range.
	if _, err := db.changeRecordsThroughCtx(context.Background(), td.Name, 1, target, 0, 30*time.Millisecond); !errors.Is(err, ErrReadUnavailable) {
		t.Fatalf("expired wait = %v, want ErrReadUnavailable", err)
	}
	if _, err := db.changeRecordsThroughCtx(canceledContext(), td.Name, 1, target, 0, time.Minute); !errors.Is(err, ErrReadUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled wait = %v", err)
	}

	register := func(desc types.TableDescriptor) error {
		raw, err := json.Marshal(desc)
		if err != nil {
			return err
		}
		return db.Set(storage.KeyCatalog(desc.Name), raw)
	}
	restoreDone := make(chan error, 1)
	go func() {
		_, err := db.RestoreTableFromBackupWithOptions("snap", td.Name, "logs_pitr", RestoreOptions{TargetChangeIndex: target}, register)
		restoreDone <- err
	}()
	// The restore must park on the watermark; returning first means it
	// replayed short of the in-flight write.
	deadline := time.Now().Add(10 * time.Second)
	for !fenceHasWaiter(db) {
		select {
		case err := <-restoreDone:
			t.Fatalf("restore returned before the in-flight write below its target settled: %v", err)
		case <-time.After(time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("restore neither waited for the watermark nor returned")
		}
	}
	release()
	if err := <-slowDone; err != nil {
		t.Fatal(err)
	}
	if err := <-restoreDone; err != nil {
		t.Fatal(err)
	}
	pitr := td
	pitr.Name = "logs_pitr"
	for _, item := range []types.Item{slow, later} {
		if _, err := db.GetItem(pitr.Name, pitr.KeySchema, keyItemFromItem(item, pitr.KeySchema)); err != nil {
			t.Fatalf("restore dropped %s: %v", item["orderKey"].S, err)
		}
	}
}

// TestChangeTimestampsNeverDecreaseInSequenceOrder: concurrent writers on
// several tables and a TTL reaper (whose records are planned long before
// they are allocated) stamp timestamps that never decrease in sequence
// order, so a time-bounded scan cannot stop before an earlier-stamped later
// sequence.
func TestChangeTimestampsNeverDecreaseInSequenceOrder(t *testing.T) {
	db, td := nativeStreamDB(t)
	other := streamTestTableWithView(types.StreamViewTypeNewImage)
	past := time.Now().Add(-time.Hour).Unix()
	for i := 0; i < 200; i++ {
		if err := db.PutItemWith(td, textEvent("t", fmt.Sprintf("old-%03d", i), "expired", past), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 60; i++ {
				var err error
				if w%2 == 0 {
					err = db.PutItemWith(td, textEvent("t", fmt.Sprintf("%d-%d", w, i), "live", time.Now().Add(time.Hour).Unix()), PutOptions{})
				} else {
					err = db.PutItemWith(other, types.Item{"id": streamS(fmt.Sprintf("%d-%d", w, i))}, PutOptions{})
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	reaper := NewReaper(db, retentionCatalog{td}, nil, ReaperConfig{BatchSize: 20})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for db.TTLStatus().Deleted < 200 {
			if err := reaper.Tick(context.Background()); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
	lower, upper := storage.PrefixChangeLog()
	it, err := db.db.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	var prev ChangeRecord
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		rec, err := decodeChangeRecord(it.Value())
		if err != nil {
			t.Fatal(err)
		}
		if n > 0 && rec.UnixNano < prev.UnixNano {
			t.Fatalf("sequence %d (%s %s) stamped %d before sequence %d (%s) stamped %d", rec.Index, rec.Table, rec.Op, rec.UnixNano, prev.Index, prev.Table, prev.UnixNano)
		}
		prev = rec
		n++
	}
	if n < 200+6*60+200 {
		t.Fatalf("changelog holds %d records", n)
	}
}

// TestTTLReaperConcurrentWithCursorsNeverSkipsOrDuplicates runs the chunked
// TTL reaper, native writers and a writer on another stream table while
// cursors poll: every committed native record (puts and reaper deletes) is
// observed exactly once by every cursor.
func TestTTLReaperConcurrentWithCursorsNeverSkipsOrDuplicates(t *testing.T) {
	for _, mode := range []string{"local", "jitter"} {
		t.Run(mode, func(t *testing.T) {
			db, td := nativeStreamDB(t)
			if mode == "jitter" {
				db.AttachReplicator(&jitterReplicator{db: db, rng: rand.New(rand.NewSource(11))})
			}
			other := streamTestTableWithView(types.StreamViewTypeNewImage)
			const expired = 3 * ttlReaperLockGroup * 4
			past := time.Now().Add(-time.Hour).Unix()
			for i := 0; i < expired; i++ {
				if err := db.PutItemWith(td, textEvent("t", fmt.Sprintf("old-%04d", i), "expired", past), PutOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			const writers, perWriter, readers = 4, 40, 3
			var wg sync.WaitGroup
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < perWriter; i++ {
						if err := db.PutItemWith(td, textEvent("t", fmt.Sprintf("%d-%d", w, i), "live", time.Now().Add(time.Hour).Unix()), PutOptions{}); err != nil {
							t.Error(err)
							return
						}
					}
				}(w)
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < writers*perWriter/2; i++ {
					if err := db.PutItemWith(other, types.Item{"id": streamS(fmt.Sprint(i))}, PutOptions{}); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			reaper := NewReaper(db, retentionCatalog{td}, nil, ReaperConfig{BatchSize: 2 * ttlReaperLockGroup})
			wg.Add(1)
			go func() {
				defer wg.Done()
				for db.TTLStatus().Deleted < expired {
					if err := reaper.Tick(context.Background()); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			done := make(chan struct{})
			type cursorResult struct {
				seen map[uint64]int
				err  error
			}
			results := make([]cursorResult, readers)
			var rwg sync.WaitGroup
			for r := 0; r < readers; r++ {
				rwg.Add(1)
				go func(r int) {
					defer rwg.Done()
					seen := map[uint64]int{}
					next := uint64(1)
					drain := func() error {
						for {
							recs, n, err := db.StreamRecordsCtx(context.Background(), td.Name, next, 0, 11, 1<<20)
							if err != nil {
								return err
							}
							for _, rec := range recs {
								seen[rec.Index]++
							}
							if n == next {
								return nil
							}
							next = n
						}
					}
					for {
						select {
						case <-done:
							results[r] = cursorResult{seen: seen, err: drain()}
							return
						default:
						}
						if err := drain(); err != nil {
							results[r] = cursorResult{err: err}
							return
						}
					}
				}(r)
			}
			wg.Wait()
			close(done)
			rwg.Wait()
			if t.Failed() {
				return
			}
			want := rawChangeIndexes(t, db, td.Name)
			if len(want) != expired+writers*perWriter+expired {
				t.Fatalf("committed native records = %d, want %d", len(want), expired+writers*perWriter+expired)
			}
			for r, res := range results {
				if res.err != nil {
					t.Fatalf("reader %d: %v", r, res.err)
				}
				for _, idx := range want {
					if res.seen[idx] != 1 {
						t.Fatalf("reader %d observed native sequence %d %d times", r, idx, res.seen[idx])
					}
				}
				if len(res.seen) != len(want) {
					t.Fatalf("reader %d observed %d records, want %d", r, len(res.seen), len(want))
				}
			}
			if s := db.StreamFenceStats(); s.InFlight != 0 || s.Watermark != s.Allocated {
				t.Fatalf("reservations leaked: %+v", s)
			}
		})
	}
}

// TestNativeStreamReservationIsTakenAtAllocation: the reservation is taken
// immediately before the first sequence allocation, so a native writer that
// fails before allocating (condition failure) never registers, and a
// successful one registers exactly once.
func TestNativeStreamReservationIsTakenAtAllocation(t *testing.T) {
	db, td := nativeStreamDB(t)
	expiry := time.Now().Add(time.Hour).Unix()
	item := textEvent("a", "1", "m", expiry)
	if err := db.PutItemWith(td, item, PutOptions{Condition: "attribute_exists(tenantId)"}); !errors.Is(err, storage.ErrConditionFailed) {
		t.Fatalf("conditional put = %v", err)
	}
	if s := db.StreamFenceStats(); s.Registrations != 0 {
		t.Fatalf("writer registered before allocating: %+v", s)
	}
	if err := db.PutItemWith(td, item, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := db.BatchWriteItem(td, []BatchOp{{Op: BatchOpPut, Item: textEvent("a", "2", "m", expiry)}, {Op: BatchOpPut, Item: textEvent("a", "3", "m", expiry)}}); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteItemWith(td, keyItemFromItem(item, td.KeySchema), DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.AtomicUpdate(td, keyItemFromItem(item, td.KeySchema), AtomicOptions{Actions: []AtomicAction{{Kind: AtomicActionSet, Attribute: "message", Value: streamS("again")}, {Kind: AtomicActionSet, Attribute: "expiresAt", Value: types.AttributeValue{T: types.AttrN, N: fmt.Sprint(expiry)}}}}); err != nil {
		t.Fatal(err)
	}
	if s := db.StreamFenceStats(); s.Registrations != 4 || s.InFlight != 0 || s.Watermark != s.Allocated || s.Allocated != 5 {
		t.Fatalf("fence stats = %+v, want 4 registrations for 5 sequences", s)
	}
}

func fenceHasWaiter(db *DB) bool {
	db.streamFence.mu.Lock()
	defer db.streamFence.mu.Unlock()
	return db.streamFence.waiters > 0
}
