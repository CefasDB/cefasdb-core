package pebble

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// recordingReplicator applies batches locally and records each commit size.
type recordingReplicator struct {
	db     *DB
	mu     sync.Mutex
	sizes  []int
	failAt int // 1-based commit number that fails; 0 = never
}

func (*recordingReplicator) IsLeader() bool              { return true }
func (*recordingReplicator) LeaderHTTPAddr() string      { return "" }
func (*recordingReplicator) Barrier(time.Duration) error { return nil }
func (r *recordingReplicator) Replicate(raw []byte) error {
	r.mu.Lock()
	r.sizes = append(r.sizes, len(raw))
	n := len(r.sizes)
	r.mu.Unlock()
	if r.failAt > 0 && n == r.failAt {
		return errors.New("fixture commit failure")
	}
	return r.db.ApplyCommittedBatch(raw)
}

func (r *recordingReplicator) commits() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.sizes...)
}

func countPrefix(t *testing.T, db *DB, low, high []byte) int {
	t.Helper()
	it, err := db.Iter(low, high)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	return n
}

func TestTTLReaperCommitsBoundedChunksWithExactCounters(t *testing.T) {
	words := strings.Fields("alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa quebec romeo sierra tango uniform victor whiskey xray yankee zulu")
	message := func(i int) string {
		var b strings.Builder
		for j := 0; j < 60; j++ {
			fmt.Fprintf(&b, "%s%d ", words[(i+j)%len(words)], j)
		}
		return b.String()
	}
	cases := []struct {
		name   string
		native bool
	}{{"native", true}, {"inline-old-image", false}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var db *DB
			var td types.TableDescriptor
			if tc.native {
				db, td = nativeStreamDB(t)
			} else {
				var err error
				db, err = Open(Options{Path: t.TempDir(), StreamRetention: StreamRetentionOptions{Interval: -1}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				td = streamTestTableWithView(types.StreamViewTypeNewAndOldImages)
				td.TTLAttribute = "expiresAt"
				raw, _ := json.Marshal(td)
				if err := db.Set(storage.KeyCatalog(td.Name), raw); err != nil {
					t.Fatal(err)
				}
			}
			const expired, live = 300, 5
			past, future := time.Now().Add(-time.Hour).Unix(), time.Now().Add(time.Hour).Unix()
			for i := 0; i < expired+live; i++ {
				expiry := past
				if i >= expired {
					expiry = future
				}
				var item types.Item
				if tc.native {
					item = textEvent("t", fmt.Sprintf("%04d", i), message(i), expiry)
				} else {
					item = types.Item{"id": streamS(fmt.Sprintf("%04d", i)), "body": streamS(strings.Repeat("x", 20000)), "expiresAt": {T: types.AttrN, N: fmt.Sprint(expiry)}}
				}
				if err := db.PutItemWith(td, item, PutOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			// A stale pointer whose row was renewed must be cleaned, counted
			// as stale, and must not delete the renewed row.
			renewed := types.Item{"id": streamS("0000")}
			if tc.native {
				renewed = textEvent("t", "0000", "renewed", future)
			} else {
				renewed["expiresAt"] = types.AttributeValue{T: types.AttrN, N: fmt.Sprint(future)}
			}
			if err := db.PutItemWith(td, renewed, PutOptions{}); err != nil {
				t.Fatal(err)
			}
			staleItem := cloneChangeItem(renewed)
			staleItem["expiresAt"] = types.AttributeValue{T: types.AttrN, N: fmt.Sprint(past)}
			stale, err := ttlKey(td.Name, td.KeySchema, td.TTLAttribute, staleItem)
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Set(stale, nil); err != nil {
				t.Fatal(err)
			}
			before, err := db.CurrentChangeIndex()
			if err != nil {
				t.Fatal(err)
			}
			rec := &recordingReplicator{db: db}
			db.AttachReplicator(rec)
			reaper := NewReaper(db, retentionCatalog{td}, nil, ReaperConfig{BatchSize: 5000})
			if err := reaper.Tick(context.Background()); err != nil {
				t.Fatal(err)
			}
			commits := rec.commits()
			if len(commits) < 2 {
				t.Fatalf("sweep committed %d chunk(s); fixture should need several", len(commits))
			}
			for i, size := range commits {
				if size > ttlReaperMaxChunkBytes {
					t.Fatalf("chunk %d is %d bytes > %d", i, size, ttlReaperMaxChunkBytes)
				}
			}
			st := db.TTLStatus()
			if st.Deleted != expired-1 || st.StalePointers != 1 || st.Sweeps != 1 || st.Failures != 0 || st.BacklogPresent {
				t.Fatalf("ttl status = %+v", st)
			}
			after, _ := db.CurrentChangeIndex()
			if after-before != expired-1 {
				t.Fatalf("allocated %d delete records, want %d", after-before, expired-1)
			}
			if s := db.StreamFenceStats(); s.InFlight != 0 || s.Watermark != after {
				t.Fatalf("fence after sweep = %+v", s)
			}
			low, high := storage.PrefixTTLBefore(td.Name, uint64(future+1))
			if n := countPrefix(t, db, low, high); n != live+1 {
				t.Fatalf("ttl pointers left = %d, want %d", n, live+1)
			}
			if _, err := db.GetItem(td.Name, td.KeySchema, keyItemFromItem(renewed, td.KeySchema)); err != nil {
				t.Fatalf("renewed row deleted: %v", err)
			}
			recs, _, err := db.StreamRecords(td.Name, before+1, 0, 1000, 64<<20)
			if err != nil || uint64(len(recs)) != expired-1 {
				t.Fatalf("delete stream records = %d %v", len(recs), err)
			}
		})
	}
}

func TestTTLReaperFailedChunkCountsOnlyCommittedProgress(t *testing.T) {
	db, td := nativeStreamDB(t)
	past := time.Now().Add(-time.Hour).Unix()
	const n = 3 * ttlReaperLockGroup
	for i := 0; i < n; i++ {
		if err := db.PutItemWith(td, textEvent("t", fmt.Sprintf("%04d", i), "short message", past), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	// Commit 1 is the expired-image cleanup, commit 2 the first lock group;
	// commit 3 (the second group) fails.
	rec := &recordingReplicator{db: db, failAt: 3}
	db.AttachReplicator(rec)
	reaper := NewReaper(db, retentionCatalog{td}, nil, ReaperConfig{BatchSize: 5000})
	if err := reaper.Tick(context.Background()); err == nil {
		t.Fatal("expected the second chunk to fail")
	}
	st := db.TTLStatus()
	if st.Deleted != ttlReaperLockGroup || st.Failures != 1 || st.Sweeps != 0 {
		t.Fatalf("ttl status after partial sweep = %+v", st)
	}
	if s := db.StreamFenceStats(); s.InFlight != 0 {
		t.Fatalf("failed chunk leaked a reservation: %+v", s)
	}
	// The next sweep resumes and finishes the remaining victims.
	rec.failAt = 0
	if err := reaper.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if st := db.TTLStatus(); st.Deleted != n || st.Sweeps != 1 {
		t.Fatalf("ttl status after resume = %+v", st)
	}
}
