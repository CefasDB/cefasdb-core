package pebble

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type retentionCatalog []types.TableDescriptor

func (c retentionCatalog) List() []types.TableDescriptor { return c }

func TestRetentionStalePointerCannotDeleteRenewedRow(t *testing.T) {
	db, err := Open(Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Now()
	td := types.TableDescriptor{Name: "retention", KeySchema: types.KeySchema{PK: "id"}, TTLAttribute: "expiry"}
	old := types.Item{"id": {T: types.AttrS, S: "same"}, "expiry": {T: types.AttrN, N: fmt.Sprint(now.Add(-time.Hour).Unix())}}
	current := types.Item{"id": old["id"], "expiry": {T: types.AttrN, N: fmt.Sprint(now.Add(time.Hour).Unix())}}
	if err := db.PutItemWith(td, current, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	stale, err := ttlKey(td.Name, td.KeySchema, td.TTLAttribute, old)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Set(stale, nil); err != nil {
		t.Fatal(err)
	}
	reaper := NewReaper(db, retentionCatalog{td}, nil, ReaperConfig{Now: func() time.Time { return now }})
	if err := reaper.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := db.GetItem(td.Name, td.KeySchema, types.Item{"id": old["id"]})
	if err != nil || got["expiry"].N != current["expiry"].N {
		t.Fatalf("renewed row deleted or replaced: %v", err)
	}
	if _, err := db.Get(stale); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale pointer was not cleaned: %v", err)
	}
}

func TestRetentionDeletesSecondaryPostings(t *testing.T) {
	for _, mode := range []string{"KEYS_ONLY", "INCLUDE", "ALL"} {
		t.Run(mode, func(t *testing.T) {
			db, err := Open(Options{Path: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			now := time.Now()
			td := types.TableDescriptor{Name: "retention", KeySchema: types.KeySchema{PK: "id"}, TTLAttribute: "expiry", GSIs: []types.GSIDescriptor{{Name: "status", KeySchema: types.KeySchema{PK: "status"}, Projection: types.IndexProjection{Mode: mode, Include: []string{"status"}}}}}
			item := types.Item{"id": {T: types.AttrS, S: "gone"}, "status": {T: types.AttrS, S: "error"}, "expiry": {T: types.AttrN, N: fmt.Sprint(now.Add(-time.Hour).Unix())}}
			if err := db.PutItemWith(td, item, PutOptions{}); err != nil {
				t.Fatal(err)
			}
			r := NewReaper(db, retentionCatalog{td}, nil, ReaperConfig{Now: func() time.Time { return now }})
			if err := r.Tick(context.Background()); err != nil {
				t.Fatal(err)
			}
			rows, err := db.QueryByGSI(td, "status", item["status"], QueryOptions{})
			if err != nil || len(rows) != 0 {
				t.Fatalf("expired secondary result remains: rows=%d err=%v", len(rows), err)
			}
			pk, _ := storage.AttrCanonicalBytes(item["status"])
			low, high := storage.PrefixGSIByPK(td.Name, "status", pk)
			it, err := db.Iter(low, high)
			if err != nil {
				t.Fatal(err)
			}
			defer it.Close()
			if it.First() {
				t.Fatal("orphan secondary posting remains physically")
			}
			if err := it.Error(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type delayedFirstCommit struct {
	db   *DB
	once sync.Once
}

func (*delayedFirstCommit) IsLeader() bool              { return true }
func (*delayedFirstCommit) LeaderHTTPAddr() string      { return "" }
func (*delayedFirstCommit) Barrier(time.Duration) error { return nil }
func (r *delayedFirstCommit) Replicate(b []byte) error {
	r.once.Do(func() { time.Sleep(25 * time.Millisecond) })
	return r.db.ApplyCommittedBatch(b)
}

func TestRetentionConditionalPutHasExactlyOneWinner(t *testing.T) {
	db, err := Open(Options{Path: t.TempDir(), ChangeLogMode: ChangeLogModeOff})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.AttachReplicator(&delayedFirstCommit{db: db})
	td := types.TableDescriptor{Name: "once", KeySchema: types.KeySchema{PK: "id"}}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var winners atomic.Int32
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			err := db.PutItemWith(td, types.Item{"id": {T: types.AttrS, S: "same"}}, PutOptions{Condition: "attribute_not_exists(id)"})
			if err == nil {
				winners.Add(1)
			} else if !errors.Is(err, storage.ErrConditionFailed) {
				t.Errorf("put: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatalf("first write must win exactly once, got %d", winners.Load())
	}
}
