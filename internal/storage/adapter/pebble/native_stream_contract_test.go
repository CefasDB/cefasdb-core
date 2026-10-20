package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func nativeStreamDB(t *testing.T) (*DB, types.TableDescriptor) {
	t.Helper()
	db, td := textDB(t)
	td.StreamSpecification = &types.StreamSpecification{StreamEnabled: true, StreamViewType: types.StreamViewTypeNewImage, RetentionSeconds: 86400}
	return db, td
}

type blockedNativeCommit struct {
	db             *DB
	first, release chan struct{}
	calls          atomic.Int32
}

func (*blockedNativeCommit) IsLeader() bool              { return true }
func (*blockedNativeCommit) LeaderHTTPAddr() string      { return "" }
func (*blockedNativeCommit) Barrier(time.Duration) error { return nil }
func (r *blockedNativeCommit) Replicate(raw []byte) error {
	if r.calls.Add(1) == 1 {
		close(r.first)
		<-r.release
	}
	return r.db.ApplyCommittedBatch(raw)
}

func TestNativeStreamCommitCannotOmitDelayedFirstWrite(t *testing.T) {
	db, td := nativeStreamDB(t)
	r := &blockedNativeCommit{db: db, first: make(chan struct{}), release: make(chan struct{})}
	db.AttachReplicator(r)
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() {
		firstDone <- db.PutItemWith(td, textEvent("a", "1", "first", time.Now().Add(time.Hour).Unix()), PutOptions{})
	}()
	<-r.first
	go func() {
		secondDone <- db.PutItemWith(td, textEvent("a", "2", "second", time.Now().Add(time.Hour).Unix()), PutOptions{})
	}()
	var records []ChangeRecord
	next := uint64(1)
	secondCommitted := false
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
		secondCommitted = true
		records, next, err = db.StreamRecords(td.Name, next, 0, 100, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(30 * time.Millisecond):
	}
	close(r.release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if !secondCommitted {
		if err := <-secondDone; err != nil {
			t.Fatal(err)
		}
	}
	remaining, _, err := db.StreamRecords(td.Name, next, 0, 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	records = append(records, remaining...)
	if len(records) != 2 {
		t.Fatalf("resumed cursor omitted an acknowledged write: got %d records", len(records))
	}
}

func TestNativeStreamImagesExpireAndDeleteWithRow(t *testing.T) {
	for _, mode := range []string{"read-expiry", "delete", "ttl"} {
		t.Run(mode, func(t *testing.T) {
			db, td := nativeStreamDB(t)
			expiry := time.Now().Add(time.Hour).Unix()
			if mode == "read-expiry" {
				expiry = 1
			}
			item := textEvent("a", "1", "custody-secret-value", expiry)
			if err := db.PutItemWith(td, item, PutOptions{}); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "delete":
				if err := db.DeleteItemWith(td, item, DeleteOptions{}); err != nil {
					t.Fatal(err)
				}
			case "ttl":
				r := NewReaper(db, retentionCatalog{td}, nil, ReaperConfig{Now: func() time.Time { return time.Unix(expiry+1, 0) }})
				if err := r.Tick(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			records, _, err := db.StreamRecords(td.Name, 1, 0, 100, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			for _, rec := range records {
				if len(rec.Item)+len(rec.OldItem)+len(rec.NewItem) != 0 {
					t.Fatalf("%s left readable CDC images", mode)
				}
			}
			if mode != "read-expiry" {
				low, high := storage.PrefixChangeLog()
				it, err := db.Iter(low, high)
				if err != nil {
					t.Fatal(err)
				}
				defer it.Close()
				for ok := it.First(); ok; ok = it.Next() {
					if bytes.Contains(it.Value(), []byte("custody-secret-value")) {
						t.Fatal("changelog retains raw image after definitive deletion")
					}
				}
				if err := it.Error(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestNativeStreamChangelogNeverDuplicatesImage(t *testing.T) {
	db, td := nativeStreamDB(t)
	if err := db.PutItemWith(td, textEvent("a", "1", "custody-secret-value", time.Now().Add(time.Hour).Unix()), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	raw, err := db.Get(storage.KeyChangeLog(1))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("custody-secret-value")) {
		t.Fatal("native CDC duplicates image inside global changelog")
	}
	records, _, err := db.StreamRecords(td.Name, 1, 0, 100, 1<<20)
	if err != nil || len(records) != 1 || records[0].NewItem["message"].S != "custody-secret-value" {
		t.Fatalf("live image did not resolve: %d %v", len(records), err)
	}
}

func TestNativeStreamRetentionDropsImagePayloads(t *testing.T) {
	db, td := nativeStreamDB(t)
	if err := db.PutItemWith(td, textEvent("a", "1", "retained", time.Now().Add(48*time.Hour).Unix()), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ApplyStreamRetention(td.Name, time.Now().Add(25*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// The primary row survives; all auxiliary CDC image data must be gone.
	low, high := storage.PrefixTable(td.Name)
	it, err := db.Iter(low, high)
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		if bytes.Contains(it.Key(), []byte("/cdc-custody/")) {
			t.Fatalf("retention orphaned custody payload %s", fmt.Sprintf("%x", it.Key()))
		}
	}
}

func TestNativeStreamPromotedReplicaCannotReuseCommittedSequence(t *testing.T) {
	primary, td := nativeStreamDB(t)
	replica, err := Open(Options{Path: t.TempDir(), FsyncOnCommit: true})
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()
	r := &textReplication{primary: primary, replica: replica, leader: true}
	primary.AttachReplicator(r)
	raw, _ := json.Marshal(td)
	if err := primary.Set(storage.KeyCatalog(td.Name), raw); err != nil {
		t.Fatal(err)
	}
	if err := primary.PutItemWith(td, textEvent("a", "1", "first", time.Now().Add(time.Hour).Unix()), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	// No restart: a follower that has applied the log becomes the new writer.
	replica.AttachReplicator(&textReplication{primary: replica, replica: primary, leader: true})
	if err := replica.PutItemWith(td, textEvent("a", "2", "second", time.Now().Add(time.Hour).Unix()), PutOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, db := range []*DB{primary, replica} {
		records, _, err := db.StreamRecords(td.Name, 1, 0, 100, 1<<20)
		if err != nil || len(records) != 2 {
			t.Fatalf("promotion overwrote committed CDC: records=%d err=%v", len(records), err)
		}
	}
}

func TestNativeStreamOldImagesExpireWithoutTrimmingNewImage(t *testing.T) {
	db, td := nativeStreamDB(t)
	td.StreamSpecification.StreamViewType = types.StreamViewTypeNewAndOldImages
	old := textEvent("a", "1", "expired old image", 1)
	if err := db.PutItemWith(td, old, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	current := textEvent("a", "1", "current image", time.Now().Add(time.Hour).Unix())
	if err := db.PutItemWith(td, current, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ApplyStreamRetention(td.Name, time.Now()); err != nil {
		t.Fatal(err)
	}
	records, next, err := db.StreamRecords(td.Name, 1, 0, 100, 1<<20)
	if err != nil || len(records) != 2 || next != 3 {
		t.Fatalf("image expiry trimmed resumable sequence: %d %d %v", len(records), next, err)
	}
	if len(records[0].NewItem) != 0 || len(records[1].OldItem) != 0 || records[1].NewItem["message"].S != "current image" {
		t.Fatal("old/new expiry was conflated")
	}
	if db.TTLStatus().PayloadDeleted != 2 {
		t.Fatalf("expired image progress missing: %+v", db.TTLStatus())
	}
}

func TestNativeStreamCustodyDeleteIsExactRow(t *testing.T) {
	db, td := nativeStreamDB(t)
	for _, tenant := range []string{"a", "b"} {
		if err := db.PutItemWith(td, textEvent(tenant, "1", "body", time.Now().Add(time.Hour).Unix()), PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.DeleteItemWith(td, textEvent("a", "1", "", 0), DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	records, _, err := db.StreamRecords(td.Name, 1, 0, 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var live int
	for _, rec := range records {
		if len(rec.NewItem) != 0 {
			live++
			if rec.NewItem["tenantId"].S != "b" {
				t.Fatal("deleted tenant payload survived")
			}
		}
	}
	if live != 1 {
		t.Fatalf("unrelated row custody was removed: %d", live)
	}
}

func TestNativeStreamScanBudgetAdvancesPastSingleOversizedLegacyRecord(t *testing.T) {
	db, err := Open(Options{Path: t.TempDir(), StreamRetention: StreamRetentionOptions{Interval: -1}})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	td := streamTestTableWithView(types.StreamViewTypeNewImage)
	if err := db.PutItemWith(td, types.Item{"id": streamS("legacy"), "body": streamS(strings.Repeat("x", 9<<20))}, PutOptions{}); err != nil {
		t.Fatal(err)
	}
	records, next, err := db.StreamRecords(td.Name, 1, 0, 100, 1<<20)
	if err != nil || len(records) != 1 || next != 2 {
		t.Fatalf("oversized record pins cursor forever: records=%d next=%d err=%v", len(records), next, err)
	}
}

func TestNativeStreamCustodyCodecRejectsExpiryOverflow(t *testing.T) {
	rec := ChangeRecord{Index: 42, UnixNano: 1, Op: ChangePut, Table: "logs", PayloadExpiresAt: 123}
	raw, err := encodeChangeRecord(nil, rec)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeChangeRecord(raw)
	if err != nil || got.PayloadExpiresAt != 123 {
		t.Fatalf("custody expiry roundtrip: %v", err)
	}
	binary.BigEndian.PutUint64(raw[len(raw)-8:], ^uint64(0))
	if _, err := decodeChangeRecord(raw); err == nil {
		t.Fatal("expiry overflow became an unbounded negative deadline")
	}
}
