package replication

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"
	cpebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// TestFSMRestoreReseedsChangeIndex: installing a Raft snapshot raises the
// applier's change-sequence allocator to the installed changelog, so a
// promoted node neither reuses a sequence nor holds a watermark below the
// records it already serves.
func TestFSMRestoreReseedsChangeIndex(t *testing.T) {
	td := types.TableDescriptor{
		Name:                "logs",
		KeySchema:           types.KeySchema{PK: "id"},
		TextIndexes:         []types.TextIndexDescriptor{{Name: "message", Field: "message"}},
		StreamSpecification: &types.StreamSpecification{StreamEnabled: true, StreamViewType: types.StreamViewTypeNewImage},
	}
	open := func() *cpebble.DB {
		db, err := cpebble.Open(cpebble.Options{Path: t.TempDir()})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		raw, _ := json.Marshal(td)
		if err := db.Set(storage.KeyCatalog(td.Name), raw); err != nil {
			t.Fatal(err)
		}
		return db
	}
	src := open()
	for i := 0; i < 7; i++ {
		item := types.Item{"id": {T: types.AttrS, S: fmt.Sprint(i)}, "message": {T: types.AttrS, S: "hello"}}
		if err := src.PutItemWith(td, item, cpebble.PutOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	snap := src.Raw().NewSnapshot()
	var buf bytes.Buffer
	if err := writeSnapshot(&buf, snap); err != nil {
		t.Fatal(err)
	}
	_ = snap.Close()

	dst := open()
	if got, _ := dst.CurrentChangeIndex(); got != 0 {
		t.Fatalf("fresh allocator = %d", got)
	}
	f := newFSM(dst.Raw(), dst)
	if err := f.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}
	if got, _ := dst.CurrentChangeIndex(); got != 7 {
		t.Fatalf("allocator after snapshot install = %d, want 7", got)
	}
	if s := dst.StreamFenceStats(); s.Watermark != 7 {
		t.Fatalf("watermark after snapshot install = %+v", s)
	}
	recs, next, err := dst.StreamRecords(td.Name, 1, 0, 100, 1<<20)
	if err != nil || len(recs) != 7 || next != 8 {
		t.Fatalf("installed stream: records=%d next=%d err=%v", len(recs), next, err)
	}
	item := types.Item{"id": {T: types.AttrS, S: "new"}, "message": {T: types.AttrS, S: "after"}, "at": {T: types.AttrN, N: fmt.Sprint(time.Now().Unix())}}
	if err := dst.PutItemWith(td, item, cpebble.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := dst.CurrentChangeIndex(); got != 8 {
		t.Fatalf("first allocation after install = %d, want 8 (no sequence reuse)", got)
	}
}
