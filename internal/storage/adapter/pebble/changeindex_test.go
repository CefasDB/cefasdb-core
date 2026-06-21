package pebble

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestChangeIndexMonotonicConcurrent(t *testing.T) {
	db := openChangeLogTestDBWithOptions(t, Options{
		ChangeLogMode:   ChangeLogModeStreamsOnly,
		StreamRetention: StreamRetentionOptions{Interval: -1},
	})
	td := streamTestTable()

	const writers, perWriter = 8, 50
	errCh := make(chan error, writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			for i := 0; i < perWriter; i++ {
				if err := db.PutItemWith(td, types.Item{
					"id":     streamS("k"),
					"writer": streamN("0"),
				}, PutOptions{}); err != nil {
					errCh <- err
					return
				}
			}
			errCh <- nil
		}(w)
	}
	for w := 0; w < writers; w++ {
		if err := <-errCh; err != nil {
			t.Fatalf("writer error: %v", err)
		}
	}

	idx, err := db.CurrentChangeIndex()
	if err != nil {
		t.Fatalf("CurrentChangeIndex: %v", err)
	}
	if want := uint64(writers * perWriter); idx != want {
		t.Fatalf("changeIndex = %d, want %d", idx, want)
	}
}

func TestSeedChangeIndexRecoversFromScan(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{
		Path:            dir,
		ChangeLogMode:   ChangeLogModeStreamsOnly,
		StreamRetention: StreamRetentionOptions{Interval: -1},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	td := streamTestTable()
	for i := 0; i < 5; i++ {
		if err := db.PutItemWith(td, types.Item{"id": streamS("x"), "v": streamS("y")}, PutOptions{}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if idx, _ := db.CurrentChangeIndex(); idx != 5 {
		t.Fatalf("pre-close index = %d, want 5", idx)
	}
	_ = db.Close()

	// Reopen — seedChangeIndex must restore counter from persisted state.
	db2, err := Open(Options{
		Path:            dir,
		ChangeLogMode:   ChangeLogModeStreamsOnly,
		StreamRetention: StreamRetentionOptions{Interval: -1},
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })
	if idx, _ := db2.CurrentChangeIndex(); idx != 5 {
		t.Fatalf("post-reopen index = %d, want 5", idx)
	}

	// Next append must use index 6 (no overlap with existing keys).
	b := db2.Batch()
	defer b.Close()
	rec, err := db2.appendChangeRecord(b, newChangeRecord(td, ChangePut,
		types.Item{"id": streamS("x")}, nil,
		types.Item{"id": streamS("x"), "v": streamS("z")}))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if rec.Index != 6 {
		t.Fatalf("next index = %d, want 6", rec.Index)
	}
}
