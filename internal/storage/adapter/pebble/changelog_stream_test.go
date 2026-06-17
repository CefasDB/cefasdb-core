package pebble

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func openChangeLogTestDB(t *testing.T) *DB {
	t.Helper()
	return openChangeLogTestDBWithOptions(t, Options{Path: t.TempDir()})
}

func openChangeLogTestDBWithOptions(t *testing.T, opts Options) *DB {
	t.Helper()
	if opts.Path == "" {
		opts.Path = t.TempDir()
	}
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func streamTestTable() types.TableDescriptor {
	return streamTestTableWithView(types.StreamViewTypeNewAndOldImages)
}

func streamTestTableWithView(view string) types.TableDescriptor {
	return types.TableDescriptor{
		Name:      "Events",
		KeySchema: types.KeySchema{PK: "id"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: view,
		},
	}
}

func streamS(s string) types.AttributeValue { return types.AttributeValue{T: types.AttrS, S: s} }
func streamN(n string) types.AttributeValue { return types.AttributeValue{T: types.AttrN, N: n} }

func appendStreamChangeAt(t *testing.T, db *DB, td types.TableDescriptor, id string, ts time.Time) {
	t.Helper()
	item := types.Item{"id": streamS(id), "status": streamS("v-" + id)}
	b := db.Batch()
	defer b.Close()
	rec := newChangeRecord(td, ChangePut, keyItemFromItem(item, td.KeySchema), nil, item)
	rec.UnixNano = ts.UnixNano()
	if _, err := db.appendChangeRecord(b, rec); err != nil {
		t.Fatalf("append change: %v", err)
	}
	if err := db.CommitBatch(b); err != nil {
		t.Fatalf("commit change: %v", err)
	}
	if _, err := db.ApplyStreamRetention(td.Name, ts); err != nil {
		t.Fatalf("apply retention: %v", err)
	}
}

type streamCatalog struct {
	tables []types.TableDescriptor
}

func (c streamCatalog) List() []types.TableDescriptor { return c.tables }

func TestStreamChangeRecordsCaptureImagesAndEventNames(t *testing.T) {
	db := openChangeLogTestDB(t)
	td := streamTestTable()

	first := types.Item{"id": streamS("1"), "status": streamS("new")}
	if err := db.PutItemWith(td, first, PutOptions{}); err != nil {
		t.Fatalf("put first: %v", err)
	}
	updated := types.Item{"id": streamS("1"), "status": streamS("updated")}
	if err := db.PutItemWith(td, updated, PutOptions{}); err != nil {
		t.Fatalf("put updated: %v", err)
	}
	if err := db.DeleteItemWith(td, types.Item{"id": streamS("1")}, DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	records, err := db.changeRecordsAfter(td.Name, 0, 0, 0)
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("record count = %d, want 3: %+v", len(records), records)
	}

	insert := records[0]
	if !insert.StreamRecord || insert.EventName != ChangeEventInsert || insert.SequenceNumber != "1" {
		t.Fatalf("insert metadata = %+v", insert)
	}
	if insert.NewItem["status"].S != "new" || insert.OldItem != nil {
		t.Fatalf("insert images = old %+v new %+v", insert.OldItem, insert.NewItem)
	}
	if insert.StreamViewType != types.StreamViewTypeNewAndOldImages || insert.SizeBytes <= 0 {
		t.Fatalf("insert stream view/size = %+v", insert)
	}

	modify := records[1]
	if !modify.StreamRecord || modify.EventName != ChangeEventModify || modify.SequenceNumber != "2" {
		t.Fatalf("modify metadata = %+v", modify)
	}
	if modify.OldItem["status"].S != "new" || modify.NewItem["status"].S != "updated" {
		t.Fatalf("modify images = old %+v new %+v", modify.OldItem, modify.NewItem)
	}

	remove := records[2]
	if !remove.StreamRecord || remove.EventName != ChangeEventRemove || remove.SequenceNumber != "3" {
		t.Fatalf("remove metadata = %+v", remove)
	}
	if remove.OldItem["status"].S != "updated" || remove.NewItem != nil {
		t.Fatalf("remove images = old %+v new %+v", remove.OldItem, remove.NewItem)
	}
}

func TestChangeLogKeepsPITRRecordWithoutStreamRecord(t *testing.T) {
	db := openChangeLogTestDB(t)
	td := types.TableDescriptor{Name: "Events", KeySchema: types.KeySchema{PK: "id"}}
	if err := db.PutItemWith(td, types.Item{"id": streamS("1"), "status": streamS("new")}, PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	records, err := db.changeRecordsAfter(td.Name, 0, 0, 0)
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("record count = %d, want 1", len(records))
	}
	rec := records[0]
	if rec.StreamRecord || rec.EventName != "" || rec.NewItem != nil || rec.OldItem != nil {
		t.Fatalf("disabled stream fields should be empty: %+v", rec)
	}
	if rec.Op != ChangePut || rec.Item["status"].S != "new" {
		t.Fatalf("PITR fields not preserved: %+v", rec)
	}
}

func TestStreamViewTypeControlsCapturedImages(t *testing.T) {
	cases := []struct {
		view    string
		wantOld bool
		wantNew bool
	}{
		{view: types.StreamViewTypeKeysOnly},
		{view: types.StreamViewTypeNewImage, wantNew: true},
		{view: types.StreamViewTypeOldImage, wantOld: true},
		{view: types.StreamViewTypeNewAndOldImages, wantOld: true, wantNew: true},
	}
	for _, tc := range cases {
		t.Run(tc.view, func(t *testing.T) {
			db := openChangeLogTestDB(t)
			td := streamTestTableWithView(tc.view)
			first := types.Item{"id": streamS("1"), "status": streamS("new")}
			if err := db.PutItemWith(td, first, PutOptions{}); err != nil {
				t.Fatalf("put first: %v", err)
			}
			updated := types.Item{"id": streamS("1"), "status": streamS("updated")}
			if err := db.PutItemWith(td, updated, PutOptions{}); err != nil {
				t.Fatalf("put updated: %v", err)
			}
			records, err := db.changeRecordsAfter(td.Name, 0, 0, 0)
			if err != nil {
				t.Fatalf("records: %v", err)
			}
			if len(records) != 2 {
				t.Fatalf("record count = %d, want 2", len(records))
			}
			modify := records[1]
			if modify.EventName != ChangeEventModify {
				t.Fatalf("event = %q", modify.EventName)
			}
			if gotOld := modify.OldItem != nil; gotOld != tc.wantOld {
				t.Fatalf("old image present = %v, want %v: %+v", gotOld, tc.wantOld, modify)
			}
			if gotNew := modify.NewItem != nil; gotNew != tc.wantNew {
				t.Fatalf("new image present = %v, want %v: %+v", gotNew, tc.wantNew, modify)
			}
		})
	}
}

func TestFailedConditionalWriteDoesNotAppendStreamRecord(t *testing.T) {
	db := openChangeLogTestDB(t)
	td := streamTestTable()
	item := types.Item{"id": streamS("1"), "status": streamS("new")}
	if err := db.PutItemWith(td, item, PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	err := db.PutItemWith(td, item, PutOptions{Condition: "attribute_not_exists(id)"})
	if !errors.Is(err, storage.ErrConditionFailed) {
		t.Fatalf("want storage.ErrConditionFailed, got %v", err)
	}
	records, err := db.changeRecordsAfter(td.Name, 0, 0, 0)
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("record count = %d, want 1 after failed condition", len(records))
	}
}

func TestDeleteMissingItemDoesNotEmitStreamRecord(t *testing.T) {
	db := openChangeLogTestDB(t)
	td := streamTestTable()
	if err := db.DeleteItemWith(td, types.Item{"id": streamS("missing")}, DeleteOptions{}); err != nil {
		t.Fatalf("delete missing: %v", err)
	}

	all, err := db.changeRecordsAfter(td.Name, 0, 0, 0)
	if err != nil {
		t.Fatalf("pitr records: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("physical changelog records = %d, want 1", len(all))
	}
	rec := all[0]
	if rec.StreamRecord || rec.EventName != "" || rec.OldItem != nil || rec.NewItem != nil {
		t.Fatalf("missing delete should not expose DynamoDB Streams fields: %+v", rec)
	}
	if rec.Op != ChangeDelete || rec.Key["id"].S != "missing" {
		t.Fatalf("PITR delete record not preserved: %+v", rec)
	}

	streamRecords, next, err := db.StreamRecords(td.Name, 1, 0, 10, 0)
	if err != nil {
		t.Fatalf("stream records: %v", err)
	}
	if len(streamRecords) != 0 {
		t.Fatalf("stream record count = %d, want 0: %+v", len(streamRecords), streamRecords)
	}
	if next != 2 {
		t.Fatalf("next sequence = %d, want 2 after skipped physical record", next)
	}
}

func TestBatchWriteItemEmitsOrderedStreamRecords(t *testing.T) {
	db := openChangeLogTestDB(t)
	td := streamTestTable()
	ops := []BatchOp{
		{Op: BatchOpPut, Item: types.Item{"id": streamS("1"), "status": streamS("one")}},
		{Op: BatchOpPut, Item: types.Item{"id": streamS("2"), "status": streamS("two")}},
	}
	if err := db.BatchWriteItem(td, ops); err != nil {
		t.Fatalf("batch: %v", err)
	}
	records, err := db.changeRecordsAfter(td.Name, 0, 0, 0)
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("record count = %d, want 2", len(records))
	}
	for i, rec := range records {
		if rec.EventName != ChangeEventInsert {
			t.Fatalf("record %d event = %q", i, rec.EventName)
		}
		wantSeq := strconv.Itoa(i + 1)
		if rec.SequenceNumber != wantSeq {
			t.Fatalf("record %d sequence = %q, want %q", i, rec.SequenceNumber, wantSeq)
		}
	}
}

func TestTTLReaperEmitsRemoveStreamRecord(t *testing.T) {
	db := openChangeLogTestDB(t)
	now := time.Now()
	td := streamTestTable()
	td.TTLAttribute = "expires_at"

	item := types.Item{
		"id":         streamS("expired"),
		"status":     streamS("gone"),
		"expires_at": streamN(fmt.Sprintf("%d", now.Add(-time.Hour).Unix())),
	}
	if err := db.PutItemWith(td, item, PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	fromIndex, err := db.CurrentChangeIndex()
	if err != nil {
		t.Fatalf("current index: %v", err)
	}

	reaper := NewReaper(db, streamCatalog{tables: []types.TableDescriptor{td}}, nil, ReaperConfig{
		BatchSize: 1024,
		Now:       func() time.Time { return now },
	})
	if err := reaper.Tick(context.Background()); err != nil {
		t.Fatalf("tick: %v", err)
	}

	records, err := db.changeRecordsAfter(td.Name, fromIndex, 0, 0)
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("record count = %d, want 1", len(records))
	}
	rec := records[0]
	if !rec.StreamRecord || rec.EventName != ChangeEventRemove {
		t.Fatalf("ttl record metadata = %+v", rec)
	}
	if rec.OldItem["status"].S != "gone" || rec.NewItem != nil {
		t.Fatalf("ttl remove images = old %+v new %+v", rec.OldItem, rec.NewItem)
	}
}

func TestStreamRetentionTrimsOldRecordsLogically(t *testing.T) {
	db := openChangeLogTestDBWithOptions(t, Options{
		Path:            t.TempDir(),
		StreamRetention: StreamRetentionOptions{Retention: time.Hour},
	})
	td := streamTestTable()
	now := time.Unix(1_700_000_000, 0)
	appendStreamChangeAt(t, db, td, "old", now.Add(-2*time.Hour))
	appendStreamChangeAt(t, db, td, "current", now)

	stats, err := db.ApplyStreamRetention(td.Name, now)
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	if stats.OldestSequence != 2 ||
		stats.NewestSequence != 2 ||
		stats.RecordsAppended != 2 ||
		stats.RecordsTrimmed != 1 ||
		stats.RetainedBytes <= 0 {
		t.Fatalf("stats = %+v", stats)
	}

	if _, _, err := db.StreamRecords(td.Name, 1, 0, 10, 0); !errors.Is(err, types.ErrStreamTrimmed) {
		t.Fatalf("old stream read err = %v, want ErrStreamTrimmed", err)
	}
	records, next, err := db.StreamRecords(td.Name, 2, 0, 10, 0)
	if err != nil {
		t.Fatalf("current stream read: %v", err)
	}
	if len(records) != 1 || records[0].Key["id"].S != "current" || next != 3 {
		t.Fatalf("records=%+v next=%d", records, next)
	}

	all, err := db.changeRecordsAfter(td.Name, 0, 0, 0)
	if err != nil {
		t.Fatalf("pitr records: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("physical changelog records = %d, want 2", len(all))
	}
}

func TestStreamRetentionMetadataSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	td := streamTestTable()
	now := time.Unix(1_700_000_100, 0)
	db, err := Open(Options{
		Path:            dir,
		StreamRetention: StreamRetentionOptions{Retention: time.Hour},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	appendStreamChangeAt(t, db, td, "old", now.Add(-2*time.Hour))
	appendStreamChangeAt(t, db, td, "current", now)
	if _, err := db.ApplyStreamRetention(td.Name, now); err != nil {
		t.Fatalf("retention: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(Options{
		Path:            dir,
		StreamRetention: StreamRetentionOptions{Retention: time.Hour},
	})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	stats, err := reopened.StreamRetentionStats(td.Name)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.OldestSequence != 2 || stats.NewestSequence != 2 || stats.RecordsTrimmed != 1 {
		t.Fatalf("reopened stats = %+v", stats)
	}
}

func TestStreamRetentionMaxBytesBoundsRetainedRecords(t *testing.T) {
	db := openChangeLogTestDBWithOptions(t, Options{
		Path:            t.TempDir(),
		StreamRetention: StreamRetentionOptions{Retention: 24 * time.Hour, MaxBytes: 1},
	})
	td := streamTestTable()
	now := time.Unix(1_700_000_200, 0)
	appendStreamChangeAt(t, db, td, "one", now)
	appendStreamChangeAt(t, db, td, "two", now.Add(time.Second))

	stats, err := db.ApplyStreamRetention(td.Name, now.Add(time.Second))
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	if stats.OldestSequence != 2 || stats.RecordsTrimmed != 1 {
		t.Fatalf("stats = %+v, want only newest retained under byte cap", stats)
	}
}
