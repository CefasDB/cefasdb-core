package storage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

func openChangeLogTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(Options{Path: t.TempDir()})
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
	if !errors.Is(err, ErrConditionFailed) {
		t.Fatalf("want ErrConditionFailed, got %v", err)
	}
	records, err := db.changeRecordsAfter(td.Name, 0, 0, 0)
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("record count = %d, want 1 after failed condition", len(records))
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
