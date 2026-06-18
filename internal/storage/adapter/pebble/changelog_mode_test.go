package pebble

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestChangeLogStreamsOnlySkipsNonStreamRecords(t *testing.T) {
	db := openChangeLogTestDBWithOptions(t, Options{ChangeLogMode: ChangeLogModeStreamsOnly})
	td := types.TableDescriptor{Name: "Events", KeySchema: types.KeySchema{PK: "id"}}

	if err := db.PutItemWith(td, types.Item{"id": streamS("1"), "status": streamS("new")}, PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}

	records, err := db.changeRecordsAfter(td.Name, 0, 0, 0)
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("record count = %d, want 0: %+v", len(records), records)
	}
	index, err := db.CurrentChangeIndex()
	if err != nil {
		t.Fatalf("current index: %v", err)
	}
	if index != 0 {
		t.Fatalf("current index = %d, want 0", index)
	}
}

func TestChangeLogStreamsOnlyKeepsStreamRecords(t *testing.T) {
	db := openChangeLogTestDBWithOptions(t, Options{ChangeLogMode: ChangeLogModeStreamsOnly})
	td := streamTestTable()

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
	if !rec.StreamRecord || rec.EventName != ChangeEventInsert || rec.SequenceNumber != "1" {
		t.Fatalf("stream metadata = %+v", rec)
	}

	streamRecords, next, err := db.StreamRecords(td.Name, 1, 0, 10, 0)
	if err != nil {
		t.Fatalf("stream records: %v", err)
	}
	if len(streamRecords) != 1 || next != 2 {
		t.Fatalf("stream records=%+v next=%d, want 1 record and next 2", streamRecords, next)
	}
}

func TestChangeLogOffSkipsStreamRecords(t *testing.T) {
	db := openChangeLogTestDBWithOptions(t, Options{ChangeLogMode: ChangeLogModeOff})
	td := streamTestTable()

	if err := db.PutItemWith(td, types.Item{"id": streamS("1"), "status": streamS("new")}, PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}

	records, err := db.changeRecordsAfter(td.Name, 0, 0, 0)
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("record count = %d, want 0: %+v", len(records), records)
	}
	streamRecords, next, err := db.StreamRecords(td.Name, 1, 0, 10, 0)
	if err != nil {
		t.Fatalf("stream records: %v", err)
	}
	if len(streamRecords) != 0 || next != 1 {
		t.Fatalf("stream records=%+v next=%d, want no records and next 1", streamRecords, next)
	}
}
