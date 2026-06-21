package pebble

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/types"
)

func sampleRecord() ChangeRecord {
	return ChangeRecord{
		Index:          42,
		SequenceNumber: "42",
		UnixNano:       1_700_000_000_000_000_000,
		Op:             ChangePut,
		Table:          "Events",
		Key:            types.Item{"id": streamS("k1")},
		Item:           types.Item{"id": streamS("k1"), "status": streamS("v")},
		StreamRecord:   true,
		EventName:      ChangeEventModify,
		OldItem:        types.Item{"id": streamS("k1"), "status": streamS("old")},
		NewItem:        types.Item{"id": streamS("k1"), "status": streamS("v")},
		StreamViewType: types.StreamViewTypeNewAndOldImages,
		SizeBytes:      256,
	}
}

func TestChangeRecordRoundTripBinary(t *testing.T) {
	rec := sampleRecord()
	raw, err := encodeChangeRecord(nil, rec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if raw[0] != changeFmtBinaryV1 {
		t.Fatalf("format byte = 0x%02x, want 0x%02x", raw[0], changeFmtBinaryV1)
	}
	got, err := decodeChangeRecord(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	assertChangeRecordEqual(t, rec, got)
}

func TestChangeRecordDecodeLegacyJSON(t *testing.T) {
	rec := sampleRecord()
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if raw[0] != '{' {
		t.Fatalf("JSON payload must start with '{', got 0x%02x", raw[0])
	}
	got, err := decodeChangeRecord(raw)
	if err != nil {
		t.Fatalf("decode legacy: %v", err)
	}
	assertChangeRecordEqual(t, rec, got)
}

func TestChangeRecordDecodeUnknownFormat(t *testing.T) {
	_, err := decodeChangeRecord([]byte{0xFF, 0x00})
	if err == nil {
		t.Fatal("expected error for unknown format byte")
	}
}

func TestChangeRecordRoundTripDelete(t *testing.T) {
	rec := ChangeRecord{
		Index:    7,
		UnixNano: 1,
		Op:       ChangeDelete,
		Table:    "Events",
		Key:      types.Item{"id": streamS("k1")},
	}
	raw, err := encodeChangeRecord(nil, rec)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := decodeChangeRecord(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	assertChangeRecordEqual(t, rec, got)
}

// TestPutItemWithStreamRoundTripsViaStorage exercises the full path:
// write through PutItemWith (binary v1), read back via changeRecordsAfter,
// and assert the decoded record matches what went in.
func TestPutItemWithStreamRoundTripsViaStorage(t *testing.T) {
	db := openChangeLogTestDBWithOptions(t, Options{
		ChangeLogMode:   ChangeLogModeStreamsOnly,
		StreamRetention: StreamRetentionOptions{Interval: -1},
	})
	td := streamTestTable()
	item := types.Item{"id": streamS("k1"), "status": streamS("ok")}
	if err := db.PutItemWith(td, item, PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	records, err := db.changeRecordsAfter(td.Name, 0, 0, 0)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	got := records[0]
	if got.Op != ChangePut || got.Table != td.Name || !got.StreamRecord {
		t.Fatalf("record mismatch: %+v", got)
	}
	if got.NewItem["status"].S != "ok" {
		t.Fatalf("NewItem = %+v", got.NewItem)
	}
}

func assertChangeRecordEqual(t *testing.T, want, got ChangeRecord) {
	t.Helper()
	if want.Index != got.Index ||
		want.SequenceNumber != got.SequenceNumber ||
		want.UnixNano != got.UnixNano ||
		want.Op != got.Op ||
		want.Table != got.Table ||
		want.StreamRecord != got.StreamRecord ||
		want.EventName != got.EventName ||
		want.StreamViewType != got.StreamViewType ||
		want.SizeBytes != got.SizeBytes {
		t.Fatalf("scalar mismatch\nwant=%+v\ngot=%+v", want, got)
	}
	if !reflect.DeepEqual(want.Key, got.Key) {
		t.Fatalf("Key mismatch\nwant=%+v\ngot=%+v", want.Key, got.Key)
	}
	if !reflect.DeepEqual(want.Item, got.Item) {
		t.Fatalf("Item mismatch\nwant=%+v\ngot=%+v", want.Item, got.Item)
	}
	if !reflect.DeepEqual(want.OldItem, got.OldItem) {
		t.Fatalf("OldItem mismatch\nwant=%+v\ngot=%+v", want.OldItem, got.OldItem)
	}
	if !reflect.DeepEqual(want.NewItem, got.NewItem) {
		t.Fatalf("NewItem mismatch\nwant=%+v\ngot=%+v", want.NewItem, got.NewItem)
	}
}
