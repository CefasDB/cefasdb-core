package pebble

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestPlainPutPriorReadFastPathEligibility(t *testing.T) {
	db, err := Open(Options{Path: t.TempDir(), ChangeLogMode: ChangeLogModeStreamsOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	simple := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	if !db.putItemCanSkipPrior(simple, "") {
		t.Fatal("simple non-streaming put should skip prior read")
	}
	if !db.batchPutItemsCanSkipPrior(simple, []BatchOp{{Op: BatchOpPut, Item: types.Item{"id": {T: types.AttrS, S: "1"}}}}) {
		t.Fatal("simple put batch should skip prior reads")
	}
	if db.putItemCanSkipPrior(simple, "attribute_not_exists(id)") {
		t.Fatal("conditional put must read prior item")
	}
	if db.batchPutItemsCanSkipPrior(simple, []BatchOp{{Op: BatchOpDelete, Key: types.Item{"id": {T: types.AttrS, S: "1"}}}}) {
		t.Fatal("delete batch must read prior item")
	}

	withIndex := simple
	withIndex.GSIs = []types.GSIDescriptor{{
		Name:      "by_status",
		KeySchema: types.KeySchema{PK: "status"},
	}}
	if db.putItemCanSkipPrior(withIndex, "") {
		t.Fatal("indexed put must read prior item")
	}

	withTTL := simple
	withTTL.TTLAttribute = "expires_at"
	if db.putItemCanSkipPrior(withTTL, "") {
		t.Fatal("TTL put must read prior item")
	}

	withStream := simple
	withStream.StreamSpecification = &types.StreamSpecification{StreamEnabled: true}
	if db.putItemCanSkipPrior(withStream, "") {
		t.Fatal("streaming put must read prior item")
	}

	defaultLogDB, err := Open(Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer defaultLogDB.Close()
	if defaultLogDB.putItemCanSkipPrior(simple, "") {
		t.Fatal("default changelog mode must read prior item")
	}
}

func TestBatchPutWithoutPriorOverwritesPrimaryRows(t *testing.T) {
	db, err := Open(Options{Path: t.TempDir(), ChangeLogMode: ChangeLogModeStreamsOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	if err := db.BatchWriteItem(td, []BatchOp{
		{Op: BatchOpPut, Item: types.Item{"id": {T: types.AttrS, S: "1"}, "value": {T: types.AttrS, S: "old"}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.BatchWriteItem(td, []BatchOp{
		{Op: BatchOpPut, Item: types.Item{"id": {T: types.AttrS, S: "1"}, "value": {T: types.AttrS, S: "new"}}},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := db.GetItem(td.Name, td.KeySchema, types.Item{"id": {T: types.AttrS, S: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if got["value"].S != "new" {
		t.Fatalf("value = %q, want new", got["value"].S)
	}
}
