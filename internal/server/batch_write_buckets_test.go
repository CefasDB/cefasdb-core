package server

import (
	"testing"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestBatchWriteBucketsWritesEachBucket(t *testing.T) {
	db1 := openBatchWriteBucketTestDB(t)
	defer db1.Close()
	db2 := openBatchWriteBucketTestDB(t)
	defer db2.Close()

	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	buckets := map[*pebble.DB][]pebble.BatchOp{
		db1: {
			{Op: pebble.BatchOpPut, Item: types.Item{"id": stringAttr("1"), "value": stringAttr("old")}},
			{Op: pebble.BatchOpPut, Item: types.Item{"id": stringAttr("1"), "value": stringAttr("new")}},
		},
		db2: {
			{Op: pebble.BatchOpPut, Item: types.Item{"id": stringAttr("2"), "value": stringAttr("other")}},
		},
	}

	if err := batchWriteBuckets(td, buckets); err != nil {
		t.Fatalf("batchWriteBuckets: %v", err)
	}
	assertBatchWriteBucketValue(t, db1, td, "1", "new")
	assertBatchWriteBucketValue(t, db2, td, "2", "other")
}

func openBatchWriteBucketTestDB(t *testing.T) *pebble.DB {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir(), ChangeLogMode: pebble.ChangeLogModeStreamsOnly})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func assertBatchWriteBucketValue(t *testing.T, db *pebble.DB, td types.TableDescriptor, id, want string) {
	t.Helper()
	got, err := db.GetItem(td.Name, td.KeySchema, types.Item{"id": stringAttr(id)})
	if err != nil {
		t.Fatal(err)
	}
	if got["value"].S != want {
		t.Fatalf("value for %s = %q, want %q", id, got["value"].S, want)
	}
}

func stringAttr(s string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: s}
}
