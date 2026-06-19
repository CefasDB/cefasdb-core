package pebble_test

import (
	"fmt"
	"testing"

	"github.com/cespare/xxhash/v2"
	pebbledb "github.com/cockroachdb/pebble"

	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func openTestDB(t testing.TB) *pebble.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(pebble.Options{Path: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func sAttr(s string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: s}
}

func nAttr(n string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrN, N: n}
}

func TestPrimaryTokenFromKey(t *testing.T) {
	pk := []byte("alice")
	key := storage.KeyPrimary("events", pk, nil)
	got, ok := storage.PrimaryTokenFromKey(key)
	if !ok {
		t.Fatal("expected primary token")
	}
	if want := xxhash.Sum64(pk); got != want {
		t.Fatalf("token = %d, want %d", got, want)
	}
	table, ok := storage.PrimaryTableFromKey(key)
	if !ok {
		t.Fatal("expected primary table")
	}
	if table != "events" {
		t.Fatalf("table = %q, want events", table)
	}
	tableBytes, ok := storage.PrimaryTableBytesFromKey(key)
	if !ok {
		t.Fatal("expected primary table bytes")
	}
	if string(tableBytes) != "events" {
		t.Fatalf("table bytes = %q, want events", tableBytes)
	}
	if _, ok := storage.PrimaryTokenFromKey(storage.KeyCatalog("events")); ok {
		t.Fatal("catalog key reported as primary")
	}
	if _, ok := storage.PrimaryTableFromKey(storage.KeyCatalog("events")); ok {
		t.Fatal("catalog key reported as primary table")
	}
}

func TestPutGetDelete(t *testing.T) {
	db := openTestDB(t)
	ks := types.KeySchema{PK: "user_id", SK: "ts"}

	item := types.Item{
		"user_id": sAttr("alice"),
		"ts":      nAttr("100"),
		"event":   sAttr("login"),
		"score":   nAttr("42"),
	}
	if err := db.PutItem("events", ks, item); err != nil {
		t.Fatalf("PutItem: %v", err)
	}

	got, err := db.GetItem("events", ks, types.Item{
		"user_id": sAttr("alice"),
		"ts":      nAttr("100"),
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got["event"].S != "login" {
		t.Fatalf("event = %q, want login", got["event"].S)
	}
	if got["score"].N != "42" {
		t.Fatalf("score = %q, want 42", got["score"].N)
	}

	if err := db.DeleteItem("events", ks, types.Item{
		"user_id": sAttr("alice"),
		"ts":      nAttr("100"),
	}); err != nil {
		t.Fatalf("DeleteItem: %v", err)
	}

	_, err = db.GetItem("events", ks, types.Item{
		"user_id": sAttr("alice"),
		"ts":      nAttr("100"),
	})
	if err != types.ErrItemNotFound {
		t.Fatalf("expected ErrItemNotFound after Delete, got %v", err)
	}
}

func TestGetItemCacheInvalidatesAfterWrites(t *testing.T) {
	db := openTestDB(t)
	td := types.TableDescriptor{Name: "cache_items", KeySchema: types.KeySchema{PK: "id"}}
	key := types.Item{"id": sAttr("k")}

	if err := db.PutItemWith(td, types.Item{"id": sAttr("k"), "v": sAttr("one")}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put one: %v", err)
	}
	got, err := db.GetItem(td.Name, td.KeySchema, key)
	if err != nil {
		t.Fatalf("get one: %v", err)
	}
	if got["v"].S != "one" {
		t.Fatalf("v = %q, want one", got["v"].S)
	}

	if err := db.PutItemWith(td, types.Item{"id": sAttr("k"), "v": sAttr("two")}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put two: %v", err)
	}
	got, err = db.GetItem(td.Name, td.KeySchema, key)
	if err != nil {
		t.Fatalf("get two: %v", err)
	}
	if got["v"].S != "two" {
		t.Fatalf("v = %q, want two", got["v"].S)
	}

	if err := db.BatchWriteItem(td, []pebble.BatchOp{{
		Op:   pebble.BatchOpPut,
		Item: types.Item{"id": sAttr("k"), "v": sAttr("three")},
	}}); err != nil {
		t.Fatalf("batch put: %v", err)
	}
	got, err = db.GetItem(td.Name, td.KeySchema, key)
	if err != nil {
		t.Fatalf("get three: %v", err)
	}
	if got["v"].S != "three" {
		t.Fatalf("v = %q, want three", got["v"].S)
	}

	if err := db.BatchWriteItem(td, []pebble.BatchOp{{
		Op:  pebble.BatchOpDelete,
		Key: key,
	}}); err != nil {
		t.Fatalf("batch delete: %v", err)
	}
	if _, err := db.GetItem(td.Name, td.KeySchema, key); err != types.ErrItemNotFound {
		t.Fatalf("get after batch delete = %v, want ErrItemNotFound", err)
	}

	if err := db.PutItemWith(td, types.Item{"id": sAttr("k"), "v": sAttr("four")}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put four: %v", err)
	}
	if _, err := db.GetItem(td.Name, td.KeySchema, key); err != nil {
		t.Fatalf("get four: %v", err)
	}
	if err := db.DeleteItem(td.Name, td.KeySchema, key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := db.GetItem(td.Name, td.KeySchema, key); err != types.ErrItemNotFound {
		t.Fatalf("get after delete = %v, want ErrItemNotFound", err)
	}
}

func TestObserveAppliedBatchInvalidatesGetItemCache(t *testing.T) {
	db := openTestDB(t)
	td := types.TableDescriptor{Name: "observed_items", KeySchema: types.KeySchema{PK: "id"}}
	keyAttrs := types.Item{"id": sAttr("k")}
	if err := db.PutItemWith(td, types.Item{"id": sAttr("k"), "v": sAttr("one")}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put one: %v", err)
	}
	if _, err := db.GetItem(td.Name, td.KeySchema, keyAttrs); err != nil {
		t.Fatalf("warm cache: %v", err)
	}

	pk, err := storage.AttrCanonicalBytes(sAttr("k"))
	if err != nil {
		t.Fatalf("pk bytes: %v", err)
	}
	primaryKey := storage.KeyPrimary(td.Name, pk, nil)
	updated, err := storage.EncodeItem(types.Item{"id": sAttr("k"), "v": sAttr("two")})
	if err != nil {
		t.Fatalf("encode updated: %v", err)
	}
	b := db.Raw().NewBatch()
	if err := b.Set(primaryKey, updated, nil); err != nil {
		t.Fatalf("raw set: %v", err)
	}
	repr := append([]byte(nil), b.Repr()...)
	db.ObservePendingBatch(repr)
	if err := b.Commit(pebbledb.NoSync); err != nil {
		t.Fatalf("raw commit: %v", err)
	}
	_ = b.Close()
	db.ObserveAppliedBatch(repr)

	got, err := db.GetItem(td.Name, td.KeySchema, keyAttrs)
	if err != nil {
		t.Fatalf("get observed update: %v", err)
	}
	if got["v"].S != "two" {
		t.Fatalf("v = %q, want two", got["v"].S)
	}

	del := db.Raw().NewBatch()
	if err := del.Delete(primaryKey, nil); err != nil {
		t.Fatalf("raw delete: %v", err)
	}
	repr = append([]byte(nil), del.Repr()...)
	db.ObservePendingBatch(repr)
	if err := del.Commit(pebbledb.NoSync); err != nil {
		t.Fatalf("raw delete commit: %v", err)
	}
	_ = del.Close()
	db.ObserveAppliedBatch(repr)

	if _, err := db.GetItem(td.Name, td.KeySchema, keyAttrs); err != types.ErrItemNotFound {
		t.Fatalf("get observed delete = %v, want ErrItemNotFound", err)
	}
}

func TestQueryByPK(t *testing.T) {
	db := openTestDB(t)
	ks := types.KeySchema{PK: "user_id", SK: "ts"}

	for _, ts := range []string{"001", "002", "003", "004", "005"} {
		item := types.Item{
			"user_id": sAttr("bob"),
			"ts":      sAttr(ts),
			"data":    sAttr("v-" + ts),
		}
		if err := db.PutItem("events", ks, item); err != nil {
			t.Fatalf("PutItem(%s): %v", ts, err)
		}
	}
	// Another partition that shouldn't show up.
	if err := db.PutItem("events", ks, types.Item{
		"user_id": sAttr("carol"),
		"ts":      sAttr("999"),
		"data":    sAttr("carol-only"),
	}); err != nil {
		t.Fatalf("PutItem(carol): %v", err)
	}

	items, err := db.QueryByPK("events", ks, sAttr("bob"), 0)
	if err != nil {
		t.Fatalf("QueryByPK: %v", err)
	}
	if len(items) != 5 {
		t.Fatalf("got %d items, want 5", len(items))
	}
	for i, it := range items {
		wantTs := fmt.Sprintf("00%d", i+1)
		if it["ts"].S != wantTs {
			t.Fatalf("item[%d].ts = %q, want %q (SK ordering broken)", i, it["ts"].S, wantTs)
		}
	}

	// Range query: ts in ["002", "004")
	rng, err := db.QueryByPKRange("events", ks, sAttr("bob"), sAttr("002"), sAttr("004"), 0)
	if err != nil {
		t.Fatalf("QueryByPKRange: %v", err)
	}
	if len(rng) != 2 || rng[0]["ts"].S != "002" || rng[1]["ts"].S != "003" {
		t.Fatalf("range returned %v, want ts 002,003", rng)
	}

	// Limit
	lim, err := db.QueryByPK("events", ks, sAttr("bob"), 2)
	if err != nil {
		t.Fatalf("QueryByPK limit: %v", err)
	}
	if len(lim) != 2 {
		t.Fatalf("limit returned %d, want 2", len(lim))
	}
}

func TestKeySchemaWithoutSK(t *testing.T) {
	db := openTestDB(t)
	ks := types.KeySchema{PK: "id"}

	item := types.Item{
		"id":   sAttr("only-key"),
		"data": sAttr("hello"),
	}
	if err := db.PutItem("singles", ks, item); err != nil {
		t.Fatalf("PutItem: %v", err)
	}
	got, err := db.GetItem("singles", ks, types.Item{"id": sAttr("only-key")})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got["data"].S != "hello" {
		t.Fatalf("data = %q, want hello", got["data"].S)
	}
}

func TestMissingKeyAttribute(t *testing.T) {
	db := openTestDB(t)
	ks := types.KeySchema{PK: "id"}
	err := db.PutItem("singles", ks, types.Item{"other": sAttr("x")})
	if err == nil {
		t.Fatalf("expected ErrMissingKey, got nil")
	}
}

func TestEncodeDecodeItemRoundTrip(t *testing.T) {
	item := types.Item{
		"s":    sAttr("hello"),
		"n":    nAttr("3.14"),
		"b":    {T: types.AttrB, B: []byte{0x01, 0x02, 0x03}},
		"bool": {T: types.AttrBOOL, BOOL: true},
		"null": {T: types.AttrNull},
		"ss":   {T: types.AttrSS, SS: []string{"a", "b", "c"}},
		"l":    {T: types.AttrL, L: []types.AttributeValue{sAttr("x"), nAttr("1")}},
		"m":    {T: types.AttrM, M: map[string]types.AttributeValue{"inner": sAttr("v")}},
		"vec":  {T: types.AttrVec, Vec: []float64{0.1, 0.2, 0.3}},
	}
	enc, err := storage.EncodeItem(item)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	dec, err := storage.DecodeItem(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	for k := range item {
		if _, ok := dec[k]; !ok {
			t.Fatalf("missing %q after round-trip", k)
		}
	}
	if dec["s"].S != "hello" || dec["n"].N != "3.14" || !dec["bool"].BOOL {
		t.Fatalf("decoded values diverge: %+v", dec)
	}
	if dec["m"].M["inner"].S != "v" {
		t.Fatalf("nested map lost: %+v", dec["m"])
	}
	if dec["vec"].T != types.AttrVec || len(dec["vec"].Vec) != 3 || dec["vec"].Vec[1] != 0.2 {
		t.Fatalf("vector lost: %+v", dec["vec"])
	}
}

func TestMemoryStorageClassValidatesVectorDimAndReportsFootprint(t *testing.T) {
	db := openTestDB(t)
	td := types.TableDescriptor{
		Name:         "docs",
		KeySchema:    types.KeySchema{PK: "id"},
		StorageClass: types.StorageClassMemory,
		AttributeDefinitions: []types.AttributeDefinition{{
			Name:             "emb",
			Type:             "V",
			VectorDimensions: 3,
		}},
	}
	if err := db.PutItemWith(td, types.Item{
		"id":  sAttr("a"),
		"emb": {T: types.AttrVec, Vec: []float64{1, 0, 0}},
	}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put memory vector: %v", err)
	}
	if db.MemoryTableFootprint("docs") <= 0 {
		t.Fatal("expected positive memory footprint")
	}
	got, err := db.GetItem("docs", td.KeySchema, types.Item{"id": sAttr("a")})
	if err != nil {
		t.Fatalf("get memory vector: %v", err)
	}
	if got["emb"].T != types.AttrVec || len(got["emb"].Vec) != 3 {
		t.Fatalf("unexpected item: %+v", got)
	}
	err = db.PutItemWith(td, types.Item{
		"id":  sAttr("bad"),
		"emb": {T: types.AttrVec, Vec: []float64{1, 0}},
	}, pebble.PutOptions{})
	if err == nil {
		t.Fatal("expected vector dimension validation error")
	}
}

// BenchmarkPutGet exercises the hot path with a NoSync engine — the
// number to watch against the plan's Phase-1 target (PutItem < 50 µs,
// GetItem < 10 µs warm).
func BenchmarkPutItem(b *testing.B) {
	db := openTestDB(b)
	ks := types.KeySchema{PK: "id"}
	item := types.Item{
		"id":   sAttr("k0"),
		"data": sAttr("payload-of-modest-size-to-mimic-real-rows"),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item["id"] = sAttr(fmt.Sprintf("k%d", i))
		if err := db.PutItem("bench", ks, item); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetItem(b *testing.B) {
	db := openTestDB(b)
	ks := types.KeySchema{PK: "id"}
	for i := 0; i < 10_000; i++ {
		_ = db.PutItem("bench", ks, types.Item{
			"id":   sAttr(fmt.Sprintf("k%d", i)),
			"data": sAttr("payload"),
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := types.Item{"id": sAttr(fmt.Sprintf("k%d", i%10_000))}
		if _, err := db.GetItem("bench", ks, k); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatchWritePlainTable(b *testing.B) {
	db := openTestDB(b)
	td := types.TableDescriptor{Name: "bench_batch", KeySchema: types.KeySchema{PK: "id"}}
	const batchSize = 500

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ops := make([]pebble.BatchOp, batchSize)
		base := i * batchSize
		for j := range ops {
			ops[j] = pebble.BatchOp{
				Op: pebble.BatchOpPut,
				Item: types.Item{
					"id":   sAttr(fmt.Sprintf("k%d", base+j)),
					"data": sAttr("payload"),
				},
			}
		}
		if err := db.BatchWriteItem(td, ops); err != nil {
			b.Fatal(err)
		}
	}
}
