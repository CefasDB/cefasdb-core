package pebble_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cespare/xxhash/v2"

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
	if _, ok := storage.PrimaryTokenFromKey(storage.KeyCatalog("events")); ok {
		t.Fatal("catalog key reported as primary")
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

func TestCounterColumnRequiresAtomicUpdate(t *testing.T) {
	db := openTestDB(t)
	td := types.TableDescriptor{
		Name:      "Counters",
		KeySchema: types.KeySchema{PK: "id"},
		AttributeDefinitions: []types.AttributeDefinition{{
			Name: "count",
			Type: types.AttributeTypeCounter,
		}},
	}

	err := db.PutItemWith(td, types.Item{
		"id":    sAttr("views"),
		"count": nAttr("0"),
	}, pebble.PutOptions{})
	if !errors.Is(err, storage.ErrInvalidCounterMutation) {
		t.Fatalf("direct counter PutItem error = %v, want ErrInvalidCounterMutation", err)
	}

	res, err := db.AtomicUpdate(td, types.Item{"id": sAttr("views")}, pebble.AtomicOptions{
		Actions: []pebble.AtomicAction{{
			Kind:      pebble.AtomicActionIncrReturn,
			Attribute: "count",
			Value:     nAttr("2"),
		}},
	})
	if err != nil {
		t.Fatalf("atomic counter increment: %v", err)
	}
	if got := res.Item["count"].N; got != "2" {
		t.Fatalf("count = %q, want 2", got)
	}

	err = db.PutItemWith(td, types.Item{
		"id":   sAttr("views"),
		"note": sAttr("replace"),
	}, pebble.PutOptions{})
	if !errors.Is(err, storage.ErrInvalidCounterMutation) {
		t.Fatalf("counter-erasing PutItem error = %v, want ErrInvalidCounterMutation", err)
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
