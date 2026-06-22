package server_test

import (
	"context"
	"testing"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// setupEagerMV creates a base table + an EAGER view whose PK/SK are
// the base's SK/PK so the test can read MV rows by their reshuffled
// key. Returns the stub so each test can drive its own writes.
func setupEagerMV(t *testing.T, baseName, mvName string) (cefaspb.CefasClient, func()) {
	t.Helper()
	stub, cleanup := startUnsecuredFixture(t)
	ctx := context.Background()
	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      baseName,
			KeySchema: &cefaspb.KeySchema{Pk: "pk", Sk: "sk"},
		},
	}); err != nil {
		cleanup()
		t.Fatalf("create base: %v", err)
	}
	if _, err := stub.CreateMaterializedView(ctx, &cefaspb.CreateMaterializedViewRequest{
		Descriptor_: &cefaspb.MaterializedViewDescriptor{
			Name:      mvName,
			BaseTable: baseName,
			KeySchema: &cefaspb.KeySchema{Pk: "sk", Sk: "pk"},
			RefreshPolicy: &cefaspb.RefreshPolicy{
				Mode: cefaspb.RefreshPolicy_EAGER,
			},
		},
	}); err != nil {
		cleanup()
		t.Fatalf("create view: %v", err)
	}
	return stub, cleanup
}

// TestEagerHook_AppliesBatchWriteItem proves the eager hook fires
// for every put / delete op inside a BatchWriteItem call.
//
// Before #507 the hook was only wired into PutItem, so the bench's
// batch path silently bypassed MV maintenance — and the post-epic
// A/B run showed misleading "zero overhead". This test guards
// against that gap returning.
func TestEagerHook_AppliesBatchWriteItem(t *testing.T) {
	stub, cleanup := setupEagerMV(t, "BWBase", "BWBase_mv")
	defer cleanup()
	ctx := context.Background()

	ops := []*cefaspb.BatchWriteOp{
		{Kind: cefaspb.BatchWriteOp_KIND_PUT, Item: map[string]*cefaspb.AttributeValue{
			"pk": {Value: &cefaspb.AttributeValue_S{S: "alpha"}},
			"sk": {Value: &cefaspb.AttributeValue_S{S: "open"}},
		}},
		{Kind: cefaspb.BatchWriteOp_KIND_PUT, Item: map[string]*cefaspb.AttributeValue{
			"pk": {Value: &cefaspb.AttributeValue_S{S: "beta"}},
			"sk": {Value: &cefaspb.AttributeValue_S{S: "open"}},
		}},
	}
	if _, err := stub.BatchWriteItem(ctx, &cefaspb.BatchWriteItemRequest{Table: "BWBase", Ops: ops}); err != nil {
		t.Fatalf("BatchWriteItem: %v", err)
	}

	for _, base := range []string{"alpha", "beta"} {
		got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
			Table: "BWBase_mv",
			Key: map[string]*cefaspb.AttributeValue{
				"sk": {Value: &cefaspb.AttributeValue_S{S: "open"}},
				"pk": {Value: &cefaspb.AttributeValue_S{S: base}},
			},
		})
		if err != nil {
			t.Fatalf("get view %s: %v", base, err)
		}
		if !got.GetFound() {
			t.Errorf("view row for base pk=%s missing after BatchWriteItem", base)
		}
	}

	// Now batch-delete one of the rows and confirm the cascade.
	if _, err := stub.BatchWriteItem(ctx, &cefaspb.BatchWriteItemRequest{
		Table: "BWBase",
		Ops: []*cefaspb.BatchWriteOp{
			{Kind: cefaspb.BatchWriteOp_KIND_DELETE, Key: map[string]*cefaspb.AttributeValue{
				"pk": {Value: &cefaspb.AttributeValue_S{S: "alpha"}},
				"sk": {Value: &cefaspb.AttributeValue_S{S: "open"}},
			}},
		},
	}); err != nil {
		t.Fatalf("BatchWriteItem delete: %v", err)
	}
	got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "BWBase_mv",
		Key: map[string]*cefaspb.AttributeValue{
			"sk": {Value: &cefaspb.AttributeValue_S{S: "open"}},
			"pk": {Value: &cefaspb.AttributeValue_S{S: "alpha"}},
		},
	})
	if err != nil {
		t.Fatalf("get view after delete: %v", err)
	}
	if got.GetFound() {
		t.Error("view row for alpha should be deleted via BatchWriteItem cascade")
	}
}

// TestEagerHook_AppliesUpdateItem covers the UpdateItem path. The
// hook fires with the post-update image so the MV reflects the new
// state.
func TestEagerHook_AppliesUpdateItem(t *testing.T) {
	stub, cleanup := setupEagerMV(t, "UpdBase", "UpdBase_mv")
	defer cleanup()
	ctx := context.Background()

	// Seed the row via PutItem (already covered) so UpdateItem has
	// a target.
	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "UpdBase",
		Item: map[string]*cefaspb.AttributeValue{
			"pk":    {Value: &cefaspb.AttributeValue_S{S: "p1"}},
			"sk":    {Value: &cefaspb.AttributeValue_S{S: "v1"}},
			"total": {Value: &cefaspb.AttributeValue_N{N: "10"}},
		},
	}); err != nil {
		t.Fatalf("seed put: %v", err)
	}

	// Update the row's projected attribute.
	if _, err := stub.UpdateItem(ctx, &cefaspb.UpdateItemRequest{
		Table: "UpdBase",
		Key: map[string]*cefaspb.AttributeValue{
			"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
			"sk": {Value: &cefaspb.AttributeValue_S{S: "v1"}},
		},
		UpdateExpression: "SET #t = :n",
		ExpressionAttributeNames: map[string]string{
			"t": "total",
		},
		ExpressionAttributeValues: map[string]*cefaspb.AttributeValue{
			":n": {Value: &cefaspb.AttributeValue_N{N: "42"}},
		},
	}); err != nil {
		t.Fatalf("UpdateItem: %v", err)
	}

	// View row carries the new total.
	got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "UpdBase_mv",
		Key: map[string]*cefaspb.AttributeValue{
			"sk": {Value: &cefaspb.AttributeValue_S{S: "v1"}},
			"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
		},
	})
	if err != nil {
		t.Fatalf("view get: %v", err)
	}
	if !got.GetFound() {
		t.Fatal("view row missing after UpdateItem")
	}
	if got.GetItem()["total"].GetN() != "42" {
		t.Errorf("view total = %q, want 42", got.GetItem()["total"].GetN())
	}
}

// TestEagerHook_AppliesDeleteItem covers the DeleteItem path. The
// hook removes the corresponding view row.
func TestEagerHook_AppliesDeleteItem(t *testing.T) {
	stub, cleanup := setupEagerMV(t, "DelBase", "DelBase_mv")
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "DelBase",
		Item: map[string]*cefaspb.AttributeValue{
			"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
			"sk": {Value: &cefaspb.AttributeValue_S{S: "v1"}},
		},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// View row exists.
	got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "DelBase_mv",
		Key: map[string]*cefaspb.AttributeValue{
			"sk": {Value: &cefaspb.AttributeValue_S{S: "v1"}},
			"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
		},
	})
	if err != nil {
		t.Fatalf("view get pre-delete: %v", err)
	}
	if !got.GetFound() {
		t.Fatal("view row should exist before DeleteItem")
	}

	if _, err := stub.DeleteItem(ctx, &cefaspb.DeleteItemRequest{
		Table: "DelBase",
		Key: map[string]*cefaspb.AttributeValue{
			"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
			"sk": {Value: &cefaspb.AttributeValue_S{S: "v1"}},
		},
	}); err != nil {
		t.Fatalf("DeleteItem: %v", err)
	}

	got, err = stub.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "DelBase_mv",
		Key: map[string]*cefaspb.AttributeValue{
			"sk": {Value: &cefaspb.AttributeValue_S{S: "v1"}},
			"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
		},
	})
	if err != nil {
		t.Fatalf("view get post-delete: %v", err)
	}
	if got.GetFound() {
		t.Error("view row should be deleted via DeleteItem cascade")
	}
}
