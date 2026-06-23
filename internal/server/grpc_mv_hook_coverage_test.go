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

func TestEagerHook_AggregatesCountAndSum(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "OrdersAgg",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	if _, err := stub.CreateMaterializedView(ctx, &cefaspb.CreateMaterializedViewRequest{
		Descriptor_: &cefaspb.MaterializedViewDescriptor{
			Name:      "OrdersAgg_by_region",
			BaseTable: "OrdersAgg",
			KeySchema: &cefaspb.KeySchema{Pk: "region"},
			GroupBy:   []string{"region"},
			Aggregations: []*cefaspb.MaterializedViewAggregation{
				{
					Function:        cefaspb.MaterializedViewAggregation_COUNT,
					TargetAttribute: "order_count",
				},
				{
					Function:        cefaspb.MaterializedViewAggregation_SUM,
					SourceAttribute: "amount",
					TargetAttribute: "total_amount",
				},
			},
			RefreshPolicy: &cefaspb.RefreshPolicy{Mode: cefaspb.RefreshPolicy_EAGER},
		},
	}); err != nil {
		t.Fatalf("create aggregate view: %v", err)
	}

	put := func(id, region, amount string) {
		t.Helper()
		if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
			Table: "OrdersAgg",
			Item: map[string]*cefaspb.AttributeValue{
				"id":     {Value: &cefaspb.AttributeValue_S{S: id}},
				"region": {Value: &cefaspb.AttributeValue_S{S: region}},
				"amount": {Value: &cefaspb.AttributeValue_N{N: amount}},
			},
		}); err != nil {
			t.Fatalf("put %s: %v", id, err)
		}
	}
	assertAgg := func(region, wantCount, wantTotal string) {
		t.Helper()
		got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
			Table: "OrdersAgg_by_region",
			Key: map[string]*cefaspb.AttributeValue{
				"region": {Value: &cefaspb.AttributeValue_S{S: region}},
			},
		})
		if err != nil {
			t.Fatalf("get aggregate %s: %v", region, err)
		}
		if !got.GetFound() {
			t.Fatalf("aggregate row %s missing", region)
		}
		if got.GetItem()["order_count"].GetN() != wantCount {
			t.Fatalf("%s order_count = %q, want %q", region, got.GetItem()["order_count"].GetN(), wantCount)
		}
		if got.GetItem()["total_amount"].GetN() != wantTotal {
			t.Fatalf("%s total_amount = %q, want %q", region, got.GetItem()["total_amount"].GetN(), wantTotal)
		}
	}

	put("o1", "us", "10")
	put("o2", "us", "7")
	assertAgg("us", "2", "17")

	if _, err := stub.UpdateItem(ctx, &cefaspb.UpdateItemRequest{
		Table: "OrdersAgg",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "o2"}},
		},
		UpdateExpression: "SET amount = :amount",
		ExpressionAttributeValues: map[string]*cefaspb.AttributeValue{
			":amount": {Value: &cefaspb.AttributeValue_N{N: "9"}},
		},
	}); err != nil {
		t.Fatalf("update amount: %v", err)
	}
	assertAgg("us", "2", "19")

	put("o1", "eu", "3")
	assertAgg("us", "1", "9")
	assertAgg("eu", "1", "3")

	if _, err := stub.DeleteItem(ctx, &cefaspb.DeleteItemRequest{
		Table: "OrdersAgg",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "o2"}},
		},
	}); err != nil {
		t.Fatalf("delete o2: %v", err)
	}
	assertAgg("us", "0", "0")
}

func TestSQLMaterializedViewAggregates(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()

	execSQL := func(q string) {
		t.Helper()
		if _, err := stub.Sql(ctx, &cefaspb.SqlRequest{Query: q}); err != nil {
			t.Fatalf("sql %q: %v", q, err)
		}
	}
	execSQL("CREATE TABLE SqlOrdersAgg (id S, region S, amount N, PRIMARY KEY (id))")
	execSQL("CREATE MATERIALIZED VIEW SqlOrdersAgg_by_region AS SELECT region, COUNT(*), SUM(amount) FROM SqlOrdersAgg GROUP BY region PRIMARY KEY (region)")
	execSQL("INSERT INTO SqlOrdersAgg (id, region, amount) VALUES ('o1', 'us', 5)")
	execSQL("UPDATE SqlOrdersAgg SET amount = 8 WHERE id = 'o1'")

	got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "SqlOrdersAgg_by_region",
		Key: map[string]*cefaspb.AttributeValue{
			"region": {Value: &cefaspb.AttributeValue_S{S: "us"}},
		},
	})
	if err != nil {
		t.Fatalf("get aggregate: %v", err)
	}
	if !got.GetFound() {
		t.Fatal("aggregate row missing")
	}
	if got.GetItem()["count"].GetN() != "1" {
		t.Fatalf("count = %q, want 1", got.GetItem()["count"].GetN())
	}
	if got.GetItem()["sum_amount"].GetN() != "8" {
		t.Fatalf("sum_amount = %q, want 8", got.GetItem()["sum_amount"].GetN())
	}
}

// TestEagerHook_BatchCoalescedAcrossMV exercises the per-(MV, shard)
// coalescing landed in #531. Two MVs attached to the same base, batch
// with mixed put + delete ops: both MVs must reflect every op
// exactly once, even though the hook now submits one batchWriteBuckets
// call per MV instead of per op.
func TestEagerHook_BatchCoalescedAcrossMV(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "Multi",
			KeySchema: &cefaspb.KeySchema{Pk: "pk", Sk: "sk"},
		},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	for _, spec := range []struct {
		name   string
		pk, sk string
	}{
		{"Multi_mvA", "sk", "pk"},
		{"Multi_mvB", "pk", "sk"},
	} {
		if _, err := stub.CreateMaterializedView(ctx, &cefaspb.CreateMaterializedViewRequest{
			Descriptor_: &cefaspb.MaterializedViewDescriptor{
				Name:      spec.name,
				BaseTable: "Multi",
				KeySchema: &cefaspb.KeySchema{Pk: spec.pk, Sk: spec.sk},
				RefreshPolicy: &cefaspb.RefreshPolicy{
					Mode: cefaspb.RefreshPolicy_EAGER,
				},
			},
		}); err != nil {
			t.Fatalf("create view %s: %v", spec.name, err)
		}
	}

	ops := []*cefaspb.BatchWriteOp{
		{Kind: cefaspb.BatchWriteOp_KIND_PUT, Item: map[string]*cefaspb.AttributeValue{
			"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
			"sk": {Value: &cefaspb.AttributeValue_S{S: "open"}},
		}},
		{Kind: cefaspb.BatchWriteOp_KIND_PUT, Item: map[string]*cefaspb.AttributeValue{
			"pk": {Value: &cefaspb.AttributeValue_S{S: "p2"}},
			"sk": {Value: &cefaspb.AttributeValue_S{S: "open"}},
		}},
		{Kind: cefaspb.BatchWriteOp_KIND_PUT, Item: map[string]*cefaspb.AttributeValue{
			"pk": {Value: &cefaspb.AttributeValue_S{S: "p3"}},
			"sk": {Value: &cefaspb.AttributeValue_S{S: "closed"}},
		}},
	}
	if _, err := stub.BatchWriteItem(ctx, &cefaspb.BatchWriteItemRequest{Table: "Multi", Ops: ops}); err != nil {
		t.Fatalf("BatchWriteItem: %v", err)
	}

	// mvA is reshuffled (PK=sk, SK=pk).
	for _, want := range [][2]string{{"open", "p1"}, {"open", "p2"}, {"closed", "p3"}} {
		got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
			Table: "Multi_mvA",
			Key: map[string]*cefaspb.AttributeValue{
				"sk": {Value: &cefaspb.AttributeValue_S{S: want[0]}},
				"pk": {Value: &cefaspb.AttributeValue_S{S: want[1]}},
			},
		})
		if err != nil || !got.GetFound() {
			t.Errorf("mvA missing %s/%s: err=%v found=%v", want[0], want[1], err, got.GetFound())
		}
	}
	// mvB carries identity key (PK=pk, SK=sk).
	for _, want := range [][2]string{{"p1", "open"}, {"p2", "open"}, {"p3", "closed"}} {
		got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
			Table: "Multi_mvB",
			Key: map[string]*cefaspb.AttributeValue{
				"pk": {Value: &cefaspb.AttributeValue_S{S: want[0]}},
				"sk": {Value: &cefaspb.AttributeValue_S{S: want[1]}},
			},
		})
		if err != nil || !got.GetFound() {
			t.Errorf("mvB missing %s/%s: err=%v found=%v", want[0], want[1], err, got.GetFound())
		}
	}

	// Now batch-delete one row; both MVs must drop their entry.
	if _, err := stub.BatchWriteItem(ctx, &cefaspb.BatchWriteItemRequest{
		Table: "Multi",
		Ops: []*cefaspb.BatchWriteOp{
			{Kind: cefaspb.BatchWriteOp_KIND_DELETE, Key: map[string]*cefaspb.AttributeValue{
				"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
				"sk": {Value: &cefaspb.AttributeValue_S{S: "open"}},
			}},
		},
	}); err != nil {
		t.Fatalf("batch delete: %v", err)
	}
	for _, q := range []struct {
		table, ka, va, kb, vb string
	}{
		{"Multi_mvA", "sk", "open", "pk", "p1"},
		{"Multi_mvB", "pk", "p1", "sk", "open"},
	} {
		got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
			Table: q.table,
			Key: map[string]*cefaspb.AttributeValue{
				q.ka: {Value: &cefaspb.AttributeValue_S{S: q.va}},
				q.kb: {Value: &cefaspb.AttributeValue_S{S: q.vb}},
			},
		})
		if err != nil {
			t.Fatalf("post-delete view get: %v", err)
		}
		if got.GetFound() {
			t.Errorf("%s should have dropped row after cascade", q.table)
		}
	}
}
