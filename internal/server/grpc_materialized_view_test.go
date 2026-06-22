package server_test

import (
	"context"
	"testing"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// TestCreateMaterializedView_RoundTrip exercises the catalog path
// for all three refresh modes via the gRPC handlers. No write hook
// runs here — Phase 2 wires that; this test only proves the
// foundation works.
func TestCreateMaterializedView_RoundTrip(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()

	// Base table the views derive from.
	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "base",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}

	cases := []struct {
		name       string
		desc       *cefaspb.MaterializedViewDescriptor
		expectMode cefaspb.RefreshPolicy_Mode
	}{
		{
			name: "eager",
			desc: &cefaspb.MaterializedViewDescriptor{
				Name:      "v_eager",
				BaseTable: "base",
				KeySchema: &cefaspb.KeySchema{Pk: "id"},
				RefreshPolicy: &cefaspb.RefreshPolicy{
					Mode: cefaspb.RefreshPolicy_EAGER,
				},
			},
			expectMode: cefaspb.RefreshPolicy_EAGER,
		},
		{
			name: "scheduled",
			desc: &cefaspb.MaterializedViewDescriptor{
				Name:      "v_sched",
				BaseTable: "base",
				KeySchema: &cefaspb.KeySchema{Pk: "id"},
				RefreshPolicy: &cefaspb.RefreshPolicy{
					Mode:            cefaspb.RefreshPolicy_SCHEDULED,
					IntervalSeconds: 60,
				},
			},
			expectMode: cefaspb.RefreshPolicy_SCHEDULED,
		},
		{
			name: "on_demand",
			desc: &cefaspb.MaterializedViewDescriptor{
				Name:      "v_demand",
				BaseTable: "base",
				KeySchema: &cefaspb.KeySchema{Pk: "id"},
				RefreshPolicy: &cefaspb.RefreshPolicy{
					Mode: cefaspb.RefreshPolicy_ON_DEMAND,
				},
			},
			expectMode: cefaspb.RefreshPolicy_ON_DEMAND,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := stub.CreateMaterializedView(ctx, &cefaspb.CreateMaterializedViewRequest{
				Descriptor_: tc.desc,
			})
			if err != nil {
				t.Fatalf("CreateMaterializedView: %v", err)
			}
			got := resp.GetDescriptor_()
			if got.GetName() != tc.desc.Name {
				t.Errorf("Name = %q, want %q", got.GetName(), tc.desc.Name)
			}
			if got.GetRefreshPolicy().GetMode() != tc.expectMode {
				t.Errorf("Mode = %v, want %v", got.GetRefreshPolicy().GetMode(), tc.expectMode)
			}
			if got.GetStatus() == "" {
				t.Errorf("Status empty, want building")
			}

			// Describe round-trip.
			described, err := stub.DescribeMaterializedView(ctx, &cefaspb.DescribeMaterializedViewRequest{
				Name: tc.desc.Name,
			})
			if err != nil {
				t.Fatalf("DescribeMaterializedView: %v", err)
			}
			if described.GetDescriptor_().GetBaseTable() != "base" {
				t.Errorf("base = %q, want base", described.GetDescriptor_().GetBaseTable())
			}
		})
	}

	// List returns all three.
	listed, err := stub.ListMaterializedViews(ctx, &cefaspb.ListMaterializedViewsRequest{})
	if err != nil {
		t.Fatalf("ListMaterializedViews: %v", err)
	}
	if len(listed.GetViews()) != 3 {
		t.Errorf("ListViews returned %d, want 3", len(listed.GetViews()))
	}

	// Filter by base.
	filtered, err := stub.ListMaterializedViews(ctx, &cefaspb.ListMaterializedViewsRequest{
		BaseTable: "base",
	})
	if err != nil {
		t.Fatalf("List by base: %v", err)
	}
	if len(filtered.GetViews()) != 3 {
		t.Errorf("filtered by base returned %d, want 3", len(filtered.GetViews()))
	}

	// Drop one and confirm the list shrinks.
	if _, err := stub.DropMaterializedView(ctx, &cefaspb.DropMaterializedViewRequest{Name: "v_demand"}); err != nil {
		t.Fatalf("DropMaterializedView: %v", err)
	}
	listed, err = stub.ListMaterializedViews(ctx, &cefaspb.ListMaterializedViewsRequest{})
	if err != nil {
		t.Fatalf("ListMaterializedViews after drop: %v", err)
	}
	if len(listed.GetViews()) != 2 {
		t.Errorf("after drop expected 2 views, got %d", len(listed.GetViews()))
	}
}

// TestEagerHook_AppliesPutToView ensures a base PutItem propagates
// to an attached EAGER view through the synchronous hook landed in
// Phase 2.
func TestEagerHook_AppliesPutToView(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "Orders",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	if _, err := stub.CreateMaterializedView(ctx, &cefaspb.CreateMaterializedViewRequest{
		Descriptor_: &cefaspb.MaterializedViewDescriptor{
			Name:      "Orders_by_status",
			BaseTable: "Orders",
			KeySchema: &cefaspb.KeySchema{Pk: "status", Sk: "id"},
			RefreshPolicy: &cefaspb.RefreshPolicy{
				Mode: cefaspb.RefreshPolicy_EAGER,
			},
		},
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}

	// Base write.
	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "Orders",
		Item: map[string]*cefaspb.AttributeValue{
			"id":     {Value: &cefaspb.AttributeValue_S{S: "o-1"}},
			"status": {Value: &cefaspb.AttributeValue_S{S: "open"}},
			"total":  {Value: &cefaspb.AttributeValue_N{N: "42"}},
		},
	}); err != nil {
		t.Fatalf("put base: %v", err)
	}

	// View read — single-node fixture uses the same storage so a
	// GetItem against the view's PK should find the derived row.
	got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "Orders_by_status",
		Key: map[string]*cefaspb.AttributeValue{
			"status": {Value: &cefaspb.AttributeValue_S{S: "open"}},
			"id":     {Value: &cefaspb.AttributeValue_S{S: "o-1"}},
		},
	})
	if err != nil {
		t.Fatalf("get view item: %v", err)
	}
	if got.GetItem() == nil {
		t.Fatal("view item missing")
	}
	if got.GetItem()["id"].GetS() != "o-1" {
		t.Errorf("view id = %q, want o-1", got.GetItem()["id"].GetS())
	}
	if got.GetItem()["total"].GetN() != "42" {
		t.Errorf("projected total = %q, want 42", got.GetItem()["total"].GetN())
	}
}

// TestNonEagerSkipsHook proves that SCHEDULED / ON_DEMAND views do
// NOT receive base writes through the eager hook — Phase 4/7 paths
// will populate them instead. This guards against accidental
// hot-path coupling.
func TestNonEagerSkipsHook(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "Logs",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	if _, err := stub.CreateMaterializedView(ctx, &cefaspb.CreateMaterializedViewRequest{
		Descriptor_: &cefaspb.MaterializedViewDescriptor{
			Name:      "Logs_daily",
			BaseTable: "Logs",
			KeySchema: &cefaspb.KeySchema{Pk: "day", Sk: "id"},
			RefreshPolicy: &cefaspb.RefreshPolicy{
				Mode:            cefaspb.RefreshPolicy_SCHEDULED,
				IntervalSeconds: 3600,
			},
		},
	}); err != nil {
		t.Fatalf("create scheduled view: %v", err)
	}

	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "Logs",
		Item: map[string]*cefaspb.AttributeValue{
			"id":  {Value: &cefaspb.AttributeValue_S{S: "L1"}},
			"day": {Value: &cefaspb.AttributeValue_S{S: "2026-06-22"}},
		},
	}); err != nil {
		t.Fatalf("put base: %v", err)
	}

	// View must be empty — scheduler / refresh engine not yet
	// implemented in this PR. GetItem returns Found=false rather
	// than an error when the row is absent.
	got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "Logs_daily",
		Key: map[string]*cefaspb.AttributeValue{
			"day": {Value: &cefaspb.AttributeValue_S{S: "2026-06-22"}},
			"id":  {Value: &cefaspb.AttributeValue_S{S: "L1"}},
		},
	})
	if err != nil {
		t.Fatalf("get view item: %v", err)
	}
	if got.GetFound() {
		t.Fatal("expected scheduled view to be empty (hook should have skipped it)")
	}
}
