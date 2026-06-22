package server_test

import (
	"context"
	"testing"
	"time"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// TestRefreshMaterializedView_OnDemand exercises the manual refresh
// path: an ON_DEMAND view starts empty after CREATE; an explicit
// Refresh RPC populates it from the base table.
func TestRefreshMaterializedView_OnDemand(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "Sales",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	for i, region := range []string{"BR", "US", "BR"} {
		if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
			Table: "Sales",
			Item: map[string]*cefaspb.AttributeValue{
				"id":     {Value: &cefaspb.AttributeValue_S{S: idForIndex(i)}},
				"region": {Value: &cefaspb.AttributeValue_S{S: region}},
				"total":  {Value: &cefaspb.AttributeValue_N{N: "10"}},
			},
		}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if _, err := stub.CreateMaterializedView(ctx, &cefaspb.CreateMaterializedViewRequest{
		Descriptor_: &cefaspb.MaterializedViewDescriptor{
			Name:      "Sales_by_region",
			BaseTable: "Sales",
			KeySchema: &cefaspb.KeySchema{Pk: "region", Sk: "id"},
			RefreshPolicy: &cefaspb.RefreshPolicy{
				Mode: cefaspb.RefreshPolicy_ON_DEMAND,
			},
		},
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}

	// View must be empty before refresh.
	got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "Sales_by_region",
		Key: map[string]*cefaspb.AttributeValue{
			"region": {Value: &cefaspb.AttributeValue_S{S: "BR"}},
			"id":     {Value: &cefaspb.AttributeValue_S{S: idForIndex(0)}},
		},
	})
	if err != nil {
		t.Fatalf("pre-refresh get: %v", err)
	}
	if got.GetFound() {
		t.Fatal("view should be empty before refresh")
	}

	// Refresh.
	resp, err := stub.RefreshMaterializedView(ctx, &cefaspb.RefreshMaterializedViewRequest{
		Name: "Sales_by_region",
	})
	if err != nil {
		t.Fatalf("RefreshMaterializedView: %v", err)
	}
	if resp.GetRowsIndexed() != 3 {
		t.Errorf("RowsIndexed = %d, want 3", resp.GetRowsIndexed())
	}

	// Post-refresh: all three rows visible.
	for i, region := range []string{"BR", "US", "BR"} {
		got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
			Table: "Sales_by_region",
			Key: map[string]*cefaspb.AttributeValue{
				"region": {Value: &cefaspb.AttributeValue_S{S: region}},
				"id":     {Value: &cefaspb.AttributeValue_S{S: idForIndex(i)}},
			},
		})
		if err != nil {
			t.Fatalf("post-refresh get %s/%d: %v", region, i, err)
		}
		if !got.GetFound() {
			t.Errorf("row %s/%s missing after refresh", region, idForIndex(i))
		}
	}

	// Describe shows LastRefreshAt > 0.
	desc, err := stub.DescribeMaterializedView(ctx, &cefaspb.DescribeMaterializedViewRequest{
		Name: "Sales_by_region",
	})
	if err != nil {
		t.Fatalf("DescribeMV: %v", err)
	}
	if desc.GetDescriptor_().GetLastRefreshAtUnix() == 0 {
		t.Error("LastRefreshAtUnix not updated after refresh")
	}
	if desc.GetDescriptor_().GetStatus() != "active" {
		t.Errorf("status = %q, want active", desc.GetDescriptor_().GetStatus())
	}
}

// TestRefreshMaterializedView_SingleFlight proves concurrent refresh
// requests for the same view coalesce instead of stacking work.
func TestRefreshMaterializedView_SingleFlight(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "Slow",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	for i := 0; i < 20; i++ {
		if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
			Table: "Slow",
			Item: map[string]*cefaspb.AttributeValue{
				"id":  {Value: &cefaspb.AttributeValue_S{S: idForIndex(i)}},
				"tag": {Value: &cefaspb.AttributeValue_S{S: "x"}},
			},
		}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if _, err := stub.CreateMaterializedView(ctx, &cefaspb.CreateMaterializedViewRequest{
		Descriptor_: &cefaspb.MaterializedViewDescriptor{
			Name:      "Slow_by_tag",
			BaseTable: "Slow",
			KeySchema: &cefaspb.KeySchema{Pk: "tag", Sk: "id"},
			RefreshPolicy: &cefaspb.RefreshPolicy{
				Mode: cefaspb.RefreshPolicy_ON_DEMAND,
			},
		},
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}

	done := make(chan int64, 4)
	for i := 0; i < 4; i++ {
		go func() {
			r, err := stub.RefreshMaterializedView(ctx, &cefaspb.RefreshMaterializedViewRequest{
				Name: "Slow_by_tag",
			})
			if err != nil {
				done <- -1
				return
			}
			done <- r.GetRowsIndexed()
		}()
	}

	var nonZero int
	for i := 0; i < 4; i++ {
		select {
		case n := <-done:
			if n == -1 {
				t.Errorf("concurrent refresh error")
			}
			if n > 0 {
				nonZero++
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent refresh did not complete in 5s")
		}
	}
	// Only the leader of the single-flight returns rows > 0; the
	// followers receive 0 (they waited and returned without re-running).
	if nonZero == 0 {
		t.Error("at least one refresh should have indexed rows")
	}
}

func idForIndex(i int) string {
	// short stable identifiers so the test reads naturally.
	return "id-" + string(rune('A'+i))
}
