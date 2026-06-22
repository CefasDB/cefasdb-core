package server_test

import (
	"context"
	"testing"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// TestPauseResumeMaterializedView covers the operator-facing pause /
// resume contract: while paused the EAGER hook short-circuits, so
// base writes do not propagate. Resume restores normal maintenance.
func TestPauseResumeMaterializedView(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "Orders2",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	if _, err := stub.CreateMaterializedView(ctx, &cefaspb.CreateMaterializedViewRequest{
		Descriptor_: &cefaspb.MaterializedViewDescriptor{
			Name:      "Orders2_by_status",
			BaseTable: "Orders2",
			KeySchema: &cefaspb.KeySchema{Pk: "status", Sk: "id"},
			RefreshPolicy: &cefaspb.RefreshPolicy{
				Mode: cefaspb.RefreshPolicy_EAGER,
			},
		},
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}

	put := func(id, st string) {
		t.Helper()
		if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
			Table: "Orders2",
			Item: map[string]*cefaspb.AttributeValue{
				"id":     {Value: &cefaspb.AttributeValue_S{S: id}},
				"status": {Value: &cefaspb.AttributeValue_S{S: st}},
			},
		}); err != nil {
			t.Fatalf("put %s/%s: %v", id, st, err)
		}
	}
	viewHas := func(status, id string) bool {
		t.Helper()
		got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
			Table: "Orders2_by_status",
			Key: map[string]*cefaspb.AttributeValue{
				"status": {Value: &cefaspb.AttributeValue_S{S: status}},
				"id":     {Value: &cefaspb.AttributeValue_S{S: id}},
			},
		})
		if err != nil {
			t.Fatalf("get view: %v", err)
		}
		return got.GetFound()
	}

	// First write — propagates.
	put("o-1", "open")
	if !viewHas("open", "o-1") {
		t.Fatal("o-1 should be visible before pause")
	}

	// Pause the view.
	if _, err := stub.PauseMaterializedView(ctx, &cefaspb.PauseMaterializedViewRequest{
		Name: "Orders2_by_status",
	}); err != nil {
		t.Fatalf("pause: %v", err)
	}

	// Write while paused — must not propagate.
	put("o-2", "open")
	if viewHas("open", "o-2") {
		t.Error("o-2 must NOT be visible while view is paused")
	}

	// Resume the view.
	if _, err := stub.ResumeMaterializedView(ctx, &cefaspb.ResumeMaterializedViewRequest{
		Name: "Orders2_by_status",
	}); err != nil {
		t.Fatalf("resume: %v", err)
	}

	// New write — propagates again.
	put("o-3", "open")
	if !viewHas("open", "o-3") {
		t.Error("o-3 should be visible after resume")
	}

	// o-2 still missing — gap requires explicit Refresh.
	if viewHas("open", "o-2") {
		t.Error("o-2 should still be missing (gap during pause)")
	}

	// Refresh closes the gap.
	if _, err := stub.RefreshMaterializedView(ctx, &cefaspb.RefreshMaterializedViewRequest{
		Name: "Orders2_by_status",
	}); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if !viewHas("open", "o-2") {
		t.Error("o-2 should be visible after refresh closes the pause gap")
	}
}
