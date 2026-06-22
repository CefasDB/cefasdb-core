package catalog

import (
	"errors"
	"testing"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func newTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("pebble open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	c, err := New(db)
	if err != nil {
		t.Fatalf("catalog new: %v", err)
	}
	return c
}

func mustCreateTable(t *testing.T, c *Catalog, name string) {
	t.Helper()
	if err := c.Create(types.TableDescriptor{
		Name:      name,
		KeySchema: types.KeySchema{PK: "pk"},
	}); err != nil {
		t.Fatalf("create table %s: %v", name, err)
	}
}

func TestCreateViewPersistsAndAttachesToBase(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "base")

	mv := types.MaterializedViewDescriptor{
		Name:      "v",
		BaseTable: "base",
		KeySchema: types.KeySchema{PK: "secondary"},
		RefreshPolicy: types.RefreshPolicy{
			Mode: types.RefreshModeEager,
		},
	}
	got, err := c.CreateView(mv)
	if err != nil {
		t.Fatalf("CreateView: %v", err)
	}
	if got.Status != types.MVStatusBuilding {
		t.Errorf("status = %q, want %q", got.Status, types.MVStatusBuilding)
	}

	// Round trip through DescribeView.
	described, err := c.DescribeView("v")
	if err != nil {
		t.Fatalf("DescribeView: %v", err)
	}
	if described.BaseTable != "base" {
		t.Errorf("BaseTable = %q, want %q", described.BaseTable, "base")
	}

	// Base table now carries the view in its attached list.
	base, err := c.Describe("base")
	if err != nil {
		t.Fatalf("Describe base: %v", err)
	}
	if len(base.MaterializedViews) != 1 || base.MaterializedViews[0] != "v" {
		t.Errorf("base.MaterializedViews = %v, want [v]", base.MaterializedViews)
	}
}

func TestCreateViewRejectsMissingBase(t *testing.T) {
	c := newTestCatalog(t)
	_, err := c.CreateView(types.MaterializedViewDescriptor{
		Name:      "v",
		BaseTable: "nonexistent",
		KeySchema: types.KeySchema{PK: "p"},
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, types.ErrTableNotFound) {
		t.Errorf("expected ErrTableNotFound, got %v", err)
	}
}

func TestCreateViewRejectsDuplicate(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "base")
	mv := types.MaterializedViewDescriptor{Name: "v", BaseTable: "base", KeySchema: types.KeySchema{PK: "p"}}
	if _, err := c.CreateView(mv); err != nil {
		t.Fatalf("first create: %v", err)
	}
	_, err := c.CreateView(mv)
	if !errors.Is(err, types.ErrMVAlreadyExists) {
		t.Errorf("expected ErrMVAlreadyExists, got %v", err)
	}
}

func TestDropViewDetachesFromBase(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "base")
	if _, err := c.CreateView(types.MaterializedViewDescriptor{
		Name:      "v",
		BaseTable: "base",
		KeySchema: types.KeySchema{PK: "p"},
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}
	if err := c.DropView("v"); err != nil {
		t.Fatalf("DropView: %v", err)
	}
	if _, err := c.DescribeView("v"); !errors.Is(err, types.ErrMVNotFound) {
		t.Errorf("expected ErrMVNotFound after drop, got %v", err)
	}
	base, err := c.Describe("base")
	if err != nil {
		t.Fatalf("Describe base: %v", err)
	}
	if len(base.MaterializedViews) != 0 {
		t.Errorf("base.MaterializedViews = %v, want empty", base.MaterializedViews)
	}
}

// TestDescribeFallsBackToViewSynthetic ensures the Describe alias
// path used by the SQL / gRPC read layers returns a synthetic
// TableDescriptor when the caller asks for a view name.
func TestDescribeFallsBackToViewSynthetic(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "base")
	if _, err := c.CreateView(types.MaterializedViewDescriptor{
		Name:      "v",
		BaseTable: "base",
		KeySchema: types.KeySchema{PK: "vpk"},
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}
	td, err := c.Describe("v")
	if err != nil {
		t.Fatalf("Describe view via alias: %v", err)
	}
	if td.Name != "v" {
		t.Errorf("Name = %q, want %q", td.Name, "v")
	}
	if td.KeySchema.PK != "vpk" {
		t.Errorf("KeySchema.PK = %q, want %q", td.KeySchema.PK, "vpk")
	}
}

func TestScheduledRequiresInterval(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "base")
	_, err := c.CreateView(types.MaterializedViewDescriptor{
		Name:      "v",
		BaseTable: "base",
		KeySchema: types.KeySchema{PK: "p"},
		RefreshPolicy: types.RefreshPolicy{
			Mode: types.RefreshModeScheduled,
		},
	})
	if err == nil {
		t.Fatal("expected error for SCHEDULED without interval")
	}
}
