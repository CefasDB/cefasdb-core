package catalog

import (
	"errors"
	"testing"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestGlobalIndex_CreateAndDescribe(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "Users")
	gi, err := c.CreateGlobalIndex(types.GlobalIndexDescriptor{
		Name:             "idx_email",
		BaseTable:        "Users",
		IndexedColumn:    "email",
		ProjectedColumns: []string{"id", "name"},
	})
	if err != nil {
		t.Fatalf("CreateGlobalIndex: %v", err)
	}
	if gi.Name != "idx_email" || gi.Status != types.GlobalIndexStatusBuilding {
		t.Errorf("descriptor mismatch: %+v", gi)
	}
	got, err := c.DescribeGlobalIndex("idx_email")
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if got.IndexedColumn != "email" || len(got.ProjectedColumns) != 2 {
		t.Errorf("describe mismatch: %+v", got)
	}
}

func TestGlobalIndex_RejectsDuplicate(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "Users")
	if _, err := c.CreateGlobalIndex(types.GlobalIndexDescriptor{
		Name: "i1", BaseTable: "Users", IndexedColumn: "email",
	}); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	if _, err := c.CreateGlobalIndex(types.GlobalIndexDescriptor{
		Name: "i1", BaseTable: "Users", IndexedColumn: "email",
	}); !errors.Is(err, types.ErrGlobalIndexExists) {
		t.Fatalf("dup Create = %v, want ErrGlobalIndexExists", err)
	}
}

func TestGlobalIndex_RejectsMissingBase(t *testing.T) {
	c := newTestCatalog(t)
	if _, err := c.CreateGlobalIndex(types.GlobalIndexDescriptor{
		Name: "i1", BaseTable: "Missing", IndexedColumn: "email",
	}); !errors.Is(err, types.ErrTableNotFound) {
		t.Fatalf("missing base = %v, want ErrTableNotFound", err)
	}
}

func TestGlobalIndex_RejectsNameClash(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "Users")
	if _, err := c.CreateGlobalIndex(types.GlobalIndexDescriptor{
		Name: "Users", BaseTable: "Users", IndexedColumn: "email",
	}); err == nil {
		t.Fatal("expected error: index name clashes with table")
	}
}

func TestGlobalIndex_DropRemoves(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "Users")
	if _, err := c.CreateGlobalIndex(types.GlobalIndexDescriptor{
		Name: "i1", BaseTable: "Users", IndexedColumn: "email",
	}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := c.DropGlobalIndex("i1"); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if _, err := c.DescribeGlobalIndex("i1"); !errors.Is(err, types.ErrGlobalIndexNotFound) {
		t.Fatalf("Describe after drop = %v, want ErrGlobalIndexNotFound", err)
	}
}

func TestGlobalIndex_ListFiltersByBase(t *testing.T) {
	c := newTestCatalog(t)
	mustCreateTable(t, c, "Users")
	mustCreateTable(t, c, "Orders")
	if _, err := c.CreateGlobalIndex(types.GlobalIndexDescriptor{
		Name: "idx_email", BaseTable: "Users", IndexedColumn: "email",
	}); err != nil {
		t.Fatalf("Create email: %v", err)
	}
	if _, err := c.CreateGlobalIndex(types.GlobalIndexDescriptor{
		Name: "idx_status", BaseTable: "Orders", IndexedColumn: "status",
	}); err != nil {
		t.Fatalf("Create status: %v", err)
	}
	all := c.ListGlobalIndexes("")
	if len(all) != 2 {
		t.Errorf("List(\"\") = %d, want 2", len(all))
	}
	users := c.ListGlobalIndexes("Users")
	if len(users) != 1 || users[0].Name != "idx_email" {
		t.Errorf("List(Users) = %+v, want [idx_email]", users)
	}
}

func TestGlobalIndex_DescribeFromColdCache(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("pebble open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	hot, err := New(db)
	if err != nil {
		t.Fatalf("hot: %v", err)
	}
	if err := hot.Create(types.TableDescriptor{
		Name:      "Users",
		KeySchema: types.KeySchema{PK: "id"},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	if _, err := hot.CreateGlobalIndex(types.GlobalIndexDescriptor{
		Name: "idx_email", BaseTable: "Users", IndexedColumn: "email",
	}); err != nil {
		t.Fatalf("create index: %v", err)
	}

	cold, err := New(db)
	if err != nil {
		t.Fatalf("cold: %v", err)
	}
	cold.mu.Lock()
	delete(cold.globalIndexes, "idx_email")
	cold.mu.Unlock()

	got, err := cold.DescribeGlobalIndex("idx_email")
	if err != nil {
		t.Fatalf("cold Describe: %v", err)
	}
	if got.IndexedColumn != "email" {
		t.Errorf("cold Describe missing column: %+v", got)
	}
}
