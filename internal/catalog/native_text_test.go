package catalog_test

import (
	"errors"
	"testing"

	"github.com/CefasDb/cefasdb/internal/catalog"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestNativeSchemaColdCatalogCannotReplaceStoredDescriptor(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	first, err := catalog.New(db)
	if err != nil {
		t.Fatal(err)
	}
	cold, err := catalog.New(db)
	if err != nil {
		t.Fatal(err)
	}
	td := types.TableDescriptor{Name: "logs", KeySchema: types.KeySchema{PK: "tenantId", SK: "orderKey"}, TTLAttribute: "expiresAt", TextIndexes: []types.TextIndexDescriptor{{Name: "message", Field: "message"}}}
	if err := first.Create(td); err != nil {
		t.Fatal(err)
	}
	changed := td
	changed.TextIndexes = []types.TextIndexDescriptor{{Name: "different", Field: "other"}}
	if err := cold.Create(changed); !errors.Is(err, types.ErrTableAlreadyExists) {
		t.Fatalf("cold metadata cache replaced native schema: %v", err)
	}
	if err := first.UpdateTable(changed); err == nil {
		t.Fatal("index change silently skipped backfill")
	}
	changed = td
	changed.TTLAttribute = "other"
	if err := first.UpdateTable(changed); err == nil {
		t.Fatal("expiration schema changed without migration")
	}
}
