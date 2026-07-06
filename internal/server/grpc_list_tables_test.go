package server_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/server"
	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestListTablesSeesDescriptorsAppliedOutsideCatalogCache(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("pebble: %v", err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if err := cat.Create(types.TableDescriptor{
		Name:      "ConsumerGeoEvents",
		KeySchema: types.KeySchema{PK: "id"},
	}); err != nil {
		t.Fatalf("create cached table: %v", err)
	}

	outOfBand := types.TableDescriptor{
		Name:      "Consumers",
		KeySchema: types.KeySchema{PK: "id"},
	}
	raw, err := json.Marshal(outOfBand)
	if err != nil {
		t.Fatalf("marshal out-of-band descriptor: %v", err)
	}
	if err := db.Set(storage.KeyCatalog(outOfBand.Name), raw); err != nil {
		t.Fatalf("persist out-of-band descriptor: %v", err)
	}

	resp, err := server.NewGRPCServer(db, cat, nil).ListTables(context.Background(), &cefaspb.ListTablesRequest{})
	if err != nil {
		t.Fatalf("ListTables: %v", err)
	}
	got := make([]string, 0, len(resp.GetTables()))
	for _, td := range resp.GetTables() {
		got = append(got, td.GetName())
	}
	if !sameServerStrings(got, []string{"ConsumerGeoEvents", "Consumers"}) {
		t.Fatalf("ListTables names = %v, want both persisted tables", got)
	}
}

func sameServerStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
