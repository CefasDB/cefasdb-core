package api

import (
	"context"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
	"github.com/osvaldoandrade/cefas/pkg/plugin/cosine"
	"github.com/osvaldoandrade/cefas/pkg/plugin/vectorlsh"
)

func TestIndexedTopKDoesNotUseExactScanFallback(t *testing.T) {
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Path: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	reg := plugin.NewRegistry()
	if err := reg.Register(vectorlsh.NewPlugin()); err != nil {
		t.Fatalf("register vectorlsh: %v", err)
	}
	if err := reg.Register(cosine.Op{}); err != nil {
		t.Fatalf("register cosine: %v", err)
	}
	srv := NewGRPCServer(db, cat, nil)
	srv.AttachPluginRegistry(reg)

	ctx := context.Background()
	table := "IndexedTopKNoScan"
	if _, err := srv.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      table,
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
			AttributeDefinitions: []*cefaspb.AttributeDefinition{{
				Name: "emb", Type: "V", VectorDimensions: 3,
			}},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for _, row := range []struct {
		id  string
		vec []float64
	}{
		{"a", []float64{1, 0, 0}},
		{"b", []float64{0.9, 0.1, 0}},
		{"c", []float64{0, 1, 0}},
	} {
		if _, err := srv.PutItem(ctx, &cefaspb.PutItemRequest{
			Table: table,
			Item: map[string]*cefaspb.AttributeValue{
				"id":  {Value: &cefaspb.AttributeValue_S{S: row.id}},
				"emb": testPBVec(row.vec...),
			},
		}); err != nil {
			t.Fatalf("put %s: %v", row.id, err)
		}
	}
	if _, err := srv.CreateIndex(ctx, &cefaspb.CreateIndexRequest{
		Descriptor_: &cefaspb.PluginIndexDescriptor{
			Table:        table,
			Name:         "emb_ann",
			PluginName:   "ann",
			PluginConfig: []byte(`{"field":"emb","dim":3,"metric":"cosine","algorithm":"lsh"}`),
			KeySchema:    &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create ann: %v", err)
	}
	defer func() {
		pluginIndexBook.mu.Lock()
		delete(pluginIndexBook.entries, indexKey(table, "emb_ann"))
		pluginIndexBook.mu.Unlock()
	}()

	scanned := false
	prev := exactTopKScanFallbackHook
	exactTopKScanFallbackHook = func(_, _, _ string) { scanned = true }
	defer func() { exactTopKScanFallbackHook = prev }()

	resp, err := srv.TopK(ctx, &cefaspb.TopKRequest{
		Table:  table,
		Field:  "emb",
		Target: testPBVec(1, 0, 0),
		K:      2,
	})
	if err != nil {
		t.Fatalf("topk: %v", err)
	}
	if scanned {
		t.Fatal("indexed TopK used exact scan fallback")
	}
	if len(resp.GetRows()) == 0 || resp.GetRows()[0].GetItem().GetAttributes()["id"].GetS() != "a" {
		t.Fatalf("unexpected rows: %+v", resp.GetRows())
	}
}

func testPBVec(xs ...float64) *cefaspb.AttributeValue {
	return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_V{V: &cefaspb.Vector{Values: xs, Dim: int32(len(xs))}}}
}
