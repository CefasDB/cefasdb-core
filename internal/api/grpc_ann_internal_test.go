package api

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
	"github.com/osvaldoandrade/cefas/pkg/plugin/cosine"
	"github.com/osvaldoandrade/cefas/pkg/plugin/vectorlsh"
)

func TestIndexedTopKDoesNotUseExactScanFallback(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(pebble.Options{Path: dir})
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

func TestPluginIndexDescriptorSurvivesRestartAndRebuild(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	table := "RestartANNDocs"

	db, err := pebble.Open(pebble.Options{Path: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	srv := NewGRPCServer(db, cat, nil)
	srv.AttachPluginRegistry(testANNRegistry(t))
	createANNTableAndItems(t, ctx, srv, table)
	if _, err := srv.CreateIndex(ctx, &cefaspb.CreateIndexRequest{
		Descriptor_: annDescriptorPB(table),
	}); err != nil {
		t.Fatalf("create ann: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close first db: %v", err)
	}
	clearPluginIndexBookForTest()

	db, err = pebble.Open(pebble.Options{Path: dir})
	if err != nil {
		t.Fatalf("reopen storage: %v", err)
	}
	defer db.Close()
	cat, err = catalog.New(db)
	if err != nil {
		t.Fatalf("reopen catalog: %v", err)
	}
	srv = NewGRPCServer(db, cat, nil)
	srv.AttachPluginRegistry(testANNRegistry(t))
	defer clearPluginIndexBookForTest()

	desc, err := srv.DescribeIndex(ctx, &cefaspb.DescribeIndexRequest{Table: table, Name: "emb_ann"})
	if err != nil {
		t.Fatalf("describe after restart: %v", err)
	}
	if desc.GetDescriptor_().GetPluginName() != "ann" {
		t.Fatalf("descriptor plugin = %q, want ann", desc.GetDescriptor_().GetPluginName())
	}
	rebuild, err := srv.RebuildIndex(ctx, &cefaspb.RebuildIndexRequest{Table: table, Name: "emb_ann"})
	if err != nil {
		t.Fatalf("rebuild after restart: %v", err)
	}
	if rebuild.GetItemsIndexed() != 3 {
		t.Fatalf("items indexed = %d, want 3", rebuild.GetItemsIndexed())
	}
	topk, err := srv.TopK(ctx, &cefaspb.TopKRequest{
		Table:  table,
		Field:  "emb",
		Target: testPBVec(1, 0, 0),
		K:      2,
	})
	if err != nil {
		t.Fatalf("topk after rebuild: %v", err)
	}
	if len(topk.GetRows()) == 0 || topk.GetRows()[0].GetItem().GetAttributes()["id"].GetS() != "a" {
		t.Fatalf("unexpected rows: %+v", topk.GetRows())
	}
}

func TestCreateIndexBuildFailureDoesNotPersistDescriptor(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(pebble.Options{Path: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	reg := plugin.NewRegistry()
	if err := reg.Register(failingIndexPlugin{}); err != nil {
		t.Fatalf("register failing plugin: %v", err)
	}
	srv := NewGRPCServer(db, cat, nil)
	srv.AttachPluginRegistry(reg)
	ctx := context.Background()
	if _, err := srv.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{Name: "FailDocs", KeySchema: &cefaspb.KeySchema{Pk: "id"}},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	_, err = srv.CreateIndex(ctx, &cefaspb.CreateIndexRequest{
		Descriptor_: &cefaspb.PluginIndexDescriptor{
			Table: "FailDocs", Name: "bad_idx", PluginName: "failindex",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("want Internal build failure, got %v", err)
	}
	if _, ok, err := db.GetPluginIndexDescriptor("FailDocs", "bad_idx"); err != nil || ok {
		t.Fatalf("descriptor persisted after failed build: ok=%v err=%v", ok, err)
	}
}

func TestConcurrentCreateIndexPublishesOneDescriptor(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(pebble.Options{Path: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	srv := NewGRPCServer(db, cat, nil)
	srv.AttachPluginRegistry(testANNRegistry(t))
	ctx := context.Background()
	if _, err := srv.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "RaceDocs",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
			AttributeDefinitions: []*cefaspb.AttributeDefinition{{
				Name: "emb", Type: "V", VectorDimensions: 3,
			}},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < cap(errs); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := srv.CreateIndex(ctx, &cefaspb.CreateIndexRequest{
				Descriptor_: annDescriptorPB("RaceDocs"),
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)

	successes := 0
	alreadyExists := 0
	for err := range errs {
		switch status.Code(err) {
		case codes.OK:
			successes++
		case codes.AlreadyExists:
			alreadyExists++
		default:
			t.Fatalf("unexpected create-index error: %v", err)
		}
	}
	if successes != 1 || alreadyExists != 7 {
		t.Fatalf("successes=%d alreadyExists=%d, want 1/7", successes, alreadyExists)
	}
	all, err := db.ListPluginIndexDescriptors()
	if err != nil {
		t.Fatalf("list descriptors: %v", err)
	}
	count := 0
	for _, desc := range all {
		if desc.Table == "RaceDocs" && desc.Name == "emb_ann" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("descriptor count = %d, want 1", count)
	}
}

type failingIndexPlugin struct{}

func (failingIndexPlugin) Manifest() plugin.Manifest {
	return plugin.Manifest{Name: "failindex", Kind: plugin.KindIndex, Version: "1"}
}
func (failingIndexPlugin) Build(index.Descriptor, func(func(model.Item) bool)) error {
	return fmt.Errorf("build failed")
}
func (failingIndexPlugin) Update(index.Descriptor, model.Item, model.Item) error { return nil }
func (failingIndexPlugin) Delete(index.Descriptor, model.Item) error             { return nil }
func (failingIndexPlugin) Query(index.Descriptor, plugin.IndexQuery) (plugin.CandidateSet, error) {
	return nil, fmt.Errorf("not implemented")
}
func (failingIndexPlugin) Estimate(index.Descriptor, plugin.IndexQuery) (int, error) { return 0, nil }

func testPBVec(xs ...float64) *cefaspb.AttributeValue {
	return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_V{V: &cefaspb.Vector{Values: xs, Dim: int32(len(xs))}}}
}

func testANNRegistry(t *testing.T) *plugin.Registry {
	t.Helper()
	reg := plugin.NewRegistry()
	if err := reg.Register(vectorlsh.NewPlugin()); err != nil {
		t.Fatalf("register vectorlsh: %v", err)
	}
	if err := reg.Register(cosine.Op{}); err != nil {
		t.Fatalf("register cosine: %v", err)
	}
	return reg
}

func annDescriptorPB(table string) *cefaspb.PluginIndexDescriptor {
	return &cefaspb.PluginIndexDescriptor{
		Table:        table,
		Name:         "emb_ann",
		PluginName:   "ann",
		PluginConfig: []byte(`{"field":"emb","dim":3,"metric":"cosine","algorithm":"lsh"}`),
		KeySchema:    &cefaspb.KeySchema{Pk: "id"},
	}
}

func createANNTableAndItems(t *testing.T, ctx context.Context, srv *GRPCServer, table string) {
	t.Helper()
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
}

func clearPluginIndexBookForTest() {
	pluginIndexBook.mu.Lock()
	pluginIndexBook.entries = map[string]index.Descriptor{}
	pluginIndexBook.mu.Unlock()
}
