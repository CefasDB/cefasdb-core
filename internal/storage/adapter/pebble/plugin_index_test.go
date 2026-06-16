package pebble_test

import (
	"testing"

	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
)

func TestPluginIndexDescriptorEncodeDecode(t *testing.T) {
	desc := index.Descriptor{
		Table:        "docs",
		Name:         "emb_ann",
		PluginName:   "ann",
		PluginConfig: []byte(`{"field":"emb","dim":3,"metric":"cosine"}`),
		KeySchema:    model.KeySchema{PK: "id"},
	}
	raw, err := pebble.EncodePluginIndexDescriptor(desc)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := pebble.DecodePluginIndexDescriptor(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Table != desc.Table || got.Name != desc.Name || got.PluginName != desc.PluginName || got.KeySchema.PK != "id" {
		t.Fatalf("descriptor mismatch: %+v", got)
	}
	if string(got.PluginConfig) != string(desc.PluginConfig) {
		t.Fatalf("config = %s, want %s", got.PluginConfig, desc.PluginConfig)
	}
}

func TestPluginIndexDescriptorStoreListAndDeleteByTable(t *testing.T) {
	db := openTestDB(t)
	descs := []index.Descriptor{
		{Table: "docs/main", Name: "emb/ann", PluginName: "ann", PluginConfig: []byte(`{"field":"emb","dim":3}`), KeySchema: model.KeySchema{PK: "id"}},
		{Table: "docs/main", Name: "name_tri", PluginName: "trigram", PluginConfig: []byte(`{"field":"name"}`), KeySchema: model.KeySchema{PK: "id"}},
		{Table: "other", Name: "emb_ann", PluginName: "ann", PluginConfig: []byte(`{"field":"emb","dim":3}`), KeySchema: model.KeySchema{PK: "id"}},
	}
	for _, desc := range descs {
		if err := db.PutPluginIndexDescriptor(desc); err != nil {
			t.Fatalf("put %s/%s: %v", desc.Table, desc.Name, err)
		}
	}
	got, ok, err := db.GetPluginIndexDescriptor("docs/main", "emb/ann")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok || got.PluginName != "ann" {
		t.Fatalf("get = %+v ok=%v", got, ok)
	}
	all, err := db.ListPluginIndexDescriptors()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("list len = %d, want 3", len(all))
	}
	if err := db.DeletePluginIndexDescriptorsForTable("docs/main"); err != nil {
		t.Fatalf("delete table descriptors: %v", err)
	}
	if _, ok, err := db.GetPluginIndexDescriptor("docs/main", "emb/ann"); err != nil || ok {
		t.Fatalf("deleted descriptor ok=%v err=%v", ok, err)
	}
	remaining, err := db.ListPluginIndexDescriptors()
	if err != nil {
		t.Fatalf("list remaining: %v", err)
	}
	if len(remaining) != 1 || remaining[0].Table != "other" {
		t.Fatalf("remaining = %+v", remaining)
	}
}

func TestPluginIndexDescriptorIncludedInBackupCheckpoint(t *testing.T) {
	db := openTestDB(t)
	desc := index.Descriptor{
		Table:        "docs",
		Name:         "emb_ann",
		PluginName:   "ann",
		PluginConfig: []byte(`{"field":"emb","dim":3}`),
		KeySchema:    model.KeySchema{PK: "id"},
	}
	if err := db.PutPluginIndexDescriptor(desc); err != nil {
		t.Fatalf("put descriptor: %v", err)
	}
	meta, err := db.CreateBackup("with-plugin-index", nil)
	if err != nil {
		t.Fatalf("create backup: %v", err)
	}
	checkpoint, err := pebble.Open(pebble.Options{Path: meta.CheckpointAt})
	if err != nil {
		t.Fatalf("open checkpoint: %v", err)
	}
	defer checkpoint.Close()
	got, ok, err := checkpoint.GetPluginIndexDescriptor("docs", "emb_ann")
	if err != nil {
		t.Fatalf("get checkpoint descriptor: %v", err)
	}
	if !ok || got.PluginName != "ann" {
		t.Fatalf("checkpoint descriptor = %+v ok=%v", got, ok)
	}
}
