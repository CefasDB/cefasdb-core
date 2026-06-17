package testharness_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/index"
	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/pkg/plugin/testharness"
)

type capturePlugin struct {
	seen []model.Item
}

func (p *capturePlugin) Manifest() plugin.Manifest {
	return plugin.Manifest{Name: "capture", Kind: plugin.KindIndex, Version: "1"}
}
func (p *capturePlugin) Build(_ index.Descriptor, items func(yield func(model.Item) bool)) error {
	items(func(it model.Item) bool { p.seen = append(p.seen, it); return true })
	return nil
}
func (p *capturePlugin) Update(index.Descriptor, model.Item, model.Item) error { return nil }
func (p *capturePlugin) Delete(index.Descriptor, model.Item) error             { return nil }
func (p *capturePlugin) Query(index.Descriptor, plugin.IndexQuery) (plugin.CandidateSet, error) {
	return nil, nil
}
func (p *capturePlugin) Estimate(index.Descriptor, plugin.IndexQuery) (int, error) { return 0, nil }

func TestHarnessSeedsAndBuilds(t *testing.T) {
	h := testharness.New(t)
	p := &capturePlugin{}
	h.MustRegister(p)
	h.SeedTable("Users",
		model.Item{"id": {T: model.AttrS, S: "u1"}},
		model.Item{"id": {T: model.AttrS, S: "u2"}},
	)
	if err := h.BuildIndex(index.Descriptor{Table: "Users", Name: "by_id", PluginName: "capture"}); err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(p.seen) != 2 {
		t.Fatalf("seen = %d, want 2", len(p.seen))
	}
}
