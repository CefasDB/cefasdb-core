package plugin_test

import (
	"errors"
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
)

type recIndexPlugin struct {
	name  string
	built []model.Item
	err   error
}

func (p *recIndexPlugin) Manifest() plugin.Manifest {
	return plugin.Manifest{Name: p.name, Kind: plugin.KindIndex, Version: "1"}
}
func (p *recIndexPlugin) Build(_ index.Descriptor, items func(yield func(model.Item) bool)) error {
	items(func(it model.Item) bool { p.built = append(p.built, it); return true })
	return p.err
}
func (p *recIndexPlugin) Update(index.Descriptor, model.Item, model.Item) error { return nil }
func (p *recIndexPlugin) Delete(index.Descriptor, model.Item) error             { return nil }
func (p *recIndexPlugin) Query(index.Descriptor, plugin.IndexQuery) (plugin.CandidateSet, error) {
	return nil, nil
}
func (p *recIndexPlugin) Estimate(index.Descriptor, plugin.IndexQuery) (int, error) { return 0, nil }

func TestIndexServiceRoutesCreateToPlugin(t *testing.T) {
	r := plugin.NewRegistry()
	p := &recIndexPlugin{name: "trigram"}
	_ = r.Register(p)
	items := []model.Item{
		{"id": {T: model.AttrS, S: "a"}},
		{"id": {T: model.AttrS, S: "b"}},
	}
	svc := plugin.NewIndexService(r, func(table string) plugin.ItemSource {
		return func(yield func(model.Item) bool) {
			for _, it := range items {
				if !yield(it) {
					return
				}
			}
		}
	})
	d := index.Descriptor{Table: "Users", Name: "x", PluginName: "trigram"}
	if err := svc.Create(d); err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(p.built) != 2 {
		t.Fatalf("built = %d, want 2", len(p.built))
	}
}

func TestIndexServiceRebuildReusesBuild(t *testing.T) {
	r := plugin.NewRegistry()
	p := &recIndexPlugin{name: "trigram"}
	_ = r.Register(p)
	svc := plugin.NewIndexService(r, func(string) plugin.ItemSource {
		return func(yield func(model.Item) bool) {
			yield(model.Item{"id": {T: model.AttrS, S: "a"}})
		}
	})
	d := index.Descriptor{Table: "T", Name: "x", PluginName: "trigram"}
	_ = svc.Create(d)
	_ = svc.Rebuild(d)
	if len(p.built) != 2 {
		t.Fatalf("built = %d, want 2 (1 create + 1 rebuild)", len(p.built))
	}
}

func TestIndexServiceErrorsOnUnknownPlugin(t *testing.T) {
	r := plugin.NewRegistry()
	svc := plugin.NewIndexService(r, func(string) plugin.ItemSource { return nil })
	err := svc.Create(index.Descriptor{Table: "T", Name: "x", PluginName: "ghost"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestIndexServiceErrorsOnEmptyPluginName(t *testing.T) {
	r := plugin.NewRegistry()
	svc := plugin.NewIndexService(r, func(string) plugin.ItemSource { return nil })
	err := svc.Create(index.Descriptor{Table: "T", Name: "x"})
	if err == nil {
		t.Fatal("expected error for empty PluginName")
	}
}

func TestIndexServiceErrorsWhenPluginNotIndexKind(t *testing.T) {
	r := plugin.NewRegistry()
	_ = r.Register(&stubPlugin{name: "cosine", kind: plugin.KindDistance})
	svc := plugin.NewIndexService(r, func(string) plugin.ItemSource { return nil })
	err := svc.Create(index.Descriptor{Table: "T", Name: "x", PluginName: "cosine"})
	if err == nil {
		t.Fatal("expected error for non-IndexPlugin")
	}
}

func TestIndexServicePropagatesBuildError(t *testing.T) {
	r := plugin.NewRegistry()
	p := &recIndexPlugin{name: "trigram", err: errors.New("boom")}
	_ = r.Register(p)
	svc := plugin.NewIndexService(r, func(string) plugin.ItemSource {
		return func(yield func(model.Item) bool) {}
	})
	err := svc.Create(index.Descriptor{Table: "T", Name: "x", PluginName: "trigram"})
	if err == nil || !errors.Is(err, p.err) {
		t.Fatalf("err = %v, want %v", err, p.err)
	}
}
