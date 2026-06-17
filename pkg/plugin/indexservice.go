package plugin

import (
	"fmt"

	"github.com/CefasDb/cefasdb/internal/core/index"
	"github.com/CefasDb/cefasdb/internal/core/model"
)

// ItemSource is a pull iterator over the base table. The IndexService
// hands it to plugin.Build / Rebuild so the engine doesn't have to
// know how a plugin consumes items. Implementations honour the
// yield-callback contract: return false from yield to stop early.
type ItemSource func(yield func(model.Item) bool)

// IndexService is the engine-side wiring for plugin-backed indexes.
// The engine constructs one with a Registry + a source factory and
// then exposes Create / Rebuild verbs that the existing IndexService
// catalog hook routes into when descriptor.PluginName != "".
//
// IndexService deliberately stays inside pkg/plugin (no engine
// imports) — callers wire it up by supplying the source factory.
type IndexService struct {
	r       *Registry
	sources func(table string) ItemSource
}

// NewIndexService composes a Registry with a base-table iterator
// factory. Engines pass a function that snapshots the table and
// streams its items into the yield callback.
func NewIndexService(r *Registry, sources func(table string) ItemSource) *IndexService {
	return &IndexService{r: r, sources: sources}
}

// Create routes a CreateIndex into the matching plugin's Build
// method. Returns an explicit error when descriptor.PluginName is
// empty (callers must check first and dispatch to the built-in path).
func (s *IndexService) Create(d index.Descriptor) error {
	plug, err := s.resolveIndex(d)
	if err != nil {
		return err
	}
	return plug.Build(d, s.sources(d.Table))
}

// Rebuild re-seeds an existing plugin-backed index from the base
// table contents.
func (s *IndexService) Rebuild(d index.Descriptor) error {
	plug, err := s.resolveIndex(d)
	if err != nil {
		return err
	}
	return plug.Build(d, s.sources(d.Table))
}

func (s *IndexService) resolveIndex(d index.Descriptor) (IndexPlugin, error) {
	if d.PluginName == "" {
		return nil, fmt.Errorf("index %q: no plugin assigned", d.Name)
	}
	raw, ok := s.r.Lookup(d.PluginName)
	if !ok {
		return nil, fmt.Errorf("index %q: plugin %q not registered or disabled", d.Name, d.PluginName)
	}
	plug, ok := raw.(IndexPlugin)
	if !ok {
		return nil, fmt.Errorf("index %q: plugin %q is not an IndexPlugin", d.Name, d.PluginName)
	}
	return plug, nil
}
