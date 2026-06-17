// Package testharness gives plugin authors a self-contained fixture
// they can run inside `go test` without booting the cefas server or
// touching the catalog.
//
// Typical usage:
//
//	h := testharness.New(t)
//	h.MustRegister(&MyPlugin{})
//	got, ok := h.Registry.Lookup("myplugin")
//	if !ok {
//	    t.Fatal("plugin missing")
//	}
package testharness

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/index"
	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
)

// Harness wraps an isolated registry + a stub item source. Plugins
// under test register against Harness.Registry; the IndexService
// helper exposes Build / Rebuild paths against an in-memory item
// stream.
type Harness struct {
	t        *testing.T
	Registry *plugin.Registry
	Service  *plugin.IndexService
	items    map[string][]model.Item // table → items
}

// New returns a fresh Harness scoped to t. The harness installs no
// global state; closing it is automatic via t.Cleanup.
func New(t *testing.T) *Harness {
	t.Helper()
	r := plugin.NewRegistry()
	h := &Harness{
		t:        t,
		Registry: r,
		items:    map[string][]model.Item{},
	}
	h.Service = plugin.NewIndexService(r, h.sourceFor)
	return h
}

// SeedTable installs items the IndexService will hand to Build /
// Rebuild for `table`. Tests use this to pre-populate a fake table.
func (h *Harness) SeedTable(table string, items ...model.Item) {
	h.items[table] = append(h.items[table], items...)
}

// MustRegister panics-as-test-failure on register error.
func (h *Harness) MustRegister(p plugin.Plugin) {
	h.t.Helper()
	if err := h.Registry.Register(p); err != nil {
		h.t.Fatalf("register %T: %v", p, err)
	}
}

// BuildIndex routes through the harness's IndexService — useful for
// asserting that a plugin-backed Create works end-to-end without
// catalog plumbing.
func (h *Harness) BuildIndex(d index.Descriptor) error {
	return h.Service.Create(d)
}

// sourceFor returns an ItemSource over the in-memory items.
func (h *Harness) sourceFor(table string) plugin.ItemSource {
	items := h.items[table]
	return func(yield func(model.Item) bool) {
		for _, it := range items {
			if !yield(it) {
				return
			}
		}
	}
}
