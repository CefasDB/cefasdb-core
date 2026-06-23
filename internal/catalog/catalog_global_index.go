package catalog

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// loadAllGlobalIndexes hydrates the in-memory map from pebble on
// open. Mirrors loadAllViews / loadAllServiceLevels.
func (c *Catalog) loadAllGlobalIndexes() error {
	lower, upper := storage.PrefixGlobalIndexes()
	it, err := c.db.Iter(lower, upper)
	if err != nil {
		return err
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		var gi types.GlobalIndexDescriptor
		if err := json.Unmarshal(it.Value(), &gi); err != nil {
			return fmt.Errorf("decode global-index at %s: %w", it.Key(), err)
		}
		c.globalIndexes[gi.Name] = gi
	}
	return it.Error()
}

// CreateGlobalIndex persists a new global secondary index descriptor.
// The base table must exist; an index name may not collide with an
// existing table, view, or other index.
func (c *Catalog) CreateGlobalIndex(gi types.GlobalIndexDescriptor) (types.GlobalIndexDescriptor, error) {
	gi.Name = strings.TrimSpace(gi.Name)
	gi.BaseTable = strings.TrimSpace(gi.BaseTable)
	gi.IndexedColumn = strings.TrimSpace(gi.IndexedColumn)
	if gi.Name == "" {
		return types.GlobalIndexDescriptor{}, fmt.Errorf("global index name required")
	}
	if gi.BaseTable == "" {
		return types.GlobalIndexDescriptor{}, fmt.Errorf("global index %q: base_table required", gi.Name)
	}
	if gi.IndexedColumn == "" {
		return types.GlobalIndexDescriptor{}, fmt.Errorf("global index %q: indexed_column required", gi.Name)
	}
	if gi.Status == "" {
		gi.Status = types.GlobalIndexStatusBuilding
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.globalIndexes[gi.Name]; exists {
		return types.GlobalIndexDescriptor{}, types.ErrGlobalIndexExists
	}
	if _, exists := c.tables[gi.Name]; exists {
		return types.GlobalIndexDescriptor{}, fmt.Errorf("name %q clashes with an existing table", gi.Name)
	}
	if _, exists := c.views[gi.Name]; exists {
		return types.GlobalIndexDescriptor{}, fmt.Errorf("name %q clashes with an existing materialized view", gi.Name)
	}
	if _, ok := c.tables[gi.BaseTable]; !ok {
		return types.GlobalIndexDescriptor{}, fmt.Errorf("base table %q: %w", gi.BaseTable, types.ErrTableNotFound)
	}
	raw, err := json.Marshal(gi)
	if err != nil {
		return types.GlobalIndexDescriptor{}, fmt.Errorf("marshal global-index: %w", err)
	}
	if err := c.db.Set(storage.KeyGlobalIndex(gi.Name), raw); err != nil {
		return types.GlobalIndexDescriptor{}, fmt.Errorf("persist global-index: %w", err)
	}
	c.globalIndexes[gi.Name] = gi
	return gi, nil
}

// DescribeGlobalIndex returns the cached descriptor; falls back to a
// pebble Get on cache miss (peer nodes whose catalog cache was never
// warmed for this index will hit this path the first time, similar
// to MV's #536 fallback).
func (c *Catalog) DescribeGlobalIndex(name string) (types.GlobalIndexDescriptor, error) {
	c.mu.RLock()
	gi, ok := c.globalIndexes[name]
	c.mu.RUnlock()
	if ok {
		return gi, nil
	}
	raw, err := c.db.Get(storage.KeyGlobalIndex(name))
	if err != nil {
		return types.GlobalIndexDescriptor{}, types.ErrGlobalIndexNotFound
	}
	var out types.GlobalIndexDescriptor
	if err := json.Unmarshal(raw, &out); err != nil {
		return types.GlobalIndexDescriptor{}, fmt.Errorf("decode global-index %s: %w", name, err)
	}
	c.mu.Lock()
	c.globalIndexes[out.Name] = out
	c.mu.Unlock()
	return out, nil
}

// DropGlobalIndex removes the descriptor. The pointer data persisted
// by Phase 2 onwards is left for the operator's reclaim path; the
// catalog-only DDL semantics match DROP MATERIALIZED VIEW.
func (c *Catalog) DropGlobalIndex(name string) error {
	if name == "" {
		return fmt.Errorf("global index name required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.globalIndexes[name]; !exists {
		return types.ErrGlobalIndexNotFound
	}
	if err := c.db.Delete(storage.KeyGlobalIndex(name)); err != nil {
		return fmt.Errorf("delete global-index: %w", err)
	}
	delete(c.globalIndexes, name)
	return nil
}

// ListGlobalIndexes returns every descriptor optionally filtered to
// a base table. Order is unspecified.
func (c *Catalog) ListGlobalIndexes(baseTable string) []types.GlobalIndexDescriptor {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]types.GlobalIndexDescriptor, 0, len(c.globalIndexes))
	for _, gi := range c.globalIndexes {
		if baseTable != "" && gi.BaseTable != baseTable {
			continue
		}
		out = append(out, gi)
	}
	return out
}

// UpdateGlobalIndex replaces the persisted descriptor. Phase 2+
// status updates (building → active, paused, etc.) go through here.
func (c *Catalog) UpdateGlobalIndex(gi types.GlobalIndexDescriptor) (types.GlobalIndexDescriptor, error) {
	if gi.Name == "" {
		return types.GlobalIndexDescriptor{}, fmt.Errorf("global index name required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.globalIndexes[gi.Name]; !exists {
		return types.GlobalIndexDescriptor{}, types.ErrGlobalIndexNotFound
	}
	raw, err := json.Marshal(gi)
	if err != nil {
		return types.GlobalIndexDescriptor{}, fmt.Errorf("marshal global-index: %w", err)
	}
	if err := c.db.Set(storage.KeyGlobalIndex(gi.Name), raw); err != nil {
		return types.GlobalIndexDescriptor{}, fmt.Errorf("persist global-index: %w", err)
	}
	c.globalIndexes[gi.Name] = gi
	return gi, nil
}
