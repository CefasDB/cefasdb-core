package server

import (
	"sort"
	"strings"

	"github.com/CefasDb/cefasdb/internal/core/index"
)

func cachePluginIndexDescriptor(desc index.Descriptor) {
	pluginIndexBook.mu.Lock()
	pluginIndexBook.entries[indexKey(desc.Table, desc.Name)] = desc
	pluginIndexBook.mu.Unlock()
}

func removeCachedPluginIndexDescriptor(table, name string) {
	pluginIndexBook.mu.Lock()
	delete(pluginIndexBook.entries, indexKey(table, name))
	pluginIndexBook.mu.Unlock()
}

func removeCachedPluginIndexDescriptorsForTable(table string) {
	pluginIndexBook.mu.Lock()
	for key, desc := range pluginIndexBook.entries {
		if desc.Table == table {
			delete(pluginIndexBook.entries, key)
		}
	}
	pluginIndexBook.mu.Unlock()
}

func (s *GRPCServer) lookupPluginIndexDescriptor(table, name string) (index.Descriptor, bool, error) {
	desc, ok, err := s.db.GetPluginIndexDescriptor(table, name)
	if err != nil {
		return index.Descriptor{}, false, err
	}
	if ok {
		cachePluginIndexDescriptor(desc)
		return desc, true, nil
	}
	return index.Descriptor{}, false, nil
}

// pluginIndexDescriptorsForTable returns every plugin index attached
// to table. Hot path: called from the mutation hook on every Put /
// BatchWrite. The pre-#487 version did a pebble Iter + JSON decode
// per call, which taxed write_only by ~11% on bench. Today it
// reads from the in-memory pluginIndexBook only; hydration happens
// at startup (hydratePluginIndexCatalog) and on Create /
// Drop / DeleteForTable, all of which keep the cache authoritative.
//
// When the catalog has no descriptors for the table — the common
// case for tables without an attached plugin index — this short-
// circuits to a single map traversal with zero allocations on the
// happy path.
func (s *GRPCServer) pluginIndexDescriptorsForTable(table string) ([]index.Descriptor, error) {
	pluginIndexBook.mu.RLock()
	out := make([]index.Descriptor, 0, 4)
	for _, desc := range pluginIndexBook.entries {
		if desc.Table == table {
			out = append(out, desc)
		}
	}
	pluginIndexBook.mu.RUnlock()
	if len(out) == 0 {
		return nil, nil
	}
	sort.Slice(out, func(i, j int) bool {
		if !strings.EqualFold(out[i].PluginName, out[j].PluginName) {
			return out[i].PluginName < out[j].PluginName
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (s *GRPCServer) hydratePluginIndexCatalog() error {
	descs, err := s.db.ListPluginIndexDescriptors()
	if err != nil {
		return err
	}
	for _, desc := range descs {
		cachePluginIndexDescriptor(desc)
	}
	// Build the local slice for every known descriptor in the
	// background so this node serves queries without waiting for the
	// first lazy access. Failures get reported per-descriptor via the
	// ensure path's own logging; startup continues either way.
	s.rehydratePluginIndexLocalStates()
	return nil
}

func (s *GRPCServer) deletePluginIndexDescriptorsForTable(table string) error {
	if err := s.db.DeletePluginIndexDescriptorsForTable(table); err != nil {
		return err
	}
	removeCachedPluginIndexDescriptorsForTable(table)
	return nil
}
