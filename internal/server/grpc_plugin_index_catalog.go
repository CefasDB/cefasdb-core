package server

import (
	"sort"
	"strings"
	"time"

	"github.com/CefasDb/cefasdb/internal/core/index"
)

const pluginIndexDescriptorRefreshInterval = time.Second

func cachePluginIndexDescriptor(desc index.Descriptor) {
	pluginIndexBook.mu.Lock()
	if pluginIndexBook.tableRefreshedAt == nil {
		pluginIndexBook.tableRefreshedAt = map[string]time.Time{}
	}
	pluginIndexBook.entries[indexKey(desc.Table, desc.Name)] = desc
	pluginIndexBook.tableRefreshedAt[desc.Table] = time.Now()
	pluginIndexBook.mu.Unlock()
}

func removeCachedPluginIndexDescriptor(table, name string) {
	pluginIndexBook.mu.Lock()
	delete(pluginIndexBook.entries, indexKey(table, name))
	delete(pluginIndexBook.tableRefreshedAt, table)
	pluginIndexBook.mu.Unlock()
}

func removeCachedPluginIndexDescriptorsForTable(table string) {
	pluginIndexBook.mu.Lock()
	for key, desc := range pluginIndexBook.entries {
		if desc.Table == table {
			delete(pluginIndexBook.entries, key)
		}
	}
	delete(pluginIndexBook.tableRefreshedAt, table)
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
// reads from the in-memory pluginIndexBook first; if the table is
// missing from this process's cache, it refreshes from durable
// descriptors at most once per second. This keeps peers that did not
// receive the CreateIndex RPC from missing plugin maintenance forever.
//
// When the catalog has no descriptors for the table — the common
// case for tables without an attached plugin index — this short-
// circuits to a single map traversal with zero allocations on the
// happy path.
func (s *GRPCServer) pluginIndexDescriptorsForTable(table string) ([]index.Descriptor, error) {
	if out := cachedPluginIndexDescriptorsForTable(table); len(out) > 0 {
		return out, nil
	}

	if !shouldRefreshPluginIndexDescriptors(table, time.Now()) {
		return nil, nil
	}
	descs, err := s.db.ListPluginIndexDescriptors()
	if err != nil {
		return nil, err
	}
	pluginIndexBook.mu.Lock()
	if pluginIndexBook.tableRefreshedAt == nil {
		pluginIndexBook.tableRefreshedAt = map[string]time.Time{}
	}
	for _, desc := range descs {
		pluginIndexBook.entries[indexKey(desc.Table, desc.Name)] = desc
	}
	pluginIndexBook.tableRefreshedAt[table] = time.Now()
	pluginIndexBook.mu.Unlock()

	return cachedPluginIndexDescriptorsForTable(table), nil
}

func cachedPluginIndexDescriptorsForTable(table string) []index.Descriptor {
	pluginIndexBook.mu.RLock()
	out := make([]index.Descriptor, 0, 4)
	for _, desc := range pluginIndexBook.entries {
		if desc.Table == table {
			out = append(out, desc)
		}
	}
	pluginIndexBook.mu.RUnlock()
	if len(out) == 0 {
		return nil
	}
	sort.Slice(out, func(i, j int) bool {
		if !strings.EqualFold(out[i].PluginName, out[j].PluginName) {
			return out[i].PluginName < out[j].PluginName
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func shouldRefreshPluginIndexDescriptors(table string, now time.Time) bool {
	pluginIndexBook.mu.RLock()
	defer pluginIndexBook.mu.RUnlock()
	last := pluginIndexBook.tableRefreshedAt[table]
	return last.IsZero() || now.Sub(last) >= pluginIndexDescriptorRefreshInterval
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
