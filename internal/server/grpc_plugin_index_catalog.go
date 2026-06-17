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

func (s *GRPCServer) pluginIndexDescriptorsForTable(table string) ([]index.Descriptor, error) {
	persisted, err := s.db.ListPluginIndexDescriptors()
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]index.Descriptor)
	pluginIndexBook.mu.RLock()
	for _, desc := range pluginIndexBook.entries {
		if desc.Table == table {
			byKey[indexKey(desc.Table, desc.Name)] = desc
		}
	}
	pluginIndexBook.mu.RUnlock()
	for _, desc := range persisted {
		cachePluginIndexDescriptor(desc)
		if desc.Table == table {
			byKey[indexKey(desc.Table, desc.Name)] = desc
		}
	}
	out := make([]index.Descriptor, 0, len(byKey))
	for _, desc := range byKey {
		out = append(out, desc)
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
	return nil
}

func (s *GRPCServer) deletePluginIndexDescriptorsForTable(table string) error {
	if err := s.db.DeletePluginIndexDescriptorsForTable(table); err != nil {
		return err
	}
	removeCachedPluginIndexDescriptorsForTable(table)
	return nil
}
