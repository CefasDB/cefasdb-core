package storage

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

func normalizeStorageClass(class string) string {
	switch strings.ToLower(strings.TrimSpace(class)) {
	case "", types.StorageClassDisk:
		return types.StorageClassDisk
	case types.StorageClassMemory:
		return types.StorageClassMemory
	default:
		return class
	}
}

func isMemoryTable(td types.TableDescriptor) bool {
	return normalizeStorageClass(td.StorageClass) == types.StorageClassMemory
}

func validateDescriptorItem(td types.TableDescriptor, item types.Item) error {
	for _, def := range td.AttributeDefinitions {
		if strings.EqualFold(def.Type, "V") && def.VectorDimensions > 0 {
			av, ok := item[def.Name]
			if !ok {
				continue
			}
			if av.T != types.AttrVec {
				return fmt.Errorf("attribute %q: expected V<%d>, got type %d", def.Name, def.VectorDimensions, av.T)
			}
			if len(av.Vec) != def.VectorDimensions {
				return fmt.Errorf("attribute %q: vector dim %d != declared dim %d", def.Name, len(av.Vec), def.VectorDimensions)
			}
		}
	}
	return nil
}

func (d *DB) memorySet(table string, key, value []byte) {
	d.memMu.Lock()
	defer d.memMu.Unlock()
	if d.memTables == nil {
		d.memTables = make(map[string]map[string][]byte)
	}
	if d.memLoaded == nil {
		d.memLoaded = make(map[string]bool)
	}
	m := d.memTables[table]
	if m == nil {
		m = make(map[string][]byte)
		d.memTables[table] = m
	}
	d.memLoaded[table] = true
	m[string(key)] = append([]byte(nil), value...)
}

func (d *DB) memoryDelete(table string, key []byte) {
	d.memMu.Lock()
	defer d.memMu.Unlock()
	if d.memTables == nil {
		return
	}
	if m := d.memTables[table]; m != nil {
		delete(m, string(key))
	}
}

func (d *DB) memoryHasTable(table string) bool {
	d.memMu.RLock()
	defer d.memMu.RUnlock()
	return d.memLoaded != nil && d.memLoaded[table]
}

func (d *DB) memoryGet(table string, key []byte) ([]byte, bool) {
	d.memMu.RLock()
	defer d.memMu.RUnlock()
	m := d.memTables[table]
	if m == nil {
		return nil, false
	}
	v, ok := m[string(key)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

func (d *DB) memoryScan(table string, lower, upper []byte, limit int) ([]types.Item, error) {
	d.memMu.RLock()
	m := d.memTables[table]
	if m == nil {
		d.memMu.RUnlock()
		return nil, nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		kb := []byte(k)
		if bytes.Compare(kb, lower) >= 0 && (upper == nil || bytes.Compare(kb, upper) < 0) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	raw := make([][]byte, 0, len(keys))
	for _, k := range keys {
		raw = append(raw, append([]byte(nil), m[k]...))
		if limit > 0 && len(raw) >= limit {
			break
		}
	}
	d.memMu.RUnlock()

	out := make([]types.Item, 0, len(raw))
	for i, v := range raw {
		item, err := DecodeItem(v)
		if err != nil {
			return nil, fmt.Errorf("decode memory item %d: %w", i, err)
		}
		out = append(out, item)
	}
	return out, nil
}

func (d *DB) ensureMemoryTableLoaded(table string) error {
	if d.memoryHasTable(table) {
		return nil
	}
	lower, upper := PrefixPrimaryAll(table)
	it, err := d.Iter(lower, upper)
	if err != nil {
		return err
	}
	defer it.Close()

	loaded := make(map[string][]byte)
	for valid := it.First(); valid; valid = it.Next() {
		key := append([]byte(nil), it.Key()...)
		value := append([]byte(nil), it.Value()...)
		loaded[string(key)] = value
	}
	if err := it.Error(); err != nil {
		return err
	}

	d.memMu.Lock()
	defer d.memMu.Unlock()
	if d.memTables == nil {
		d.memTables = make(map[string]map[string][]byte)
	}
	if d.memLoaded == nil {
		d.memLoaded = make(map[string]bool)
	}
	if !d.memLoaded[table] {
		d.memTables[table] = loaded
		d.memLoaded[table] = true
	}
	return nil
}

func (d *DB) MemoryTableFootprint(table string) int64 {
	d.memMu.RLock()
	defer d.memMu.RUnlock()
	var total int64
	for k, v := range d.memTables[table] {
		total += int64(len(k) + len(v))
	}
	return total
}

func (d *DB) LoadMemoryTable(table string) error {
	return d.ensureMemoryTableLoaded(table)
}
