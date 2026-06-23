package pebble

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/CefasDb/cefasdb/internal/storage"

	"github.com/CefasDb/cefasdb/pkg/types"
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
		switch types.NormalizeAttributeType(def.Type) {
		case types.AttributeTypeVector:
			if def.VectorDimensions <= 0 {
				continue
			}
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
		case types.AttributeTypeCounter:
			av, ok := item[def.Name]
			if !ok {
				continue
			}
			if av.T != types.AttrN {
				return fmt.Errorf("%w: attribute %q stores as N, got type %d", storage.ErrInvalidCounterMutation, def.Name, av.T)
			}
		}
	}
	return nil
}

func validatePutItemCounters(td types.TableDescriptor, prior, item types.Item, allowCounterWrite bool) error {
	if allowCounterWrite {
		return nil
	}
	for _, def := range td.AttributeDefinitions {
		if !types.IsCounterAttributeType(def.Type) {
			continue
		}
		if _, ok := item[def.Name]; ok {
			return fmt.Errorf("%w: attribute %q", storage.ErrInvalidCounterMutation, def.Name)
		}
		if _, ok := prior[def.Name]; ok {
			return fmt.Errorf("%w: PutItem would overwrite existing counter attribute %q", storage.ErrInvalidCounterMutation, def.Name)
		}
	}
	return nil
}

func tableHasCounterDefinition(td types.TableDescriptor) bool {
	for _, def := range td.AttributeDefinitions {
		if types.IsCounterAttributeType(def.Type) {
			return true
		}
	}
	return false
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
		item, err := storage.DecodeItem(v)
		if err != nil {
			return nil, fmt.Errorf("decode memory item %d: %w", i, err)
		}
		out = append(out, item)
	}
	return out, nil
}

// memoryScanWith is the streaming counterpart of memoryScan used by
// ScanTableWith. Snapshots the key set under the read lock so the
// visit callback can run without holding it (visit may be slow:
// filter evaluation, network sends, decoding upstream).
func (d *DB) memoryScanWith(table string, lower, upper []byte, visit func(types.Item) bool) error {
	d.memMu.RLock()
	m := d.memTables[table]
	if m == nil {
		d.memMu.RUnlock()
		return nil
	}
	keys := make([]string, 0, len(m))
	values := make([][]byte, 0, len(m))
	for k, v := range m {
		kb := []byte(k)
		if bytes.Compare(kb, lower) >= 0 && (upper == nil || bytes.Compare(kb, upper) < 0) {
			keys = append(keys, k)
			values = append(values, append([]byte(nil), v...))
		}
	}
	d.memMu.RUnlock()

	order := make([]int, len(keys))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool { return keys[order[i]] < keys[order[j]] })

	for _, idx := range order {
		item, err := storage.DecodeItem(values[idx])
		if err != nil {
			return fmt.Errorf("decode memory item %q: %w", keys[idx], err)
		}
		if !visit(item) {
			return nil
		}
	}
	return nil
}

func (d *DB) ensureMemoryTableLoaded(table string) error {
	if d.memoryHasTable(table) {
		return nil
	}
	lower, upper := storage.PrefixPrimaryAll(table)
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
