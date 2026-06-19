package pebble

import (
	"sync"
	"sync/atomic"

	"github.com/CefasDb/cefasdb/internal/storage"
)

const defaultItemCacheEntries = 32768

type itemCache struct {
	mu      sync.RWMutex
	max     int
	entries map[string][]byte
	slots   map[string]int
	order   []string
	next    int
	tables  map[string]int
	epoch   atomic.Uint64
}

func newItemCache(max int) *itemCache {
	if max < 0 {
		max = 0
	}
	return &itemCache{
		max:     max,
		entries: make(map[string][]byte, max),
		slots:   make(map[string]int, max),
		tables:  make(map[string]int),
	}
}

func (c *itemCache) get(key []byte) ([]byte, bool) {
	if c == nil || c.max <= 0 {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	v, ok := c.entries[string(key)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), v...), true
}

func (c *itemCache) set(key, value []byte) {
	if c == nil || c.max <= 0 || len(key) == 0 || value == nil {
		return
	}
	k := string(key)
	v := append([]byte(nil), value...)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.epoch.Add(1)
	table, hasTable := storage.PrimaryTableFromKey(key)
	c.setLocked(k, v, table, hasTable)
}

func (c *itemCache) setIfEpoch(key, value []byte, epoch uint64) {
	if c == nil || c.max <= 0 || len(key) == 0 || value == nil {
		return
	}
	k := string(key)
	v := append([]byte(nil), value...)
	if c.epoch.Load() != epoch {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.epoch.Load() != epoch {
		return
	}
	table, hasTable := storage.PrimaryTableFromKey(key)
	c.setLocked(k, v, table, hasTable)
}

func (c *itemCache) setLocked(k string, v []byte, table string, hasTable bool) {
	if _, ok := c.entries[k]; ok {
		c.entries[k] = v
		return
	}

	if len(c.order) < c.max {
		c.entries[k] = v
		c.slots[k] = len(c.order)
		c.order = append(c.order, k)
		if hasTable {
			c.tables[table]++
		}
		return
	}

	if len(c.order) == 0 {
		return
	}
	slot := c.next % len(c.order)
	evict := c.order[slot]
	if evict != "" {
		delete(c.entries, evict)
		delete(c.slots, evict)
		c.removeTableLocked([]byte(evict))
	}
	c.order[slot] = k
	c.slots[k] = slot
	c.entries[k] = v
	if hasTable {
		c.tables[table]++
	}
	c.next = (slot + 1) % len(c.order)
}

func (c *itemCache) mutationEpoch() uint64 {
	if c == nil {
		return 0
	}
	return c.epoch.Load()
}

func (c *itemCache) markMutation() {
	if c == nil || c.max <= 0 {
		return
	}
	c.epoch.Add(1)
}

func (c *itemCache) hasEntries() bool {
	if c == nil || c.max <= 0 {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries) > 0
}

func (c *itemCache) hasAnyTable(tables []string) bool {
	if c == nil || c.max <= 0 || len(tables) == 0 {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, table := range tables {
		if table != "" && c.tables[table] > 0 {
			return true
		}
	}
	return false
}

func (c *itemCache) cachedTableBytes() [][]byte {
	if c == nil || c.max <= 0 {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.tables) == 0 {
		return nil
	}
	tables := make([][]byte, 0, len(c.tables))
	for table := range c.tables {
		tables = append(tables, []byte(table))
	}
	return tables
}

func (c *itemCache) clearIfNotEmpty() {
	if c == nil || c.max <= 0 {
		return
	}
	c.mu.RLock()
	empty := len(c.entries) == 0
	c.mu.RUnlock()
	if empty {
		return
	}
	c.clear()
}

func (c *itemCache) delete(key []byte) {
	c.deleteKeys([][]byte{key})
}

func (c *itemCache) deleteKeysForTables(keys [][]byte, tables []string) {
	if c == nil || c.max <= 0 {
		return
	}
	if len(keys) == 0 {
		return
	}
	keys = nonEmptyKeys(keys)
	if len(keys) == 0 {
		return
	}
	c.markMutation()
	if !c.hasAnyTable(tables) {
		return
	}
	c.deleteKeysAfterMutation(keys)
}

func (c *itemCache) deleteKeys(keys [][]byte) {
	if c == nil || c.max <= 0 {
		return
	}
	keys = nonEmptyKeys(keys)
	if len(keys) == 0 {
		return
	}

	c.epoch.Add(1)
	c.deleteKeysAfterMutation(keys)
}

func (c *itemCache) deleteKeysAfterMutation(keys [][]byte) {
	c.mu.RLock()
	if len(c.entries) == 0 {
		c.mu.RUnlock()
		return
	}
	exists := false
	for _, key := range keys {
		if _, ok := c.entries[string(key)]; ok {
			exists = true
			break
		}
	}
	c.mu.RUnlock()
	if !exists {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, key := range keys {
		k := string(key)
		if slot, ok := c.slots[k]; ok {
			delete(c.slots, k)
			if slot >= 0 && slot < len(c.order) {
				c.order[slot] = ""
			}
			c.removeTableLocked(key)
		}
		delete(c.entries, k)
	}
}

func (c *itemCache) deleteMutations(mutations []itemCacheMutation) {
	if c == nil || c.max <= 0 || len(mutations) == 0 {
		return
	}

	c.epoch.Add(1)

	c.mu.RLock()
	if len(c.entries) == 0 {
		c.mu.RUnlock()
		return
	}
	exists := false
	for _, mutation := range mutations {
		if len(mutation.key) == 0 {
			continue
		}
		if _, ok := c.entries[string(mutation.key)]; ok {
			exists = true
			break
		}
	}
	c.mu.RUnlock()
	if !exists {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, mutation := range mutations {
		if len(mutation.key) == 0 {
			continue
		}
		k := string(mutation.key)
		if slot, ok := c.slots[k]; ok {
			delete(c.slots, k)
			if slot >= 0 && slot < len(c.order) {
				c.order[slot] = ""
			}
			c.removeTableLocked(mutation.key)
		}
		delete(c.entries, k)
	}
}

func (c *itemCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string][]byte, c.max)
	c.slots = make(map[string]int, c.max)
	c.tables = make(map[string]int)
	c.order = nil
	c.next = 0
	c.epoch.Add(1)
}

func nonEmptyKeys(keys [][]byte) [][]byte {
	out := keys[:0]
	for _, key := range keys {
		if len(key) > 0 {
			out = append(out, key)
		}
	}
	return out
}

func (c *itemCache) removeTableLocked(key []byte) {
	table, ok := storage.PrimaryTableFromKey(key)
	if !ok {
		return
	}
	if c.tables[table] <= 1 {
		delete(c.tables, table)
		return
	}
	c.tables[table]--
}
