package pebble

import (
	"bytes"

	"github.com/CefasDb/cefasdb/internal/storage"

	pebbledb "github.com/cockroachdb/pebble"
)

// ObservePendingBatch invalidates primary keys before a batch is committed.
// Missing after a failed commit is acceptable; serving a stale post-commit
// value is not.
func (d *DB) ObservePendingBatch(repr []byte) {
	d.ObservePendingBatchWithTables(repr, nil)
}

func (d *DB) ObservePendingBatchWithTables(repr []byte, itemCacheTables []string) {
	d.observeItemCacheBatch(repr, itemCacheTables)
}

// ObserveAppliedBatch lets commit/apply paths keep per-node item caches
// coherent when they apply a pebble.Batch.Repr directly to the raw DB.
func (d *DB) ObserveAppliedBatch(repr []byte) {
	d.ObserveAppliedBatchWithTables(repr, nil)
}

func (d *DB) ObserveAppliedBatchWithTables(repr []byte, itemCacheTables []string) {
	if d == nil || d.items == nil {
		return
	}
	if len(repr) == 0 {
		d.items.clear()
		return
	}
	d.observeItemCacheBatch(repr, itemCacheTables)
}

type itemCacheMutation struct {
	key []byte
}

func itemCacheBatchHasPrimaryMutation(repr []byte) bool {
	if len(repr) == 0 {
		return false
	}
	r, _ := pebbledb.ReadBatch(repr)
	for {
		kind, key, _, ok, err := r.Next()
		if err != nil || !ok {
			return false
		}
		switch kind {
		case pebbledb.InternalKeyKindSet, pebbledb.InternalKeyKindSetWithDelete,
			pebbledb.InternalKeyKindDelete, pebbledb.InternalKeyKindSingleDelete,
			pebbledb.InternalKeyKindDeleteSized:
			if _, isPrimary := storage.PrimaryTableBytesFromKey(key); isPrimary {
				return true
			}
		}
	}
}

func itemCacheMutationsForTables(repr []byte, tables [][]byte) ([]itemCacheMutation, bool) {
	if len(repr) == 0 {
		return nil, false
	}
	r, _ := pebbledb.ReadBatch(repr)
	var mutations []itemCacheMutation
	var hasPrimary bool
	for {
		kind, key, _, ok, err := r.Next()
		if err != nil || !ok {
			return mutations, hasPrimary
		}
		switch kind {
		case pebbledb.InternalKeyKindSet, pebbledb.InternalKeyKindSetWithDelete,
			pebbledb.InternalKeyKindDelete, pebbledb.InternalKeyKindSingleDelete,
			pebbledb.InternalKeyKindDeleteSized:
			table, isPrimary := storage.PrimaryTableBytesFromKey(key)
			if !isPrimary {
				continue
			}
			hasPrimary = true
			if itemCacheTableInSet(table, tables) {
				mutations = append(mutations, itemCacheMutation{key: key})
			}
		}
	}
}

func itemCacheTableInSet(table []byte, tables [][]byte) bool {
	for _, cached := range tables {
		if bytes.Equal(table, cached) {
			return true
		}
	}
	return false
}

func (d *DB) observeItemCacheBatch(repr []byte, itemCacheTables []string) {
	if d == nil || d.items == nil {
		return
	}
	if len(repr) == 0 {
		return
	}
	if len(itemCacheTables) > 0 && !d.items.hasAnyTable(itemCacheTables) {
		d.items.markMutation()
		return
	}
	tables := itemCacheTableBytes(itemCacheTables)
	if len(tables) == 0 {
		tables = d.items.cachedTableBytes()
	}
	if len(tables) == 0 {
		if itemCacheBatchHasPrimaryMutation(repr) {
			d.items.markMutation()
			d.items.clearIfNotEmpty()
		}
		return
	}
	mutations, hasPrimary := itemCacheMutationsForTables(repr, tables)
	if len(mutations) > 0 {
		d.items.deleteMutations(mutations)
		return
	}
	if hasPrimary {
		d.items.markMutation()
	}
}

func itemCacheTableBytes(tables []string) [][]byte {
	if len(tables) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(tables))
	for _, table := range tables {
		if table != "" {
			out = append(out, []byte(table))
		}
	}
	return out
}
