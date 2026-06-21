package server

import (
	"time"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// batchGetGroupByShard fans BatchGetItem out across the shards that own
// the requested keys. Single-shard / single-node clusters take the
// fast path of one round-trip with the original slice.
//
// The previous implementation routed and issued one BatchGet per key,
// which is O(K · routing + K) RPCs even though every shard only sees
// K/S of the keys (where S is the number of shards visited). This
// helper does the routing once per key, partitions by destination,
// issues exactly one BatchGet per destination, and splices the result
// back into the original index slot so callers still observe the input
// order. Misses stay nil.
//
// observeRead is invoked once per shard with the started time for that
// shard's RPC; the per-key range observations are aggregated downstream
// where the granularity matters (range hotspots are PK-prefix based,
// not per-key timed).
func batchGetGroupByShard(
	table string,
	ks types.KeySchema,
	keys []types.Item,
	routeFor func(pkBytes []byte) (*pebble.DB, error),
	observeRead func(pkBytes []byte, approxBytes uint64, started time.Time),
) ([]types.Item, error) {
	out := make([]types.Item, len(keys))
	if len(keys) == 0 {
		return out, nil
	}

	pkBytesByIdx := make([][]byte, len(keys))
	dbByIdx := make([]*pebble.DB, len(keys))
	groups := make(map[*pebble.DB][]int, 4)
	subsets := make(map[*pebble.DB][]types.Item, 4)

	for i, k := range keys {
		pkBytes, err := pkBytesFromItem(k, ks)
		if err != nil {
			return nil, err
		}
		db, err := routeFor(pkBytes)
		if err != nil {
			return nil, err
		}
		pkBytesByIdx[i] = pkBytes
		dbByIdx[i] = db
		groups[db] = append(groups[db], i)
		subsets[db] = append(subsets[db], k)
	}

	for db, indices := range groups {
		started := time.Now()
		got, err := db.BatchGetItem(table, ks, subsets[db])
		if err != nil {
			return nil, err
		}
		for j, srcIdx := range indices {
			if j < len(got) && got[j] != nil {
				out[srcIdx] = got[j]
			}
			approxBytes := uint64(len(pkBytesByIdx[srcIdx]))
			if out[srcIdx] != nil {
				approxBytes = estimatedItemBytes(out[srcIdx])
			}
			observeRead(pkBytesByIdx[srcIdx], approxBytes, started)
		}
	}
	return out, nil
}
