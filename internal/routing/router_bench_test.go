package routing

import (
	"encoding/binary"
	"testing"

	"github.com/CefasDb/cefasdb/internal/placement"
)

// BenchmarkRouterShardForPK measures the per-request routing cost as a
// function of shard count. With the binary-search fast path the curve
// should look like O(log N); the prior linear scan was O(N).
func BenchmarkRouterShardForPK(b *testing.B) {
	for _, shards := range []int{8, 24, 64, 256} {
		b.Run(benchName(shards), func(b *testing.B) {
			cat := placement.DefaultPlacement(shards, "", nil, nil, placement.NodeCapacity{}, placement.PlacementStrategyTokenRange)
			r, err := NewRouterFromCatalog(cat)
			if err != nil {
				b.Fatalf("router: %v", err)
			}
			keys := make([][]byte, 1024)
			for i := range keys {
				k := make([]byte, 8)
				binary.BigEndian.PutUint64(k, uint64(i))
				keys[i] = k
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := r.ShardForPK(keys[i%len(keys)]); err != nil {
					b.Fatalf("ShardForPK: %v", err)
				}
			}
		})
	}
}

func benchName(shards int) string {
	switch shards {
	case 8:
		return "shards=8"
	case 24:
		return "shards=24"
	case 64:
		return "shards=64"
	case 256:
		return "shards=256"
	}
	return "shards=?"
}
