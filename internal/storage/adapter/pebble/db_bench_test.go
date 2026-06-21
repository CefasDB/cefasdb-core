package pebble

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"testing"

	pebbledb "github.com/cockroachdb/pebble"
)

// openBenchDB opens a Pebble DB under a per-benchmark temp directory with
// the supplied options. The DB is closed via b.Cleanup so consecutive
// benchmarks do not share state.
func openBenchDB(b *testing.B, opts Options) *DB {
	b.Helper()
	if opts.Path == "" {
		opts.Path = b.TempDir()
	}
	db, err := Open(opts)
	if err != nil {
		b.Fatalf("open storage: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

// benchKey builds a fixed-width key for predictable hot-path measurements.
func benchKey(i uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, i)
	return k
}

// benchValue returns a deterministic slice of the requested size.
func benchValue(size int) []byte {
	v := make([]byte, size)
	for i := range v {
		v[i] = byte(i)
	}
	return v
}

func BenchmarkCommitBatchSolo(b *testing.B) {
	const valueSize = 1024
	db := openBenchDB(b, Options{})
	val := benchValue(valueSize)
	b.ReportAllocs()
	b.SetBytes(int64(valueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := db.Batch()
		if err := batch.Set(benchKey(uint64(i)), val, nil); err != nil {
			b.Fatalf("batch set: %v", err)
		}
		if err := db.CommitBatch(batch); err != nil {
			b.Fatalf("commit: %v", err)
		}
		_ = batch.Close()
	}
}

// BenchmarkCommitBatchParallel measures how the group-commit coalescer
// behaves when many goroutines submit batches concurrently. Wave 2 #428
// will swing this number significantly once the write-lane stops
// throttling the coalescer to its 8 default workers.
func BenchmarkCommitBatchParallel(b *testing.B) {
	const valueSize = 1024
	for _, parallelism := range []int{8, 32, 64, 128} {
		b.Run(fmt.Sprintf("p=%d", parallelism), func(b *testing.B) {
			db := openBenchDB(b, Options{})
			val := benchValue(valueSize)
			b.ReportAllocs()
			b.SetBytes(int64(valueSize))
			b.SetParallelism(parallelism)
			b.ResetTimer()
			var counter atomic.Uint64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := counter.Add(1)
					batch := db.Batch()
					if err := batch.Set(benchKey(i), val, nil); err != nil {
						b.Fatalf("batch set: %v", err)
					}
					if err := db.CommitBatch(batch); err != nil {
						b.Fatalf("commit: %v", err)
					}
					_ = batch.Close()
				}
			})
		})
	}
}

func BenchmarkGetSolo(b *testing.B) {
	const (
		valueSize = 256
		keyCount  = 10000
	)
	db := openBenchDB(b, Options{})
	seedKeys(b, db, keyCount, valueSize)
	b.ReportAllocs()
	b.SetBytes(int64(valueSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, err := db.Get(benchKey(uint64(i % keyCount)))
		if err != nil {
			b.Fatalf("get: %v", err)
		}
		if len(v) != valueSize {
			b.Fatalf("value len = %d, want %d", len(v), valueSize)
		}
	}
}

// BenchmarkGetParallel measures Get under read-lane contention. Wave 4
// #433 / #434 will move this number once the read-lane scales with
// GOMAXPROCS and the fast-path bypass lands.
func BenchmarkGetParallel(b *testing.B) {
	const (
		valueSize = 256
		keyCount  = 10000
	)
	for _, parallelism := range []int{8, 64, 256, 512} {
		b.Run(fmt.Sprintf("p=%d", parallelism), func(b *testing.B) {
			db := openBenchDB(b, Options{})
			seedKeys(b, db, keyCount, valueSize)
			b.ReportAllocs()
			b.SetBytes(int64(valueSize))
			b.SetParallelism(parallelism)
			b.ResetTimer()
			var counter atomic.Uint64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := counter.Add(1) % keyCount
					if _, err := db.Get(benchKey(i)); err != nil {
						b.Fatalf("get: %v", err)
					}
				}
			})
		})
	}
}

// seedKeys writes keyCount entries via direct Pebble batch (bypassing
// the coalescer to keep setup time bounded). Run before any read bench.
func seedKeys(b *testing.B, db *DB, keyCount, valueSize int) {
	b.Helper()
	val := benchValue(valueSize)
	batch := db.Raw().NewBatch()
	defer batch.Close()
	for i := uint64(0); i < uint64(keyCount); i++ {
		if err := batch.Set(benchKey(i), val, nil); err != nil {
			b.Fatalf("seed set: %v", err)
		}
	}
	if err := batch.Commit(pebbledb.NoSync); err != nil {
		b.Fatalf("seed commit: %v", err)
	}
}
