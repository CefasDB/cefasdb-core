package replication

import (
	"fmt"
	"testing"

	pebbledb "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

// openMemPebble returns an in-memory Pebble DB for benchmarks. Memory
// FS keeps the micro-bench numbers focused on the encode / batch merge
// CPU rather than disk variance.
func openMemPebble(b *testing.B) *pebbledb.DB {
	b.Helper()
	opts := &pebbledb.Options{FS: vfs.NewMem()}
	d, err := pebbledb.Open("bench", opts)
	if err != nil {
		b.Fatalf("open mem pebble: %v", err)
	}
	b.Cleanup(func() { _ = d.Close() })
	return d
}

// makeBatchRepr returns the binary repr of a Pebble batch with one Set
// of the requested value size. Used as the per-Replicate payload that
// applyLoop would coalesce.
func makeBatchRepr(b *testing.B, d *pebbledb.DB, key, value []byte) []byte {
	b.Helper()
	batch := d.NewBatch()
	defer batch.Close()
	if err := batch.Set(key, value, nil); err != nil {
		b.Fatalf("set: %v", err)
	}
	repr := batch.Repr()
	cp := make([]byte, len(repr))
	copy(cp, repr)
	return cp
}

// BenchmarkApplyLoopMergeMicro replays the hot section of db.go
// applyLoop: take N independent payload reprs, build a merged batch
// via SetRepr+Apply, and produce the final encoded payload. Wave 3
// #432 will remove the two append-copies inside the loop body; this
// benchmark captures the savings.
func BenchmarkApplyLoopMergeMicro(b *testing.B) {
	const valueSize = 256
	for _, mergeSize := range []int{8, 32, 64, 128} {
		b.Run(fmt.Sprintf("merge=%d", mergeSize), func(b *testing.B) {
			d := openMemPebble(b)
			reprs := make([][]byte, mergeSize)
			val := make([]byte, valueSize)
			for i := range reprs {
				key := []byte(fmt.Sprintf("k%08d", i))
				reprs[i] = makeBatchRepr(b, d, key, val)
			}
			enc, err := newRaftPayloadEncoder(Config{})
			if err != nil {
				b.Fatalf("encoder: %v", err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(valueSize * mergeSize))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				merged := d.NewBatch()
				// First payload — same shape as applyLoop "first" path.
				if err := merged.SetRepr(append([]byte(nil), reprs[0]...)); err != nil {
					b.Fatalf("setrepr first: %v", err)
				}
				// Remaining payloads — same shape as the drain loop.
				for j := 1; j < mergeSize; j++ {
					tmp := d.NewBatch()
					if err := tmp.SetRepr(append([]byte(nil), reprs[j]...)); err != nil {
						b.Fatalf("setrepr merge: %v", err)
					}
					if err := merged.Apply(tmp, nil); err != nil {
						b.Fatalf("apply: %v", err)
					}
					_ = tmp.Close()
				}
				if _, err := enc.Encode(merged.Repr()); err != nil {
					b.Fatalf("encode: %v", err)
				}
				_ = merged.Close()
			}
		})
	}
}

// BenchmarkRaftPayloadEncodeNoCompression isolates the encoder itself
// for the case where compression is disabled (the default in the bench
// config and in tests).
func BenchmarkRaftPayloadEncodeNoCompression(b *testing.B) {
	const size = 4096
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}
	enc, err := newRaftPayloadEncoder(Config{LogCompression: "none"})
	if err != nil {
		b.Fatalf("encoder: %v", err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := enc.Encode(payload); err != nil {
			b.Fatalf("encode: %v", err)
		}
	}
}

func BenchmarkRaftPayloadEncodeSnappy(b *testing.B) {
	const size = 4096
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}
	enc, err := newRaftPayloadEncoder(Config{LogCompression: "snappy"})
	if err != nil {
		b.Fatalf("encoder: %v", err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := enc.Encode(payload); err != nil {
			b.Fatalf("encode: %v", err)
		}
	}
}
