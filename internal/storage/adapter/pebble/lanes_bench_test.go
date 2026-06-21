package pebble

import (
	"fmt"
	"sync/atomic"
	"testing"
)

// BenchmarkRunReadNoOp measures the pure overhead of the read-lane
// submit / worker / done round-trip with a zero-work function. This is
// the lower bound for how much each read pays just to enter the lane.
// Wave 4 #434 (bypass for solo Get) removes this overhead for Get.
func BenchmarkRunReadNoOp(b *testing.B) {
	for _, parallelism := range []int{1, 8, 64, 256, 512} {
		b.Run(fmt.Sprintf("p=%d", parallelism), func(b *testing.B) {
			db := openBenchDB(b, Options{Lanes: LaneOptions{Mode: LaneModeOn}})
			noop := func() error { return nil }
			b.ReportAllocs()
			b.SetParallelism(parallelism)
			b.ResetTimer()
			var counter atomic.Uint64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = counter.Add(1)
					if err := db.runRead(noop); err != nil {
						b.Fatalf("runRead: %v", err)
					}
				}
			})
		})
	}
}

// BenchmarkRunWriteNoOp is the write-lane analogue. Wave 2 #428
// removes this overhead from the CommitBatch path.
func BenchmarkRunWriteNoOp(b *testing.B) {
	for _, parallelism := range []int{1, 8, 64, 128} {
		b.Run(fmt.Sprintf("p=%d", parallelism), func(b *testing.B) {
			db := openBenchDB(b, Options{Lanes: LaneOptions{Mode: LaneModeOn}})
			noop := func() error { return nil }
			b.ReportAllocs()
			b.SetParallelism(parallelism)
			b.ResetTimer()
			var counter atomic.Uint64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = counter.Add(1)
					if err := db.runWrite(noop); err != nil {
						b.Fatalf("runWrite: %v", err)
					}
				}
			})
		})
	}
}

// BenchmarkLaneDisabledVsEnabled compares solo Get with the lane on
// vs off, giving a single-number view on the lane's per-op cost.
func BenchmarkLaneDisabledVsEnabled(b *testing.B) {
	const (
		valueSize = 256
		keyCount  = 1000
	)
	for _, mode := range []string{LaneModeOff, LaneModeOn} {
		b.Run(mode, func(b *testing.B) {
			db := openBenchDB(b, Options{Lanes: LaneOptions{Mode: mode}})
			seedKeys(b, db, keyCount, valueSize)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := db.Get(benchKey(uint64(i % keyCount))); err != nil {
					b.Fatalf("get: %v", err)
				}
			}
		})
	}
}
