package pebble

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// BenchmarkNativeStreamConcurrentWriters measures 24 concurrent native-stream
// writers (text index + stream enabled, fsync on), the Logs events shape.
// The "with_reader" variant adds one GetRecords-style poller and reports its
// p99, the readiness-check path that queued behind writers (FU-01).
func BenchmarkNativeStreamConcurrentWriters(b *testing.B) {
	for _, withReader := range []bool{false, true} {
		name := "writers_only"
		if withReader {
			name = "with_reader"
		}
		b.Run(name, func(b *testing.B) { benchNativeStreamWriters(b, 24, withReader) })
	}
}

func benchNativeStreamWriters(b *testing.B, writers int, withReader bool) {
	db, err := Open(Options{Path: b.TempDir(), FsyncOnCommit: true, StreamRetention: StreamRetentionOptions{Interval: -1}})
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	td := textTable()
	td.StreamSpecification = &types.StreamSpecification{StreamEnabled: true, StreamViewType: types.StreamViewTypeNewImage, RetentionSeconds: 86400}
	raw, _ := json.Marshal(td)
	if err := db.Set(storage.KeyCatalog(td.Name), raw); err != nil {
		b.Fatal(err)
	}
	expiry := time.Now().Add(time.Hour).Unix()

	var seq atomic.Int64
	lat := make([][]time.Duration, writers)
	stop := make(chan struct{})
	var readerLat []time.Duration
	var readerWG sync.WaitGroup
	if withReader {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			next := uint64(1)
			for {
				select {
				case <-stop:
					return
				default:
				}
				start := time.Now()
				_, n, err := db.StreamRecords(td.Name, next, 0, 100, 1<<20)
				readerLat = append(readerLat, time.Since(start))
				if err != nil {
					b.Error(err)
					return
				}
				next = n
				time.Sleep(time.Millisecond)
			}
		}()
	}
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				i := seq.Add(1)
				if i > int64(b.N) {
					return
				}
				item := textEvent("tenant", fmt.Sprint(i), "connection failed while reading upstream response", expiry)
				start := time.Now()
				if err := db.PutItemWith(td, item, PutOptions{}); err != nil {
					b.Error(err)
					return
				}
				lat[w] = append(lat[w], time.Since(start))
			}
		}(w)
	}
	wg.Wait()
	elapsed := b.Elapsed()
	b.StopTimer()
	close(stop)
	readerWG.Wait()
	var all []time.Duration
	for _, l := range lat {
		all = append(all, l...)
	}
	b.ReportMetric(float64(len(all))/elapsed.Seconds(), "ops/s")
	b.ReportMetric(float64(percentile(all, 0.99).Microseconds()), "p99-us")
	if withReader {
		b.ReportMetric(float64(percentile(readerLat, 0.99).Microseconds()), "reader-p99-us")
	}
}

func percentile(in []time.Duration, p float64) time.Duration {
	if len(in) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), in...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return sorted[int(float64(len(sorted)-1)*p)]
}
