package pebble

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/types"
)

// BenchmarkAppendChangeRecord measures the per-write changelog cost on a
// stream-enabled table — the path that today serializes on d.changeMu
// (Wave 2 #429 removes that lock).
func BenchmarkAppendChangeRecord(b *testing.B) {
	db := openBenchDB(b, Options{ChangeLogMode: ChangeLogModeStreamsOnly})
	td := streamTestTable()
	item := types.Item{"id": streamS("k"), "status": streamS("v")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := db.Batch()
		rec := newChangeRecord(td, ChangePut, keyItemFromItem(item, td.KeySchema), nil, item)
		if _, err := db.appendChangeRecord(batch, rec); err != nil {
			b.Fatalf("append: %v", err)
		}
		_ = batch.Close()
	}
}

// BenchmarkAppendChangeRecordParallel captures contention on
// d.changeMu under concurrent writers — the metric Wave 2 #429 must
// move significantly.
func BenchmarkAppendChangeRecordParallel(b *testing.B) {
	for _, parallelism := range []int{8, 32, 64} {
		b.Run(fmt.Sprintf("p=%d", parallelism), func(b *testing.B) {
			db := openBenchDB(b, Options{ChangeLogMode: ChangeLogModeStreamsOnly})
			td := streamTestTable()
			item := types.Item{"id": streamS("k"), "status": streamS("v")}
			b.ReportAllocs()
			b.SetParallelism(parallelism)
			b.ResetTimer()
			var counter atomic.Uint64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_ = counter.Add(1)
					batch := db.Batch()
					rec := newChangeRecord(td, ChangePut, keyItemFromItem(item, td.KeySchema), nil, item)
					if _, err := db.appendChangeRecord(batch, rec); err != nil {
						b.Fatalf("append: %v", err)
					}
					_ = batch.Close()
				}
			})
		})
	}
}

// BenchmarkPutItemWithStream covers the full write path used by the
// route-loadtest: condition eval skipped (no condition), GSI/LSI absent,
// changelog appended, retention refresh fired. Wave 1 #426 (move
// retention to background) should be the single biggest mover here.
func BenchmarkPutItemWithStream(b *testing.B) {
	db := openBenchDB(b, Options{ChangeLogMode: ChangeLogModeStreamsOnly})
	td := streamTestTable()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item := types.Item{"id": streamS(fmt.Sprintf("k%d", i)), "status": streamS("v")}
		if err := db.PutItemWith(td, item, PutOptions{}); err != nil {
			b.Fatalf("put: %v", err)
		}
	}
}

// BenchmarkEncodeChangeRecord measures the JSON marshal cost paid by
// every stream-enabled write. Wave 3 #431 (binary codec) targets this.
func BenchmarkEncodeChangeRecord(b *testing.B) {
	rec := newChangeRecord(streamTestTable(), ChangePut,
		types.Item{"id": streamS("k")},
		nil,
		types.Item{"id": streamS("k"), "status": streamS("v"), "payload": streamS("0123456789abcdef0123456789abcdef")},
	)
	rec.Index = 42
	rec.SequenceNumber = "42"
	rec.UnixNano = 1_700_000_000_000_000_000
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, err := json.Marshal(rec)
		if err != nil {
			b.Fatalf("marshal: %v", err)
		}
		if len(buf) == 0 {
			b.Fatalf("empty buffer")
		}
	}
}

func BenchmarkDecodeChangeRecord(b *testing.B) {
	rec := newChangeRecord(streamTestTable(), ChangePut,
		types.Item{"id": streamS("k")},
		nil,
		types.Item{"id": streamS("k"), "status": streamS("v"), "payload": streamS("0123456789abcdef0123456789abcdef")},
	)
	rec.Index = 42
	rec.SequenceNumber = "42"
	rec.UnixNano = 1_700_000_000_000_000_000
	raw, err := json.Marshal(rec)
	if err != nil {
		b.Fatalf("marshal: %v", err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out ChangeRecord
		if err := json.Unmarshal(raw, &out); err != nil {
			b.Fatalf("unmarshal: %v", err)
		}
	}
}

// BenchmarkEncodeChangeRecordBinary measures the binary-v1 codec
// introduced by #431. Comparison point: BenchmarkEncodeChangeRecord
// (the json.Marshal baseline) above.
func BenchmarkEncodeChangeRecordBinary(b *testing.B) {
	rec := newChangeRecord(streamTestTable(), ChangePut,
		types.Item{"id": streamS("k")},
		nil,
		types.Item{"id": streamS("k"), "status": streamS("v"), "payload": streamS("0123456789abcdef0123456789abcdef")},
	)
	rec.Index = 42
	rec.SequenceNumber = "42"
	rec.UnixNano = 1_700_000_000_000_000_000
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, err := encodeChangeRecord(nil, rec)
		if err != nil {
			b.Fatalf("encode: %v", err)
		}
		if len(buf) == 0 {
			b.Fatalf("empty buffer")
		}
	}
}

func BenchmarkDecodeChangeRecordBinary(b *testing.B) {
	rec := newChangeRecord(streamTestTable(), ChangePut,
		types.Item{"id": streamS("k")},
		nil,
		types.Item{"id": streamS("k"), "status": streamS("v"), "payload": streamS("0123456789abcdef0123456789abcdef")},
	)
	rec.Index = 42
	rec.SequenceNumber = "42"
	rec.UnixNano = 1_700_000_000_000_000_000
	raw, err := encodeChangeRecord(nil, rec)
	if err != nil {
		b.Fatalf("encode: %v", err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decodeChangeRecord(raw); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

// BenchmarkApproximateChangeRecordSize covers the second json.Marshal
// per write that Wave 1 #427 deletes.
func BenchmarkApproximateChangeRecordSize(b *testing.B) {
	rec := newChangeRecord(streamTestTable(), ChangePut,
		types.Item{"id": streamS("k")},
		nil,
		types.Item{"id": streamS("k"), "status": streamS("v"), "payload": streamS("0123456789abcdef0123456789abcdef")},
	)
	rec.Index = 42
	rec.SequenceNumber = "42"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if approximateChangeRecordSize(rec) == 0 {
			b.Fatal("size 0")
		}
	}
}
