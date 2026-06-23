package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/CefasDb/cefasdb/internal/catalog"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// BenchmarkBatchWriteFanOutSingleShard exercises the single-shard
// (no manager) path of batchWriteFanOut with K = 500 ops. The issue
// #455 quadratic shape lives in the multi-shard path; this case
// is the floor used as a sanity baseline.
func BenchmarkBatchWriteFanOutSingleShard(b *testing.B) {
	db, err := pebble.Open(pebble.Options{Path: b.TempDir()})
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		b.Fatalf("catalog: %v", err)
	}
	srv := NewGRPCServer(db, cat, nil)
	defer srv.StopMVScheduler()
	td := types.TableDescriptor{
		Name:      "BenchBW",
		KeySchema: types.KeySchema{PK: "pk"},
	}
	if err := cat.Create(td); err != nil {
		b.Fatalf("create table: %v", err)
	}
	const K = 500
	ops := make([]pebble.BatchOp, 0, K)
	for i := 0; i < K; i++ {
		ops = append(ops, pebble.BatchOp{
			Op: pebble.BatchOpPut,
			Item: types.Item{
				"pk":      {T: types.AttrS, S: fmt.Sprintf("k%04d", i)},
				"payload": {T: types.AttrS, S: "x"},
			},
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := srv.batchWriteFanOut(context.Background(), td, ops); err != nil {
			b.Fatalf("fanOut: %v", err)
		}
	}
}
