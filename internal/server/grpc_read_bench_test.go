package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/metrics"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func BenchmarkGRPCGetItem(b *testing.B) {
	for _, withMetrics := range []bool{false, true} {
		name := "metrics_off"
		if withMetrics {
			name = "metrics_on"
		}
		b.Run(name, func(b *testing.B) {
			db, err := pebble.Open(pebble.Options{Path: b.TempDir()})
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			catStore, err := catalog.New(db)
			if err != nil {
				b.Fatal(err)
			}
			td := types.TableDescriptor{Name: "Bench", KeySchema: types.KeySchema{PK: "id"}}
			if err := catStore.Create(td); err != nil {
				b.Fatal(err)
			}
			for i := 0; i < 10_000; i++ {
				if err := db.PutItemWith(td, types.Item{
					"id":      {T: types.AttrS, S: fmt.Sprintf("k%d", i)},
					"payload": {T: types.AttrS, S: "payload-of-modest-size"},
				}, pebble.PutOptions{}); err != nil {
					b.Fatal(err)
				}
			}
			srv := NewGRPCServer(db, catStore, nil)
			if withMetrics {
				srv.AttachMetrics(metrics.New())
			}
			ctx := context.Background()
			reqs := make([]*cefaspb.GetItemRequest, 10_000)
			for i := range reqs {
				reqs[i] = &cefaspb.GetItemRequest{
					Table: "Bench",
					Key: map[string]*cefaspb.AttributeValue{
						"id": {Value: &cefaspb.AttributeValue_S{S: fmt.Sprintf("k%d", i)}},
					},
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := srv.GetItem(ctx, reqs[i%len(reqs)]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
