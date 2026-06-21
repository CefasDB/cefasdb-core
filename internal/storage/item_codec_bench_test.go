package storage

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/types"
)

func sampleItem(n int) types.Item {
	item := make(types.Item, n)
	for i := 0; i < n; i++ {
		key := "attr_"
		if i < 10 {
			key += string(rune('0' + i))
		} else {
			key += string(rune('a' + i - 10))
		}
		item[key] = types.AttributeValue{T: types.AttrS, S: "value-payload-bytes-bytes-bytes-bytes-bytes-bytes"}
	}
	return item
}

func BenchmarkEncodeItem(b *testing.B) {
	for _, n := range []int{4, 8, 16, 32} {
		item := sampleItem(n)
		b.Run(benchName(n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf, err := EncodeItem(item)
				if err != nil {
					b.Fatalf("encode: %v", err)
				}
				if len(buf) == 0 {
					b.Fatal("empty buffer")
				}
			}
		})
	}
}

func BenchmarkEncodeItemParallel(b *testing.B) {
	item := sampleItem(8)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := EncodeItem(item); err != nil {
				b.Fatalf("encode: %v", err)
			}
		}
	})
}

func benchName(n int) string {
	switch n {
	case 4:
		return "attrs=4"
	case 8:
		return "attrs=8"
	case 16:
		return "attrs=16"
	case 32:
		return "attrs=32"
	}
	return "attrs=?"
}
