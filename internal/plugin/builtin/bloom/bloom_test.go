package bloom_test

import (
	"fmt"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/index"
	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/bloom"
	"github.com/CefasDb/cefasdb/pkg/plugin/testharness"
)

func cfg(field string, m, k int) []byte {
	return []byte(fmt.Sprintf(`{"field":%q,"m":%d,"k":%d}`, field, m, k))
}

func desc(field string, m, k int) index.Descriptor {
	return index.Descriptor{Table: "T", Name: "x", PluginName: "bloom", PluginConfig: cfg(field, m, k)}
}

func TestBuildContainsKnownMembers(t *testing.T) {
	h := testharness.New(t)
	h.MustRegister(bloom.NewPlugin())
	for i := 0; i < 1000; i++ {
		h.SeedTable("T", model.Item{"email": {T: model.AttrS, S: fmt.Sprintf("user-%d@example.com", i)}})
	}
	d := desc("email", 16384, 6)
	if err := h.BuildIndex(d); err != nil {
		t.Fatalf("build: %v", err)
	}
	p, _ := h.Registry.Lookup("bloom")
	ip := p.(plugin.IndexPlugin)
	for i := 0; i < 1000; i++ {
		got, err := ip.Query(d, plugin.IndexQuery{
			Binds: map[string]model.AttributeValue{":value": {T: model.AttrS, S: fmt.Sprintf("user-%d@example.com", i)}},
		})
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		_, ok := got.Next()
		if !ok {
			t.Fatalf("missing known member user-%d", i)
		}
		got.Close()
	}
}

func TestQueryAbsentReturnsEmptyEnoughOfTheTime(t *testing.T) {
	// A 16k-bit / 6-hash filter with 1k inserted items has ~1.3% FPR.
	// Sampling 10k absent values must produce >9000 negatives.
	h := testharness.New(t)
	h.MustRegister(bloom.NewPlugin())
	for i := 0; i < 1000; i++ {
		h.SeedTable("T", model.Item{"email": {T: model.AttrS, S: fmt.Sprintf("user-%d@example.com", i)}})
	}
	d := desc("email", 16384, 6)
	_ = h.BuildIndex(d)
	p, _ := h.Registry.Lookup("bloom")
	ip := p.(plugin.IndexPlugin)
	negatives := 0
	for i := 0; i < 10000; i++ {
		got, _ := ip.Query(d, plugin.IndexQuery{
			Binds: map[string]model.AttributeValue{":value": {T: model.AttrS, S: fmt.Sprintf("absent-%d", i)}},
		})
		if _, ok := got.Next(); !ok {
			negatives++
		}
		got.Close()
	}
	if negatives < 9500 {
		t.Fatalf("negatives = %d, want >= 9500 (FPR too high)", negatives)
	}
}

func TestDeleteIsUnsupported(t *testing.T) {
	p := bloom.NewPlugin()
	if err := p.Delete(desc("x", 64, 2), nil); err == nil {
		t.Fatal("expected ErrDeleteUnsupported")
	}
}

func TestSerializeRoundTrip(t *testing.T) {
	f, err := bloom.New(cfg("v", 256, 3))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	for i := 0; i < 50; i++ {
		f.Add(fmt.Appendf(nil, "x-%d", i))
	}
	buf, err := f.Serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	g, err := bloom.Deserialize(buf)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	for i := 0; i < 50; i++ {
		if !g.Contains(fmt.Appendf(nil, "x-%d", i)) {
			t.Fatalf("round-trip lost member %d", i)
		}
	}
}

func TestInvalidConfigRejected(t *testing.T) {
	if _, err := bloom.New([]byte(`{"field":""}`)); err == nil {
		t.Fatal("expected field-required error")
	}
	if _, err := bloom.New([]byte(`{"field":"x","m":0,"k":2}`)); err == nil {
		t.Fatal("expected m-positive error")
	}
}

func BenchmarkBloomAdd1k(b *testing.B)   { benchAdd(b, 1_000) }
func BenchmarkBloomAdd100k(b *testing.B) { benchAdd(b, 100_000) }

func benchAdd(b *testing.B, n int) {
	f, _ := bloom.New(cfg("x", 16*n, 7))
	values := make([][]byte, n)
	for i := 0; i < n; i++ {
		values[i] = fmt.Appendf(nil, "user-%d", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Add(values[i%n])
	}
}
