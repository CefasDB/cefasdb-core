package minhash_test

import (
	"fmt"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/index"
	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/minhash"
	"github.com/CefasDb/cefasdb/pkg/plugin/testharness"
)

func item(pk string, tags ...string) model.Item {
	return model.Item{
		"pk":   {T: model.AttrS, S: pk},
		"tags": {T: model.AttrSS, SS: tags},
	}
}

func desc() index.Descriptor {
	return index.Descriptor{
		Table:        "Users",
		Name:         "tag_sim",
		PluginName:   "minhash",
		PluginConfig: []byte(`{"field":"tags","k":64,"r":4}`),
		KeySchema:    model.KeySchema{PK: "pk"},
	}
}

func TestSimilarItemsBucketTogether(t *testing.T) {
	h := testharness.New(t)
	h.MustRegister(minhash.NewPlugin())
	h.SeedTable("Users",
		item("near1", "a", "b", "c", "d", "e", "f", "g", "h"),
		item("near2", "a", "b", "c", "d", "e", "f", "g", "h", "i"),
		item("far", "z", "y", "x", "w", "v", "u"),
	)
	if err := h.BuildIndex(desc()); err != nil {
		t.Fatalf("build: %v", err)
	}
	p, _ := h.Registry.Lookup("minhash")
	ip := p.(plugin.IndexPlugin)
	cs, err := ip.Query(desc(), plugin.IndexQuery{
		Binds: map[string]model.AttributeValue{
			":values": {T: model.AttrSS, SS: []string{"a", "b", "c", "d", "e", "f", "g", "h"}},
		},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	got := map[string]bool{}
	for {
		c, ok := cs.Next()
		if !ok {
			break
		}
		got[c.Key["pk"].S] = true
	}
	// At minimum, near1 + near2 must be candidates; "far" should not
	// appear (Jaccard 0 → no shared band hashes with high probability).
	if !got["near1"] {
		t.Errorf("missing near1 (identical set)")
	}
	if !got["near2"] {
		t.Errorf("missing near2 (Jaccard ~8/9)")
	}
	if got["far"] {
		t.Errorf("unexpected far in candidate set")
	}
}

func TestUpdateAndDelete(t *testing.T) {
	p := minhash.NewPlugin()
	d := desc()
	_ = p.Update(d, nil, item("u1", "a", "b", "c"))
	_ = p.Delete(d, item("u1", "a", "b", "c"))
	cs, _ := p.Query(d, plugin.IndexQuery{
		Binds: map[string]model.AttributeValue{":values": {T: model.AttrSS, SS: []string{"a", "b", "c"}}},
	})
	if _, ok := cs.Next(); ok {
		t.Fatal("expected no candidates after delete")
	}
}

func TestConfigRequiresFieldAndDivisibleK(t *testing.T) {
	p := minhash.NewPlugin()
	bad := index.Descriptor{Table: "T", Name: "x", PluginName: "minhash",
		PluginConfig: []byte(`{"field":"","k":64,"r":4}`)}
	if err := p.Build(bad, func(yield func(model.Item) bool) {}); err == nil {
		t.Fatal("expected field-required error")
	}
	bad.PluginConfig = []byte(fmt.Sprintf(`{"field":"x","k":%d,"r":%d}`, 100, 7))
	if err := p.Build(bad, func(yield func(model.Item) bool) {}); err == nil {
		t.Fatal("expected k%r != 0 error")
	}
}
