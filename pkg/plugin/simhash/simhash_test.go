package simhash_test

import (
	"fmt"
	"math/bits"
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
	"github.com/osvaldoandrade/cefas/pkg/plugin/simhash"
	"github.com/osvaldoandrade/cefas/pkg/plugin/testharness"
)

func item(pk, body string) model.Item {
	return model.Item{
		"pk":   {T: model.AttrS, S: pk},
		"body": {T: model.AttrS, S: body},
	}
}

func desc() index.Descriptor {
	return index.Descriptor{
		Table:        "Docs",
		Name:         "dedupe",
		PluginName:   "simhash",
		PluginConfig: []byte(`{"field":"body","prefix_bits":8,"max_radius":16}`),
		KeySchema:    model.KeySchema{PK: "pk"},
	}
}

func TestNearDuplicatesAreCandidates(t *testing.T) {
	h := testharness.New(t)
	h.MustRegister(simhash.NewPlugin())
	h.SeedTable("Docs",
		item("d1", "the quick brown fox jumps over the lazy dog"),
		item("d2", "the quick brown fox jumps over the lazy dog!"), // 1 char diff
		item("d3", "completely unrelated text about pebble storage"),
	)
	if err := h.BuildIndex(desc()); err != nil {
		t.Fatalf("build: %v", err)
	}
	p, _ := h.Registry.Lookup("simhash")
	ip := p.(plugin.IndexPlugin)
	cs, err := ip.Query(desc(), plugin.IndexQuery{Predicate: "the quick brown fox jumps over the lazy dog"})
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
	if !got["d1"] || !got["d2"] {
		t.Fatalf("expected d1+d2 candidates, got %v", got)
	}
	if got["d3"] {
		t.Errorf("d3 (unrelated) should not be a candidate")
	}
}

func TestSimHashIdentical(t *testing.T) {
	a := simhash.SimHash("hello world")
	b := simhash.SimHash("hello world")
	if a != b {
		t.Fatalf("SimHash not deterministic: %x vs %x", a, b)
	}
	if bits.OnesCount64(a^b) != 0 {
		t.Fatalf("identical strings should have hamming 0")
	}
}

func TestConfigValidation(t *testing.T) {
	p := simhash.NewPlugin()
	bad := index.Descriptor{Table: "T", Name: "x", PluginName: "simhash",
		PluginConfig: []byte(`{"field":"x","prefix_bits":40}`)}
	if err := p.Build(bad, func(yield func(model.Item) bool) {}); err == nil {
		t.Fatal("expected prefix_bits range error")
	}
	bad.PluginConfig = []byte(fmt.Sprintf(`{"field":"x","max_radius":%d}`, 17))
	if err := p.Build(bad, func(yield func(model.Item) bool) {}); err == nil {
		t.Fatal("expected max_radius range error")
	}
}
