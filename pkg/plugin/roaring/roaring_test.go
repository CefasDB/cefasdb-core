package roaring_test

import (
	"fmt"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/index"
	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/pkg/plugin/roaring"
	"github.com/CefasDb/cefasdb/pkg/plugin/testharness"
)

func numItem(n int) model.Item {
	return model.Item{"user_id": {T: model.AttrN, N: fmt.Sprintf("%d", n)}}
}

func TestBuildContainsAndCardinality(t *testing.T) {
	h := testharness.New(t)
	h.MustRegister(roaring.NewPlugin())
	for i := 0; i < 1000; i++ {
		h.SeedTable("Users", numItem(i*2)) // even ids only
	}
	d := index.Descriptor{Table: "Users", Name: "even", PluginName: "roaring",
		PluginConfig: []byte(`{"field":"user_id"}`)}
	if err := h.BuildIndex(d); err != nil {
		t.Fatalf("build: %v", err)
	}
	p, _ := h.Registry.Lookup("roaring")
	ip := p.(plugin.IndexPlugin)
	got, err := ip.Estimate(d, plugin.IndexQuery{})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if got != 1000 {
		t.Fatalf("cardinality = %d, want 1000", got)
	}
	// Member probe.
	cs, _ := ip.Query(d, plugin.IndexQuery{Binds: map[string]model.AttributeValue{":id": {T: model.AttrN, N: "100"}}})
	if _, ok := cs.Next(); !ok {
		t.Fatal("100 should be present")
	}
	cs.Close()
	cs, _ = ip.Query(d, plugin.IndexQuery{Binds: map[string]model.AttributeValue{":id": {T: model.AttrN, N: "101"}}})
	if _, ok := cs.Next(); ok {
		t.Fatal("101 should be absent")
	}
	cs.Close()
}

func TestCohortIntersect(t *testing.T) {
	a := roaring.NewCohort(roaring.Config{Field: "id"})
	b := roaring.NewCohort(roaring.Config{Field: "id"})
	for i := 0; i < 100; i++ {
		a.Add(uint32(i))
	}
	for i := 50; i < 150; i++ {
		b.Add(uint32(i))
	}
	inter := a.Intersect(b)
	if inter.Cardinality() != 50 {
		t.Fatalf("intersect cardinality = %d, want 50", inter.Cardinality())
	}
}

func TestCohortUnion(t *testing.T) {
	a := roaring.NewCohort(roaring.Config{Field: "id"})
	b := roaring.NewCohort(roaring.Config{Field: "id"})
	for i := 0; i < 100; i++ {
		a.Add(uint32(i))
	}
	for i := 100; i < 200; i++ {
		b.Add(uint32(i))
	}
	uni := a.Union(b)
	if uni.Cardinality() != 200 {
		t.Fatalf("union cardinality = %d, want 200", uni.Cardinality())
	}
}

func TestSerializeRoundTrip(t *testing.T) {
	c := roaring.NewCohort(roaring.Config{Field: "id"})
	for i := 0; i < 50; i++ {
		c.Add(uint32(i * 3))
	}
	buf, err := c.Serialize()
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}
	g, err := roaring.Deserialize(buf)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}
	if g.Cardinality() != 50 {
		t.Fatalf("cardinality = %d, want 50", g.Cardinality())
	}
	for i := 0; i < 50; i++ {
		if !g.Contains(uint32(i * 3)) {
			t.Fatalf("missing %d", i*3)
		}
	}
}

func TestUpdateRemovesOldAddsNew(t *testing.T) {
	p := roaring.NewPlugin()
	d := index.Descriptor{Table: "Users", Name: "x", PluginName: "roaring",
		PluginConfig: []byte(`{"field":"user_id"}`)}
	old := numItem(10)
	neu := numItem(20)
	_ = p.Update(d, nil, old)
	_ = p.Update(d, old, neu)
	cs, _ := p.Query(d, plugin.IndexQuery{Binds: map[string]model.AttributeValue{":id": {T: model.AttrN, N: "10"}}})
	if _, ok := cs.Next(); ok {
		t.Fatal("old id 10 should have been removed")
	}
	cs.Close()
}

func TestNonNumericFieldRejected(t *testing.T) {
	p := roaring.NewPlugin()
	d := index.Descriptor{Table: "T", Name: "x", PluginName: "roaring",
		PluginConfig: []byte(`{"field":"name"}`)}
	err := p.Update(d, nil, model.Item{"name": {T: model.AttrS, S: "ova"}})
	if err == nil {
		t.Fatal("expected non-numeric error")
	}
}
