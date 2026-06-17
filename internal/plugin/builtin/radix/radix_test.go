package radix_test

import (
	"fmt"
	"sort"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/index"
	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/radix"
	"github.com/CefasDb/cefasdb/pkg/plugin/testharness"
)

func item(pk, name string) model.Item {
	return model.Item{
		"pk":   {T: model.AttrS, S: pk},
		"name": {T: model.AttrS, S: name},
	}
}

func desc() index.Descriptor {
	return index.Descriptor{
		Table:        "Users",
		Name:         "name_prefix",
		PluginName:   "radix",
		PluginConfig: []byte(`{"field":"name"}`),
		KeySchema:    model.KeySchema{PK: "pk"},
	}
}

func names(cs plugin.CandidateSet) []string {
	var out []string
	for {
		c, ok := cs.Next()
		if !ok {
			break
		}
		out = append(out, c.Key["name"].S)
	}
	sort.Strings(out)
	return out
}

func TestPrefixQueryMatchesBruteForce(t *testing.T) {
	h := testharness.New(t)
	h.MustRegister(radix.NewPlugin())
	seed := []string{"alpha", "alps", "alaska", "beta", "alphabet", "ant"}
	for i, n := range seed {
		h.SeedTable("Users", item(fmt.Sprintf("u%d", i), n))
	}
	if err := h.BuildIndex(desc()); err != nil {
		t.Fatalf("build: %v", err)
	}
	p, _ := h.Registry.Lookup("radix")
	ip := p.(plugin.IndexPlugin)
	cs, err := ip.Query(desc(), plugin.IndexQuery{Predicate: "alp"})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	got := names(cs)
	want := []string{"alpha", "alphabet", "alps"}
	if !equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestUpdateReplacesIndexedValue(t *testing.T) {
	p := radix.NewPlugin()
	d := desc()
	_ = p.Update(d, nil, item("u1", "abacus"))
	_ = p.Update(d, item("u1", "abacus"), item("u1", "zebra"))
	cs, _ := p.Query(d, plugin.IndexQuery{Predicate: "aba"})
	if _, ok := cs.Next(); ok {
		t.Fatal("expected no match after rename")
	}
	cs, _ = p.Query(d, plugin.IndexQuery{Predicate: "zeb"})
	if _, ok := cs.Next(); !ok {
		t.Fatal("expected match on new name")
	}
}

func TestDeleteRemovesEntry(t *testing.T) {
	p := radix.NewPlugin()
	d := desc()
	_ = p.Update(d, nil, item("u1", "alpha"))
	_ = p.Delete(d, item("u1", "alpha"))
	cs, _ := p.Query(d, plugin.IndexQuery{Predicate: "alp"})
	if _, ok := cs.Next(); ok {
		t.Fatal("expected no match after delete")
	}
}

func TestEstimateMatchesQueryCount(t *testing.T) {
	p := radix.NewPlugin()
	d := desc()
	for i, n := range []string{"alpha", "alps", "ant"} {
		_ = p.Update(d, nil, item(fmt.Sprintf("u%d", i), n))
	}
	est, _ := p.Estimate(d, plugin.IndexQuery{Predicate: "al"})
	if est != 2 {
		t.Fatalf("estimate = %d, want 2", est)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
