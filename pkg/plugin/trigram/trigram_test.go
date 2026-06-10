package trigram_test

import (
	"fmt"
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
	"github.com/osvaldoandrade/cefas/pkg/plugin/testharness"
	"github.com/osvaldoandrade/cefas/pkg/plugin/trigram"
)

func item(pk, name string) model.Item {
	return model.Item{
		"pk":   {T: model.AttrS, S: pk},
		"name": {T: model.AttrS, S: name},
	}
}

func desc() index.Descriptor {
	return index.Descriptor{
		Table:        "Merchants",
		Name:         "name_tri",
		PluginName:   "trigram",
		PluginConfig: []byte(`{"field":"name","min_overlap":0.3}`),
		KeySchema:    model.KeySchema{PK: "pk"},
	}
}

func collectNames(cs plugin.CandidateSet) []string {
	var out []string
	for {
		c, ok := cs.Next()
		if !ok {
			break
		}
		out = append(out, c.Key["name"].S)
	}
	return out
}

func TestCandidateSupersetOfBruteForce(t *testing.T) {
	h := testharness.New(t)
	h.MustRegister(trigram.NewPlugin())
	names := []string{"habibs", "habib", "abibas", "starbucks", "haviano"}
	for i, n := range names {
		h.SeedTable("Merchants", item(fmt.Sprintf("m%d", i), n))
	}
	if err := h.BuildIndex(desc()); err != nil {
		t.Fatalf("build: %v", err)
	}
	p, _ := h.Registry.Lookup("trigram")
	ip := p.(plugin.IndexPlugin)
	cs, _ := ip.Query(desc(), plugin.IndexQuery{Predicate: "habibs"})
	got := collectNames(cs)
	// Brute-force: any name with ≥30% trigram overlap with "habibs"
	// (4 trigrams). habibs+habib share 3 → 75%, abibas shares 2 →
	// 50%, others share 0.
	wantIn := map[string]bool{"habibs": true, "habib": true, "abibas": true}
	for _, n := range got {
		if !wantIn[n] {
			t.Errorf("unexpected candidate %q", n)
		}
		delete(wantIn, n)
	}
	if len(wantIn) > 0 {
		t.Errorf("missing candidates: %v", wantIn)
	}
}

func TestUpdateRewritesTrigrams(t *testing.T) {
	p := trigram.NewPlugin()
	d := desc()
	_ = p.Update(d, nil, item("m1", "alpha"))
	_ = p.Update(d, item("m1", "alpha"), item("m1", "omega"))
	cs, _ := p.Query(d, plugin.IndexQuery{Predicate: "alpha"})
	if got := collectNames(cs); len(got) != 0 {
		t.Fatalf("expected no match after rename, got %v", got)
	}
	cs, _ = p.Query(d, plugin.IndexQuery{Predicate: "omega"})
	if got := collectNames(cs); len(got) == 0 {
		t.Fatal("expected match on new name")
	}
}

func TestShingleEmptyAndShort(t *testing.T) {
	if got := trigram.Shingle(""); len(got) != 0 {
		t.Fatalf("empty Shingle = %v, want 0", got)
	}
	if got := trigram.Shingle("ab"); len(got) != 1 || got[0] != "ab" {
		t.Fatalf("short Shingle = %v, want [ab]", got)
	}
	if got := trigram.Shingle("abcd"); len(got) != 2 {
		t.Fatalf("Shingle(abcd) = %v, want 2 trigrams", got)
	}
}
