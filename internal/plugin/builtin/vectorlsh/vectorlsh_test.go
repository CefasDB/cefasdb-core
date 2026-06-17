package vectorlsh_test

import (
	"strconv"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/index"
	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/pkg/plugin/testharness"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/vectorlsh"
)

func vec(pk string, xs ...float64) model.Item {
	out := make([]model.AttributeValue, len(xs))
	for i, x := range xs {
		out[i] = model.AttributeValue{T: model.AttrN, N: strconv.FormatFloat(x, 'f', -1, 64)}
	}
	return model.Item{
		"pk":  {T: model.AttrS, S: pk},
		"emb": {T: model.AttrL, L: out},
	}
}

func desc() index.Descriptor {
	return index.Descriptor{
		Table:        "Docs",
		Name:         "emb_lsh",
		PluginName:   "vectorlsh",
		PluginConfig: []byte(`{"field":"emb","dim":4,"sketches":8,"bits_per_sketch":6}`),
		KeySchema:    model.KeySchema{PK: "pk"},
	}
}

func collectIDs(cs plugin.CandidateSet) []string {
	var out []string
	for {
		c, ok := cs.Next()
		if !ok {
			break
		}
		out = append(out, c.Key["pk"].S)
	}
	return out
}

func TestSimilarVectorRecallsTarget(t *testing.T) {
	h := testharness.New(t)
	h.MustRegister(vectorlsh.NewPlugin())
	h.SeedTable("Docs",
		vec("a", 1, 0, 0, 0),
		vec("a_jitter", 0.99, 0.05, 0.0, 0.0), // near-parallel to "a"
		vec("b", 0, 1, 0, 0),
		vec("c", 0, 0, 1, 0),
		vec("d", 0, 0, 0, 1),
		vec("ab", 0.7, 0.7, 0, 0),
	)
	if err := h.BuildIndex(desc()); err != nil {
		t.Fatalf("build: %v", err)
	}
	p, _ := h.Registry.Lookup("vectorlsh")
	ip := p.(plugin.IndexPlugin)
	cs, err := ip.Query(desc(), plugin.IndexQuery{
		Binds: map[string]model.AttributeValue{
			":vector": {T: model.AttrL, L: vec("q", 1, 0, 0, 0)["emb"].L},
		},
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	got := collectIDs(cs)
	have := map[string]bool{}
	for _, id := range got {
		have[id] = true
	}
	// a should always be recalled — query is identical to its vector.
	// a_jitter has > 0.99 cosine with the query so the LSH should
	// bucket it together with high probability.
	if !have["a"] {
		t.Fatalf("missing identical-vector candidate a; got %v", got)
	}
}

func TestUpdateAndDelete(t *testing.T) {
	p := vectorlsh.NewPlugin()
	d := desc()
	v1 := vec("u1", 1, 0, 0, 0)
	_ = p.Update(d, nil, v1)
	_ = p.Delete(d, v1)
	cs, _ := p.Query(d, plugin.IndexQuery{
		Binds: map[string]model.AttributeValue{":vector": {T: model.AttrL, L: v1["emb"].L}},
	})
	if got := collectIDs(cs); len(got) != 0 {
		t.Fatalf("expected no candidates after delete, got %v", got)
	}
}

func TestDimMismatchRejected(t *testing.T) {
	p := vectorlsh.NewPlugin()
	d := desc()
	bad := vec("u1", 1, 0, 0) // 3-dim, config says 4
	if err := p.Update(d, nil, bad); err == nil {
		t.Fatal("expected dim mismatch")
	}
}
