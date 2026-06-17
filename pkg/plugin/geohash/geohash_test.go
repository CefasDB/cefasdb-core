package geohash_test

import (
	"strconv"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/index"
	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/pkg/plugin/geohash"
	"github.com/CefasDb/cefasdb/pkg/plugin/testharness"
)

func item(pk string, lat, lon float64) model.Item {
	return model.Item{
		"pk": {T: model.AttrS, S: pk},
		"loc": {T: model.AttrM, M: map[string]model.AttributeValue{
			"lat": {T: model.AttrN, N: strconv.FormatFloat(lat, 'f', -1, 64)},
			"lon": {T: model.AttrN, N: strconv.FormatFloat(lon, 'f', -1, 64)},
		}},
	}
}

func desc() index.Descriptor {
	return index.Descriptor{
		Table:        "Stores",
		Name:         "loc_geo",
		PluginName:   "geohash",
		PluginConfig: []byte(`{"field":"loc","precision":5}`),
		KeySchema:    model.KeySchema{PK: "pk"},
	}
}

func centerAttr(lat, lon float64) model.AttributeValue {
	return model.AttributeValue{T: model.AttrM, M: map[string]model.AttributeValue{
		"lat": {T: model.AttrN, N: strconv.FormatFloat(lat, 'f', -1, 64)},
		"lon": {T: model.AttrN, N: strconv.FormatFloat(lon, 'f', -1, 64)},
	}}
}

func TestKnownHashes(t *testing.T) {
	// São Paulo (-23.5505, -46.6333) → "6gyf4..."
	got := geohash.Encode(-23.5505, -46.6333, 5)
	if got[:3] != "6gy" {
		t.Fatalf("SP hash[:3] = %q, want %q", got[:3], "6gy")
	}
}

func TestNeighborsReturns8(t *testing.T) {
	got := geohash.Neighbors("6gyf4")
	if len(got) != 8 {
		t.Fatalf("neighbors = %d, want 8", len(got))
	}
}

func TestQueryFindsItemsInCenterAndNeighbors(t *testing.T) {
	h := testharness.New(t)
	h.MustRegister(geohash.NewPlugin())
	// Pack a few items near São Paulo.
	h.SeedTable("Stores",
		item("near", -23.5510, -46.6340),  // same cell
		item("close", -23.5550, -46.6300), // adjacent cell
		item("far", 0, 0),                 // very different
	)
	if err := h.BuildIndex(desc()); err != nil {
		t.Fatalf("build: %v", err)
	}
	p, _ := h.Registry.Lookup("geohash")
	ip := p.(plugin.IndexPlugin)
	cs, err := ip.Query(desc(), plugin.IndexQuery{
		Binds: map[string]model.AttributeValue{":center": centerAttr(-23.5505, -46.6333)},
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
	if !got["near"] {
		t.Errorf("missing near item from candidate set")
	}
	if got["far"] {
		t.Errorf("(0,0) leaked into the candidate set")
	}
}

func TestConfigPrecisionRange(t *testing.T) {
	p := geohash.NewPlugin()
	bad := index.Descriptor{Table: "T", Name: "x", PluginName: "geohash",
		PluginConfig: []byte(`{"field":"loc","precision":15}`)}
	if err := p.Build(bad, func(yield func(model.Item) bool) {}); err == nil {
		t.Fatal("expected precision range error")
	}
}
