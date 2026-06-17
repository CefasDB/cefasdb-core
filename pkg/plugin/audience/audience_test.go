package audience_test

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/pkg/core/index"
	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/pkg/plugin/audience"
	"github.com/CefasDb/cefasdb/pkg/plugin/geohash"
	"github.com/CefasDb/cefasdb/pkg/plugin/hll"
)

func storeItem(pk string, lat, lon float64) model.Item {
	return model.Item{
		"pk": {T: model.AttrS, S: pk},
		"loc": {T: model.AttrM, M: map[string]model.AttributeValue{
			"lat": {T: model.AttrN, N: strconv.FormatFloat(lat, 'f', -1, 64)},
			"lon": {T: model.AttrN, N: strconv.FormatFloat(lon, 'f', -1, 64)},
		}},
	}
}

func newBound(t *testing.T) (*audience.Plugin, *geohash.Plugin, index.Descriptor) {
	t.Helper()
	geo := geohash.NewPlugin()
	clock := time.Unix(1_700_000_000, 0)
	a := audience.NewPluginWith(geo, hll.NewPlugin(), func() time.Time { return clock })
	d := index.Descriptor{
		Table:        "Stores",
		Name:         "loc_geo",
		PluginName:   "geohash",
		PluginConfig: []byte(`{"field":"loc","precision":5}`),
		KeySchema:    model.KeySchema{PK: "pk"},
	}
	a.Bind(audience.IndexBinding{Geohash: d})
	return a, geo, d
}

func TestSelectAppliesHaversinePostFilter(t *testing.T) {
	a, geo, d := newBound(t)
	// São Paulo neighborhood: pack items at various distances.
	_ = geo.Build(d, func(yield func(model.Item) bool) {
		_ = yield(storeItem("inside", -23.5510, -46.6340)) // very close to center
		_ = yield(storeItem("edge", -23.5605, -46.6240))   // ~1.5km away
		_ = yield(storeItem("far", -23.9608, -46.3336))    // Santos, ~55km
	})
	cs, err := a.Select(plugin.AudienceRequest{Lat: -23.5505, Lon: -46.6333, Radius: 2000})
	if err != nil {
		t.Fatalf("select: %v", err)
	}
	got := map[string]bool{}
	for {
		c, ok := cs.Next()
		if !ok {
			break
		}
		got[c.Key["pk"].S] = true
	}
	if !got["inside"] {
		t.Errorf("inside store missing")
	}
	if got["far"] {
		t.Errorf("far store (Santos) leaked through the haversine post-filter")
	}
}

func TestEstimateReturnsApproximateReach(t *testing.T) {
	a, geo, d := newBound(t)
	_ = geo.Build(d, func(yield func(model.Item) bool) {
		for i := 0; i < 20; i++ {
			pk := "u" + strconv.Itoa(i)
			_ = yield(storeItem(pk, -23.5505+float64(i)*1e-5, -46.6333))
		}
	})
	got, err := a.Estimate(plugin.AudienceRequest{Lat: -23.5505, Lon: -46.6333, Radius: 5000})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	// HLL precision-14 standard error ≈ 0.8% → for 20 items the
	// returned value is essentially exact, allow ±2.
	if got < 18 || got > 22 {
		t.Fatalf("estimate = %d, want ~20", got)
	}
}

func TestDedupWindow(t *testing.T) {
	now := time.Unix(1, 0)
	clock := func() time.Time { return now }
	a := audience.NewPluginWith(nil, nil, clock)
	// First call inside the window — allowed.
	ok, err := a.Dedup("camp1", "u1", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first dedup got (%v, %v)", ok, err)
	}
	// Second call inside the window — blocked.
	now = now.Add(30 * time.Second)
	ok, _ = a.Dedup("camp1", "u1", time.Minute)
	if ok {
		t.Fatal("duplicate inside window should return false")
	}
	// After TTL — allowed again.
	now = now.Add(time.Hour)
	ok, _ = a.Dedup("camp1", "u1", time.Minute)
	if !ok {
		t.Fatal("after TTL the entry should be allowed again")
	}
}

func TestFreqCapSlidingWindow(t *testing.T) {
	now := time.Unix(1, 0)
	clock := func() time.Time { return now }
	a := audience.NewPluginWith(nil, nil, clock)
	scope, key := "merchant", "u1"
	for i := 0; i < 3; i++ {
		ok, _ := a.FreqCap(scope, key, 3, time.Hour)
		if !ok {
			t.Fatalf("hit %d should be allowed under cap", i+1)
		}
	}
	ok, _ := a.FreqCap(scope, key, 3, time.Hour)
	if ok {
		t.Fatal("4th hit under cap-of-3 should be blocked")
	}
	// Slide past the window — everything resets.
	now = now.Add(2 * time.Hour)
	ok, _ = a.FreqCap(scope, key, 3, time.Hour)
	if !ok {
		t.Fatal("after window expiry the next hit should be allowed")
	}
}

func TestAggregateEnforcesMinGroupSize(t *testing.T) {
	items := []model.Item{
		{"campaign": {T: model.AttrS, S: "c1"}, "imp": {T: model.AttrN, N: "1"}},
		{"campaign": {T: model.AttrS, S: "c1"}, "imp": {T: model.AttrN, N: "1"}},
		{"campaign": {T: model.AttrS, S: "c2"}, "imp": {T: model.AttrN, N: "1"}}, // singleton
	}
	_, err := audience.Aggregate(items, audience.AggregateSpec{
		GroupBy: []string{"campaign"}, Metrics: []string{"imp"}, MinGroupSize: 2,
	})
	if !errors.Is(err, audience.ErrMinGroupSize) {
		t.Fatalf("err = %v, want ErrMinGroupSize", err)
	}
}

func TestAggregateProducesRowsAtThreshold(t *testing.T) {
	items := []model.Item{
		{"campaign": {T: model.AttrS, S: "c1"}, "imp": {T: model.AttrN, N: "2"}},
		{"campaign": {T: model.AttrS, S: "c1"}, "imp": {T: model.AttrN, N: "3"}},
		{"campaign": {T: model.AttrS, S: "c2"}, "imp": {T: model.AttrN, N: "5"}},
		{"campaign": {T: model.AttrS, S: "c2"}, "imp": {T: model.AttrN, N: "7"}},
	}
	got, err := audience.Aggregate(items, audience.AggregateSpec{
		GroupBy: []string{"campaign"}, Metrics: []string{"imp"}, MinGroupSize: 2,
	})
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("groups = %d, want 2", len(got))
	}
	if got[0].GroupKey["campaign"] != "c1" || got[0].Counts["imp"] != 5 {
		t.Fatalf("c1 = %+v", got[0])
	}
	if got[1].GroupKey["campaign"] != "c2" || got[1].Counts["imp"] != 12 {
		t.Fatalf("c2 = %+v", got[1])
	}
}

func TestEligibilityComposesDedupAndFreqCap(t *testing.T) {
	now := time.Unix(1, 0)
	clock := func() time.Time { return now }
	a := audience.NewPluginWith(nil, nil, clock)
	req := audience.EligibilityRequest{
		Campaign: "c1", UserKey: "u1",
		DedupTTL: time.Hour, FreqLimit: 2, FreqWindow: time.Hour,
	}
	// 1st: allowed (freq 1/2, dedup new).
	got, _ := a.Eligibility(req)
	if !got.Eligible {
		t.Fatalf("1st = %+v, want eligible", got)
	}
	// 2nd: freq increments to 2/2 (still allowed), then dedup blocks.
	got, _ = a.Eligibility(req)
	if got.Reason != "duplicate" {
		t.Fatalf("2nd reason = %q, want duplicate", got.Reason)
	}
	// 3rd: freq finds 2 hits in window → at limit, blocks before
	// dedup runs. Documents that v1 increments freq greedily even
	// when dedup later rejects.
	got, _ = a.Eligibility(req)
	if got.Reason != "freq_cap" {
		t.Fatalf("3rd reason = %q, want freq_cap", got.Reason)
	}
}
