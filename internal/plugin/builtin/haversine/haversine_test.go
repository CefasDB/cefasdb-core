package haversine_test

import (
	"math"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/haversine"
)

func loc(lat, lon string) model.AttributeValue {
	return model.AttributeValue{
		T: model.AttrM,
		M: map[string]model.AttributeValue{
			"lat": {T: model.AttrN, N: lat},
			"lon": {T: model.AttrN, N: lon},
		},
	}
}

func TestKnownPairs(t *testing.T) {
	// São Paulo → Santos: ~55 km great-circle (the ~70 km figure is by road).
	got, err := haversine.Op{}.Eval(loc("-23.5505", "-46.6333"), loc("-23.9608", "-46.3336"))
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got < 50_000 || got > 60_000 {
		t.Fatalf("SP→Santos = %.0fm, want ~55km", got)
	}

	// Same point → 0.
	got, _ = haversine.Op{}.Eval(loc("0", "0"), loc("0", "0"))
	if got != 0 {
		t.Fatalf("same-point = %g, want 0", got)
	}

	// Antipodal: half the circumference (~20015 km).
	got, _ = haversine.Op{}.Eval(loc("0", "0"), loc("0", "180"))
	if math.Abs(got-math.Pi*haversine.EarthRadiusMeters) > 1 {
		t.Fatalf("antipodal = %.0fm, want ~%.0fm", got, math.Pi*haversine.EarthRadiusMeters)
	}
}

func TestRejectsMissingLat(t *testing.T) {
	bad := model.AttributeValue{T: model.AttrM, M: map[string]model.AttributeValue{
		"lon": {T: model.AttrN, N: "0"},
	}}
	if _, err := (haversine.Op{}).Eval(bad, loc("0", "0")); err == nil {
		t.Fatal("expected missing-lat error")
	}
}

func TestRejectsOutOfRange(t *testing.T) {
	if _, err := (haversine.Op{}).Eval(loc("100", "0"), loc("0", "0")); err == nil {
		t.Fatal("expected lat-range error")
	}
	if _, err := (haversine.Op{}).Eval(loc("0", "200"), loc("0", "0")); err == nil {
		t.Fatal("expected lon-range error")
	}
}
