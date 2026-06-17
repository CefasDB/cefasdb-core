package cosine_test

import (
	"math"
	"strconv"
	"testing"

	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/cosine"
)

func vec(xs ...float64) model.AttributeValue {
	out := make([]model.AttributeValue, len(xs))
	for i, x := range xs {
		out[i] = model.AttributeValue{T: model.AttrN, N: strconv.FormatFloat(x, 'f', -1, 64)}
	}
	return model.AttributeValue{T: model.AttrL, L: out}
}

func TestCanonicalVectors(t *testing.T) {
	cases := []struct {
		a, b []float64
		want float64 // distance = 1 - cosine_sim
	}{
		{[]float64{1, 0}, []float64{1, 0}, 0},
		{[]float64{1, 0}, []float64{0, 1}, 1},
		{[]float64{1, 1}, []float64{1, 0}, 1 - 1/math.Sqrt(2)},
		{[]float64{1, 0, 0}, []float64{-1, 0, 0}, 2},
	}
	for _, tc := range cases {
		got, err := cosine.Op{}.Eval(vec(tc.a...), vec(tc.b...))
		if err != nil {
			t.Errorf("eval: %v", err)
			continue
		}
		if math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("cosine(%v,%v) = %.6f, want %.6f", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestDimensionMismatchErrors(t *testing.T) {
	if _, err := (cosine.Op{}).Eval(vec(1, 2), vec(1, 2, 3)); err == nil {
		t.Fatal("expected dim-mismatch error")
	}
}

func TestZeroVectorReturnsMaxDistance(t *testing.T) {
	got, err := cosine.Op{}.Eval(vec(0, 0, 0), vec(1, 2, 3))
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got != 1 {
		t.Fatalf("got %g, want 1", got)
	}
}
