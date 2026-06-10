package euclidean_test

import (
	"math"
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/plugin/euclidean"
)

func vec(xs ...string) model.AttributeValue {
	out := make([]model.AttributeValue, len(xs))
	for i, x := range xs {
		out[i] = model.AttributeValue{T: model.AttrN, N: x}
	}
	return model.AttributeValue{T: model.AttrL, L: out}
}

func TestCanonicalVectors(t *testing.T) {
	cases := []struct {
		a, b []string
		want float64
	}{
		{[]string{"0", "0"}, []string{"3", "4"}, 5},
		{[]string{"1", "1", "1"}, []string{"2", "2", "2"}, math.Sqrt(3)},
		{[]string{"0"}, []string{"0"}, 0},
	}
	for _, tc := range cases {
		got, err := euclidean.Op{}.Eval(vec(tc.a...), vec(tc.b...))
		if err != nil {
			t.Errorf("eval: %v", err)
			continue
		}
		if math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("euclidean(%v,%v) = %g, want %g", tc.a, tc.b, got, tc.want)
		}
	}
}
