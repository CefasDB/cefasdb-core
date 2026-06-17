package manhattan_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/manhattan"
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
		{[]string{"0", "0"}, []string{"3", "4"}, 7},
		{[]string{"1", "1", "1"}, []string{"2", "2", "2"}, 3},
		{[]string{"-1", "0"}, []string{"1", "0"}, 2},
	}
	for _, tc := range cases {
		got, err := manhattan.Op{}.Eval(vec(tc.a...), vec(tc.b...))
		if err != nil {
			t.Errorf("eval: %v", err)
			continue
		}
		if got != tc.want {
			t.Errorf("manhattan(%v,%v) = %g, want %g", tc.a, tc.b, got, tc.want)
		}
	}
}
