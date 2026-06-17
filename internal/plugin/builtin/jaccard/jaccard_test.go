package jaccard_test

import (
	"math"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/jaccard"
)

func ss(v ...string) model.AttributeValue { return model.AttributeValue{T: model.AttrSS, SS: v} }
func s(v string) model.AttributeValue     { return model.AttributeValue{T: model.AttrS, S: v} }

func TestSetSimilarity(t *testing.T) {
	cases := []struct {
		a, b []string
		want float64 // distance = 1 - jaccard
	}{
		{[]string{"a", "b"}, []string{"a", "b", "c"}, 1.0 - 2.0/3.0},
		{[]string{"a"}, []string{"b"}, 1.0},
		{[]string{"a", "b"}, []string{"a", "b"}, 0.0},
		{nil, nil, 0.0},
	}
	for _, tc := range cases {
		got, err := jaccard.Op{}.Eval(ss(tc.a...), ss(tc.b...))
		if err != nil {
			t.Errorf("eval: %v", err)
			continue
		}
		if math.Abs(got-tc.want) > 1e-9 {
			t.Errorf("jaccard(%v,%v) = %.3f, want %.3f", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestStringShinglesToTrigrams(t *testing.T) {
	got, err := jaccard.Op{}.Eval(s("habibs"), s("habib"))
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	// Trigrams of "habibs" = {hab, abi, bib, ibs} ; "habib" = {hab, abi, bib}
	// Intersection = 3, union = 4, distance = 1 - 3/4 = 0.25.
	if math.Abs(got-0.25) > 1e-9 {
		t.Fatalf("got %.3f, want 0.25", got)
	}
}

func TestStringShorterThanThree(t *testing.T) {
	got, err := jaccard.Op{}.Eval(s("ab"), s("ab"))
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got != 0 {
		t.Fatalf("got %g, want 0", got)
	}
}
