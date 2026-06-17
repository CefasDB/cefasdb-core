package damerau_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin/damerau"
)

func s(v string) model.AttributeValue { return model.AttributeValue{T: model.AttrS, S: v} }

func TestCanonicalVectors(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"ab", "ba", 1},           // single transposition
		{"abcdef", "abcfed", 2},   // d/f swap is non-adjacent under OSA → 2 substitutions
		{"recieve", "receive", 1}, // typo: ie ↔ ei (adjacent transposition)
		{"", "", 0},
		{"abc", "abc", 0},
	}
	for _, tc := range cases {
		if got := damerau.Distance(tc.a, tc.b); got != tc.want {
			t.Errorf("Distance(%q,%q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestOpEval(t *testing.T) {
	got, err := damerau.Op{}.Eval(s("ab"), s("ba"))
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got != 1 {
		t.Fatalf("got %g, want 1", got)
	}
}
