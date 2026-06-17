package levenshtein_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/levenshtein"
)

func s(v string) model.AttributeValue { return model.AttributeValue{T: model.AttrS, S: v} }

func TestCanonicalVectors(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"kitten", "sitting", 3},
		{"", "abc", 3},
		{"abc", "", 3},
		{"flaw", "lawn", 2},
		{"", "", 0},
		{"abc", "abc", 0},
		{"saturday", "sunday", 3},
		{"habibs", "habib", 1},
	}
	for _, tc := range cases {
		if got := levenshtein.Distance(tc.a, tc.b); got != tc.want {
			t.Errorf("Distance(%q,%q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestEvalWiresThroughOp(t *testing.T) {
	got, err := levenshtein.Op{}.Eval(s("kitten"), s("sitting"))
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got != 3 {
		t.Fatalf("got %g, want 3", got)
	}
}

func TestRejectsNonStringInputs(t *testing.T) {
	bad := model.AttributeValue{T: model.AttrN, N: "1"}
	if _, err := (levenshtein.Op{}).Eval(s("x"), bad); err == nil {
		t.Fatal("expected type error")
	}
}

func BenchmarkLevenshteinShort(b *testing.B) {
	op := levenshtein.Op{}
	x, y := s("habibs"), s("habibo")
	for i := 0; i < b.N; i++ {
		_, _ = op.Eval(x, y)
	}
}

func BenchmarkLevenshteinMedium(b *testing.B) {
	op := levenshtein.Op{}
	x := s("the quick brown fox jumps over the lazy dog")
	y := s("a quick browne fox jumped over the lazy bear")
	for i := 0; i < b.N; i++ {
		_, _ = op.Eval(x, y)
	}
}
