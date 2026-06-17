package jarowinkler_test

import (
	"math"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/jarowinkler"
)

func s(v string) model.AttributeValue { return model.AttributeValue{T: model.AttrS, S: v} }

func TestCanonicalVectors(t *testing.T) {
	cases := []struct {
		a, b string
		want float64
	}{
		{"MARTHA", "MARHTA", 0.961},
		{"DIXON", "DICKSONX", 0.813},
		{"DWAYNE", "DUANE", 0.840},
		{"", "abc", 0},
		{"abc", "abc", 1},
	}
	for _, tc := range cases {
		got := jarowinkler.Similarity(tc.a, tc.b)
		if math.Abs(got-tc.want) > 0.01 {
			t.Errorf("Similarity(%q,%q) = %.3f, want %.3f", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestEvalReturnsDistance(t *testing.T) {
	got, err := jarowinkler.Op{}.Eval(s("MARTHA"), s("MARHTA"))
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if math.Abs(got-(1-0.961)) > 0.01 {
		t.Fatalf("distance = %.3f, want ~0.039", got)
	}
}

func TestIdenticalReturnsZeroDistance(t *testing.T) {
	got, _ := jarowinkler.Op{}.Eval(s("identical"), s("identical"))
	if got != 0 {
		t.Fatalf("identical distance = %g, want 0", got)
	}
}
