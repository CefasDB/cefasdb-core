package hamming_test

import (
	"testing"

	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/hamming"
)

func s(v string) model.AttributeValue { return model.AttributeValue{T: model.AttrS, S: v} }
func b(v []byte) model.AttributeValue { return model.AttributeValue{T: model.AttrB, B: v} }

func TestVectorsString(t *testing.T) {
	cases := []struct {
		a, b string
		want float64
	}{
		{"1010", "1011", 1},
		{"1010", "0101", 4},
		{"abcd", "abcd", 0},
		{"karolin", "kathrin", 3},
	}
	for _, tc := range cases {
		got, err := hamming.Op{}.Eval(s(tc.a), s(tc.b))
		if err != nil {
			t.Errorf("eval(%q,%q): %v", tc.a, tc.b, err)
			continue
		}
		if got != tc.want {
			t.Errorf("hamming(%q,%q) = %g, want %g", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestVectorsBytes(t *testing.T) {
	got, err := hamming.Op{}.Eval(b([]byte{0xFF, 0x00, 0xAA}), b([]byte{0xF0, 0x01, 0xAA}))
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	if got != 2 {
		t.Fatalf("hamming(bytes) = %g, want 2", got)
	}
}

func TestLengthMismatchErrors(t *testing.T) {
	if _, err := (hamming.Op{}).Eval(s("ab"), s("abc")); err == nil {
		t.Fatal("expected length-mismatch error")
	}
}

func TestTypeMismatchErrors(t *testing.T) {
	if _, err := (hamming.Op{}).Eval(s("ab"), b([]byte("ab"))); err == nil {
		t.Fatal("expected type-mismatch error")
	}
}

func BenchmarkHamming64(b *testing.B) {
	op := hamming.Op{}
	x := s("the quick brown fox jumped over the lazy dog 1234567 8901234")
	y := s("the quick brown fox jumped over the lazy DOG 1234567 8901234")
	for i := 0; i < b.N; i++ {
		_, _ = op.Eval(x, y)
	}
}
