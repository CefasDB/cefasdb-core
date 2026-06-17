package cosine_test

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/CefasDb/cefasdb/internal/plugin/builtin/cosine"
)

// Property tests for the cosine distance plugin.
//
// Cosine distance here is d(x, y) = 1 - cos(theta). The axioms covered:
//   1. Identity:        d(x, x) ~= 0  for any non-zero x.
//   2. Non-negativity:  d(x, y) >= 0.
//   3. Symmetry:        d(x, y) ~= d(y, x).
//   4. Upper bound:     d(x, y) <= 2 + epsilon (anti-parallel = 2).
//
// Triangle inequality is intentionally NOT tested. Plain 1 - cos(theta) is
// not a metric on arbitrary vectors; it only becomes one (in the angular
// sense) after taking arccos. The implementation does not normalise to
// arccos form, so triangle inequality is unsupported by contract.

const cosineEps = 1e-9

// nonZeroVec is a quick.Generator producing a small, finite, non-zero vector.
type nonZeroVec struct {
	V []float64
}

func (nonZeroVec) Generate(r *rand.Rand, _ int) reflect.Value {
	n := r.Intn(7) + 1 // 1..7 dims
	for {
		xs := make([]float64, n)
		var norm float64
		for i := range xs {
			// Bounded range keeps floats well-behaved and away from inf.
			xs[i] = (r.Float64()*2 - 1) * 100
			norm += xs[i] * xs[i]
		}
		if norm > 0 {
			return reflect.ValueOf(nonZeroVec{V: xs})
		}
	}
}

// vecPair carries two non-zero vectors of the same dimension.
type vecPair struct {
	A, B []float64
}

func (vecPair) Generate(r *rand.Rand, _ int) reflect.Value {
	n := r.Intn(7) + 1
	mk := func() []float64 {
		for {
			xs := make([]float64, n)
			var norm float64
			for i := range xs {
				xs[i] = (r.Float64()*2 - 1) * 100
				norm += xs[i] * xs[i]
			}
			if norm > 0 {
				return xs
			}
		}
	}
	return reflect.ValueOf(vecPair{A: mk(), B: mk()})
}

func TestProperty_CosineIdentity(t *testing.T) {
	op := cosine.Op{}
	f := func(v nonZeroVec) bool {
		got, err := op.Eval(vec(v.V...), vec(v.V...))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		return math.Abs(got) <= cosineEps
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_CosineNonNegativity(t *testing.T) {
	op := cosine.Op{}
	f := func(p vecPair) bool {
		got, err := op.Eval(vec(p.A...), vec(p.B...))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		// Allow tiny negative float drift below zero.
		return got >= -cosineEps
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_CosineSymmetry(t *testing.T) {
	op := cosine.Op{}
	f := func(p vecPair) bool {
		ab, err := op.Eval(vec(p.A...), vec(p.B...))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		ba, err := op.Eval(vec(p.B...), vec(p.A...))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		return math.Abs(ab-ba) <= cosineEps
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_CosineUpperBound(t *testing.T) {
	op := cosine.Op{}
	f := func(p vecPair) bool {
		got, err := op.Eval(vec(p.A...), vec(p.B...))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		return got <= 2+cosineEps
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}
