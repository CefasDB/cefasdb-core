package hamming_test

import (
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/CefasDb/cefasdb/pkg/plugin/hamming"
)

// Property tests for the Hamming distance plugin.
//
// Hamming distance counts position-wise differences between two equal-length
// sequences. The implementation supports model.AttrS (rune-wise) and
// model.AttrB (byte-wise); both are covered. Axioms asserted:
//   1. Identity:            d(x, x) == 0.
//   2. Non-negativity:      d(x, y) >= 0.
//   3. Symmetry:            d(x, y) == d(y, x).
//   4. Upper bound:         d(x, y) <= len(x) (byte/rune positions).
//   5. Triangle inequality: d(x, z) <= d(x, y) + d(y, z).

// --- byte generators ------------------------------------------------------

type bytePair struct{ A, B []byte }

func (bytePair) Generate(r *rand.Rand, _ int) reflect.Value {
	n := r.Intn(64) + 1
	a := make([]byte, n)
	b := make([]byte, n)
	r.Read(a)
	r.Read(b)
	return reflect.ValueOf(bytePair{A: a, B: b})
}

type byteTriple struct{ A, B, C []byte }

func (byteTriple) Generate(r *rand.Rand, _ int) reflect.Value {
	n := r.Intn(64) + 1
	a := make([]byte, n)
	b := make([]byte, n)
	c := make([]byte, n)
	r.Read(a)
	r.Read(b)
	r.Read(c)
	return reflect.ValueOf(byteTriple{A: a, B: b, C: c})
}

type byteSingle struct{ V []byte }

func (byteSingle) Generate(r *rand.Rand, _ int) reflect.Value {
	n := r.Intn(64) + 1
	v := make([]byte, n)
	r.Read(v)
	return reflect.ValueOf(byteSingle{V: v})
}

// --- string generators (printable ASCII keeps rune count == byte count) ---

func randRunes(r *rand.Rand, n int) []rune {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
	out := make([]rune, n)
	for i := range out {
		out[i] = rune(alphabet[r.Intn(len(alphabet))])
	}
	return out
}

type stringPair struct{ A, B string }

func (stringPair) Generate(r *rand.Rand, _ int) reflect.Value {
	n := r.Intn(48) + 1
	return reflect.ValueOf(stringPair{
		A: string(randRunes(r, n)),
		B: string(randRunes(r, n)),
	})
}

type stringTriple struct{ A, B, C string }

func (stringTriple) Generate(r *rand.Rand, _ int) reflect.Value {
	n := r.Intn(48) + 1
	return reflect.ValueOf(stringTriple{
		A: string(randRunes(r, n)),
		B: string(randRunes(r, n)),
		C: string(randRunes(r, n)),
	})
}

type stringSingle struct{ V string }

func (stringSingle) Generate(r *rand.Rand, _ int) reflect.Value {
	n := r.Intn(48) + 1
	return reflect.ValueOf(stringSingle{V: string(randRunes(r, n))})
}

// --- properties: bytes ----------------------------------------------------

func TestProperty_HammingBytesIdentity(t *testing.T) {
	op := hamming.Op{}
	f := func(v byteSingle) bool {
		got, err := op.Eval(b(v.V), b(v.V))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		return got == 0
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_HammingBytesNonNegativity(t *testing.T) {
	op := hamming.Op{}
	f := func(p bytePair) bool {
		got, err := op.Eval(b(p.A), b(p.B))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		return got >= 0
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_HammingBytesSymmetry(t *testing.T) {
	op := hamming.Op{}
	f := func(p bytePair) bool {
		ab, err := op.Eval(b(p.A), b(p.B))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		ba, err := op.Eval(b(p.B), b(p.A))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		return ab == ba
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_HammingBytesUpperBound(t *testing.T) {
	op := hamming.Op{}
	f := func(p bytePair) bool {
		got, err := op.Eval(b(p.A), b(p.B))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		return got <= float64(len(p.A))
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_HammingBytesTriangleInequality(t *testing.T) {
	op := hamming.Op{}
	f := func(tr byteTriple) bool {
		dxz, err := op.Eval(b(tr.A), b(tr.C))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		dxy, err := op.Eval(b(tr.A), b(tr.B))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		dyz, err := op.Eval(b(tr.B), b(tr.C))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		return dxz <= dxy+dyz
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

// --- properties: strings (rune-wise) --------------------------------------

func TestProperty_HammingStringsIdentity(t *testing.T) {
	op := hamming.Op{}
	f := func(v stringSingle) bool {
		got, err := op.Eval(s(v.V), s(v.V))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		return got == 0
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_HammingStringsSymmetry(t *testing.T) {
	op := hamming.Op{}
	f := func(p stringPair) bool {
		ab, err := op.Eval(s(p.A), s(p.B))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		ba, err := op.Eval(s(p.B), s(p.A))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		return ab == ba
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_HammingStringsUpperBound(t *testing.T) {
	op := hamming.Op{}
	f := func(p stringPair) bool {
		got, err := op.Eval(s(p.A), s(p.B))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		return got <= float64(len([]rune(p.A)))
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}

func TestProperty_HammingStringsTriangleInequality(t *testing.T) {
	op := hamming.Op{}
	f := func(tr stringTriple) bool {
		dxz, err := op.Eval(s(tr.A), s(tr.C))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		dxy, err := op.Eval(s(tr.A), s(tr.B))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		dyz, err := op.Eval(s(tr.B), s(tr.C))
		if err != nil {
			t.Logf("eval err: %v", err)
			return false
		}
		return dxz <= dxy+dyz
	}
	if err := quick.Check(f, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}
