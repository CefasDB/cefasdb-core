package spatial

import (
	"bytes"
	"testing"
)

func TestMortonRoundTrip(t *testing.T) {
	cases := [][]uint32{
		{0, 0},
		{1, 0},
		{0, 1},
		{0xffffffff, 0xffffffff},
		{0xdeadbeef, 0x12345678},
		{1, 2, 3, 4},
		{42},
	}
	for _, c := range cases {
		enc, err := EncodeMorton(c)
		if err != nil {
			t.Fatalf("encode %v: %v", c, err)
		}
		dec, err := DecodeMorton(enc, len(c))
		if err != nil {
			t.Fatalf("decode %v: %v", c, err)
		}
		if len(dec) != len(c) {
			t.Fatalf("len mismatch %d != %d", len(dec), len(c))
		}
		for i := range c {
			if dec[i] != c[i] {
				t.Errorf("dim %d: %d != %d", i, dec[i], c[i])
			}
		}
	}
}

func TestMortonOrderingPreservesPrefix(t *testing.T) {
	// Points sharing a high-bit prefix should sort together: encode a
	// few points and verify the small "near origin" cluster comes
	// before the "far from origin" cluster.
	low, _ := EncodeMorton([]uint32{0, 0})
	mid, _ := EncodeMorton([]uint32{1 << 30, 1 << 30})
	high, _ := EncodeMorton([]uint32{0xffffffff, 0xffffffff})
	if !(bytes.Compare(low, mid) < 0 && bytes.Compare(mid, high) < 0) {
		t.Fatalf("Morton ordering broken: low=%x mid=%x high=%x", low, mid, high)
	}
}

func TestZRangeNormalize(t *testing.T) {
	r := ZRange{Lo: -90, Hi: 90}
	if r.Normalize(-90) != 0 {
		t.Errorf("Lo not zero")
	}
	if r.Normalize(90) != 0xffffffff {
		t.Errorf("Hi not max")
	}
	mid := r.Normalize(0)
	if mid < 0x7fff0000 || mid > 0x80010000 {
		t.Errorf("mid normalize = %x, want ~middle", mid)
	}
}

func TestMortonDimMismatch(t *testing.T) {
	enc, err := EncodeMorton([]uint32{1, 2, 3})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if _, err := DecodeMorton(enc, 2); err == nil {
		t.Errorf("expected error decoding 3-dim key as 2-dim")
	}
}

func TestZBBoxContains(t *testing.T) {
	box := ZBBox{Lo: []uint32{10, 20}, Hi: []uint32{30, 40}}
	if !box.Contains([]uint32{15, 25}) {
		t.Error("inside point not contained")
	}
	if box.Contains([]uint32{5, 25}) {
		t.Error("outside point reported contained")
	}
	if box.Contains([]uint32{15}) {
		t.Error("dim-mismatched point reported contained")
	}
}
