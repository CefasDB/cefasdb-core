package spatial

import (
	"encoding/binary"
	"fmt"
	"math"
)

// MaxZDims is the upper bound on the number of dimensions a single
// Z-order key can encode. We cap at 4 because the bit-interleave loop
// below is O(64×dims) and beyond that the keys get unwieldy without
// adding query value over a different index type.
const MaxZDims = 4

// ZRange is a numeric range used to normalize a dimension's value to
// the unsigned domain Z-order requires. Lo and Hi are the inclusive
// bounds the encoder expects for input values; values outside the
// range are clamped, the same convention the geohash encoder uses for
// out-of-bounds coordinates.
type ZRange struct {
	Lo, Hi float64
}

// Normalize maps v from [Lo, Hi] to a uint32 in [0, 1<<32 - 1]. We
// use 32 bits per dimension so up to 4 dims fit in one 128-bit Morton
// key — enough resolution for any real-world coordinate without
// blowing up the key length.
func (r ZRange) Normalize(v float64) uint32 {
	if r.Hi <= r.Lo {
		return 0
	}
	if v <= r.Lo {
		return 0
	}
	if v >= r.Hi {
		return math.MaxUint32
	}
	frac := (v - r.Lo) / (r.Hi - r.Lo)
	return uint32(frac * float64(math.MaxUint32))
}

// Denormalize is the inverse of Normalize — restores a uint32 cell
// index to a float roughly in [Lo, Hi]. Useful for tests and for the
// post-filter when a query needs to decode a key back to coordinates.
func (r ZRange) Denormalize(u uint32) float64 {
	if r.Hi <= r.Lo {
		return r.Lo
	}
	return r.Lo + float64(u)/float64(math.MaxUint32)*(r.Hi-r.Lo)
}

// EncodeMorton interleaves the bits of `dims` into a single byte slice
// of length (4 × len(dims)). The most significant bit of dimension 0
// ends up as the most significant bit of the output, so range scans
// over the byte slice naturally walk Z-order space.
//
// Returns an error if dims is empty or longer than MaxZDims.
func EncodeMorton(dims []uint32) ([]byte, error) {
	if len(dims) == 0 {
		return nil, fmt.Errorf("spatial: EncodeMorton requires at least one dim")
	}
	if len(dims) > MaxZDims {
		return nil, fmt.Errorf("spatial: %d dims exceeds MaxZDims=%d", len(dims), MaxZDims)
	}

	totalBits := 32 * len(dims)
	out := make([]byte, totalBits/8)

	// Walk MSB-first: for bit position p (0 = LSB, 31 = MSB) across
	// each dim, we emit dim[0].bit(p), dim[1].bit(p), ..., dim[D-1].bit(p)
	// in order of decreasing p. The output bit position is
	// (31 - p) * D + d.
	d := len(dims)
	for p := 31; p >= 0; p-- {
		base := (31 - p) * d
		for di, dim := range dims {
			bit := (dim >> uint(p)) & 1
			outBit := base + di
			byteIdx := outBit / 8
			bitInByte := 7 - (outBit % 8)
			if bit == 1 {
				out[byteIdx] |= 1 << uint(bitInByte)
			}
		}
	}
	return out, nil
}

// DecodeMorton reverses EncodeMorton. The caller must pass the same
// dimension count `d` it used to encode.
func DecodeMorton(key []byte, d int) ([]uint32, error) {
	if d <= 0 || d > MaxZDims {
		return nil, fmt.Errorf("spatial: bad dim count %d", d)
	}
	expected := 32 * d / 8
	if len(key) != expected {
		return nil, fmt.Errorf("spatial: key length %d does not match %d dims (want %d)", len(key), d, expected)
	}
	out := make([]uint32, d)
	for p := 31; p >= 0; p-- {
		base := (31 - p) * d
		for di := 0; di < d; di++ {
			inBit := base + di
			byteIdx := inBit / 8
			bitInByte := 7 - (inBit % 8)
			bit := (key[byteIdx] >> uint(bitInByte)) & 1
			if bit == 1 {
				out[di] |= 1 << uint(p)
			}
		}
	}
	return out, nil
}

// ZBBox is the bounding box (inclusive) used by Z-order range queries.
// All slices must have len = number of dims.
type ZBBox struct {
	Lo []uint32
	Hi []uint32
}

// Valid reports whether the box has consistent dimensionality and
// non-inverted bounds.
func (b ZBBox) Valid() bool {
	if len(b.Lo) == 0 || len(b.Lo) != len(b.Hi) {
		return false
	}
	for i := range b.Lo {
		if b.Lo[i] > b.Hi[i] {
			return false
		}
	}
	return true
}

// Contains reports whether the supplied point falls inside the box on
// every dimension.
func (b ZBBox) Contains(point []uint32) bool {
	if len(point) != len(b.Lo) {
		return false
	}
	for i, v := range point {
		if v < b.Lo[i] || v > b.Hi[i] {
			return false
		}
	}
	return true
}

// MortonRange returns the inclusive [low, high] Morton-encoded byte
// keys for a Z-order range query over `box`. The caller iterates
// Pebble in that range and filters out false positives with
// ZBBox.Contains — Z-order is approximate by design and the encoded
// range contains points outside the box.
//
// A more efficient implementation would decompose the box into
// disjoint Morton subranges via the standard litMax/bigMin algorithm;
// that's a future optimization that does not change the public API.
func MortonRange(box ZBBox) (low, high []byte, err error) {
	if !box.Valid() {
		return nil, nil, fmt.Errorf("spatial: invalid ZBBox %+v", box)
	}
	low, err = EncodeMorton(box.Lo)
	if err != nil {
		return nil, nil, err
	}
	high, err = EncodeMorton(box.Hi)
	if err != nil {
		return nil, nil, err
	}
	return low, high, nil
}

// _ ensures encoding/binary is kept for future range-decomposition
// work without churning imports later.
var _ = binary.BigEndian
