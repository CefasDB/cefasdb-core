// Package spatial implements the geohash and Z-order (Morton) encoders
// that back cefas's multidimensional indexes. Encoders are pure — they
// take floats and produce bytes; the storage layer is responsible for
// composing those bytes with table-namespaced prefixes.
package spatial

import (
	"errors"
	"fmt"
	"math"
)

// Geohash represents a base32-encoded geohash. The empty string is the
// zero value and represents "no location".
type Geohash string

const (
	// geohashAlphabet is the 32-character base32 used by the original
	// geohash spec (no a, i, l, o to reduce ambiguity).
	geohashAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz"

	// MinPrecision and MaxPrecision bound the character length the
	// encoder accepts. 1 char ≈ 5000 km cells; 12 char ≈ ±3.7 cm.
	MinPrecision = 1
	MaxPrecision = 12
)

// BBox is an axis-aligned latitude/longitude bounding box. Latitudes
// are in degrees, longitudes in degrees. The convention is min ≤ max
// for both axes; callers crossing the antimeridian should split the
// query into two boxes upstream.
type BBox struct {
	MinLat, MinLon float64
	MaxLat, MaxLon float64
}

// Valid reports whether the box has sensible bounds.
func (b BBox) Valid() bool {
	return b.MinLat <= b.MaxLat && b.MinLon <= b.MaxLon &&
		b.MinLat >= -90 && b.MaxLat <= 90 &&
		b.MinLon >= -180 && b.MaxLon <= 180
}

// Contains reports whether (lat, lon) is inside the box (boundary
// inclusive).
func (b BBox) Contains(lat, lon float64) bool {
	return lat >= b.MinLat && lat <= b.MaxLat &&
		lon >= b.MinLon && lon <= b.MaxLon
}

// ErrInvalidGeohash is returned when an encoded geohash contains a
// character outside the standard alphabet.
var ErrInvalidGeohash = errors.New("spatial: invalid geohash character")

// reverseAlphabet maps each rune in geohashAlphabet back to its
// 5-bit value. -1 marks an invalid character. Indexed by byte (ASCII).
var reverseAlphabet = func() [256]int8 {
	var r [256]int8
	for i := range r {
		r[i] = -1
	}
	for i, c := range geohashAlphabet {
		r[c] = int8(i)
	}
	return r
}()

// EncodeGeohash returns the geohash for (lat, lon) at the requested
// precision. Latitude must lie in [-90, 90], longitude in [-180, 180];
// values outside those ranges are clamped to the nearest valid edge
// rather than returning an error — same convention every embedded geo
// library follows because callers typically derive coordinates from
// noisy sensor data.
func EncodeGeohash(lat, lon float64, precision int) (Geohash, error) {
	if precision < MinPrecision || precision > MaxPrecision {
		return "", fmt.Errorf("spatial: precision %d out of range [%d, %d]", precision, MinPrecision, MaxPrecision)
	}
	lat = clamp(lat, -90, 90)
	lon = clamp(lon, -180, 180)

	latLo, latHi := -90.0, 90.0
	lonLo, lonHi := -180.0, 180.0

	buf := make([]byte, precision)
	bitsTotal := precision * 5

	// Geohash interleaves longitude/latitude bits starting with
	// longitude. We accumulate 5 bits at a time and emit a char.
	var ch byte
	var bitsInChar int
	var charsWritten int
	evenBit := true // true → next bit is longitude

	for i := 0; i < bitsTotal; i++ {
		var bit byte
		if evenBit {
			mid := (lonLo + lonHi) / 2
			if lon >= mid {
				bit = 1
				lonLo = mid
			} else {
				lonHi = mid
			}
		} else {
			mid := (latLo + latHi) / 2
			if lat >= mid {
				bit = 1
				latLo = mid
			} else {
				latHi = mid
			}
		}
		ch = (ch << 1) | bit
		bitsInChar++
		if bitsInChar == 5 {
			buf[charsWritten] = geohashAlphabet[ch]
			charsWritten++
			ch = 0
			bitsInChar = 0
		}
		evenBit = !evenBit
	}
	return Geohash(buf), nil
}

// DecodeGeohash returns the bounding box that the geohash addresses.
// All points whose true geohash at this precision matches h fall
// inside the returned box.
func DecodeGeohash(h Geohash) (BBox, error) {
	if len(h) == 0 {
		return BBox{}, fmt.Errorf("spatial: empty geohash")
	}
	latLo, latHi := -90.0, 90.0
	lonLo, lonHi := -180.0, 180.0
	evenBit := true

	for i := 0; i < len(h); i++ {
		v := reverseAlphabet[h[i]]
		if v < 0 {
			return BBox{}, fmt.Errorf("%w: %q", ErrInvalidGeohash, h[i])
		}
		for b := 4; b >= 0; b-- {
			bit := (v >> b) & 1
			if evenBit {
				mid := (lonLo + lonHi) / 2
				if bit == 1 {
					lonLo = mid
				} else {
					lonHi = mid
				}
			} else {
				mid := (latLo + latHi) / 2
				if bit == 1 {
					latLo = mid
				} else {
					latHi = mid
				}
			}
			evenBit = !evenBit
		}
	}
	return BBox{MinLat: latLo, MinLon: lonLo, MaxLat: latHi, MaxLon: lonHi}, nil
}

// MaxCoverCells caps the number of geohash prefixes Cover can return.
// Callers that hit this limit are using too high a precision for the
// query box and should drop precision or split the query. The cap
// exists to keep query latency bounded.
const MaxCoverCells = 50_000

// ErrCoverTooLarge is returned by CoverBBox when the query box would
// require more than MaxCoverCells prefixes at the requested precision.
// Callers typically respond by retrying with precision-1.
var ErrCoverTooLarge = errors.New("spatial: cover exceeds cell budget at this precision")

// CoverBBox returns the set of geohash prefixes at `precision` that
// covers `box`. The cover is over-approximate — callers should post-
// filter with BBox.Contains on decoded item coordinates.
//
// Implementation walks the grid cell-by-cell: start at the box's
// south-west corner, encode that geohash, decode the cell to read its
// real extent, and step to the next cell along longitude. After
// exhausting a longitude row, jump to the next row using the cell at
// the start of that row. This visits each cell exactly once, so the
// runtime is O(cells_in_box) — the inverse of the earlier
// "uniform sub-cell stride" approach which over-iterated by a factor
// of cell_size / step_size.
//
// Returns ErrCoverTooLarge if the cell count would exceed
// MaxCoverCells; that lets callers fail fast rather than allocate a
// multi-million-entry map for an unreasonable query.
func CoverBBox(box BBox, precision int) ([]Geohash, error) {
	if !box.Valid() {
		return nil, fmt.Errorf("spatial: invalid bbox %+v", box)
	}
	if precision < MinPrecision || precision > MaxPrecision {
		return nil, fmt.Errorf("spatial: precision %d out of range", precision)
	}

	// Cheap upper-bound estimate via centroid cell size so we can
	// reject ridiculous queries before allocating anything.
	centerLat := (box.MinLat + box.MaxLat) / 2
	centerLon := (box.MinLon + box.MaxLon) / 2
	probe, err := EncodeGeohash(centerLat, centerLon, precision)
	if err != nil {
		return nil, err
	}
	probeBox, err := DecodeGeohash(probe)
	if err != nil {
		return nil, err
	}
	latCellSize := probeBox.MaxLat - probeBox.MinLat
	lonCellSize := probeBox.MaxLon - probeBox.MinLon
	if latCellSize <= 0 || lonCellSize <= 0 {
		return nil, fmt.Errorf("spatial: degenerate cell at precision %d", precision)
	}
	estimate := ((box.MaxLat - box.MinLat) / latCellSize) * ((box.MaxLon - box.MinLon) / lonCellSize)
	if estimate > float64(MaxCoverCells) {
		return nil, fmt.Errorf("%w: estimate=%.0f, budget=%d", ErrCoverTooLarge, estimate, MaxCoverCells)
	}

	seen := make(map[Geohash]struct{})
	cells := make([]Geohash, 0, int(estimate)+8)

	// Outer loop: walk rows of latitude.
	const epsilon = 1e-12
	lat := box.MinLat
	for lat <= box.MaxLat+epsilon {
		// Encode the row's south-west cell to learn the row's
		// vertical extent.
		rowHash, err := EncodeGeohash(lat, box.MinLon, precision)
		if err != nil {
			return nil, err
		}
		rowCell, err := DecodeGeohash(rowHash)
		if err != nil {
			return nil, err
		}

		lon := box.MinLon
		for lon <= box.MaxLon+epsilon {
			h, err := EncodeGeohash(lat, lon, precision)
			if err != nil {
				return nil, err
			}
			if _, ok := seen[h]; !ok {
				seen[h] = struct{}{}
				cells = append(cells, h)
				if len(cells) > MaxCoverCells {
					return nil, ErrCoverTooLarge
				}
			}
			cell, err := DecodeGeohash(h)
			if err != nil {
				return nil, err
			}
			next := cell.MaxLon + epsilon
			if next <= lon { // float safety
				next = lon + epsilon
			}
			lon = next
		}

		next := rowCell.MaxLat + epsilon
		if next <= lat {
			next = lat + epsilon
		}
		lat = next
	}
	return cells, nil
}

// HaversineMeters returns the great-circle distance between two
// lat/lon points in meters, using the haversine formula. Used by the
// radius-query post-filter.
func HaversineMeters(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusM = 6371000.0
	toRad := math.Pi / 180.0
	dLat := (lat2 - lat1) * toRad
	dLon := (lon2 - lon1) * toRad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*toRad)*math.Cos(lat2*toRad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusM * math.Asin(math.Sqrt(a))
}

// BBoxAround returns the smallest bounding box that contains the
// disk of radius `meters` centred at (lat, lon). Used by radius
// queries to feed the cover algorithm.
func BBoxAround(lat, lon, meters float64) BBox {
	// 1 degree latitude ≈ 111_320 m everywhere.
	const metersPerLatDeg = 111_320.0
	dLat := meters / metersPerLatDeg
	// Longitude degrees shrink with latitude.
	cos := math.Cos(lat * math.Pi / 180)
	if cos < 1e-9 {
		cos = 1e-9
	}
	dLon := meters / (metersPerLatDeg * cos)
	return BBox{
		MinLat: clamp(lat-dLat, -90, 90),
		MaxLat: clamp(lat+dLat, -90, 90),
		MinLon: clamp(lon-dLon, -180, 180),
		MaxLon: clamp(lon+dLon, -180, 180),
	}
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
