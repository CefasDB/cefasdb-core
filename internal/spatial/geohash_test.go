package spatial

import (
	"math"
	"strings"
	"testing"
)

// Golden vectors checked against the original geohash specification —
// the canonical (57.64911, 10.40744) example from the spec, plus the
// commonly-cited Eiffel Tower and Null Island cases. Coordinates that
// have not been independently audited at higher precisions are
// covered by the round-trip / cover tests below.
func TestGeohashGoldenVectors(t *testing.T) {
	cases := []struct {
		lat, lon  float64
		precision int
		want      Geohash
	}{
		// Original geohash.org canonical example.
		{57.64911, 10.40744, 11, "u4pruydqqvj"},
		// Eiffel Tower at the most common precision.
		{48.858260, 2.294495, 7, "u09tunq"},
		// Null island.
		{0, 0, 6, "s00000"},
		// Single-character cells partition the globe into 32 cells of
		// known assignment.
		{45, -90, 1, "f"},
		{-45, 90, 1, "q"},
	}
	for _, c := range cases {
		got, err := EncodeGeohash(c.lat, c.lon, c.precision)
		if err != nil {
			t.Fatalf("Encode(%v): %v", c, err)
		}
		if got != c.want {
			t.Errorf("Encode(%v, %v, %d) = %q, want %q", c.lat, c.lon, c.precision, got, c.want)
		}
	}
}

func TestGeohashRoundTripBox(t *testing.T) {
	lat, lon := 37.4220, -122.0841 // Googleplex.
	h, err := EncodeGeohash(lat, lon, 9)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	box, err := DecodeGeohash(h)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !box.Contains(lat, lon) {
		t.Fatalf("decoded box %+v does not contain (%v, %v)", box, lat, lon)
	}
}

func TestGeohashCoverContainsKnownPoints(t *testing.T) {
	box := BBox{MinLat: 40.74, MinLon: -73.99, MaxLat: 40.76, MaxLon: -73.97}
	prefixes, err := CoverBBox(box, 6)
	if err != nil {
		t.Fatalf("cover: %v", err)
	}
	if len(prefixes) == 0 {
		t.Fatalf("cover returned no prefixes")
	}

	// Every prefix should overlap the original box.
	for _, p := range prefixes {
		cell, err := DecodeGeohash(p)
		if err != nil {
			t.Fatalf("decode %q: %v", p, err)
		}
		if cell.MaxLat < box.MinLat || cell.MinLat > box.MaxLat ||
			cell.MaxLon < box.MinLon || cell.MinLon > box.MaxLon {
			t.Errorf("cover prefix %q has no overlap with bbox", p)
		}
	}

	// A point inside the box must hash to a cell whose prefix is in
	// the cover set.
	inside := []struct{ lat, lon float64 }{
		{40.7500, -73.9800},
		{40.7400, -73.9899},
		{40.7599, -73.9701},
	}
	for _, pt := range inside {
		h, err := EncodeGeohash(pt.lat, pt.lon, 6)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		matched := false
		for _, p := range prefixes {
			if strings.HasPrefix(string(h), string(p)) || strings.HasPrefix(string(p), string(h)) {
				matched = true
				break
			}
		}
		if !matched {
			t.Errorf("point %v (hash %s) not covered by prefixes %v", pt, h, prefixes)
		}
	}
}

func TestGeohashInvalidPrecision(t *testing.T) {
	if _, err := EncodeGeohash(0, 0, 0); err == nil {
		t.Error("expected error for precision=0")
	}
	if _, err := EncodeGeohash(0, 0, 13); err == nil {
		t.Error("expected error for precision=13")
	}
}

func TestHaversineSanity(t *testing.T) {
	// NYC to LAX ≈ 3935 km (great-circle, JFK→LAX coordinates).
	d := HaversineMeters(40.6413, -73.7781, 33.9416, -118.4085)
	if math.Abs(d-3974000) > 100_000 {
		t.Errorf("NYC→LAX distance = %.0f m, want ~3974 km ±100km", d)
	}

	// Same point distance is 0.
	if d := HaversineMeters(1, 2, 1, 2); d != 0 {
		t.Errorf("same-point distance = %v, want 0", d)
	}
}

func TestBBoxAroundCoversCircle(t *testing.T) {
	lat, lon := 40.7128, -74.0060
	radius := 1000.0
	box := BBoxAround(lat, lon, radius)
	if !box.Contains(lat, lon) {
		t.Errorf("center not in derived bbox")
	}
	// A point 500 m due north must be inside the bbox.
	north := lat + (500.0 / 111_320.0)
	if !box.Contains(north, lon) {
		t.Errorf("500m north point not in bbox")
	}
}
