package storage_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/spatial"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func tableWithGeo() types.TableDescriptor {
	return types.TableDescriptor{
		Name:      "places",
		KeySchema: types.KeySchema{PK: "id"},
		SpatialIndexes: []types.SpatialIndexDescriptor{{
			Name:       "by_location",
			Kind:       storage.SpatialKindGeohash,
			Attributes: []string{"lat", "lon"},
			Precision:  6,
		}},
	}
}

func tableWithZorder() types.TableDescriptor {
	return types.TableDescriptor{
		Name:      "fleet",
		KeySchema: types.KeySchema{PK: "id"},
		SpatialIndexes: []types.SpatialIndexDescriptor{{
			Name:       "by_xy",
			Kind:       storage.SpatialKindZorder,
			Attributes: []string{"x", "y"},
			Ranges:     []types.NumRange{{Lo: 0, Hi: 1000}, {Lo: 0, Hi: 1000}},
		}},
	}
}

func TestSpatialGeohashBBox(t *testing.T) {
	db := openTestDB(t)
	td := tableWithGeo()

	put := func(id string, lat, lon float64) {
		t.Helper()
		if err := db.PutItemWith(td, types.Item{
			"id":  sAttr(id),
			"lat": nAttr(fmt.Sprintf("%.6f", lat)),
			"lon": nAttr(fmt.Sprintf("%.6f", lon)),
		}, storage.PutOptions{}); err != nil {
			t.Fatalf("put %s: %v", id, err)
		}
	}
	put("nyc", 40.7128, -74.0060)
	put("brooklyn", 40.6782, -73.9442)
	put("la", 34.0522, -118.2437)
	put("sf", 37.7749, -122.4194)

	// BBox tight around NYC area should match NYC + Brooklyn only.
	box := spatial.BBox{MinLat: 40.6, MinLon: -74.1, MaxLat: 40.8, MaxLon: -73.9}
	items, err := db.SpatialQueryItems(td, "by_location", storage.SpatialQuery{BBox: &box})
	if err != nil {
		t.Fatalf("spatial query: %v", err)
	}
	got := map[string]bool{}
	for _, it := range items {
		got[it["id"].S] = true
	}
	if !got["nyc"] || !got["brooklyn"] {
		t.Fatalf("expected nyc+brooklyn, got %v", got)
	}
	if got["la"] || got["sf"] {
		t.Fatalf("west coast leaked into bbox: %v", got)
	}
}

func TestSpatialGeohashRadius(t *testing.T) {
	db := openTestDB(t)
	td := tableWithGeo()

	// Cluster of points around (37.7749, -122.4194). Radius 2 km
	// should catch the cluster but not the distant cousin.
	_ = db.PutItemWith(td, types.Item{"id": sAttr("near1"), "lat": nAttr("37.7745"), "lon": nAttr("-122.4194")}, storage.PutOptions{})
	_ = db.PutItemWith(td, types.Item{"id": sAttr("near2"), "lat": nAttr("37.7752"), "lon": nAttr("-122.4181")}, storage.PutOptions{})
	_ = db.PutItemWith(td, types.Item{"id": sAttr("far"), "lat": nAttr("37.6"), "lon": nAttr("-122.5")}, storage.PutOptions{})

	items, err := db.SpatialQueryItems(td, "by_location", storage.SpatialQuery{
		Radius: &storage.RadiusQuery{Lat: 37.7749, Lon: -122.4194, Meters: 2000},
	})
	if err != nil {
		t.Fatalf("radius query: %v", err)
	}
	ids := map[string]bool{}
	for _, it := range items {
		ids[it["id"].S] = true
	}
	if !ids["near1"] || !ids["near2"] {
		t.Fatalf("missing near hits: %v", ids)
	}
	if ids["far"] {
		t.Fatalf("far point inside 2km radius: %v", ids)
	}
}

func TestSpatialGeohashUpdateRoutes(t *testing.T) {
	db := openTestDB(t)
	td := tableWithGeo()

	item := types.Item{"id": sAttr("mover"), "lat": nAttr("40.7128"), "lon": nAttr("-74.0060")}
	if err := db.PutItemWith(td, item, storage.PutOptions{}); err != nil {
		t.Fatalf("initial put: %v", err)
	}
	// Move to SF.
	item["lat"] = nAttr("37.7749")
	item["lon"] = nAttr("-122.4194")
	if err := db.PutItemWith(td, item, storage.PutOptions{}); err != nil {
		t.Fatalf("move: %v", err)
	}

	// NYC box should not find the item anymore.
	nyc := spatial.BBox{MinLat: 40.6, MinLon: -74.1, MaxLat: 40.8, MaxLon: -73.9}
	items, _ := db.SpatialQueryItems(td, "by_location", storage.SpatialQuery{BBox: &nyc})
	if len(items) != 0 {
		t.Fatalf("item still indexed under NYC: %+v", items)
	}
	// SF box should.
	sf := spatial.BBox{MinLat: 37.7, MinLon: -122.5, MaxLat: 37.8, MaxLon: -122.4}
	items, _ = db.SpatialQueryItems(td, "by_location", storage.SpatialQuery{BBox: &sf})
	if len(items) != 1 || items[0]["id"].S != "mover" {
		t.Fatalf("SF index missing mover: %+v", items)
	}
}

func TestSpatialGeohashDeleteCleansPointer(t *testing.T) {
	db := openTestDB(t)
	td := tableWithGeo()

	item := types.Item{"id": sAttr("gone"), "lat": nAttr("40.7128"), "lon": nAttr("-74.0060")}
	_ = db.PutItemWith(td, item, storage.PutOptions{})
	if err := db.DeleteItemWith(td, types.Item{"id": sAttr("gone")}, storage.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	nyc := spatial.BBox{MinLat: 40.6, MinLon: -74.1, MaxLat: 40.8, MaxLon: -73.9}
	items, _ := db.SpatialQueryItems(td, "by_location", storage.SpatialQuery{BBox: &nyc})
	if len(items) != 0 {
		t.Fatalf("spatial pointer not cleaned: %+v", items)
	}
}

func TestSpatialSparseIndex(t *testing.T) {
	db := openTestDB(t)
	td := tableWithGeo()

	// Item without coordinates should not appear in any spatial scan.
	if err := db.PutItemWith(td, types.Item{"id": sAttr("ghost"), "other": sAttr("x")}, storage.PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Use a small bbox — a world-wide bbox at precision 6 would
	// exceed MaxCoverCells, which is intentional behaviour.
	bbox := spatial.BBox{MinLat: -1, MinLon: -1, MaxLat: 1, MaxLon: 1}
	items, _ := db.SpatialQueryItems(td, "by_location", storage.SpatialQuery{BBox: &bbox})
	if len(items) != 0 {
		t.Fatalf("sparse item leaked: %+v", items)
	}
}

func TestSpatialCoverBudget(t *testing.T) {
	db := openTestDB(t)
	td := tableWithGeo()

	// A worldwide bbox at high precision should be rejected as too
	// large rather than scanning billions of cells.
	world := spatial.BBox{MinLat: -90, MinLon: -180, MaxLat: 90, MaxLon: 180}
	_, err := db.SpatialQueryItems(td, "by_location", storage.SpatialQuery{BBox: &world})
	if err == nil {
		t.Fatalf("expected ErrCoverTooLarge, got nil")
	}
}

func TestSpatialZorderBox(t *testing.T) {
	db := openTestDB(t)
	td := tableWithZorder()

	for i, p := range [][2]float64{{10, 10}, {20, 20}, {500, 500}, {990, 990}} {
		if err := db.PutItemWith(td, types.Item{
			"id": sAttr(fmt.Sprintf("p%d", i)),
			"x":  nAttr(fmt.Sprintf("%f", p[0])),
			"y":  nAttr(fmt.Sprintf("%f", p[1])),
		}, storage.PutOptions{}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Box covering low-quadrant.
	zr := spatial.ZRange{Lo: 0, Hi: 1000}
	low := []uint32{zr.Normalize(0), zr.Normalize(0)}
	high := []uint32{zr.Normalize(30), zr.Normalize(30)}
	box := &spatial.ZBBox{Lo: low, Hi: high}
	items, err := db.SpatialQueryItems(td, "by_xy", storage.SpatialQuery{Z: box})
	if err != nil {
		t.Fatalf("zorder query: %v", err)
	}
	ids := map[string]bool{}
	for _, it := range items {
		ids[it["id"].S] = true
	}
	if !ids["p0"] || !ids["p1"] {
		t.Fatalf("low cluster missing: %v", ids)
	}
	if ids["p2"] || ids["p3"] {
		t.Fatalf("far points leaked: %v", ids)
	}
}

// Bench / accuracy harness: scatter 5_000 synthetic points in a small
// world, run a bbox query, compare against the brute-force answer.
// Validates that the cover algorithm is sound (no missed matches) and
// the false-positive filter is engaging.
func TestSpatialBruteForceAgreement(t *testing.T) {
	db := openTestDB(t)
	td := tableWithGeo()

	rng := rand.New(rand.NewSource(42))
	const N = 5_000
	type point struct {
		id       string
		lat, lon float64
	}
	all := make([]point, N)
	for i := 0; i < N; i++ {
		p := point{
			id:  fmt.Sprintf("p%d", i),
			lat: 40 + rng.Float64()*2,    // [40, 42)
			lon: -74 + rng.Float64()*2,   // [-74, -72)
		}
		all[i] = p
		if err := db.PutItemWith(td, types.Item{
			"id":  sAttr(p.id),
			"lat": nAttr(fmt.Sprintf("%.6f", p.lat)),
			"lon": nAttr(fmt.Sprintf("%.6f", p.lon)),
		}, storage.PutOptions{}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	box := spatial.BBox{MinLat: 40.5, MinLon: -73.6, MaxLat: 41.0, MaxLon: -73.0}
	want := map[string]bool{}
	for _, p := range all {
		if box.Contains(p.lat, p.lon) {
			want[p.id] = true
		}
	}

	items, err := db.SpatialQueryItems(td, "by_location", storage.SpatialQuery{BBox: &box})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	got := map[string]bool{}
	for _, it := range items {
		got[it["id"].S] = true
	}
	if len(got) != len(want) {
		t.Fatalf("count mismatch: got %d, want %d", len(got), len(want))
	}
	for id := range want {
		if !got[id] {
			t.Fatalf("brute-force hit %s missing from spatial result", id)
		}
	}
}

func BenchmarkSpatialBBoxQuery(b *testing.B) {
	db := openTestDB(b)
	td := tableWithGeo()
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 10_000; i++ {
		_ = db.PutItemWith(td, types.Item{
			"id":  sAttr(fmt.Sprintf("p%d", i)),
			"lat": nAttr(fmt.Sprintf("%.6f", 40+rng.Float64()*2)),
			"lon": nAttr(fmt.Sprintf("%.6f", -74+rng.Float64()*2)),
		}, storage.PutOptions{})
	}
	box := spatial.BBox{MinLat: 40.5, MinLon: -73.6, MaxLat: 41.0, MaxLon: -73.0}
	q := storage.SpatialQuery{BBox: &box}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.SpatialQueryItems(td, "by_location", q); err != nil {
			b.Fatal(err)
		}
	}
}
