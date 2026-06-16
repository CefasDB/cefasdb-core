package storage

import "github.com/osvaldoandrade/cefas/internal/spatial"

// SpatialQuery describes a multidimensional read. Exactly one of
// BBox / Radius / Z must be populated.
type SpatialQuery struct {
	// BBox triggers a geohash bounding-box scan.
	BBox *spatial.BBox
	// Radius triggers a geohash radius scan centred at (Lat, Lon)
	// with `Meters` great-circle radius.
	Radius *RadiusQuery
	// Z triggers a Z-order box scan over the same dims as the index.
	Z *spatial.ZBBox
	// Limit ≤ 0 means no limit.
	Limit int
}

// RadiusQuery is the shape consumed by SpatialQuery.Radius.
type RadiusQuery struct {
	Lat, Lon float64
	Meters   float64
}
