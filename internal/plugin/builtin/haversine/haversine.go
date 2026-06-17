// Package haversine is the great-circle-distance operator plugin
// over (lat, lon) attribute maps. Returns meters using the WGS-84
// mean Earth radius. Adequate for ads / geo workloads at radii up to
// a few hundred kilometers; for cm-precision use a Vincenty plugin
// (out of scope).
package haversine

import (
	"fmt"
	"math"
	"strconv"

	"github.com/CefasDb/cefasdb/pkg/core/model"
	"github.com/CefasDb/cefasdb/pkg/plugin"
)

// EarthRadiusMeters is the WGS-84 mean Earth radius.
const EarthRadiusMeters = 6_371_008.8

type Op struct{}

func (Op) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "haversine",
		Kind:        plugin.KindDistance,
		Version:     "1",
		Description: "Haversine great-circle distance (meters) over {lat,lon} maps",
	}
}

func (Op) Name() string { return "haversine" }

func (Op) Supports(a, b model.AttrType) bool {
	return a == model.AttrM && b == model.AttrM
}

func (Op) Eval(a, b model.AttributeValue) (float64, error) {
	la1, lo1, err := latLon(a, "a")
	if err != nil {
		return 0, err
	}
	la2, lo2, err := latLon(b, "b")
	if err != nil {
		return 0, err
	}
	return Distance(la1, lo1, la2, lo2), nil
}

// Distance returns the great-circle distance in meters between two
// (lat, lon) pairs in degrees.
func Distance(lat1, lon1, lat2, lon2 float64) float64 {
	rLat1 := lat1 * math.Pi / 180
	rLat2 := lat2 * math.Pi / 180
	dLat := (lat2 - lat1) * math.Pi / 180
	dLon := (lon2 - lon1) * math.Pi / 180
	s1 := math.Sin(dLat / 2)
	s2 := math.Sin(dLon / 2)
	a := s1*s1 + math.Cos(rLat1)*math.Cos(rLat2)*s2*s2
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
	return EarthRadiusMeters * c
}

func latLon(av model.AttributeValue, label string) (float64, float64, error) {
	if av.T != model.AttrM {
		return 0, 0, fmt.Errorf("haversine: %s must be a map (got %v)", label, av.T)
	}
	lat, err := pickNum(av.M, "lat", label)
	if err != nil {
		return 0, 0, err
	}
	lon, err := pickNum(av.M, "lon", label)
	if err != nil {
		return 0, 0, err
	}
	if lat < -90 || lat > 90 {
		return 0, 0, fmt.Errorf("haversine: %s.lat %g out of range [-90, 90]", label, lat)
	}
	if lon < -180 || lon > 180 {
		return 0, 0, fmt.Errorf("haversine: %s.lon %g out of range [-180, 180]", label, lon)
	}
	return lat, lon, nil
}

func pickNum(m map[string]model.AttributeValue, field, label string) (float64, error) {
	v, ok := m[field]
	if !ok {
		return 0, fmt.Errorf("haversine: %s missing %q", label, field)
	}
	if v.T != model.AttrN {
		return 0, fmt.Errorf("haversine: %s.%s must be N (got %v)", label, field, v.T)
	}
	f, err := strconv.ParseFloat(v.N, 64)
	if err != nil {
		return 0, fmt.Errorf("haversine: parse %s.%s %q: %w", label, field, v.N, err)
	}
	return f, nil
}

func init() { plugin.Default.MustRegister(Op{}) }
