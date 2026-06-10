package storage

import (
	"fmt"
	"strconv"

	"github.com/osvaldoandrade/cefas/internal/spatial"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

const (
	SpatialKindGeohash = "geohash"
	SpatialKindZorder  = "zorder"
)

// planSpatial returns the index ops required to move every spatial
// index on `table` from prior to next. Sparse semantics match GSIs:
// an item lacking any of the indexed numeric attributes is excluded
// from the index.
func planSpatial(
	table string,
	ks types.KeySchema,
	indexes []types.SpatialIndexDescriptor,
	prior, next types.Item,
) ([]indexOp, error) {
	if len(indexes) == 0 {
		return nil, nil
	}
	ops := make([]indexOp, 0, len(indexes)*2)
	for _, idx := range indexes {
		if err := validateSpatial(idx); err != nil {
			return nil, fmt.Errorf("spatial %q: %w", idx.Name, err)
		}
		priorKey, _, err := spatialEntry(table, idx, ks, prior)
		if err != nil {
			return nil, fmt.Errorf("spatial %q (prior): %w", idx.Name, err)
		}
		nextKey, nextVal, err := spatialEntry(table, idx, ks, next)
		if err != nil {
			return nil, fmt.Errorf("spatial %q (next): %w", idx.Name, err)
		}
		if priorKey != nil && nextKey != nil && bytesEqual(priorKey, nextKey) {
			continue
		}
		if priorKey != nil {
			ops = append(ops, indexOp{op: indexOpDelete, key: priorKey})
		}
		if nextKey != nil {
			ops = append(ops, indexOp{op: indexOpSet, key: nextKey, value: nextVal})
		}
	}
	return ops, nil
}

func validateSpatial(idx types.SpatialIndexDescriptor) error {
	switch idx.Kind {
	case SpatialKindGeohash:
		if len(idx.Attributes) != 2 {
			return fmt.Errorf("%w: geohash needs exactly 2 attributes (lat, lon)", types.ErrInvalidSpatial)
		}
		if idx.Precision < spatial.MinPrecision || idx.Precision > spatial.MaxPrecision {
			return fmt.Errorf("%w: geohash precision out of range [%d, %d]", types.ErrInvalidSpatial, spatial.MinPrecision, spatial.MaxPrecision)
		}
	case SpatialKindZorder:
		if len(idx.Attributes) == 0 || len(idx.Attributes) > spatial.MaxZDims {
			return fmt.Errorf("%w: zorder needs 1..%d attributes, got %d", types.ErrInvalidSpatial, spatial.MaxZDims, len(idx.Attributes))
		}
		if len(idx.Ranges) != len(idx.Attributes) {
			return fmt.Errorf("%w: zorder needs one Range per attribute (have %d ranges, %d attrs)", types.ErrInvalidSpatial, len(idx.Ranges), len(idx.Attributes))
		}
	default:
		return fmt.Errorf("%w: unknown kind %q", types.ErrInvalidSpatial, idx.Kind)
	}
	return nil
}

func spatialEntry(
	table string,
	idx types.SpatialIndexDescriptor,
	ks types.KeySchema,
	item types.Item,
) ([]byte, []byte, error) {
	if item == nil {
		return nil, nil, nil
	}

	primaryPK, primarySK, err := primaryKeyBytes(item, ks)
	if err != nil {
		return nil, nil, err
	}
	val := EncodeGSIPointer(primaryPK, primarySK)

	switch idx.Kind {
	case SpatialKindGeohash:
		lat, ok1, err := numericAttr(item, idx.Attributes[0])
		if err != nil {
			return nil, nil, fmt.Errorf("lat %q: %w", idx.Attributes[0], err)
		}
		lon, ok2, err := numericAttr(item, idx.Attributes[1])
		if err != nil {
			return nil, nil, fmt.Errorf("lon %q: %w", idx.Attributes[1], err)
		}
		if !ok1 || !ok2 {
			return nil, nil, nil // sparse
		}
		h, err := spatial.EncodeGeohash(lat, lon, idx.Precision)
		if err != nil {
			return nil, nil, err
		}
		return KeyGeo(table, idx.Name, string(h), primaryPK, primarySK), val, nil
	case SpatialKindZorder:
		dims := make([]uint32, len(idx.Attributes))
		for i, attr := range idx.Attributes {
			v, ok, err := numericAttr(item, attr)
			if err != nil {
				return nil, nil, fmt.Errorf("dim %q: %w", attr, err)
			}
			if !ok {
				return nil, nil, nil // sparse
			}
			r := spatial.ZRange{Lo: idx.Ranges[i].Lo, Hi: idx.Ranges[i].Hi}
			dims[i] = r.Normalize(v)
		}
		mortonBytes, err := spatial.EncodeMorton(dims)
		if err != nil {
			return nil, nil, err
		}
		return KeyZorder(table, idx.Name, mortonBytes, primaryPK, primarySK), val, nil
	}
	return nil, nil, fmt.Errorf("unknown spatial kind %q", idx.Kind)
}

func primaryKeyBytes(item types.Item, ks types.KeySchema) (pk, sk []byte, err error) {
	pkAttr, ok := item[ks.PK]
	if !ok {
		return nil, nil, fmt.Errorf("primary PK %q missing on item", ks.PK)
	}
	pk, err = AttrCanonicalBytes(pkAttr)
	if err != nil {
		return nil, nil, fmt.Errorf("primary PK %q: %w", ks.PK, err)
	}
	if ks.SK == "" {
		return pk, nil, nil
	}
	skAttr, ok := item[ks.SK]
	if !ok {
		return nil, nil, fmt.Errorf("primary SK %q missing on item", ks.SK)
	}
	sk, err = AttrCanonicalBytes(skAttr)
	if err != nil {
		return nil, nil, fmt.Errorf("primary SK %q: %w", ks.SK, err)
	}
	return pk, sk, nil
}

// numericAttr returns the float64 value of `name` on `item`, true
// for ok if the attribute is present and of a numeric kind (N).
// Missing attribute returns (0, false, nil) — sparse indexing.
func numericAttr(item types.Item, name string) (float64, bool, error) {
	av, ok := item[name]
	if !ok {
		return 0, false, nil
	}
	if av.T != types.AttrN {
		return 0, false, fmt.Errorf("attribute %q is not numeric", name)
	}
	v, err := strconv.ParseFloat(av.N, 64)
	if err != nil {
		return 0, false, fmt.Errorf("attribute %q parse: %w", name, err)
	}
	return v, true, nil
}

// ---------- spatial queries ----------

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

// SpatialQueryItems iterates the requested spatial index and returns
// the underlying items in iteration order. The post-filter (bbox or
// distance) is applied to every candidate to suppress false positives
// from the cover algorithm and from the Z-order range over-estimate.
func (d *DB) SpatialQueryItems(td types.TableDescriptor, idxName string, q SpatialQuery) ([]types.Item, error) {
	descriptor, ok := findSpatial(td, idxName)
	if !ok {
		return nil, fmt.Errorf("%w: %q", types.ErrSpatialNotFound, idxName)
	}
	if err := validateSpatial(descriptor); err != nil {
		return nil, err
	}

	switch descriptor.Kind {
	case SpatialKindGeohash:
		return d.spatialQueryGeohash(td, descriptor, q)
	case SpatialKindZorder:
		return d.spatialQueryZorder(td, descriptor, q)
	}
	return nil, fmt.Errorf("unsupported spatial kind %q", descriptor.Kind)
}

func (d *DB) spatialQueryGeohash(td types.TableDescriptor, idx types.SpatialIndexDescriptor, q SpatialQuery) ([]types.Item, error) {
	var box spatial.BBox
	var filterRadius *RadiusQuery
	switch {
	case q.BBox != nil:
		box = *q.BBox
	case q.Radius != nil:
		filterRadius = q.Radius
		box = spatial.BBoxAround(q.Radius.Lat, q.Radius.Lon, q.Radius.Meters)
	default:
		return nil, fmt.Errorf("geohash query needs BBox or Radius")
	}

	prefixes, err := spatial.CoverBBox(box, idx.Precision)
	if err != nil {
		return nil, err
	}

	// Use a set so the same item is not returned twice when its
	// pointer happens to satisfy multiple cover prefixes (it should
	// not, since each item has exactly one geohash entry, but
	// belt-and-braces).
	seen := make(map[string]struct{})
	var out []types.Item

	for _, p := range prefixes {
		lower, upper := PrefixGeoCell(td.Name, idx.Name, string(p))
		it, err := d.Iter(lower, upper)
		if err != nil {
			return nil, err
		}
		err = iterateAndResolve(it, d, td.Name, &out, seen, func(item types.Item) bool {
			lat, ok1, err := numericAttr(item, idx.Attributes[0])
			if err != nil || !ok1 {
				return false
			}
			lon, ok2, err := numericAttr(item, idx.Attributes[1])
			if err != nil || !ok2 {
				return false
			}
			if filterRadius != nil {
				return spatial.HaversineMeters(filterRadius.Lat, filterRadius.Lon, lat, lon) <= filterRadius.Meters
			}
			return box.Contains(lat, lon)
		}, q.Limit)
		it.Close()
		if err != nil {
			return nil, err
		}
		if q.Limit > 0 && len(out) >= q.Limit {
			break
		}
	}
	return out, nil
}

func (d *DB) spatialQueryZorder(td types.TableDescriptor, idx types.SpatialIndexDescriptor, q SpatialQuery) ([]types.Item, error) {
	if q.Z == nil {
		return nil, fmt.Errorf("zorder query needs Z bbox")
	}
	if len(q.Z.Lo) != len(idx.Attributes) {
		return nil, fmt.Errorf("zorder query dim count %d != index %d", len(q.Z.Lo), len(idx.Attributes))
	}
	low, high, err := spatial.MortonRange(*q.Z)
	if err != nil {
		return nil, err
	}
	lower, upper := RangeZorder(td.Name, idx.Name, low, high)
	it, err := d.Iter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	seen := make(map[string]struct{})
	var out []types.Item
	err = iterateAndResolve(it, d, td.Name, &out, seen, func(item types.Item) bool {
		point := make([]uint32, len(idx.Attributes))
		for i, attr := range idx.Attributes {
			v, ok, err := numericAttr(item, attr)
			if err != nil || !ok {
				return false
			}
			r := spatial.ZRange{Lo: idx.Ranges[i].Lo, Hi: idx.Ranges[i].Hi}
			point[i] = r.Normalize(v)
		}
		return q.Z.Contains(point)
	}, q.Limit)
	return out, err
}

func findSpatial(td types.TableDescriptor, name string) (types.SpatialIndexDescriptor, bool) {
	for _, s := range td.SpatialIndexes {
		if s.Name == name {
			return s, true
		}
	}
	return types.SpatialIndexDescriptor{}, false
}

// iterateAndResolve walks `it`, dereferences each entry's GSI-style
// pointer to load the primary item, applies `accept`, and accumulates
// matches in `out`. Items already in `seen` are skipped.
func iterateAndResolve(
	it iter,
	d *DB,
	table string,
	out *[]types.Item,
	seen map[string]struct{},
	accept func(types.Item) bool,
	limit int,
) error {
	for valid := it.First(); valid; valid = it.Next() {
		v := it.Value()
		ptrCopy := make([]byte, len(v))
		copy(ptrCopy, v)
		primaryPK, primarySK, err := DecodeGSIPointer(ptrCopy)
		if err != nil {
			return fmt.Errorf("decode pointer: %w", err)
		}
		dedupe := string(primaryPK) + "\x00" + string(primarySK)
		if _, ok := seen[dedupe]; ok {
			continue
		}
		raw, err := d.Get(KeyPrimary(table, primaryPK, primarySK))
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			return err
		}
		item, err := DecodeItem(raw)
		if err != nil {
			return fmt.Errorf("decode item: %w", err)
		}
		if !accept(item) {
			continue
		}
		seen[dedupe] = struct{}{}
		*out = append(*out, item)
		if limit > 0 && len(*out) >= limit {
			return nil
		}
	}
	return it.Error()
}

// iter is the minimal iterator interface this file consumes from
// pebble — pulled into its own type to keep iterateAndResolve
// testable.
type iter interface {
	First() bool
	Next() bool
	Value() []byte
	Error() error
}
