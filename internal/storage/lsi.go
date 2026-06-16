package storage

import (
	"fmt"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

// planLSI mirrors planGSI but routes through the primary partition's
// key. Sparse semantics: items missing the LSI SK attribute are
// simply not indexed.
func PlanLSI(
	table string,
	ks types.KeySchema,
	lsis []types.LSIDescriptor,
	prior, next types.Item,
) ([]IndexOp, error) {
	if len(lsis) == 0 {
		return nil, nil
	}
	ops := make([]IndexOp, 0, len(lsis)*2)
	for _, l := range lsis {
		if l.SK == "" {
			return nil, fmt.Errorf("lsi %q: missing SK attribute name", l.Name)
		}
		priorKey, _, err := lsiEntry(table, l, ks, prior)
		if err != nil {
			return nil, fmt.Errorf("lsi %q (prior): %w", l.Name, err)
		}
		nextKey, nextVal, err := lsiEntry(table, l, ks, next)
		if err != nil {
			return nil, fmt.Errorf("lsi %q (next): %w", l.Name, err)
		}
		if priorKey != nil && nextKey != nil && BytesEqual(priorKey, nextKey) {
			continue
		}
		if priorKey != nil {
			ops = append(ops, IndexOp{Op: IndexOpDelete, Key: priorKey})
		}
		if nextKey != nil {
			ops = append(ops, IndexOp{Op: IndexOpSet, Key: nextKey, Value: nextVal})
		}
	}
	return ops, nil
}

func lsiEntry(
	table string,
	l types.LSIDescriptor,
	ks types.KeySchema,
	item types.Item,
) ([]byte, []byte, error) {
	if item == nil {
		return nil, nil, nil
	}
	skAttr, ok := item[l.SK]
	if !ok {
		// Sparse: item not indexed by this LSI.
		return nil, nil, nil
	}
	lsiSKBytes, err := AttrCanonicalBytes(skAttr)
	if err != nil {
		return nil, nil, fmt.Errorf("lsi SK %q: %w", l.SK, err)
	}
	primaryPKAttr, ok := item[ks.PK]
	if !ok {
		return nil, nil, fmt.Errorf("primary PK %q missing on item", ks.PK)
	}
	primaryPKBytes, err := AttrCanonicalBytes(primaryPKAttr)
	if err != nil {
		return nil, nil, fmt.Errorf("primary PK %q: %w", ks.PK, err)
	}
	var primarySKBytes []byte
	if ks.SK != "" {
		skAttr, ok := item[ks.SK]
		if !ok {
			return nil, nil, fmt.Errorf("primary SK %q missing on item", ks.SK)
		}
		primarySKBytes, err = AttrCanonicalBytes(skAttr)
		if err != nil {
			return nil, nil, fmt.Errorf("primary SK %q: %w", ks.SK, err)
		}
	}
	key := KeyLSI(table, l.Name, primaryPKBytes, lsiSKBytes, primarySKBytes)
	val := buildIndexPointer(item, primaryPKBytes, primarySKBytes, l.Projection)
	return key, val, nil
}

// buildIndexPointer returns the value blob stored alongside a
// secondary-index key. Shared between GSI and LSI so projection
// handling stays in one place. KEYS_ONLY is the default and falls
// back to the legacy pointer codec for wire-compatibility.
func buildIndexPointer(item types.Item, primaryPK, primarySK []byte, p types.IndexProjection) []byte {
	switch p.Mode {
	case "INCLUDE":
		subset := make(types.Item, len(p.Include))
		for _, attr := range p.Include {
			if v, ok := item[attr]; ok {
				subset[attr] = v
			}
		}
		enc, err := EncodeItem(subset)
		if err != nil {
			// Encoding cefas attributes never fails on well-formed
			// input; fall back to KEYS_ONLY rather than panicking the
			// write path.
			return EncodeGSIPointer(primaryPK, primarySK)
		}
		return EncodeProjectedPointer(primaryPK, primarySK, "INCLUDE", enc)
	case "ALL":
		enc, err := EncodeItem(item)
		if err != nil {
			return EncodeGSIPointer(primaryPK, primarySK)
		}
		return EncodeProjectedPointer(primaryPK, primarySK, "ALL", enc)
	}
	return EncodeGSIPointer(primaryPK, primarySK)
}
