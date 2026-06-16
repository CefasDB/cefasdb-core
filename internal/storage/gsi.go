package storage

import (
	"fmt"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

// IndexOp is one mutation against a secondary-index keyspace. The
// storage writer translates these into pebble.Batch operations in the
// same batch as the primary write — that's what gives cefas all-or-
// nothing semantics across primary + indexes.
type IndexOp struct {
	Op    IndexOpKind
	Key   []byte
	Value []byte
}

type IndexOpKind uint8

const (
	IndexOpSet IndexOpKind = iota + 1
	IndexOpDelete
)

// planGSI returns the index operations required to move every GSI on
// `table` from `prior` to `next`. Either side may be nil:
//
//   - prior == nil, next != nil: a fresh insert. Each GSI emits OpSet.
//   - prior != nil, next == nil: a delete. Each GSI emits OpDelete.
//   - both != nil: an update. Per-index, key-equal items emit nothing;
//     diverged keys emit OpDelete(old) + OpSet(new).
//
// Sparse semantics: an item lacking the GSI's PK (or SK, when present)
// is simply not indexed — same behaviour as DynamoDB sparse indexes.
func PlanGSI(
	table string,
	ks types.KeySchema,
	gsis []types.GSIDescriptor,
	prior, next types.Item,
) ([]IndexOp, error) {
	if len(gsis) == 0 {
		return nil, nil
	}
	ops := make([]IndexOp, 0, len(gsis)*2)

	for _, g := range gsis {
		if g.KeySchema.PK == "" {
			return nil, fmt.Errorf("gsi %q: missing PK attribute name", g.Name)
		}
		priorKey, _, err := gsiEntry(table, g, ks, prior)
		if err != nil {
			return nil, fmt.Errorf("gsi %q (prior): %w", g.Name, err)
		}
		nextKey, nextVal, err := gsiEntry(table, g, ks, next)
		if err != nil {
			return nil, fmt.Errorf("gsi %q (next): %w", g.Name, err)
		}

		if priorKey != nil && nextKey != nil && BytesEqual(priorKey, nextKey) {
			// Identical pointer — the value is derived purely from the
			// primary key bytes (which don't change for a given item),
			// so byte-equal keys imply byte-equal values. Nothing to
			// do.
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

func gsiEntry(
	table string,
	g types.GSIDescriptor,
	ks types.KeySchema,
	item types.Item,
) ([]byte, []byte, error) {
	if item == nil {
		return nil, nil, nil
	}

	gsiPKAttr, ok := item[g.KeySchema.PK]
	if !ok {
		// Sparse: item not indexed by this GSI.
		return nil, nil, nil
	}
	gsiPKBytes, err := AttrCanonicalBytes(gsiPKAttr)
	if err != nil {
		return nil, nil, fmt.Errorf("gsi PK %q: %w", g.KeySchema.PK, err)
	}

	var gsiSKBytes []byte
	if g.KeySchema.SK != "" {
		skAttr, ok := item[g.KeySchema.SK]
		if !ok {
			return nil, nil, nil
		}
		gsiSKBytes, err = AttrCanonicalBytes(skAttr)
		if err != nil {
			return nil, nil, fmt.Errorf("gsi SK %q: %w", g.KeySchema.SK, err)
		}
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

	key := KeyGSI(table, g.Name, gsiPKBytes, gsiSKBytes, primaryPKBytes, primarySKBytes)
	val := buildIndexPointer(item, primaryPKBytes, primarySKBytes, g.Projection)
	return key, val, nil
}

func BytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
