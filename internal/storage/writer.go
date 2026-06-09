package storage

import (
	"fmt"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

// extractKeyBytes pulls the canonical serialized PK and SK bytes from an
// Item for a given KeySchema. SK is allowed to be empty when the schema
// has no SK; otherwise the attribute must be present.
func extractKeyBytes(item types.Item, ks types.KeySchema) (pk, sk []byte, err error) {
	pkAttr, ok := item[ks.PK]
	if !ok {
		return nil, nil, fmt.Errorf("%w: PK %q", types.ErrMissingKey, ks.PK)
	}
	pk, err = AttrCanonicalBytes(pkAttr)
	if err != nil {
		return nil, nil, fmt.Errorf("PK %q: %w", ks.PK, err)
	}
	if ks.SK == "" {
		return pk, nil, nil
	}
	skAttr, ok := item[ks.SK]
	if !ok {
		return nil, nil, fmt.Errorf("%w: SK %q", types.ErrMissingKey, ks.SK)
	}
	sk, err = AttrCanonicalBytes(skAttr)
	if err != nil {
		return nil, nil, fmt.Errorf("SK %q: %w", ks.SK, err)
	}
	return pk, sk, nil
}

// PutItem writes (or overwrites) an item in the table. Atomic at the
// Pebble batch level; in Raft mode (Phase 4) the same batch is what gets
// proposed to the log.
func (d *DB) PutItem(table string, ks types.KeySchema, item types.Item) error {
	pk, sk, err := extractKeyBytes(item, ks)
	if err != nil {
		return err
	}
	encoded, err := EncodeItem(item)
	if err != nil {
		return fmt.Errorf("encode item: %w", err)
	}

	b := d.Batch()
	defer b.Close()
	if err := b.Set(KeyPrimary(table, pk, sk), encoded, nil); err != nil {
		return fmt.Errorf("batch set: %w", err)
	}
	// GSI / spatial / TTL writes hook in here in later phases. Keeping
	// the whole write in one batch is what gives us all-or-nothing
	// semantics across the primary + indexes.
	return d.CommitBatch(b)
}

// DeleteItem removes an item identified by its PK (+ SK) attribute
// values. attrs is a partial item containing only the key attributes.
func (d *DB) DeleteItem(table string, ks types.KeySchema, keyAttrs types.Item) error {
	pk, sk, err := extractKeyBytes(keyAttrs, ks)
	if err != nil {
		return err
	}
	b := d.Batch()
	defer b.Close()
	if err := b.Delete(KeyPrimary(table, pk, sk), nil); err != nil {
		return fmt.Errorf("batch delete: %w", err)
	}
	return d.CommitBatch(b)
}

// GetItem loads an item by its key attributes. Returns ErrItemNotFound
// when the key is absent.
func (d *DB) GetItem(table string, ks types.KeySchema, keyAttrs types.Item) (types.Item, error) {
	pk, sk, err := extractKeyBytes(keyAttrs, ks)
	if err != nil {
		return nil, err
	}
	v, err := d.Get(KeyPrimary(table, pk, sk))
	if err == ErrNotFound {
		return nil, types.ErrItemNotFound
	}
	if err != nil {
		return nil, err
	}
	return DecodeItem(v)
}

// QueryByPK returns every item under a single PK. SK ordering is
// determined by the underlying byte ordering of the canonical SK bytes
// (lexicographic). Limit ≤ 0 means "no limit".
func (d *DB) QueryByPK(table string, ks types.KeySchema, pkAttr types.AttributeValue, limit int) ([]types.Item, error) {
	pk, err := AttrCanonicalBytes(pkAttr)
	if err != nil {
		return nil, fmt.Errorf("PK %q: %w", ks.PK, err)
	}
	lower, upper := PrefixPrimaryByPK(table, pk)
	return d.scan(lower, upper, limit)
}

// QueryByPKRange constrains SK to [skLow, skHigh). Either bound may be
// the zero value of AttributeValue to leave it open on that side.
func (d *DB) QueryByPKRange(table string, ks types.KeySchema, pkAttr, skLow, skHigh types.AttributeValue, limit int) ([]types.Item, error) {
	if ks.SK == "" {
		return nil, fmt.Errorf("table %q has no sort key", table)
	}
	pk, err := AttrCanonicalBytes(pkAttr)
	if err != nil {
		return nil, fmt.Errorf("PK %q: %w", ks.PK, err)
	}
	var lowBytes, highBytes []byte
	if skLow.T != types.AttrNull {
		lowBytes, err = AttrCanonicalBytes(skLow)
		if err != nil {
			return nil, fmt.Errorf("SK low: %w", err)
		}
	}
	if skHigh.T != types.AttrNull {
		highBytes, err = AttrCanonicalBytes(skHigh)
		if err != nil {
			return nil, fmt.Errorf("SK high: %w", err)
		}
	}
	lower, upper := RangePrimaryBySK(table, pk, lowBytes, highBytes)
	return d.scan(lower, upper, limit)
}

func (d *DB) scan(lower, upper []byte, limit int) ([]types.Item, error) {
	it, err := d.Iter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var out []types.Item
	for valid := it.First(); valid; valid = it.Next() {
		v := it.Value()
		// Copy because pebble reuses the value buffer between Next calls.
		cp := make([]byte, len(v))
		copy(cp, v)
		item, err := DecodeItem(cp)
		if err != nil {
			return nil, fmt.Errorf("decode at key %q: %w", it.Key(), err)
		}
		out = append(out, item)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

// _ ensures pebbledb is referenced even if all writer helpers eventually
// move to db.go — silences an unused-import warning during refactors.
var _ = (*pebbledb.Batch)(nil)
