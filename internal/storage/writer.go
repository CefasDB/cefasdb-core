package storage

import (
	"errors"
	"fmt"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

// PutOptions controls optional behaviour for PutItem-style writes.
type PutOptions struct {
	// Condition, when non-empty, is evaluated against the prior item
	// before the write. If it returns false the write is aborted with
	// ErrConditionFailed. Empty means "no precondition".
	Condition string
	// Binds resolves :name placeholders in Condition.
	Binds map[string]types.AttributeValue
}

// DeleteOptions mirrors PutOptions for deletes.
type DeleteOptions struct {
	Condition string
	Binds     map[string]types.AttributeValue
}

// QueryOptions configures a Query / QueryByGSI call.
type QueryOptions struct {
	// SKLow / SKHigh, when their type is not AttrNull, constrain the
	// sort key to [SKLow, SKHigh).
	SKLow  types.AttributeValue
	SKHigh types.AttributeValue
	// Limit ≤ 0 means no limit.
	Limit int
}

// BatchOp describes a single mutation inside a BatchWriteItem call.
// Exactly one of Item / Key is set; Op selects which.
type BatchOp struct {
	Op   BatchOpKind
	Item types.Item
	Key  types.Item // for delete: just the key attributes
}

type BatchOpKind uint8

const (
	BatchOpPut BatchOpKind = iota + 1
	BatchOpDelete
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

// PutItem is the convenience wrapper used by code paths that do not
// care about GSIs or conditional writes. It composes a one-shot
// TableDescriptor and delegates to PutItemWith.
func (d *DB) PutItem(table string, ks types.KeySchema, item types.Item) error {
	return d.PutItemWith(types.TableDescriptor{Name: table, KeySchema: ks}, item, PutOptions{})
}

// PutItemWith writes (or overwrites) an item, maintaining every GSI
// declared in td and applying an optional ConditionExpression against
// the prior item.
//
// The whole operation lands in a single pebble.Batch so the primary
// write and every index pointer are atomic with respect to readers and
// (in Phase 4) the Raft log.
func (d *DB) PutItemWith(td types.TableDescriptor, item types.Item, opts PutOptions) error {
	pk, sk, err := extractKeyBytes(item, td.KeySchema)
	if err != nil {
		return err
	}
	encoded, err := EncodeItem(item)
	if err != nil {
		return fmt.Errorf("encode item: %w", err)
	}

	cond, err := ParseCondition(opts.Condition)
	if err != nil {
		return fmt.Errorf("condition: %w", err)
	}

	// Read prior under a snapshot so GSI maintenance and the condition
	// see a consistent view of the row at the moment we plan the
	// write. The snapshot is cheap (it's just a sequence number in
	// Pebble) and we drop it immediately after planning.
	prior, err := d.snapshotGet(KeyPrimary(td.Name, pk, sk))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("read prior: %w", err)
	}
	var priorItem types.Item
	if prior != nil {
		priorItem, err = DecodeItem(prior)
		if err != nil {
			return fmt.Errorf("decode prior: %w", err)
		}
	}

	if !cond.IsZero() {
		ok, err := cond.Evaluate(priorItem, opts.Binds)
		if err != nil {
			return fmt.Errorf("evaluate condition: %w", err)
		}
		if !ok {
			return ErrConditionFailed
		}
	}

	ops, err := planGSI(td.Name, td.KeySchema, td.GSIs, priorItem, item)
	if err != nil {
		return fmt.Errorf("plan gsi: %w", err)
	}

	b := d.Batch()
	defer b.Close()
	if err := b.Set(KeyPrimary(td.Name, pk, sk), encoded, nil); err != nil {
		return fmt.Errorf("batch set primary: %w", err)
	}
	if err := applyIndexOps(b, ops); err != nil {
		return err
	}
	return d.CommitBatch(b)
}

// DeleteItem removes an item identified by its key attributes (no GSI
// maintenance, no condition).
func (d *DB) DeleteItem(table string, ks types.KeySchema, keyAttrs types.Item) error {
	return d.DeleteItemWith(types.TableDescriptor{Name: table, KeySchema: ks}, keyAttrs, DeleteOptions{})
}

// DeleteItemWith removes an item AND every GSI pointer that referenced
// it, atomically. Honors an optional ConditionExpression.
func (d *DB) DeleteItemWith(td types.TableDescriptor, keyAttrs types.Item, opts DeleteOptions) error {
	pk, sk, err := extractKeyBytes(keyAttrs, td.KeySchema)
	if err != nil {
		return err
	}
	cond, err := ParseCondition(opts.Condition)
	if err != nil {
		return fmt.Errorf("condition: %w", err)
	}

	prior, err := d.snapshotGet(KeyPrimary(td.Name, pk, sk))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("read prior: %w", err)
	}
	var priorItem types.Item
	if prior != nil {
		priorItem, err = DecodeItem(prior)
		if err != nil {
			return fmt.Errorf("decode prior: %w", err)
		}
	}

	if !cond.IsZero() {
		ok, err := cond.Evaluate(priorItem, opts.Binds)
		if err != nil {
			return fmt.Errorf("evaluate condition: %w", err)
		}
		if !ok {
			return ErrConditionFailed
		}
	}

	// Plan GSI delta from priorItem → nil.
	ops, err := planGSI(td.Name, td.KeySchema, td.GSIs, priorItem, nil)
	if err != nil {
		return fmt.Errorf("plan gsi: %w", err)
	}

	b := d.Batch()
	defer b.Close()
	if err := b.Delete(KeyPrimary(td.Name, pk, sk), nil); err != nil {
		return fmt.Errorf("batch delete primary: %w", err)
	}
	if err := applyIndexOps(b, ops); err != nil {
		return err
	}
	return d.CommitBatch(b)
}

// snapshotGet performs a single-key read under a Pebble snapshot. The
// snapshot is closed before return so we do not hold a pin on the LSM.
func (d *DB) snapshotGet(key []byte) ([]byte, error) {
	snap := d.db.NewSnapshot()
	defer snap.Close()
	v, closer, err := snap.Get(key)
	if err == pebbledb.ErrNotFound {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(v))
	copy(out, v)
	closer.Close()
	return out, nil
}

func applyIndexOps(b *pebbledb.Batch, ops []indexOp) error {
	for _, op := range ops {
		switch op.op {
		case indexOpSet:
			if err := b.Set(op.key, op.value, nil); err != nil {
				return fmt.Errorf("batch set index: %w", err)
			}
		case indexOpDelete:
			if err := b.Delete(op.key, nil); err != nil {
				return fmt.Errorf("batch delete index: %w", err)
			}
		}
	}
	return nil
}

// GetItem loads an item by its key attributes. Returns ErrItemNotFound
// when the key is absent.
func (d *DB) GetItem(table string, ks types.KeySchema, keyAttrs types.Item) (types.Item, error) {
	pk, sk, err := extractKeyBytes(keyAttrs, ks)
	if err != nil {
		return nil, err
	}
	v, err := d.Get(KeyPrimary(table, pk, sk))
	if errors.Is(err, ErrNotFound) {
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
	return d.scanItems(lower, upper, limit)
}

// QueryByPKRange constrains SK to [skLow, skHigh).
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
	return d.scanItems(lower, upper, limit)
}

// QueryByGSI iterates a GSI partition, resolves pointers, and returns
// the underlying items in the order the index produces them.
func (d *DB) QueryByGSI(td types.TableDescriptor, idxName string, gsiPKVal types.AttributeValue, opts QueryOptions) ([]types.Item, error) {
	descriptor, ok := findGSI(td, idxName)
	if !ok {
		return nil, fmt.Errorf("table %q has no GSI named %q", td.Name, idxName)
	}
	gsiPK, err := AttrCanonicalBytes(gsiPKVal)
	if err != nil {
		return nil, fmt.Errorf("gsi PK: %w", err)
	}
	var lower, upper []byte
	if opts.SKLow.T == types.AttrNull && opts.SKHigh.T == types.AttrNull {
		lower, upper = PrefixGSIByPK(td.Name, idxName, gsiPK)
	} else {
		if descriptor.KeySchema.SK == "" {
			return nil, fmt.Errorf("gsi %q has no sort key", idxName)
		}
		var lo, hi []byte
		if opts.SKLow.T != types.AttrNull {
			lo, err = AttrCanonicalBytes(opts.SKLow)
			if err != nil {
				return nil, fmt.Errorf("gsi SK low: %w", err)
			}
		}
		if opts.SKHigh.T != types.AttrNull {
			hi, err = AttrCanonicalBytes(opts.SKHigh)
			if err != nil {
				return nil, fmt.Errorf("gsi SK high: %w", err)
			}
		}
		lower, upper = RangeGSIBySK(td.Name, idxName, gsiPK, lo, hi)
	}

	it, err := d.Iter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var out []types.Item
	for valid := it.First(); valid; valid = it.Next() {
		v := it.Value()
		ptrCopy := make([]byte, len(v))
		copy(ptrCopy, v)
		primaryPK, primarySK, err := DecodeGSIPointer(ptrCopy)
		if err != nil {
			return nil, fmt.Errorf("decode pointer: %w", err)
		}
		raw, err := d.Get(KeyPrimary(td.Name, primaryPK, primarySK))
		if errors.Is(err, ErrNotFound) {
			// Pointer dangles (would happen mid-rebuild on a Raft
			// follower if it ever de-coupled, but is otherwise an
			// invariant violation). Skip rather than fail the whole
			// query.
			continue
		}
		if err != nil {
			return nil, err
		}
		item, err := DecodeItem(raw)
		if err != nil {
			return nil, fmt.Errorf("decode item: %w", err)
		}
		out = append(out, item)
		if opts.Limit > 0 && len(out) >= opts.Limit {
			break
		}
	}
	return out, it.Error()
}

func findGSI(td types.TableDescriptor, name string) (types.GSIDescriptor, bool) {
	for _, g := range td.GSIs {
		if g.Name == name {
			return g, true
		}
	}
	return types.GSIDescriptor{}, false
}

func (d *DB) scanItems(lower, upper []byte, limit int) ([]types.Item, error) {
	it, err := d.Iter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var out []types.Item
	for valid := it.First(); valid; valid = it.Next() {
		v := it.Value()
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
	return out, it.Error()
}

// BatchWriteItem applies N Put / Delete operations atomically: every op
// commits or none does. GSIs declared on td are maintained inside the
// same batch — this is exactly the read-your-writes consistency
// callers expect from a "batch" operation.
//
// Note: BatchWriteItem reads the prior version of each touched item
// under a single snapshot, so within-batch reordering of writes on the
// same primary key is undefined. Callers should not include two ops
// targeting the same key in a single batch.
func (d *DB) BatchWriteItem(td types.TableDescriptor, ops []BatchOp) error {
	if len(ops) == 0 {
		return nil
	}
	snap := d.db.NewSnapshot()
	defer snap.Close()

	b := d.Batch()
	defer b.Close()

	for i, op := range ops {
		switch op.Op {
		case BatchOpPut:
			pk, sk, err := extractKeyBytes(op.Item, td.KeySchema)
			if err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
			enc, err := EncodeItem(op.Item)
			if err != nil {
				return fmt.Errorf("op %d encode: %w", i, err)
			}
			primaryKey := KeyPrimary(td.Name, pk, sk)
			priorItem, err := readSnapshotItem(snap, primaryKey)
			if err != nil {
				return fmt.Errorf("op %d prior: %w", i, err)
			}
			gsiOps, err := planGSI(td.Name, td.KeySchema, td.GSIs, priorItem, op.Item)
			if err != nil {
				return fmt.Errorf("op %d gsi: %w", i, err)
			}
			if err := b.Set(primaryKey, enc, nil); err != nil {
				return err
			}
			if err := applyIndexOps(b, gsiOps); err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
		case BatchOpDelete:
			pk, sk, err := extractKeyBytes(op.Key, td.KeySchema)
			if err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
			primaryKey := KeyPrimary(td.Name, pk, sk)
			priorItem, err := readSnapshotItem(snap, primaryKey)
			if err != nil {
				return fmt.Errorf("op %d prior: %w", i, err)
			}
			gsiOps, err := planGSI(td.Name, td.KeySchema, td.GSIs, priorItem, nil)
			if err != nil {
				return fmt.Errorf("op %d gsi: %w", i, err)
			}
			if err := b.Delete(primaryKey, nil); err != nil {
				return err
			}
			if err := applyIndexOps(b, gsiOps); err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
		default:
			return fmt.Errorf("op %d: unknown kind %d", i, op.Op)
		}
	}
	return d.CommitBatch(b)
}

// BatchGetItem fetches multiple items by primary key in one pass. The
// result slice has the same length and order as `keys`; missing items
// are represented as nil entries.
func (d *DB) BatchGetItem(table string, ks types.KeySchema, keys []types.Item) ([]types.Item, error) {
	out := make([]types.Item, len(keys))
	for i, ka := range keys {
		item, err := d.GetItem(table, ks, ka)
		if errors.Is(err, types.ErrItemNotFound) {
			out[i] = nil
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("key %d: %w", i, err)
		}
		out[i] = item
	}
	return out, nil
}

func readSnapshotItem(snap *pebbledb.Snapshot, key []byte) (types.Item, error) {
	v, closer, err := snap.Get(key)
	if err == pebbledb.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	cp := make([]byte, len(v))
	copy(cp, v)
	return DecodeItem(cp)
}
