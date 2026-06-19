package pebble

import (
	"errors"
	"fmt"
	"strings"

	"github.com/CefasDb/cefasdb/internal/storage"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/CefasDb/cefasdb/pkg/types"
)

// PutOptions / DeleteOptions / QueryOptions / BatchOp / BatchOpKind
// and the BatchOp kind constants are aliased here so existing
// `pebble.X` references keep compiling. The canonical declarations
// live in internal/storage so they can be referenced by the
// Reader / Writer / Engine boundary interfaces without forcing
// callers to import the pebble adapter.
type (
	PutOptions    = storage.PutOptions
	DeleteOptions = storage.DeleteOptions
	QueryOptions  = storage.QueryOptions
	BatchOp       = storage.BatchOp
	BatchOpKind   = storage.BatchOpKind
)

const (
	BatchOpPut    = storage.BatchOpPut
	BatchOpDelete = storage.BatchOpDelete
)

// extractKeyBytes pulls the canonical serialized PK and SK bytes from an
// Item for a given KeySchema. SK is allowed to be empty when the schema
// has no SK; otherwise the attribute must be present.
func extractKeyBytes(item types.Item, ks types.KeySchema) (pk, sk []byte, err error) {
	pkAttr, ok := item[ks.PK]
	if !ok {
		return nil, nil, fmt.Errorf("%w: PK %q", types.ErrMissingKey, ks.PK)
	}
	pk, err = storage.AttrCanonicalBytes(pkAttr)
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
	sk, err = storage.AttrCanonicalBytes(skAttr)
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
	if err := d.checkWritePressure(); err != nil {
		return err
	}
	if err := validateDescriptorItem(td, item); err != nil {
		return err
	}
	pk, sk, err := extractKeyBytes(item, td.KeySchema)
	if err != nil {
		return err
	}
	primaryKey := storage.KeyPrimary(td.Name, pk, sk)
	encoded, err := storage.EncodeItem(item)
	if err != nil {
		return fmt.Errorf("encode item: %w", err)
	}

	if d.putItemCanSkipPrior(td, opts.Condition) {
		return d.putPrimaryWithoutPrior(td, primaryKey, encoded)
	}

	cond, err := storage.ParseCondition(opts.Condition)
	if err != nil {
		return fmt.Errorf("condition: %w", err)
	}

	// Read prior under a snapshot so GSI maintenance and the condition
	// see a consistent view of the row at the moment we plan the
	// write. The snapshot is cheap (it's just a sequence number in
	// Pebble) and we drop it immediately after planning.
	prior, err := d.snapshotGet(primaryKey)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("read prior: %w", err)
	}
	var priorItem types.Item
	if prior != nil {
		priorItem, err = storage.DecodeItem(prior)
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
			return storage.ErrConditionFailed
		}
	}

	gsiOps, err := storage.PlanGSI(td.Name, td.KeySchema, td.GSIs, priorItem, item)
	if err != nil {
		return fmt.Errorf("plan gsi: %w", err)
	}
	lsiOps, err := storage.PlanLSI(td.Name, td.KeySchema, td.LSIs, priorItem, item)
	if err != nil {
		return fmt.Errorf("plan lsi: %w", err)
	}
	spatialOps, err := planSpatial(td.Name, td.KeySchema, td.SpatialIndexes, priorItem, item)
	if err != nil {
		return fmt.Errorf("plan spatial: %w", err)
	}
	ttlOps, err := planTTL(td.Name, td.KeySchema, td.TTLAttribute, priorItem, item)
	if err != nil {
		return fmt.Errorf("plan ttl: %w", err)
	}

	b := d.Batch()
	defer b.Close()
	if err := b.Set(primaryKey, encoded, nil); err != nil {
		return fmt.Errorf("batch set primary: %w", err)
	}
	if err := applyIndexOps(b, gsiOps); err != nil {
		return err
	}
	if err := applyIndexOps(b, lsiOps); err != nil {
		return err
	}
	if err := applyIndexOps(b, spatialOps); err != nil {
		return err
	}
	if err := applyIndexOps(b, ttlOps); err != nil {
		return err
	}
	if d.shouldAppendChangeRecord(td) {
		if _, err := d.appendChangeRecord(b, newChangeRecord(td, ChangePut, keyItemFromItem(item, td.KeySchema), priorItem, item)); err != nil {
			return fmt.Errorf("change log: %w", err)
		}
	}
	if err := d.CommitBatch(b); err != nil {
		return err
	}
	if isMemoryTable(td) {
		d.memorySet(td.Name, primaryKey, encoded)
	}
	return d.refreshStreamRetentionAfterWrite(td)
}

func (d *DB) putItemCanSkipPrior(td types.TableDescriptor, condition string) bool {
	if strings.TrimSpace(condition) != "" {
		return false
	}
	if len(td.GSIs) > 0 || len(td.LSIs) > 0 || len(td.SpatialIndexes) > 0 || td.TTLAttribute != "" {
		return false
	}
	return !d.shouldAppendChangeRecord(td)
}

func (d *DB) putPrimaryWithoutPrior(td types.TableDescriptor, primaryKey, encoded []byte) error {
	b := d.Batch()
	defer b.Close()
	if err := b.Set(primaryKey, encoded, nil); err != nil {
		return fmt.Errorf("batch set primary: %w", err)
	}
	if err := d.CommitBatch(b); err != nil {
		return err
	}
	if isMemoryTable(td) {
		d.memorySet(td.Name, primaryKey, encoded)
	}
	return d.refreshStreamRetentionAfterWrite(td)
}

// DeleteItem removes an item identified by its key attributes (no GSI
// maintenance, no condition).
func (d *DB) DeleteItem(table string, ks types.KeySchema, keyAttrs types.Item) error {
	return d.DeleteItemWith(types.TableDescriptor{Name: table, KeySchema: ks}, keyAttrs, DeleteOptions{})
}

// DeleteItemWith removes an item AND every GSI pointer that referenced
// it, atomically. Honors an optional ConditionExpression.
func (d *DB) DeleteItemWith(td types.TableDescriptor, keyAttrs types.Item, opts DeleteOptions) error {
	if err := d.checkWritePressure(); err != nil {
		return err
	}
	pk, sk, err := extractKeyBytes(keyAttrs, td.KeySchema)
	if err != nil {
		return err
	}
	primaryKey := storage.KeyPrimary(td.Name, pk, sk)
	cond, err := storage.ParseCondition(opts.Condition)
	if err != nil {
		return fmt.Errorf("condition: %w", err)
	}

	prior, err := d.snapshotGet(primaryKey)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("read prior: %w", err)
	}
	var priorItem types.Item
	if prior != nil {
		priorItem, err = storage.DecodeItem(prior)
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
			return storage.ErrConditionFailed
		}
	}

	// Plan every secondary delta from priorItem → nil.
	gsiOps, err := storage.PlanGSI(td.Name, td.KeySchema, td.GSIs, priorItem, nil)
	if err != nil {
		return fmt.Errorf("plan gsi: %w", err)
	}
	lsiOps, err := storage.PlanLSI(td.Name, td.KeySchema, td.LSIs, priorItem, nil)
	if err != nil {
		return fmt.Errorf("plan lsi: %w", err)
	}
	spatialOps, err := planSpatial(td.Name, td.KeySchema, td.SpatialIndexes, priorItem, nil)
	if err != nil {
		return fmt.Errorf("plan spatial: %w", err)
	}
	ttlOps, err := planTTL(td.Name, td.KeySchema, td.TTLAttribute, priorItem, nil)
	if err != nil {
		return fmt.Errorf("plan ttl: %w", err)
	}

	b := d.Batch()
	defer b.Close()
	if err := b.Delete(primaryKey, nil); err != nil {
		return fmt.Errorf("batch delete primary: %w", err)
	}
	if err := applyIndexOps(b, gsiOps); err != nil {
		return err
	}
	if err := applyIndexOps(b, lsiOps); err != nil {
		return err
	}
	if err := applyIndexOps(b, spatialOps); err != nil {
		return err
	}
	if err := applyIndexOps(b, ttlOps); err != nil {
		return err
	}
	if d.shouldAppendChangeRecord(td) {
		if _, err := d.appendChangeRecord(b, newChangeRecord(td, ChangeDelete, keyItemFromItem(keyAttrs, td.KeySchema), priorItem, nil)); err != nil {
			return fmt.Errorf("change log: %w", err)
		}
	}
	if err := d.CommitBatch(b); err != nil {
		return err
	}
	if isMemoryTable(td) {
		d.memoryDelete(td.Name, primaryKey)
	}
	return d.refreshStreamRetentionAfterWrite(td)
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

func applyIndexOps(b *pebbledb.Batch, ops []storage.IndexOp) error {
	for _, op := range ops {
		switch op.Op {
		case storage.IndexOpSet:
			if err := b.Set(op.Key, op.Value, nil); err != nil {
				return fmt.Errorf("batch set index: %w", err)
			}
		case storage.IndexOpDelete:
			if err := b.Delete(op.Key, nil); err != nil {
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
	return d.GetItemByKeyBytes(table, pk, sk)
}

// GetItemByKeyBytes loads an item when the caller already has canonical
// PK/SK bytes. It is the point-read fast path for protocol handlers
// that can parse key bytes without materializing a full Item map.
func (d *DB) GetItemByKeyBytes(table string, pk, sk []byte) (types.Item, error) {
	v, err := d.GetEncodedItemByKeyBytes(table, pk, sk)
	if err != nil {
		return nil, err
	}
	return storage.DecodeItem(v)
}

// GetEncodedItemByKeyBytes loads an encoded item when the caller already
// has canonical PK/SK bytes. The returned buffer is owned by the caller.
func (d *DB) GetEncodedItemByKeyBytes(table string, pk, sk []byte) ([]byte, error) {
	primaryKey := storage.KeyPrimary(table, pk, sk)
	if v, ok := d.memoryGet(table, primaryKey); ok {
		return v, nil
	}
	v, err := d.Get(primaryKey)
	if errors.Is(err, ErrNotFound) {
		return nil, types.ErrItemNotFound
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

// QueryByPK returns every item under a single PK. SK ordering is
// determined by the underlying byte ordering of the canonical SK bytes
// (lexicographic). Limit ≤ 0 means "no limit".
func (d *DB) QueryByPK(table string, ks types.KeySchema, pkAttr types.AttributeValue, limit int) ([]types.Item, error) {
	pk, err := storage.AttrCanonicalBytes(pkAttr)
	if err != nil {
		return nil, fmt.Errorf("PK %q: %w", ks.PK, err)
	}
	lower, upper := storage.PrefixPrimaryByPK(table, pk)
	if d.memoryHasTable(table) {
		return d.memoryScan(table, lower, upper, limit)
	}
	return d.scanItems(lower, upper, limit)
}

// QueryByPKRange constrains SK to [skLow, skHigh).
func (d *DB) QueryByPKRange(table string, ks types.KeySchema, pkAttr, skLow, skHigh types.AttributeValue, limit int) ([]types.Item, error) {
	if ks.SK == "" {
		return nil, fmt.Errorf("table %q has no sort key", table)
	}
	pk, err := storage.AttrCanonicalBytes(pkAttr)
	if err != nil {
		return nil, fmt.Errorf("PK %q: %w", ks.PK, err)
	}
	var lowBytes, highBytes []byte
	if skLow.T != types.AttrNull {
		lowBytes, err = storage.AttrCanonicalBytes(skLow)
		if err != nil {
			return nil, fmt.Errorf("SK low: %w", err)
		}
	}
	if skHigh.T != types.AttrNull {
		highBytes, err = storage.AttrCanonicalBytes(skHigh)
		if err != nil {
			return nil, fmt.Errorf("SK high: %w", err)
		}
	}
	lower, upper := storage.RangePrimaryBySK(table, pk, lowBytes, highBytes)
	if d.memoryHasTable(table) {
		return d.memoryScan(table, lower, upper, limit)
	}
	return d.scanItems(lower, upper, limit)
}

// QueryByLSI iterates a local-secondary-index partition, resolves
// pointers, and returns the underlying items. The supplied primaryPKVal
// pins the partition; the LSI's own SK supplies ordering.
func (d *DB) QueryByLSI(td types.TableDescriptor, idxName string, primaryPKVal types.AttributeValue, opts QueryOptions) ([]types.Item, error) {
	var descriptor types.LSIDescriptor
	found := false
	for _, l := range td.LSIs {
		if l.Name == idxName {
			descriptor = l
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("table %q has no LSI named %q", td.Name, idxName)
	}
	pkBytes, err := storage.AttrCanonicalBytes(primaryPKVal)
	if err != nil {
		return nil, fmt.Errorf("primary PK: %w", err)
	}
	var lower, upper []byte
	if opts.SKLow.T == types.AttrNull && opts.SKHigh.T == types.AttrNull {
		lower, upper = storage.PrefixLSIByPK(td.Name, idxName, pkBytes)
	} else {
		var lo, hi []byte
		if opts.SKLow.T != types.AttrNull {
			lo, err = storage.AttrCanonicalBytes(opts.SKLow)
			if err != nil {
				return nil, fmt.Errorf("lsi SK low: %w", err)
			}
		}
		if opts.SKHigh.T != types.AttrNull {
			hi, err = storage.AttrCanonicalBytes(opts.SKHigh)
			if err != nil {
				return nil, fmt.Errorf("lsi SK high: %w", err)
			}
		}
		lower, upper = storage.RangeLSIBySK(td.Name, idxName, pkBytes, lo, hi)
	}
	return d.iterateIndex(td, lower, upper, descriptor.Projection, opts.Limit)
}

// iterateIndex walks an index keyspace and resolves each pointer
// according to the supplied projection mode. INCLUDE / ALL pointers
// avoid the dereference Get by carrying the projected payload in the
// value blob.
func (d *DB) iterateIndex(td types.TableDescriptor, lower, upper []byte, projection types.IndexProjection, limit int) ([]types.Item, error) {
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
		pk, sk, projectedBytes, mode, err := storage.DecodeProjectedPointer(ptrCopy)
		if err != nil {
			return nil, fmt.Errorf("decode pointer: %w", err)
		}
		switch mode {
		case "ALL":
			item, err := storage.DecodeItem(projectedBytes)
			if err != nil {
				return nil, fmt.Errorf("decode ALL item: %w", err)
			}
			out = append(out, item)
		case "INCLUDE":
			item, err := storage.DecodeItem(projectedBytes)
			if err != nil {
				return nil, fmt.Errorf("decode INCLUDE item: %w", err)
			}
			out = append(out, item)
		default:
			raw, err := d.Get(storage.KeyPrimary(td.Name, pk, sk))
			if errors.Is(err, ErrNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			item, err := storage.DecodeItem(raw)
			if err != nil {
				return nil, fmt.Errorf("decode item: %w", err)
			}
			out = append(out, item)
		}
		_ = projection
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, it.Error()
}

// QueryByGSI iterates a GSI partition, resolves pointers, and returns
// the underlying items in the order the index produces them.
func (d *DB) QueryByGSI(td types.TableDescriptor, idxName string, gsiPKVal types.AttributeValue, opts QueryOptions) ([]types.Item, error) {
	descriptor, ok := findGSI(td, idxName)
	if !ok {
		return nil, fmt.Errorf("table %q has no GSI named %q", td.Name, idxName)
	}
	gsiPK, err := storage.AttrCanonicalBytes(gsiPKVal)
	if err != nil {
		return nil, fmt.Errorf("gsi PK: %w", err)
	}
	var lower, upper []byte
	if opts.SKLow.T == types.AttrNull && opts.SKHigh.T == types.AttrNull {
		lower, upper = storage.PrefixGSIByPK(td.Name, idxName, gsiPK)
	} else {
		if descriptor.KeySchema.SK == "" {
			return nil, fmt.Errorf("gsi %q has no sort key", idxName)
		}
		var lo, hi []byte
		if opts.SKLow.T != types.AttrNull {
			lo, err = storage.AttrCanonicalBytes(opts.SKLow)
			if err != nil {
				return nil, fmt.Errorf("gsi SK low: %w", err)
			}
		}
		if opts.SKHigh.T != types.AttrNull {
			hi, err = storage.AttrCanonicalBytes(opts.SKHigh)
			if err != nil {
				return nil, fmt.Errorf("gsi SK high: %w", err)
			}
		}
		lower, upper = storage.RangeGSIBySK(td.Name, idxName, gsiPK, lo, hi)
	}
	return d.iterateIndex(td, lower, upper, descriptor.Projection, opts.Limit)
}

func findGSI(td types.TableDescriptor, name string) (types.GSIDescriptor, bool) {
	for _, g := range td.GSIs {
		if g.Name == name {
			return g, true
		}
	}
	return types.GSIDescriptor{}, false
}

// ScanTable streams every primary item in `table`, capped by limit
// (limit ≤ 0 means "no limit"). Decoding happens here so the caller
// never sees raw pebble bytes. Multi-shard mode requires the caller
// to scatter the call across each shard's DB.
func (d *DB) ScanTable(table string, limit int) ([]types.Item, error) {
	lower, upper := storage.PrefixPrimaryAll(table)
	if d.memoryHasTable(table) {
		return d.memoryScan(table, lower, upper, limit)
	}
	return d.scanItems(lower, upper, limit)
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
		item, err := storage.DecodeItem(cp)
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
	if err := d.checkWritePressure(); err != nil {
		return err
	}
	if d.batchPutItemsCanSkipPrior(td, ops) {
		return d.batchPutItemsWithoutPrior(td, ops)
	}
	snap := d.db.NewSnapshot()
	defer snap.Close()

	b := d.Batch()
	defer b.Close()
	type memDelta struct {
		key    []byte
		value  []byte
		delete bool
	}
	var memDeltas []memDelta

	for i, op := range ops {
		switch op.Op {
		case BatchOpPut:
			if err := validateDescriptorItem(td, op.Item); err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
			pk, sk, err := extractKeyBytes(op.Item, td.KeySchema)
			if err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
			enc, err := storage.EncodeItem(op.Item)
			if err != nil {
				return fmt.Errorf("op %d encode: %w", i, err)
			}
			primaryKey := storage.KeyPrimary(td.Name, pk, sk)
			priorItem, err := readSnapshotItem(snap, primaryKey)
			if err != nil {
				return fmt.Errorf("op %d prior: %w", i, err)
			}
			gsiOps, err := storage.PlanGSI(td.Name, td.KeySchema, td.GSIs, priorItem, op.Item)
			if err != nil {
				return fmt.Errorf("op %d gsi: %w", i, err)
			}
			lsiOps, err := storage.PlanLSI(td.Name, td.KeySchema, td.LSIs, priorItem, op.Item)
			if err != nil {
				return fmt.Errorf("op %d lsi: %w", i, err)
			}
			spatialOps, err := planSpatial(td.Name, td.KeySchema, td.SpatialIndexes, priorItem, op.Item)
			if err != nil {
				return fmt.Errorf("op %d spatial: %w", i, err)
			}
			ttlOps, err := planTTL(td.Name, td.KeySchema, td.TTLAttribute, priorItem, op.Item)
			if err != nil {
				return fmt.Errorf("op %d ttl: %w", i, err)
			}
			if err := b.Set(primaryKey, enc, nil); err != nil {
				return err
			}
			if isMemoryTable(td) {
				memDeltas = append(memDeltas, memDelta{key: append([]byte(nil), primaryKey...), value: append([]byte(nil), enc...)})
			}
			if d.shouldAppendChangeRecord(td) {
				if _, err := d.appendChangeRecord(b, newChangeRecord(td, ChangePut, keyItemFromItem(op.Item, td.KeySchema), priorItem, op.Item)); err != nil {
					return fmt.Errorf("op %d change log: %w", i, err)
				}
			}
			if err := applyIndexOps(b, gsiOps); err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
			if err := applyIndexOps(b, lsiOps); err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
			if err := applyIndexOps(b, spatialOps); err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
			if err := applyIndexOps(b, ttlOps); err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
		case BatchOpDelete:
			pk, sk, err := extractKeyBytes(op.Key, td.KeySchema)
			if err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
			primaryKey := storage.KeyPrimary(td.Name, pk, sk)
			priorItem, err := readSnapshotItem(snap, primaryKey)
			if err != nil {
				return fmt.Errorf("op %d prior: %w", i, err)
			}
			gsiOps, err := storage.PlanGSI(td.Name, td.KeySchema, td.GSIs, priorItem, nil)
			if err != nil {
				return fmt.Errorf("op %d gsi: %w", i, err)
			}
			lsiOps, err := storage.PlanLSI(td.Name, td.KeySchema, td.LSIs, priorItem, nil)
			if err != nil {
				return fmt.Errorf("op %d lsi: %w", i, err)
			}
			spatialOps, err := planSpatial(td.Name, td.KeySchema, td.SpatialIndexes, priorItem, nil)
			if err != nil {
				return fmt.Errorf("op %d spatial: %w", i, err)
			}
			ttlOps, err := planTTL(td.Name, td.KeySchema, td.TTLAttribute, priorItem, nil)
			if err != nil {
				return fmt.Errorf("op %d ttl: %w", i, err)
			}
			if err := b.Delete(primaryKey, nil); err != nil {
				return err
			}
			if isMemoryTable(td) {
				memDeltas = append(memDeltas, memDelta{key: append([]byte(nil), primaryKey...), delete: true})
			}
			if d.shouldAppendChangeRecord(td) {
				if _, err := d.appendChangeRecord(b, newChangeRecord(td, ChangeDelete, keyItemFromItem(op.Key, td.KeySchema), priorItem, nil)); err != nil {
					return fmt.Errorf("op %d change log: %w", i, err)
				}
			}
			if err := applyIndexOps(b, gsiOps); err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
			if err := applyIndexOps(b, lsiOps); err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
			if err := applyIndexOps(b, spatialOps); err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
			if err := applyIndexOps(b, ttlOps); err != nil {
				return fmt.Errorf("op %d: %w", i, err)
			}
		default:
			return fmt.Errorf("op %d: unknown kind %d", i, op.Op)
		}
	}
	if err := d.CommitBatch(b); err != nil {
		return err
	}
	if isMemoryTable(td) {
		for _, delta := range memDeltas {
			if delta.delete {
				d.memoryDelete(td.Name, delta.key)
			} else {
				d.memorySet(td.Name, delta.key, delta.value)
			}
		}
	}
	return d.refreshStreamRetentionAfterWrite(td)
}

func (d *DB) batchPutItemsCanSkipPrior(td types.TableDescriptor, ops []BatchOp) bool {
	if !d.putItemCanSkipPrior(td, "") {
		return false
	}
	for _, op := range ops {
		if op.Op != BatchOpPut {
			return false
		}
	}
	return true
}

func (d *DB) batchPutItemsWithoutPrior(td types.TableDescriptor, ops []BatchOp) error {
	b := d.Batch()
	defer b.Close()
	type memDelta struct {
		key   []byte
		value []byte
	}
	var memDeltas []memDelta
	for i, op := range ops {
		if err := validateDescriptorItem(td, op.Item); err != nil {
			return fmt.Errorf("op %d: %w", i, err)
		}
		pk, sk, err := extractKeyBytes(op.Item, td.KeySchema)
		if err != nil {
			return fmt.Errorf("op %d: %w", i, err)
		}
		enc, err := storage.EncodeItem(op.Item)
		if err != nil {
			return fmt.Errorf("op %d encode: %w", i, err)
		}
		primaryKey := storage.KeyPrimary(td.Name, pk, sk)
		if err := b.Set(primaryKey, enc, nil); err != nil {
			return err
		}
		if isMemoryTable(td) {
			memDeltas = append(memDeltas, memDelta{key: append([]byte(nil), primaryKey...), value: append([]byte(nil), enc...)})
		}
	}
	if err := d.CommitBatch(b); err != nil {
		return err
	}
	if isMemoryTable(td) {
		for _, delta := range memDeltas {
			d.memorySet(td.Name, delta.key, delta.value)
		}
	}
	return d.refreshStreamRetentionAfterWrite(td)
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
	return storage.DecodeItem(cp)
}

func keyItemFromItem(item types.Item, ks types.KeySchema) types.Item {
	out := types.Item{}
	if v, ok := item[ks.PK]; ok {
		out[ks.PK] = v
	}
	if ks.SK != "" {
		if v, ok := item[ks.SK]; ok {
			out[ks.SK] = v
		}
	}
	return out
}
