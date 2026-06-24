package pebble

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/CefasDb/cefasdb/pkg/types"
)

type ChangeOp string

const (
	ChangePut    ChangeOp = "put"
	ChangeDelete ChangeOp = "delete"
)

type ChangeEventName string

const (
	ChangeEventInsert ChangeEventName = "INSERT"
	ChangeEventModify ChangeEventName = "MODIFY"
	ChangeEventRemove ChangeEventName = "REMOVE"
)

type ChangeRecord struct {
	Index          uint64          `json:"index"`
	SequenceNumber string          `json:"sequenceNumber,omitempty"`
	UnixNano       int64           `json:"unixNano"`
	Op             ChangeOp        `json:"op"`
	Table          string          `json:"table"`
	Key            types.Item      `json:"key,omitempty"`
	Item           types.Item      `json:"item,omitempty"`
	StreamRecord   bool            `json:"streamRecord,omitempty"`
	EventName      ChangeEventName `json:"eventName,omitempty"`
	OldItem        types.Item      `json:"oldItem,omitempty"`
	NewItem        types.Item      `json:"newItem,omitempty"`
	StreamViewType string          `json:"streamViewType,omitempty"`
	SizeBytes      int64           `json:"sizeBytes,omitempty"`
	// Idempotency markers (#524). BatchID is unique per
	// BatchWriteItem invocation; SeqInBatch is the 0-indexed
	// position within that batch. Single-op mutations get a
	// per-call BatchID and SeqInBatch=0. OpKind exposes the
	// INSERT / MODIFY / REMOVE classification independently of
	// the EventName field (which only fires when stream view
	// is set on the table). Zero values on legacy records mean
	// "unknown batch" / "unknown kind" for forward-compat reads.
	BatchID    string `json:"batchId,omitempty"`
	SeqInBatch int32  `json:"seqInBatch,omitempty"`
	OpKind     string `json:"opKind,omitempty"`
}

// StreamRetentionStats is the persisted logical retention state for one table
// stream. OldestSequence is the first readable stream sequence. Sequences below
// it are considered trimmed even though the physical changelog remains for PITR.
type StreamRetentionStats struct {
	Table            string `json:"table"`
	OldestSequence   uint64 `json:"oldestSequence,omitempty"`
	NewestSequence   uint64 `json:"newestSequence,omitempty"`
	RetainedBytes    int64  `json:"retainedBytes,omitempty"`
	RecordsAppended  uint64 `json:"recordsAppended,omitempty"`
	RecordsTrimmed   uint64 `json:"recordsTrimmed,omitempty"`
	LastTrimUnixNano int64  `json:"lastTrimUnixNano,omitempty"`
}

type streamRetentionRecord struct {
	Index    uint64
	UnixNano int64
	Bytes    int64
}

func (d *DB) shouldAppendChangeRecord(td types.TableDescriptor) bool {
	switch d.changeLogMode {
	case ChangeLogModeOff:
		return false
	case ChangeLogModeStreamsOnly:
		return td.StreamSpecification != nil && td.StreamSpecification.StreamEnabled
	default:
		return true
	}
}

func (d *DB) appendChangeRecord(b *pebbledb.Batch, rec ChangeRecord) (ChangeRecord, error) {
	if rec.Table == "" {
		return rec, fmt.Errorf("change record table required")
	}
	rec.Index = d.changeIndex.Add(1)
	if rec.StreamRecord {
		rec.SequenceNumber = strconv.FormatUint(rec.Index, 10)
	}
	if rec.UnixNano == 0 {
		rec.UnixNano = time.Now().UnixNano()
	}
	if rec.StreamRecord && rec.SizeBytes == 0 {
		rec.SizeBytes = estimateChangeRecordSize(rec)
	}
	raw, err := encodeChangeRecord(nil, rec)
	if err != nil {
		return rec, fmt.Errorf("encode change record: %w", err)
	}
	// The persisted index lives implicitly in the largest KeyChangeLog
	// suffix. seedChangeIndex recovers from that scan on Open, so the
	// old hot-key write of storage.ChangeCounterKey is unnecessary and
	// only added one rewrite of the same key per mutation. Old
	// deployments still keep the key on disk; loadPersistedChangeIndex
	// reads it for forward compatibility and the max with the scan wins.
	if err := b.Set(storage.KeyChangeLog(rec.Index), raw, nil); err != nil {
		return rec, err
	}
	if rec.StreamRecord {
		d.trackStreamTable(rec.Table)
	}
	return rec, nil
}

func newChangeRecord(td types.TableDescriptor, op ChangeOp, key, oldItem, newItem types.Item) ChangeRecord {
	rec := ChangeRecord{
		Op:     op,
		Table:  td.Name,
		Key:    cloneChangeItem(key),
		OpKind: deriveOpKind(op, oldItem),
	}
	if op == ChangePut {
		rec.Item = cloneChangeItem(newItem)
	}
	applyStreamRecordFields(td, &rec, oldItem, newItem)
	return rec
}

// deriveOpKind classifies the mutation independently of streams view
// — INSERT when a put had no prior, MODIFY when it replaced an
// existing row, REMOVE for deletes. The values match
// ChangeEventName so consumers reading either field see the same
// classification (#524).
func deriveOpKind(op ChangeOp, oldItem types.Item) string {
	switch op {
	case ChangePut:
		if oldItem == nil {
			return string(ChangeEventInsert)
		}
		return string(ChangeEventModify)
	case ChangeDelete:
		return string(ChangeEventRemove)
	}
	return ""
}

// nextBatchID returns a unique-per-call identifier for a batch of
// change records. The value is monotonic per-process (atomic
// counter) and tagged with the wall-clock nanosecond so that
// post-restart batches do not collide with pre-restart ones. Used
// to tag every ChangeRecord produced by a single BatchWriteItem
// invocation so consumers can dedup retried events (#524).
func (d *DB) nextBatchID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), d.batchSeqCounter.Add(1))
}

func applyStreamRecordFields(td types.TableDescriptor, rec *ChangeRecord, oldItem, newItem types.Item) {
	if td.StreamSpecification == nil || !td.StreamSpecification.StreamEnabled {
		return
	}
	view := types.NormalizeStreamViewType(td.StreamSpecification.StreamViewType)
	if view == "" {
		view = types.StreamViewTypeNewAndOldImages
	}
	if !types.IsValidStreamViewType(view) {
		return
	}

	switch rec.Op {
	case ChangePut:
		if oldItem == nil {
			rec.EventName = ChangeEventInsert
		} else {
			rec.EventName = ChangeEventModify
		}
	case ChangeDelete:
		if oldItem == nil {
			return
		}
		rec.EventName = ChangeEventRemove
	default:
		return
	}
	if streamRecordIncludesOldImage(view, rec.EventName) {
		rec.OldItem = cloneChangeItem(oldItem)
	}
	if streamRecordIncludesNewImage(view, rec.EventName) {
		rec.NewItem = cloneChangeItem(newItem)
	}
	// DELTA_IMAGE (#522): when an UpdateItem touches only a subset
	// of columns, emit just those columns in NewItem. INSERT keeps
	// the full row (no diff target); DELETE keeps key only and
	// returns early above without populating NewItem.
	if view == types.StreamViewTypeDeltaImage && rec.EventName == ChangeEventModify {
		rec.NewItem = deltaImageItem(oldItem, newItem)
	}
	rec.StreamRecord = true
	rec.StreamViewType = view
}

func streamRecordIncludesOldImage(view string, event ChangeEventName) bool {
	if event == ChangeEventInsert {
		return false
	}
	switch view {
	case types.StreamViewTypeOldImage, types.StreamViewTypeNewAndOldImages:
		return true
	}
	return false
}

func streamRecordIncludesNewImage(view string, event ChangeEventName) bool {
	if event == ChangeEventRemove {
		return false
	}
	switch view {
	case types.StreamViewTypeNewImage,
		types.StreamViewTypeNewAndOldImages,
		types.StreamViewTypeDeltaImage:
		// DELTA_IMAGE on INSERT keeps the full new image (handled in
		// the standard path); the MODIFY override happens after, in
		// applyStreamRecordFields, replacing the full image with the
		// diff.
		return true
	}
	return false
}

// deltaImageItem returns only the attributes that differ between
// oldItem and newItem (#522). Equality is value-shape-aware: any
// difference in T / S / N / B / SS / NS / BS / L / M / Vec /
// BOOL / NULL marks the attribute as changed. The returned map
// always carries no fields beyond the changed ones — the consumer
// reconstructs the rest from prior state upstream.
func deltaImageItem(oldItem, newItem types.Item) types.Item {
	if newItem == nil {
		return nil
	}
	if oldItem == nil {
		// No prior — every attribute is "new". This branch is
		// defensive; INSERT short-circuits before this function
		// runs (handled in the caller).
		return cloneChangeItem(newItem)
	}
	out := types.Item{}
	for k, nv := range newItem {
		if ov, ok := oldItem[k]; !ok || !attributeValuesEqual(ov, nv) {
			out[k] = cloneChangeAttr(nv)
		}
	}
	// Attribute removed in the new image — record a Null-typed
	// marker so consumers know to drop it.
	for k := range oldItem {
		if _, ok := newItem[k]; !ok {
			out[k] = types.AttributeValue{T: types.AttrNull}
		}
	}
	return out
}

// attributeValuesEqual compares two AttributeValue records for
// "no observable change". Aliased into the same package so it can
// stay decoupled from the protobuf wire shape.
func attributeValuesEqual(a, b types.AttributeValue) bool {
	if a.T != b.T {
		return false
	}
	if a.S != b.S || a.N != b.N || a.BOOL != b.BOOL {
		return false
	}
	if !bytesEqual(a.B, b.B) {
		return false
	}
	if !stringSlicesEqual(a.SS, b.SS) || !stringSlicesEqual(a.NS, b.NS) {
		return false
	}
	if !byteSlicesEqual(a.BS, b.BS) {
		return false
	}
	if len(a.L) != len(b.L) {
		return false
	}
	for i := range a.L {
		if !attributeValuesEqual(a.L[i], b.L[i]) {
			return false
		}
	}
	if len(a.M) != len(b.M) {
		return false
	}
	for k, av := range a.M {
		bv, ok := b.M[k]
		if !ok || !attributeValuesEqual(av, bv) {
			return false
		}
	}
	if !float64SlicesEqual(a.Vec, b.Vec) {
		return false
	}
	return true
}

func bytesEqual(a, b []byte) bool {
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

func stringSlicesEqual(a, b []string) bool {
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

func byteSlicesEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytesEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func float64SlicesEqual(a, b []float64) bool {
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

// estimateChangeRecordSize returns a cheap O(n) estimate of the serialized
// stream-payload byte count without allocating an intermediate buffer.
// Used on the hot write path to populate ChangeRecord.SizeBytes before the
// single json.Marshal happens. The estimate is within ~5% of the exact
// JSON length for typical DynamoDB-shaped items; consumers reading
// SizeBytes (DynamoDB Streams API parity) get a stable, monotonic value
// they can budget against without paying a second marshal per write.
func estimateChangeRecordSize(rec ChangeRecord) int64 {
	const fieldOverhead = 16 // braces, commas, quotes, separators
	s := int64(fieldOverhead)
	s += int64(len(rec.SequenceNumber) + len(rec.EventName) + len(rec.Table) + len(rec.StreamViewType))
	s += estimateItemSize(rec.Key)
	s += estimateItemSize(rec.OldItem)
	s += estimateItemSize(rec.NewItem)
	return s
}

func estimateItemSize(item types.Item) int64 {
	if len(item) == 0 {
		return 0
	}
	var s int64 = 2 // {}
	for k, v := range item {
		s += int64(len(k)) + 4 // "k":
		s += estimateAttrSize(v)
		s++ // ,
	}
	return s
}

func estimateAttrSize(v types.AttributeValue) int64 {
	s := int64(8) // {"T":N,...}
	s += int64(len(v.S) + len(v.N) + len(v.B))
	for _, ss := range v.SS {
		s += int64(len(ss)) + 3
	}
	for _, ns := range v.NS {
		s += int64(len(ns)) + 3
	}
	for _, bs := range v.BS {
		s += int64(len(bs)) + 3
	}
	for _, lv := range v.L {
		s += estimateAttrSize(lv)
	}
	for k, mv := range v.M {
		s += int64(len(k)) + 4
		s += estimateAttrSize(mv)
	}
	s += int64(len(v.Vec)) * 9
	return s
}

func approximateChangeRecordSize(rec ChangeRecord) int64 {
	return estimateChangeRecordSize(rec)
}

func cloneChangeItem(in types.Item) types.Item {
	if in == nil {
		return nil
	}
	out := make(types.Item, len(in))
	for k, v := range in {
		out[k] = cloneChangeAttr(v)
	}
	return out
}

func cloneChangeAttr(in types.AttributeValue) types.AttributeValue {
	out := in
	if in.B != nil {
		out.B = append([]byte(nil), in.B...)
	}
	if in.SS != nil {
		out.SS = append([]string(nil), in.SS...)
	}
	if in.NS != nil {
		out.NS = append([]string(nil), in.NS...)
	}
	if in.BS != nil {
		out.BS = make([][]byte, len(in.BS))
		for i := range in.BS {
			out.BS[i] = append([]byte(nil), in.BS[i]...)
		}
	}
	if in.L != nil {
		out.L = make([]types.AttributeValue, len(in.L))
		for i := range in.L {
			out.L[i] = cloneChangeAttr(in.L[i])
		}
	}
	if in.M != nil {
		out.M = make(map[string]types.AttributeValue, len(in.M))
		for k, v := range in.M {
			out.M[k] = cloneChangeAttr(v)
		}
	}
	if in.Vec != nil {
		out.Vec = append([]float64(nil), in.Vec...)
	}
	return out
}

// loadPersistedChangeIndex reads ChangeCounterKey or returns 0 when absent.
// May trail the true MAX(KeyChangeLog) by up to one commit window after
// a crash because the counter is written from inside batches the caller
// owns; seedChangeIndex covers that gap with a key-range scan.
func (d *DB) loadPersistedChangeIndex() (uint64, error) {
	raw, err := d.getNoLane(storage.ChangeCounterKey)
	if errors.Is(err, ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid change counter length %d", len(raw))
	}
	return binary.BigEndian.Uint64(raw), nil
}

// scanMaxChangeIndex walks the changelog prefix backwards and returns the
// largest known index, or 0 if the changelog is empty. Used in Open to
// recover from a stale ChangeCounterKey after an unclean shutdown.
func (d *DB) scanMaxChangeIndex() (uint64, error) {
	lower, upper := storage.PrefixChangeLog()
	it, err := d.Iter(lower, upper)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	if !it.Last() {
		return 0, it.Error()
	}
	key := it.Key()
	idx, err := storage.ChangeLogIndexFromKey(key)
	if err != nil {
		return 0, fmt.Errorf("decode change log key: %w", err)
	}
	return idx, nil
}

// seedChangeIndex initialises d.changeIndex on Open. Takes the larger of
// the persisted counter and a tail scan so a crash that lost the counter
// write cannot produce overlapping indexes on the next append.
func (d *DB) seedChangeIndex() error {
	persisted, err := d.loadPersistedChangeIndex()
	if err != nil {
		return err
	}
	scanned, err := d.scanMaxChangeIndex()
	if err != nil {
		return err
	}
	if scanned > persisted {
		persisted = scanned
	}
	d.changeIndex.Store(persisted)
	return nil
}

func (d *DB) CurrentChangeIndex() (uint64, error) {
	return d.changeIndex.Load(), nil
}

// ApplyStreamRetention advances the logical stream trim point for table using
// the configured retention policy. It preserves physical changelog entries so
// PITR and backups keep seeing the full change history.
func (d *DB) ApplyStreamRetention(table string, now time.Time) (StreamRetentionStats, error) {
	if table == "" {
		return StreamRetentionStats{}, fmt.Errorf("stream retention table required")
	}
	if now.IsZero() {
		now = time.Now()
	}
	b := d.Batch()
	defer b.Close()
	stats, err := d.applyStreamRetentionLocked(b, table, now, nil)
	if err != nil {
		return StreamRetentionStats{}, err
	}
	if err := d.CommitBatch(b); err != nil {
		return StreamRetentionStats{}, err
	}
	return stats, nil
}

// StreamRetentionStats returns the latest persisted logical retention state.
// If the state is missing (for stores created before Streams retention), it is
// reconstructed from the preserved changelog without mutating storage.
func (d *DB) StreamRetentionStats(table string) (StreamRetentionStats, error) {
	if table == "" {
		return StreamRetentionStats{}, fmt.Errorf("stream retention table required")
	}
	if stats, ok, err := d.loadStreamRetentionState(table); err != nil || ok {
		return stats, err
	}
	records, err := d.scanStreamRetentionRecords(table, nil)
	if err != nil {
		return StreamRetentionStats{}, err
	}
	return d.computeStreamRetentionStats(table, records, StreamRetentionStats{}, time.Now()), nil
}

// PreviewStreamRetention computes the trim state as of now without writing it.
// Read paths use this so stream polling stays safe on Raft followers.
func (d *DB) PreviewStreamRetention(table string, now time.Time) (StreamRetentionStats, error) {
	if table == "" {
		return StreamRetentionStats{}, fmt.Errorf("stream retention table required")
	}
	if now.IsZero() {
		now = time.Now()
	}
	previous, _, err := d.loadStreamRetentionState(table)
	if err != nil {
		return StreamRetentionStats{}, err
	}
	records, err := d.scanStreamRetentionRecords(table, nil)
	if err != nil {
		return StreamRetentionStats{}, err
	}
	return d.computeStreamRetentionStats(table, records, previous, now), nil
}

// ListStreamRetentionStats returns every persisted table stream retention
// snapshot. It is intentionally read-only so metrics collection cannot mutate
// application data.
func (d *DB) ListStreamRetentionStats() ([]StreamRetentionStats, error) {
	lower, upper := storage.PrefixStreamRetention()
	it, err := d.Iter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []StreamRetentionStats
	for valid := it.First(); valid; valid = it.Next() {
		var stats StreamRetentionStats
		if err := json.Unmarshal(append([]byte(nil), it.Value()...), &stats); err != nil {
			return nil, fmt.Errorf("decode stream retention state at %x: %w", it.Key(), err)
		}
		out = append(out, stats)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

func (d *DB) applyStreamRetentionLocked(b *pebbledb.Batch, table string, now time.Time, extra *ChangeRecord) (StreamRetentionStats, error) {
	previous, _, err := d.loadStreamRetentionState(table)
	if err != nil {
		return StreamRetentionStats{}, err
	}
	records, err := d.scanStreamRetentionRecords(table, extra)
	if err != nil {
		return StreamRetentionStats{}, err
	}
	stats := d.computeStreamRetentionStats(table, records, previous, now)
	raw, err := json.Marshal(stats)
	if err != nil {
		return StreamRetentionStats{}, fmt.Errorf("marshal stream retention state: %w", err)
	}
	if err := b.Set(storage.KeyStreamRetention(table), raw, nil); err != nil {
		return StreamRetentionStats{}, err
	}
	return stats, nil
}

func (d *DB) loadStreamRetentionState(table string) (StreamRetentionStats, bool, error) {
	raw, err := d.Get(storage.KeyStreamRetention(table))
	if errors.Is(err, ErrNotFound) {
		return StreamRetentionStats{}, false, nil
	}
	if err != nil {
		return StreamRetentionStats{}, false, err
	}
	var stats StreamRetentionStats
	if err := json.Unmarshal(raw, &stats); err != nil {
		return StreamRetentionStats{}, false, fmt.Errorf("decode stream retention state: %w", err)
	}
	return stats, true, nil
}

func (d *DB) scanStreamRetentionRecords(table string, extra *ChangeRecord) ([]streamRetentionRecord, error) {
	lower, upper := storage.PrefixChangeLog()
	it, err := d.Iter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var records []streamRetentionRecord
	for valid := it.First(); valid; valid = it.Next() {
		rec, err := decodeChangeRecord(it.Value())
		if err != nil {
			return nil, fmt.Errorf("decode change record at %x: %w", it.Key(), err)
		}
		if rec.Table == table && rec.StreamRecord {
			records = append(records, retentionRecordFromChange(rec))
		}
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if extra != nil && extra.Table == table && extra.StreamRecord {
		records = append(records, retentionRecordFromChange(*extra))
	}
	return records, nil
}

func retentionRecordFromChange(rec ChangeRecord) streamRetentionRecord {
	size := rec.SizeBytes
	if size <= 0 {
		size = approximateChangeRecordSize(rec)
	}
	return streamRetentionRecord{
		Index:    rec.Index,
		UnixNano: rec.UnixNano,
		Bytes:    size,
	}
}

func (d *DB) computeStreamRetentionStats(table string, records []streamRetentionRecord, previous StreamRetentionStats, now time.Time) StreamRetentionStats {
	stats := StreamRetentionStats{
		Table:            table,
		RecordsTrimmed:   previous.RecordsTrimmed,
		LastTrimUnixNano: previous.LastTrimUnixNano,
	}
	if len(records) == 0 {
		return stats
	}

	stats.RecordsAppended = uint64(len(records))
	stats.NewestSequence = records[len(records)-1].Index

	start := 0
	retention := d.streamRetention.Retention
	// Per-table override from #521. The resolver returns the
	// StreamSpecification.RetentionSeconds for table or 0 when no
	// override is set; non-zero replaces the cluster default.
	if d.streamRetentionResolver != nil {
		if secs := d.streamRetentionResolver(table); secs > 0 {
			retention = time.Duration(secs) * time.Second
		}
	}
	if retention > 0 {
		cutoff := now.Add(-retention).UnixNano()
		for start < len(records) && records[start].UnixNano < cutoff {
			start++
		}
	}
	if d.streamRetention.MaxBytes > 0 && start < len(records) {
		byteStart := len(records)
		var retained int64
		for i := len(records) - 1; i >= start; i-- {
			if byteStart < len(records) && retained+records[i].Bytes > d.streamRetention.MaxBytes {
				break
			}
			retained += records[i].Bytes
			byteStart = i
		}
		if byteStart == len(records) {
			byteStart = len(records) - 1
		}
		if byteStart > start {
			start = byteStart
		}
	}
	if previous.OldestSequence > 0 {
		for start < len(records) && records[start].Index < previous.OldestSequence {
			start++
		}
	}

	if start < len(records) {
		stats.OldestSequence = records[start].Index
		for _, rec := range records[start:] {
			stats.RetainedBytes += rec.Bytes
		}
	} else {
		stats.OldestSequence = stats.NewestSequence + 1
	}

	var trimmed uint64
	for _, rec := range records {
		if rec.Index < stats.OldestSequence {
			trimmed++
		}
	}
	if trimmed > stats.RecordsTrimmed {
		stats.RecordsTrimmed = trimmed
		stats.LastTrimUnixNano = now.UnixNano()
	}
	return stats
}

// ChangeRecordsAfter exposes changeRecordsAfter for FAST refresh
// consumers that need to drain the changelog for a single base
// table since a cursor.
func (d *DB) ChangeRecordsAfter(table string, fromExclusive, toInclusive uint64, untilUnixNano int64) ([]ChangeRecord, error) {
	return d.changeRecordsAfter(table, fromExclusive, toInclusive, untilUnixNano)
}

// ScanCDC returns every changelog record for table within the
// inclusive [fromIndex, toIndex] window (0,0 means "all"), bounded
// by limit. Used by the CDC queryable-table alias (#523) so a
// Scan / Query against "<table>_cdc" sees the raw changelog as
// rows. The result is naturally ordered by Index (monotonic).
func (d *DB) ScanCDC(table string, fromIndex, toIndex uint64, limit int) ([]ChangeRecord, error) {
	if table == "" {
		return nil, fmt.Errorf("cdc scan: table required")
	}
	if limit <= 0 {
		limit = 1000
	}
	from := fromIndex
	if from > 0 {
		from -= 1 // changeRecordsAfter excludes the lower bound
	}
	all, err := d.changeRecordsAfter(table, from, toIndex, 0)
	if err != nil {
		return nil, err
	}
	if len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

func (d *DB) changeRecordsAfter(table string, fromExclusive, toInclusive uint64, untilUnixNano int64) ([]ChangeRecord, error) {
	lower := storage.KeyChangeLog(fromExclusive + 1)
	_, upperAll := storage.PrefixChangeLog()
	upper := upperAll
	if toInclusive > 0 {
		upper = storage.KeyChangeLog(toInclusive + 1)
	}
	it, err := d.Iter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var out []ChangeRecord
	for valid := it.First(); valid; valid = it.Next() {
		rec, err := decodeChangeRecord(it.Value())
		if err != nil {
			return nil, fmt.Errorf("decode change record at %x: %w", it.Key(), err)
		}
		if rec.Table != table {
			continue
		}
		if untilUnixNano > 0 && rec.UnixNano > untilUnixNano {
			break
		}
		out = append(out, rec)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	return out, nil
}

// StreamRecords returns stream-enabled change records for table starting at
// fromSequence. nextSequence is the next changelog sequence a caller should use
// even when records from other tables or non-stream writes are skipped.
func (d *DB) StreamRecords(table string, fromSequence, toSequence uint64, limit int, maxBytes int64) ([]ChangeRecord, uint64, error) {
	if fromSequence == 0 {
		fromSequence = 1
	}
	if stats, ok, err := d.loadStreamRetentionState(table); err != nil {
		return nil, fromSequence, err
	} else if ok && stats.OldestSequence > 0 && fromSequence < stats.OldestSequence {
		return nil, fromSequence, types.ErrStreamTrimmed
	}
	if limit <= 0 {
		limit = 1000
	}
	lower := storage.KeyChangeLog(fromSequence)
	_, upperAll := storage.PrefixChangeLog()
	upper := upperAll
	if toSequence > 0 {
		upper = storage.KeyChangeLog(toSequence + 1)
	}
	it, err := d.Iter(lower, upper)
	if err != nil {
		return nil, fromSequence, err
	}
	defer it.Close()

	nextSequence := fromSequence
	var out []ChangeRecord
	var bytes int64
	for valid := it.First(); valid; valid = it.Next() {
		raw := it.Value()
		rec, err := decodeChangeRecord(raw)
		if err != nil {
			return nil, nextSequence, fmt.Errorf("decode change record at %x: %w", it.Key(), err)
		}
		if rec.Table != table || !rec.StreamRecord {
			if rec.Index >= nextSequence {
				nextSequence = rec.Index + 1
			}
			continue
		}
		recBytes := int64(len(raw))
		if maxBytes > 0 && len(out) > 0 && bytes+recBytes > maxBytes {
			break
		}
		out = append(out, rec)
		bytes += recBytes
		if rec.Index >= nextSequence {
			nextSequence = rec.Index + 1
		}
		if len(out) >= limit {
			break
		}
	}
	if err := it.Error(); err != nil {
		return nil, nextSequence, err
	}
	return out, nextSequence, nil
}
