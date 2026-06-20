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
	d.changeMu.Lock()
	defer d.changeMu.Unlock()
	if d.changeIndex == 0 {
		idx, err := d.loadChangeIndexLocked()
		if err != nil {
			return rec, err
		}
		d.changeIndex = idx
	}
	d.changeIndex++
	rec.Index = d.changeIndex
	if rec.StreamRecord {
		rec.SequenceNumber = strconv.FormatUint(rec.Index, 10)
	}
	if rec.UnixNano == 0 {
		rec.UnixNano = time.Now().UnixNano()
	}
	if rec.StreamRecord && rec.SizeBytes == 0 {
		rec.SizeBytes = approximateChangeRecordSize(rec)
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return rec, fmt.Errorf("marshal change record: %w", err)
	}
	var counter [8]byte
	binary.BigEndian.PutUint64(counter[:], rec.Index)
	if err := b.Set(storage.ChangeCounterKey, counter[:], nil); err != nil {
		return rec, err
	}
	if err := b.Set(storage.KeyChangeLog(rec.Index), raw, nil); err != nil {
		return rec, err
	}
	return rec, nil
}

func newChangeRecord(td types.TableDescriptor, op ChangeOp, key, oldItem, newItem types.Item) ChangeRecord {
	rec := ChangeRecord{
		Op:    op,
		Table: td.Name,
		Key:   cloneChangeItem(key),
	}
	if op == ChangePut {
		rec.Item = cloneChangeItem(newItem)
	}
	applyStreamRecordFields(td, &rec, oldItem, newItem)
	return rec
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
	rec.StreamRecord = true
	rec.StreamViewType = view
}

func streamRecordIncludesOldImage(view string, event ChangeEventName) bool {
	if event == ChangeEventInsert {
		return false
	}
	return view == types.StreamViewTypeOldImage || view == types.StreamViewTypeNewAndOldImages
}

func streamRecordIncludesNewImage(view string, event ChangeEventName) bool {
	if event == ChangeEventRemove {
		return false
	}
	return view == types.StreamViewTypeNewImage || view == types.StreamViewTypeNewAndOldImages
}

func approximateChangeRecordSize(rec ChangeRecord) int64 {
	payload := struct {
		SequenceNumber string          `json:"sequenceNumber,omitempty"`
		EventName      ChangeEventName `json:"eventName,omitempty"`
		Table          string          `json:"table,omitempty"`
		Key            types.Item      `json:"key,omitempty"`
		OldItem        types.Item      `json:"oldItem,omitempty"`
		NewItem        types.Item      `json:"newItem,omitempty"`
		StreamViewType string          `json:"streamViewType,omitempty"`
	}{
		SequenceNumber: rec.SequenceNumber,
		EventName:      rec.EventName,
		Table:          rec.Table,
		Key:            rec.Key,
		OldItem:        rec.OldItem,
		NewItem:        rec.NewItem,
		StreamViewType: rec.StreamViewType,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0
	}
	return int64(len(raw))
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

func (d *DB) loadChangeIndexLocked() (uint64, error) {
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

func (d *DB) CurrentChangeIndex() (uint64, error) {
	d.changeMu.Lock()
	defer d.changeMu.Unlock()
	if d.changeIndex != 0 {
		return d.changeIndex, nil
	}
	idx, err := d.loadChangeIndexLocked()
	if err != nil {
		return 0, err
	}
	d.changeIndex = idx
	return idx, nil
}

func (d *DB) refreshStreamRetentionAfterWrite(td types.TableDescriptor) error {
	if td.StreamSpecification == nil || !td.StreamSpecification.StreamEnabled {
		return nil
	}
	_, err := d.ApplyStreamRetention(td.Name, time.Now())
	return err
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
	d.changeMu.Lock()
	defer d.changeMu.Unlock()
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
		var rec ChangeRecord
		raw := append([]byte(nil), it.Value()...)
		if err := json.Unmarshal(raw, &rec); err != nil {
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
		var rec ChangeRecord
		raw := append([]byte(nil), it.Value()...)
		if err := json.Unmarshal(raw, &rec); err != nil {
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
		var rec ChangeRecord
		raw := append([]byte(nil), it.Value()...)
		if err := json.Unmarshal(raw, &rec); err != nil {
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
