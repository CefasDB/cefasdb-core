package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/osvaldoandrade/cefas/pkg/types"
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
	if err := b.Set(changeCounterKey, counter[:], nil); err != nil {
		return rec, err
	}
	if err := b.Set(KeyChangeLog(rec.Index), raw, nil); err != nil {
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
	raw, err := d.Get(changeCounterKey)
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

func (d *DB) changeRecordsAfter(table string, fromExclusive, toInclusive uint64, untilUnixNano int64) ([]ChangeRecord, error) {
	lower := KeyChangeLog(fromExclusive + 1)
	_, upperAll := PrefixChangeLog()
	upper := upperAll
	if toInclusive > 0 {
		upper = KeyChangeLog(toInclusive + 1)
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
