package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

type ChangeOp string

const (
	ChangePut    ChangeOp = "put"
	ChangeDelete ChangeOp = "delete"
)

type ChangeRecord struct {
	Index    uint64     `json:"index"`
	UnixNano int64      `json:"unixNano"`
	Op       ChangeOp   `json:"op"`
	Table    string     `json:"table"`
	Key      types.Item `json:"key,omitempty"`
	Item     types.Item `json:"item,omitempty"`
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
	if rec.UnixNano == 0 {
		rec.UnixNano = time.Now().UnixNano()
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
