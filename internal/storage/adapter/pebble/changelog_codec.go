package pebble

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// Wire format dispatch byte at offset 0 of every changelog record value.
//
//	0x7B ('{')          legacy JSON record (json.Marshal(ChangeRecord))
//	changeFmtBinaryV1   length-prefixed binary record (see encodeChangeRecordBinaryV1)
//
// Readers route on this byte; writers always emit changeFmtBinaryV1.
// JSON support stays in the reader until at least one release after this
// PR ships so existing changelogs replay.
const (
	changeFmtBinaryV1 byte = 0x02

	// Op opcodes inside binary v1.
	opPutByte    byte = 'p'
	opDeleteByte byte = 'd'

	// EventName opcodes inside binary v1.
	evNone   byte = 0
	evInsert byte = 'I'
	evModify byte = 'M'
	evRemove byte = 'R'

	// Field bitmap for optional fields in binary v1.
	flagSeqNum         uint16 = 1 << 0
	flagItem           uint16 = 1 << 1
	flagStreamRecord   uint16 = 1 << 2
	flagOldItem        uint16 = 1 << 3
	flagNewItem        uint16 = 1 << 4
	flagStreamViewType uint16 = 1 << 5
	flagSizeBytes      uint16 = 1 << 6
	flagKey            uint16 = 1 << 7
	// #524 idempotency markers — additive, off-default for legacy
	// records that pre-date the field set.
	flagBatchID    uint16 = 1 << 8
	flagSeqInBatch uint16 = 1 << 9
	flagOpKind     uint16 = 1 << 10
)

// encodeChangeRecord serializes rec to the on-disk binary format. Always
// emits the current version (changeFmtBinaryV1). dst is appended to and
// may be nil; pass a pooled buffer to avoid per-call allocations.
func encodeChangeRecord(dst []byte, rec ChangeRecord) ([]byte, error) {
	dst = append(dst, changeFmtBinaryV1)
	dst = binary.BigEndian.AppendUint64(dst, rec.Index)
	dst = binary.BigEndian.AppendUint64(dst, uint64(rec.UnixNano))

	switch rec.Op {
	case ChangePut:
		dst = append(dst, opPutByte)
	case ChangeDelete:
		dst = append(dst, opDeleteByte)
	default:
		return dst, fmt.Errorf("encode change record: unknown op %q", rec.Op)
	}

	var flags uint16
	if rec.SequenceNumber != "" {
		flags |= flagSeqNum
	}
	if rec.Item != nil {
		flags |= flagItem
	}
	if rec.StreamRecord {
		flags |= flagStreamRecord
	}
	if rec.OldItem != nil {
		flags |= flagOldItem
	}
	if rec.NewItem != nil {
		flags |= flagNewItem
	}
	if rec.StreamViewType != "" {
		flags |= flagStreamViewType
	}
	if rec.SizeBytes != 0 {
		flags |= flagSizeBytes
	}
	if rec.Key != nil {
		flags |= flagKey
	}
	if rec.BatchID != "" {
		flags |= flagBatchID
	}
	if rec.SeqInBatch != 0 {
		flags |= flagSeqInBatch
	}
	if rec.OpKind != "" {
		flags |= flagOpKind
	}
	dst = binary.BigEndian.AppendUint16(dst, flags)

	// Event name as a single byte: cheap to read, deterministic, and
	// covers every value Streams currently exposes.
	switch rec.EventName {
	case "":
		dst = append(dst, evNone)
	case ChangeEventInsert:
		dst = append(dst, evInsert)
	case ChangeEventModify:
		dst = append(dst, evModify)
	case ChangeEventRemove:
		dst = append(dst, evRemove)
	default:
		return dst, fmt.Errorf("encode change record: unknown event name %q", rec.EventName)
	}

	dst = appendLenString(dst, rec.Table)
	if flags&flagSeqNum != 0 {
		dst = appendLenString(dst, rec.SequenceNumber)
	}
	if flags&flagStreamViewType != 0 {
		dst = appendLenString(dst, rec.StreamViewType)
	}
	if flags&flagSizeBytes != 0 {
		dst = binary.BigEndian.AppendUint64(dst, uint64(rec.SizeBytes))
	}

	var err error
	if flags&flagKey != 0 {
		dst, err = appendItem(dst, rec.Key)
		if err != nil {
			return dst, fmt.Errorf("encode change record key: %w", err)
		}
	}
	if flags&flagItem != 0 {
		dst, err = appendItem(dst, rec.Item)
		if err != nil {
			return dst, fmt.Errorf("encode change record item: %w", err)
		}
	}
	if flags&flagOldItem != 0 {
		dst, err = appendItem(dst, rec.OldItem)
		if err != nil {
			return dst, fmt.Errorf("encode change record old item: %w", err)
		}
	}
	if flags&flagNewItem != 0 {
		dst, err = appendItem(dst, rec.NewItem)
		if err != nil {
			return dst, fmt.Errorf("encode change record new item: %w", err)
		}
	}
	if flags&flagBatchID != 0 {
		dst = appendLenString(dst, rec.BatchID)
	}
	if flags&flagSeqInBatch != 0 {
		dst = binary.BigEndian.AppendUint32(dst, uint32(rec.SeqInBatch))
	}
	if flags&flagOpKind != 0 {
		dst = appendLenString(dst, rec.OpKind)
	}
	return dst, nil
}

// decodeChangeRecord parses raw produced by encodeChangeRecord or by the
// legacy json.Marshal path. Dispatch on the first byte: binary records
// start with changeFmtBinaryV1; legacy JSON records start with '{'.
func decodeChangeRecord(raw []byte) (ChangeRecord, error) {
	if len(raw) == 0 {
		return ChangeRecord{}, fmt.Errorf("decode change record: empty payload")
	}
	switch raw[0] {
	case changeFmtBinaryV1:
		return decodeChangeRecordBinaryV1(raw[1:])
	case '{':
		var rec ChangeRecord
		if err := json.Unmarshal(raw, &rec); err != nil {
			return ChangeRecord{}, fmt.Errorf("decode change record (json): %w", err)
		}
		return rec, nil
	default:
		return ChangeRecord{}, fmt.Errorf("decode change record: unknown format byte 0x%02x", raw[0])
	}
}

func decodeChangeRecordBinaryV1(p []byte) (ChangeRecord, error) {
	var rec ChangeRecord
	if len(p) < 8+8+1+2+1 {
		return rec, fmt.Errorf("decode change record v1: short header")
	}
	rec.Index = binary.BigEndian.Uint64(p[:8])
	p = p[8:]
	rec.UnixNano = int64(binary.BigEndian.Uint64(p[:8]))
	p = p[8:]

	switch p[0] {
	case opPutByte:
		rec.Op = ChangePut
	case opDeleteByte:
		rec.Op = ChangeDelete
	default:
		return rec, fmt.Errorf("decode change record v1: unknown op byte 0x%02x", p[0])
	}
	p = p[1:]

	flags := binary.BigEndian.Uint16(p[:2])
	p = p[2:]
	rec.StreamRecord = flags&flagStreamRecord != 0

	switch p[0] {
	case evNone:
		rec.EventName = ""
	case evInsert:
		rec.EventName = ChangeEventInsert
	case evModify:
		rec.EventName = ChangeEventModify
	case evRemove:
		rec.EventName = ChangeEventRemove
	default:
		return rec, fmt.Errorf("decode change record v1: unknown event byte 0x%02x", p[0])
	}
	p = p[1:]

	var (
		s   string
		err error
	)
	if s, p, err = readLenString(p); err != nil {
		return rec, fmt.Errorf("decode change record v1 table: %w", err)
	}
	rec.Table = s

	if flags&flagSeqNum != 0 {
		if s, p, err = readLenString(p); err != nil {
			return rec, fmt.Errorf("decode change record v1 seq: %w", err)
		}
		rec.SequenceNumber = s
	}
	if flags&flagStreamViewType != 0 {
		if s, p, err = readLenString(p); err != nil {
			return rec, fmt.Errorf("decode change record v1 view: %w", err)
		}
		rec.StreamViewType = s
	}
	if flags&flagSizeBytes != 0 {
		if len(p) < 8 {
			return rec, fmt.Errorf("decode change record v1 size: short")
		}
		rec.SizeBytes = int64(binary.BigEndian.Uint64(p[:8]))
		p = p[8:]
	}

	if flags&flagKey != 0 {
		var item types.Item
		if item, p, err = readItem(p); err != nil {
			return rec, fmt.Errorf("decode change record v1 key: %w", err)
		}
		rec.Key = item
	}
	if flags&flagItem != 0 {
		var item types.Item
		if item, p, err = readItem(p); err != nil {
			return rec, fmt.Errorf("decode change record v1 item: %w", err)
		}
		rec.Item = item
	}
	if flags&flagOldItem != 0 {
		var item types.Item
		if item, p, err = readItem(p); err != nil {
			return rec, fmt.Errorf("decode change record v1 old item: %w", err)
		}
		rec.OldItem = item
	}
	if flags&flagNewItem != 0 {
		var item types.Item
		if item, p, err = readItem(p); err != nil {
			return rec, fmt.Errorf("decode change record v1 new item: %w", err)
		}
		rec.NewItem = item
	}
	if flags&flagBatchID != 0 {
		if s, p, err = readLenString(p); err != nil {
			return rec, fmt.Errorf("decode change record v1 batch id: %w", err)
		}
		rec.BatchID = s
	}
	if flags&flagSeqInBatch != 0 {
		if len(p) < 4 {
			return rec, fmt.Errorf("decode change record v1 seq in batch: short")
		}
		rec.SeqInBatch = int32(binary.BigEndian.Uint32(p[:4]))
		p = p[4:]
	}
	if flags&flagOpKind != 0 {
		if s, _, err = readLenString(p); err != nil {
			return rec, fmt.Errorf("decode change record v1 op kind: %w", err)
		}
		rec.OpKind = s
	}
	return rec, nil
}

func appendLenString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func readLenString(p []byte) (string, []byte, error) {
	n, rest, err := readUvarintLocal(p)
	if err != nil {
		return "", nil, err
	}
	if uint64(len(rest)) < n {
		return "", nil, fmt.Errorf("short string")
	}
	return string(rest[:n]), rest[n:], nil
}

func appendItem(dst []byte, item types.Item) ([]byte, error) {
	enc, err := storage.EncodeItem(item)
	if err != nil {
		return dst, err
	}
	dst = binary.AppendUvarint(dst, uint64(len(enc)))
	return append(dst, enc...), nil
}

func readItem(p []byte) (types.Item, []byte, error) {
	n, rest, err := readUvarintLocal(p)
	if err != nil {
		return nil, nil, err
	}
	if uint64(len(rest)) < n {
		return nil, nil, fmt.Errorf("short item")
	}
	item, err := storage.DecodeItem(rest[:n])
	if err != nil {
		return nil, nil, err
	}
	return item, rest[n:], nil
}

func readUvarintLocal(p []byte) (uint64, []byte, error) {
	v, n := binary.Uvarint(p)
	if n <= 0 {
		return 0, nil, fmt.Errorf("read uvarint: %d", n)
	}
	return v, p[n:], nil
}
