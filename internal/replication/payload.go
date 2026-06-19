package replication

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
)

const (
	LogCompressionNone   = "none"
	LogCompressionSnappy = "snappy"

	raftPayloadCompressionDefaultMinBytes = 1024
)

var (
	raftSnappyPayloadMagic   = []byte{0x89, 'C', 'F', 'R', 'A', 'F', 'T', 0x01}
	raftMetadataPayloadMagic = []byte{0x89, 'C', 'F', 'R', 'A', 'F', 'T', 0x02}
)

// LogCompressionStats is a snapshot of raft log payload compression work.
// Values are cumulative from DB open time.
type LogCompressionStats struct {
	RawBytes           uint64
	EncodedBytes       uint64
	CompressedPayloads uint64
	RawPayloads        uint64
	SkippedPayloads    uint64
}

type raftPayloadEncoder struct {
	mode            string
	minBytes        int
	minSavingsRatio float64
	skipCooldown    time.Duration
	skipUntilUnixNS atomic.Int64

	rawBytes           atomic.Uint64
	encodedBytes       atomic.Uint64
	compressedPayloads atomic.Uint64
	rawPayloads        atomic.Uint64
	skippedPayloads    atomic.Uint64
}

func normalizeLogCompression(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", LogCompressionNone, "off", "false", "disabled":
		return LogCompressionNone, nil
	case LogCompressionSnappy:
		return LogCompressionSnappy, nil
	default:
		return "", fmt.Errorf("unsupported raft log compression %q", mode)
	}
}

func newRaftPayloadEncoder(cfg Config) (*raftPayloadEncoder, error) {
	mode, err := normalizeLogCompression(cfg.LogCompression)
	if err != nil {
		return nil, err
	}
	if cfg.LogCompressionMinBytes < 0 {
		return nil, fmt.Errorf("raft log compression min bytes must be >= 0")
	}
	if cfg.LogCompressionMinSavingsRatio < 0 || cfg.LogCompressionMinSavingsRatio >= 1 {
		return nil, fmt.Errorf("raft log compression min savings ratio must be >= 0 and < 1")
	}
	if cfg.LogCompressionSkipCooldown < 0 {
		return nil, fmt.Errorf("raft log compression skip cooldown must be >= 0")
	}
	minBytes := cfg.LogCompressionMinBytes
	if minBytes == 0 {
		minBytes = raftPayloadCompressionDefaultMinBytes
	}
	return &raftPayloadEncoder{
		mode:            mode,
		minBytes:        minBytes,
		minSavingsRatio: cfg.LogCompressionMinSavingsRatio,
		skipCooldown:    cfg.LogCompressionSkipCooldown,
	}, nil
}

func (e *raftPayloadEncoder) Encode(repr []byte, itemCacheTables []string) ([]byte, error) {
	payload := repr
	if tables := normalizePayloadTables(itemCacheTables); len(tables) > 0 {
		payload = encodeRaftMetadataPayload(repr, tables)
	}
	if e == nil {
		return append([]byte(nil), payload...), nil
	}
	e.rawBytes.Add(uint64(len(repr)))
	if e.mode != LogCompressionSnappy || len(payload) < e.minBytes {
		return e.recordRaw(payload), nil
	}

	now := time.Now()
	if e.skipCooldown > 0 && now.UnixNano() < e.skipUntilUnixNS.Load() {
		return e.recordSkipped(payload), nil
	}

	compressed := snappy.Encode(nil, payload)
	encodedLen := len(compressed) + len(raftSnappyPayloadMagic)
	savingsRatio := 1 - float64(encodedLen)/float64(len(payload))
	if encodedLen >= len(payload) || savingsRatio < e.minSavingsRatio {
		if e.skipCooldown > 0 {
			e.skipUntilUnixNS.Store(now.Add(e.skipCooldown).UnixNano())
		}
		return e.recordRaw(payload), nil
	}

	out := make([]byte, 0, encodedLen)
	out = append(out, raftSnappyPayloadMagic...)
	out = append(out, compressed...)
	e.encodedBytes.Add(uint64(len(out)))
	e.compressedPayloads.Add(1)
	return out, nil
}

func (e *raftPayloadEncoder) recordRaw(repr []byte) []byte {
	e.encodedBytes.Add(uint64(len(repr)))
	e.rawPayloads.Add(1)
	return append([]byte(nil), repr...)
}

func (e *raftPayloadEncoder) recordSkipped(repr []byte) []byte {
	e.encodedBytes.Add(uint64(len(repr)))
	e.skippedPayloads.Add(1)
	return append([]byte(nil), repr...)
}

func (e *raftPayloadEncoder) Stats() LogCompressionStats {
	if e == nil {
		return LogCompressionStats{}
	}
	return LogCompressionStats{
		RawBytes:           e.rawBytes.Load(),
		EncodedBytes:       e.encodedBytes.Load(),
		CompressedPayloads: e.compressedPayloads.Load(),
		RawPayloads:        e.rawPayloads.Load(),
		SkippedPayloads:    e.skippedPayloads.Load(),
	}
}

func encodeRaftPayload(repr []byte, compression string) ([]byte, error) {
	return encodeRaftPayloadWithTables(repr, nil, compression)
}

func encodeRaftPayloadWithTables(repr []byte, itemCacheTables []string, compression string) ([]byte, error) {
	encoder, err := newRaftPayloadEncoder(Config{
		LogCompression:         compression,
		LogCompressionMinBytes: raftPayloadCompressionDefaultMinBytes,
	})
	if err != nil {
		return nil, err
	}
	return encoder.Encode(repr, itemCacheTables)
}

func decodeRaftPayload(data []byte) ([]byte, error) {
	payload, err := decodeRaftPayloadWithMetadata(data)
	if err != nil {
		return nil, err
	}
	return payload.Repr, nil
}

type decodedRaftPayload struct {
	Repr            []byte
	ItemCacheTables []string
}

func decodeRaftPayloadWithMetadata(data []byte) (decodedRaftPayload, error) {
	if !bytes.HasPrefix(data, raftSnappyPayloadMagic) {
		return decodeRaftMetadataPayload(data)
	}
	out, err := snappy.Decode(nil, data[len(raftSnappyPayloadMagic):])
	if err != nil {
		return decodedRaftPayload{}, fmt.Errorf("snappy decode: %w", err)
	}
	return decodeRaftMetadataPayload(out)
}

func encodeRaftMetadataPayload(repr []byte, tables []string) []byte {
	out := make([]byte, 0, len(raftMetadataPayloadMagic)+len(repr)+len(tables)*16)
	out = append(out, raftMetadataPayloadMagic...)
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], uint64(len(tables)))
	out = append(out, scratch[:n]...)
	for _, table := range tables {
		n = binary.PutUvarint(scratch[:], uint64(len(table)))
		out = append(out, scratch[:n]...)
		out = append(out, table...)
	}
	out = append(out, repr...)
	return out
}

func decodeRaftMetadataPayload(data []byte) (decodedRaftPayload, error) {
	if !bytes.HasPrefix(data, raftMetadataPayloadMagic) {
		return decodedRaftPayload{Repr: append([]byte(nil), data...)}, nil
	}
	rest := data[len(raftMetadataPayloadMagic):]
	count, n := binary.Uvarint(rest)
	if n <= 0 {
		return decodedRaftPayload{}, fmt.Errorf("metadata payload: table count")
	}
	rest = rest[n:]
	tables := make([]string, 0, count)
	for i := uint64(0); i < count; i++ {
		l, n := binary.Uvarint(rest)
		if n <= 0 {
			return decodedRaftPayload{}, fmt.Errorf("metadata payload: table length")
		}
		rest = rest[n:]
		if uint64(len(rest)) < l {
			return decodedRaftPayload{}, fmt.Errorf("metadata payload: truncated table")
		}
		tables = append(tables, string(rest[:l]))
		rest = rest[l:]
	}
	return decodedRaftPayload{
		Repr:            append([]byte(nil), rest...),
		ItemCacheTables: tables,
	}, nil
}

func normalizePayloadTables(tables []string) []string {
	if len(tables) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(tables))
	out := make([]string, 0, len(tables))
	for _, table := range tables {
		if table == "" {
			continue
		}
		if _, ok := seen[table]; ok {
			continue
		}
		seen[table] = struct{}{}
		out = append(out, table)
	}
	return out
}
