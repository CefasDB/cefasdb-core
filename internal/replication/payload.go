package replication

import (
	"bytes"
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

var raftSnappyPayloadMagic = []byte{0x89, 'C', 'F', 'R', 'A', 'F', 'T', 0x01}

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

func (e *raftPayloadEncoder) Encode(repr []byte) ([]byte, error) {
	if e == nil {
		return append([]byte(nil), repr...), nil
	}
	e.rawBytes.Add(uint64(len(repr)))
	if e.mode != LogCompressionSnappy || len(repr) < e.minBytes {
		return e.recordRaw(repr), nil
	}

	now := time.Now()
	if e.skipCooldown > 0 && now.UnixNano() < e.skipUntilUnixNS.Load() {
		return e.recordSkipped(repr), nil
	}

	compressed := snappy.Encode(nil, repr)
	encodedLen := len(compressed) + len(raftSnappyPayloadMagic)
	savingsRatio := 1 - float64(encodedLen)/float64(len(repr))
	if encodedLen >= len(repr) || savingsRatio < e.minSavingsRatio {
		if e.skipCooldown > 0 {
			e.skipUntilUnixNS.Store(now.Add(e.skipCooldown).UnixNano())
		}
		return e.recordRaw(repr), nil
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
	encoder, err := newRaftPayloadEncoder(Config{
		LogCompression:         compression,
		LogCompressionMinBytes: raftPayloadCompressionDefaultMinBytes,
	})
	if err != nil {
		return nil, err
	}
	return encoder.Encode(repr)
}

func decodeRaftPayload(data []byte) ([]byte, error) {
	if !bytes.HasPrefix(data, raftSnappyPayloadMagic) {
		return append([]byte(nil), data...), nil
	}
	out, err := snappy.Decode(nil, data[len(raftSnappyPayloadMagic):])
	if err != nil {
		return nil, fmt.Errorf("snappy decode: %w", err)
	}
	return out, nil
}
