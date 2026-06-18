package replication

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/golang/snappy"
)

const (
	LogCompressionNone   = "none"
	LogCompressionSnappy = "snappy"

	raftPayloadCompressionMinBytes = 1024
)

var raftSnappyPayloadMagic = []byte{0x89, 'C', 'F', 'R', 'A', 'F', 'T', 0x01}

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

func encodeRaftPayload(repr []byte, compression string) ([]byte, error) {
	mode, err := normalizeLogCompression(compression)
	if err != nil {
		return nil, err
	}
	if mode != LogCompressionSnappy || len(repr) < raftPayloadCompressionMinBytes {
		return append([]byte(nil), repr...), nil
	}

	compressed := snappy.Encode(nil, repr)
	if len(compressed)+len(raftSnappyPayloadMagic) >= len(repr) {
		return append([]byte(nil), repr...), nil
	}
	out := make([]byte, 0, len(raftSnappyPayloadMagic)+len(compressed))
	out = append(out, raftSnappyPayloadMagic...)
	out = append(out, compressed...)
	return out, nil
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
