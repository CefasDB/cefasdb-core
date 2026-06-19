package replication

import (
	"bytes"
	"strings"
	"testing"
	"time"

	pebbledb "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	hraft "github.com/hashicorp/raft"
)

func TestRaftPayloadCompressionRoundTrip(t *testing.T) {
	repr := []byte(strings.Repeat("payload-", 2048))

	encoded, err := encodeRaftPayload(repr, LogCompressionSnappy)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if !bytes.HasPrefix(encoded, raftSnappyPayloadMagic) {
		t.Fatalf("encoded payload is not marked as compressed")
	}
	if len(encoded) >= len(repr) {
		t.Fatalf("encoded length = %d, want less than %d", len(encoded), len(repr))
	}

	decoded, err := decodeRaftPayload(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(decoded, repr) {
		t.Fatalf("decoded payload mismatch")
	}
}

func TestRaftPayloadCompressionKeepsSmallPayloadRaw(t *testing.T) {
	repr := []byte("small payload")

	encoded, err := encodeRaftPayload(repr, LogCompressionSnappy)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if bytes.HasPrefix(encoded, raftSnappyPayloadMagic) {
		t.Fatalf("small payload should not be compressed")
	}
	if !bytes.Equal(encoded, repr) {
		t.Fatalf("small payload changed")
	}
}

func TestRaftPayloadCompressionNoneKeepsPayloadRaw(t *testing.T) {
	repr := []byte(strings.Repeat("payload-", 2048))

	encoded, err := encodeRaftPayload(repr, LogCompressionNone)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if bytes.HasPrefix(encoded, raftSnappyPayloadMagic) {
		t.Fatalf("compression disabled but payload was compressed")
	}
	if !bytes.Equal(encoded, repr) {
		t.Fatalf("raw payload changed")
	}
}

func TestRaftPayloadMetadataRoundTrip(t *testing.T) {
	repr := []byte(strings.Repeat("payload-", 2048))
	encoded, err := encodeRaftPayloadWithTables(repr, []string{"t1", "t2", "t1"}, LogCompressionSnappy)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	decoded, err := decodeRaftPayloadWithMetadata(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(decoded.Repr, repr) {
		t.Fatalf("decoded repr mismatch")
	}
	if got, want := strings.Join(decoded.ItemCacheTables, ","), "t1,t2"; got != want {
		t.Fatalf("tables = %q, want %q", got, want)
	}
}

func TestRaftPayloadCompressionSkipsAfterUnhelpfulPayload(t *testing.T) {
	repr := deterministicBytes(4096)
	encoder, err := newRaftPayloadEncoder(Config{
		LogCompression:             LogCompressionSnappy,
		LogCompressionMinBytes:     1024,
		LogCompressionSkipCooldown: time.Second,
	})
	if err != nil {
		t.Fatalf("encoder: %v", err)
	}

	encoded, err := encoder.Encode(repr, nil)
	if err != nil {
		t.Fatalf("encode first: %v", err)
	}
	if bytes.HasPrefix(encoded, raftSnappyPayloadMagic) {
		t.Fatalf("incompressible payload should stay raw")
	}
	if !bytes.Equal(encoded, repr) {
		t.Fatalf("raw payload changed")
	}

	encoded, err = encoder.Encode(repr, nil)
	if err != nil {
		t.Fatalf("encode second: %v", err)
	}
	if bytes.HasPrefix(encoded, raftSnappyPayloadMagic) {
		t.Fatalf("payload should be skipped during cooldown")
	}

	stats := encoder.Stats()
	if stats.RawBytes != uint64(len(repr)*2) || stats.EncodedBytes != uint64(len(repr)*2) {
		t.Fatalf("byte stats = %+v", stats)
	}
	if stats.CompressedPayloads != 0 || stats.RawPayloads != 1 || stats.SkippedPayloads != 1 {
		t.Fatalf("payload stats = %+v", stats)
	}
}

func TestFSMApplyCompressedPayload(t *testing.T) {
	db, err := pebbledb.Open("/test", &pebbledb.Options{FS: vfs.NewMem()})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	defer db.Close()

	batch := db.NewBatch()
	if err := batch.Set([]byte("cefas/table/T/item/1"), []byte(strings.Repeat("value-", 256)), nil); err != nil {
		t.Fatalf("batch set: %v", err)
	}
	repr := append([]byte(nil), batch.Repr()...)
	if err := batch.Close(); err != nil {
		t.Fatalf("batch close: %v", err)
	}

	encoded, err := encodeRaftPayload(repr, LogCompressionSnappy)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	resp := newFSM(db, nil, nil).Apply(&hraft.Log{Index: 1, Type: hraft.LogCommand, Data: encoded})
	if resp != nil {
		t.Fatalf("apply response = %v", resp)
	}

	got, closer, err := db.Get([]byte("cefas/table/T/item/1"))
	if err != nil {
		t.Fatalf("get applied key: %v", err)
	}
	defer closer.Close()
	if !bytes.Equal(got, []byte(strings.Repeat("value-", 256))) {
		t.Fatalf("applied value mismatch")
	}
}

func deterministicBytes(n int) []byte {
	out := make([]byte, n)
	x := uint64(0x9e3779b97f4a7c15)
	for i := range out {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z ^= z >> 31
		out[i] = byte(z)
	}
	return out
}
