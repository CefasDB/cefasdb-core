package replication

import (
	"bytes"
	"strings"
	"testing"

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
	resp := newFSM(db).Apply(&hraft.Log{Index: 1, Type: hraft.LogCommand, Data: encoded})
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
