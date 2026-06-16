package replication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	pebbledb "github.com/cockroachdb/pebble"
	codec "github.com/hashicorp/go-msgpack/v2/codec"
	hraft "github.com/hashicorp/raft"
)

// logStore implements hashicorp/raft.LogStore over Pebble. Entries
// live under raft/log/<be8 index> in the raft metadata store. Values are msgpack-encoded
// hraft.Log structs — same encoding hashicorp/raft-boltdb uses, so the
// on-disk shape is unsurprising.
//
// The raft engine never interleaves LogStore writers with itself, so
// no internal locking is needed; multi-write atomicity uses Pebble
// batches.
type logStore struct {
	pebble *pebbledb.DB
}

var (
	logKeyPrefix    = []byte("raft/log/")
	logKeyPrefixEnd = []byte("raft/log0") // one past '/' (0x2F → 0x30)

	errLogNotFound = hraft.ErrLogNotFound

	msgpackHandle = &codec.MsgpackHandle{}
)

func newLogStore(p *pebbledb.DB) *logStore { return &logStore{pebble: p} }

func logKey(index uint64) []byte {
	out := make([]byte, len(logKeyPrefix)+8)
	copy(out, logKeyPrefix)
	binary.BigEndian.PutUint64(out[len(logKeyPrefix):], index)
	return out
}

func indexFromKey(key []byte) (uint64, bool) {
	if !bytes.HasPrefix(key, logKeyPrefix) {
		return 0, false
	}
	suffix := key[len(logKeyPrefix):]
	if len(suffix) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(suffix), true
}

func encodeLog(log *hraft.Log) ([]byte, error) {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf, msgpackHandle)
	if err := enc.Encode(log); err != nil {
		return nil, fmt.Errorf("encode log: %w", err)
	}
	return buf.Bytes(), nil
}

func decodeLog(value []byte, out *hraft.Log) error {
	dec := codec.NewDecoderBytes(value, msgpackHandle)
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("decode log: %w", err)
	}
	return nil
}

func (s *logStore) FirstIndex() (uint64, error) {
	iter, err := s.pebble.NewIter(&pebbledb.IterOptions{LowerBound: logKeyPrefix, UpperBound: logKeyPrefixEnd})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	if !iter.First() {
		return 0, iter.Error()
	}
	idx, ok := indexFromKey(iter.Key())
	if !ok {
		return 0, fmt.Errorf("logStore: malformed key %q", iter.Key())
	}
	return idx, iter.Error()
}

func (s *logStore) LastIndex() (uint64, error) {
	iter, err := s.pebble.NewIter(&pebbledb.IterOptions{LowerBound: logKeyPrefix, UpperBound: logKeyPrefixEnd})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	if !iter.Last() {
		return 0, iter.Error()
	}
	idx, ok := indexFromKey(iter.Key())
	if !ok {
		return 0, fmt.Errorf("logStore: malformed key %q", iter.Key())
	}
	return idx, iter.Error()
}

func (s *logStore) GetLog(index uint64, log *hraft.Log) error {
	val, closer, err := s.pebble.Get(logKey(index))
	if errors.Is(err, pebbledb.ErrNotFound) {
		return errLogNotFound
	}
	if err != nil {
		return err
	}
	defer closer.Close()
	return decodeLog(val, log)
}

func (s *logStore) StoreLog(log *hraft.Log) error {
	enc, err := encodeLog(log)
	if err != nil {
		return err
	}
	return s.pebble.Set(logKey(log.Index), enc, pebbledb.NoSync)
}

func (s *logStore) StoreLogs(logs []*hraft.Log) error {
	if len(logs) == 0 {
		return nil
	}
	b := s.pebble.NewBatch()
	defer b.Close()
	for _, log := range logs {
		enc, err := encodeLog(log)
		if err != nil {
			return err
		}
		if err := b.Set(logKey(log.Index), enc, nil); err != nil {
			return err
		}
	}
	return b.Commit(pebbledb.NoSync)
}

func (s *logStore) DeleteRange(min, max uint64) error {
	if min > max {
		return fmt.Errorf("logStore.DeleteRange: min=%d > max=%d", min, max)
	}
	// Pebble DeleteRange end-key is exclusive; pass max+1 to make
	// the range inclusive on both ends.
	end := logKey(max)
	endNext := make([]byte, len(end))
	copy(endNext, end)
	for i := len(endNext) - 1; i >= len(logKeyPrefix); i-- {
		endNext[i]++
		if endNext[i] != 0 {
			break
		}
	}
	return s.pebble.DeleteRange(logKey(min), endNext, pebbledb.NoSync)
}
