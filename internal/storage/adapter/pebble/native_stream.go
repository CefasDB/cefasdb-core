package pebble

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
	pebbledb "github.com/cockroachdb/pebble"
)

const nativeImageExpiryPrefix = storage.Namespace + "cdc-image-expiry/"

// Native-stream ordering is enforced by the committed watermark in
// stream_fence.go: writers register before allocating a sequence (after their
// primary-key locks) and readers bound their snapshot at the watermark, so
// another table's committed sequence cannot let a cursor skip a native write
// whose sequence was allocated but not committed.

func hideExpiredNativeImages(rec *ChangeRecord, now time.Time) {
	if rec.PayloadExpiresAt > 0 && rec.PayloadExpiresAt <= now.Unix() {
		rec.Item, rec.NewItem = nil, nil
	}
	if rec.OldPayloadExpiresAt > 0 && rec.OldPayloadExpiresAt <= now.Unix() {
		rec.OldItem = nil
	}
}

func nativeCustodyPrefix(td types.TableDescriptor, key types.Item) ([]byte, []byte, error) {
	pk, sk, err := extractKeyBytes(key, td.KeySchema)
	if err != nil {
		return nil, nil, err
	}
	// Hash the complete canonical keys, not the legacy truncated primary hash.
	raw := binary.BigEndian.AppendUint64(nil, uint64(len(pk)))
	raw = append(raw, pk...)
	raw = append(raw, sk...)
	digest := sha256.Sum256(raw)
	low := append([]byte(storage.TableBase(td.Name)+"cdc-custody/"), digest[:]...)
	low = append(low, '/')
	high := append([]byte(nil), low...)
	high[len(high)-1]++
	return low, high, nil
}

func deleteNativeCustody(b *pebbledb.Batch, td types.TableDescriptor, key types.Item) error {
	if len(td.TextIndexes) == 0 {
		return nil
	}
	low, high, err := nativeCustodyPrefix(td, key)
	if err != nil {
		return err
	}
	return b.DeleteRange(low, high, nil)
}

func nativeImageExpiry(td types.TableDescriptor, item types.Item) int64 {
	if td.TTLAttribute == "" || item == nil {
		return 0
	}
	av, ok := item[td.TTLAttribute]
	if !ok {
		return 0
	}
	expiry, err := strconv.ParseInt(av.N, 10, 64)
	if av.T != types.AttrN || err != nil || expiry < 0 {
		return 1
	}
	return expiry
}

func imageExpiryKey(expiry int64, payloadKey []byte) []byte {
	if expiry < 0 {
		return nil
	}
	key := binary.BigEndian.AppendUint64([]byte(nativeImageExpiryPrefix), uint64(expiry))
	return append(key, payloadKey...)
}

// persistNativeImages keeps every payload outside the resumable metadata log.
// Per-image expiration also covers old images of an updated native row.
func persistNativeImages(b *pebbledb.Batch, rec *ChangeRecord) error {
	td := rec.nativeDescriptor
	if td == nil {
		return nil
	}
	if rec.Op == ChangeDelete {
		rec.Item, rec.OldItem, rec.NewItem = nil, nil, nil
		return nil
	}
	prefix, _, err := nativeCustodyPrefix(*td, rec.Key)
	if err != nil {
		return err
	}
	prefix = binary.BigEndian.AppendUint64(prefix, rec.Index)
	rec.PayloadKey = string(prefix)
	rec.PayloadExpiresAt = nativeImageExpiry(*td, rec.Item)
	rec.OldPayloadExpiresAt = nativeImageExpiry(*td, rec.OldItem)
	for _, image := range []struct {
		suffix byte
		item   types.Item
		expiry int64
	}{{'n', rec.Item, rec.PayloadExpiresAt}, {'o', rec.OldItem, rec.OldPayloadExpiresAt}} {
		if image.item == nil {
			continue
		}
		key := append(append([]byte(nil), prefix...), image.suffix)
		raw, err := storage.EncodeItem(image.item)
		if err != nil {
			return err
		}
		if err := b.Set(key, raw, nil); err != nil {
			return err
		}
		if image.expiry > 0 {
			if err := b.Set(imageExpiryKey(image.expiry, key), nil, nil); err != nil {
				return err
			}
		}
	}
	if rec.Item != nil {
		rec.PayloadImages |= 1
	}
	if rec.OldItem != nil {
		rec.PayloadImages |= 2
	}
	if rec.NewItem != nil {
		rec.PayloadImages |= 4
	}
	rec.Item, rec.OldItem, rec.NewItem = nil, nil, nil
	return nil
}

func (d *DB) resolveNativeImages(snap *pebbledb.Snapshot, rec *ChangeRecord, now time.Time) error {
	if rec.PayloadKey == "" {
		return nil
	}
	td, err := d.visibilityDescriptor(rec.Table)
	if err != nil {
		return err
	}
	if len(td.TextIndexes) == 0 {
		return fmt.Errorf("native custody descriptor unavailable")
	}
	prefix, _, err := nativeCustodyPrefix(td, rec.Key)
	if err != nil {
		return err
	}
	expected := binary.BigEndian.AppendUint64(prefix, rec.Index)
	if !bytes.Equal(expected, []byte(rec.PayloadKey)) || rec.PayloadImages&^uint8(7) != 0 {
		return fmt.Errorf("native custody identity mismatch")
	}
	read := func(suffix byte, expiry int64) (types.Item, error) {
		if expiry > 0 && expiry <= now.Unix() {
			return nil, nil
		}
		raw, closer, err := snap.Get(append([]byte(rec.PayloadKey), suffix))
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		defer closer.Close()
		item, err := storage.DecodeItem(raw)
		if err != nil {
			return nil, err
		}
		if nativeImageExpiry(td, item) != expiry {
			return nil, fmt.Errorf("native custody expiry mismatch")
		}
		key, _, err := nativeCustodyPrefix(td, keyItemFromItem(item, td.KeySchema))
		if err != nil || !bytes.Equal(key, prefix) {
			return nil, fmt.Errorf("native custody image identity mismatch")
		}
		return item, nil
	}
	if rec.PayloadImages&5 != 0 {
		item, err := read('n', rec.PayloadExpiresAt)
		if err != nil {
			return err
		}
		if rec.PayloadImages&1 != 0 {
			rec.Item = item
		}
		if rec.PayloadImages&4 != 0 {
			rec.NewItem = item
		}
	}
	if rec.PayloadImages&2 != 0 {
		item, err := read('o', rec.OldPayloadExpiresAt)
		if err != nil {
			return err
		}
		rec.OldItem = item
	}
	return nil
}

func deleteNativeRecordPayload(b *pebbledb.Batch, rec ChangeRecord) error {
	if rec.PayloadKey == "" {
		return nil
	}
	for _, image := range []struct {
		suffix byte
		expiry int64
	}{{'n', rec.PayloadExpiresAt}, {'o', rec.OldPayloadExpiresAt}} {
		key := append([]byte(rec.PayloadKey), image.suffix)
		if err := b.Delete(key, nil); err != nil {
			return err
		}
		if image.expiry > 0 {
			if err := b.Delete(imageExpiryKey(image.expiry, key), nil); err != nil {
				return err
			}
		}
	}
	return nil
}

// Expired images are a separate bounded cleanup, not a CDC trim: consumers can
// still advance over the payload-free sequence without a fabricated gap.
func (d *DB) expireNativeImages(now time.Time) (int, error) {
	if d.repl != nil && !d.repl.IsLeader() {
		return 0, nil
	}
	unix := now.Unix()
	if unix < 0 {
		return 0, nil
	}
	lower := []byte(nativeImageExpiryPrefix)
	upper := binary.BigEndian.AppendUint64(append([]byte(nil), lower...), uint64(unix)+1)
	it, err := d.Iter(lower, upper)
	if err != nil {
		return 0, err
	}
	defer it.Close()
	b := d.Batch()
	defer b.Close()
	limit := d.streamRetention.BatchSize
	if limit <= 0 {
		limit = DefaultStreamRetentionCleanupBatch
	}
	if limit > 10000 {
		limit = 10000
	}
	n := 0
	for ok := it.First(); ok && n < limit; ok = it.Next() {
		key := it.Key()
		if len(key) <= len(lower)+8 {
			return 0, fmt.Errorf("invalid native image expiry key")
		}
		if err := b.Delete(key[len(lower)+8:], nil); err != nil {
			return 0, err
		}
		if err := b.Delete(key, nil); err != nil {
			return 0, err
		}
		n++
	}
	if err := it.Error(); err != nil {
		return 0, err
	}
	if n <= 0 {
		return 0, nil
	}
	if err := d.CommitBatch(b); err != nil {
		return 0, err
	}
	d.ttlMu.Lock()
	d.ttlStatus.PayloadDeleted += uint64(n)
	if it.Valid() {
		d.ttlStatus.BacklogPresent = true
	}
	d.ttlMu.Unlock()
	return n, nil
}

// CommittedChangeIndex reads durable state, not allocated producer sequences.
// It is CommittedChangeIndexCtx without a caller deadline.
func (d *DB) CommittedChangeIndex() (uint64, error) {
	return d.CommittedChangeIndexCtx(context.Background())
}

// CommittedChangeIndexCtx returns the highest committed changelog sequence
// that no cursor can overtake: max(persisted counter, last changelog key),
// capped at the native-stream watermark. Every native sequence at or below
// the result has committed (or was abandoned), so a LATEST iterator at
// result+1 cannot skip an in-flight native write. The watermark is read
// before the read barrier for the reason given on nativeStreamReadBound.
// It does not block writers; ctx bounds the read barrier.
func (d *DB) CommittedChangeIndexCtx(ctx context.Context) (uint64, error) {
	watermark := d.nativeStreamWatermark()
	if err := d.ReadBarrierCtx(ctx, defaultReadBarrierTimeout); err != nil {
		return 0, err
	}
	persisted, err := d.loadPersistedChangeIndex()
	if err != nil {
		return 0, err
	}
	last, err := d.scanMaxChangeIndex()
	if err != nil {
		return 0, err
	}
	if last > persisted {
		persisted = last
	}
	if persisted > watermark {
		persisted = watermark
	}
	return persisted, nil
}
