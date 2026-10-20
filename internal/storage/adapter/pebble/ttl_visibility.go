package pebble

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type TTLDescriptorResolver func(string) (types.TableDescriptor, error)

// AttachTTLResolver uses the metadata catalog when it lives on another shard.
// The resolver is copied under the lock and called outside it.
func (d *DB) AttachTTLResolver(resolver TTLDescriptorResolver) {
	d.ttlMu.Lock()
	d.ttlResolver = resolver
	d.ttlMu.Unlock()
}

func itemExpired(td types.TableDescriptor, item types.Item, now time.Time) bool {
	if td.TTLAttribute == "" {
		return false
	}
	av, exists := item[td.TTLAttribute]
	if !exists {
		return false
	}
	if av.T != types.AttrN {
		return true
	}
	expiry, err := strconv.ParseInt(av.N, 10, 64)
	return err != nil || (expiry != 0 && expiry <= now.Unix())
}

// TTL visibility is additive for native text tables. Reading the descriptor
// once per operation avoids a schema cache becoming stale after replication.
func (d *DB) visibilityDescriptor(table string) (types.TableDescriptor, error) {
	if table == "" {
		return types.TableDescriptor{}, nil
	}
	d.ttlMu.RLock()
	resolver := d.ttlResolver
	d.ttlMu.RUnlock()
	if resolver != nil {
		td, err := resolver(table)
		if err == nil {
			if len(td.TextIndexes) == 0 {
				td.TTLAttribute = ""
			}
			return td, nil
		}
		// Global-index backing tables are synthetic and may have no catalog
		// descriptor. Only confirmed absence falls back to the local lookup;
		// storage/authority errors must never turn into a TTL visibility bypass.
		if !errors.Is(err, types.ErrTableNotFound) {
			return td, err
		}
	}
	raw, err := d.getNoLane(storage.KeyCatalog(table))
	if errors.Is(err, ErrNotFound) {
		return types.TableDescriptor{}, nil
	}
	if err != nil {
		return types.TableDescriptor{}, err
	}
	var td types.TableDescriptor
	if err := json.Unmarshal(raw, &td); err != nil {
		return td, err
	}
	if len(td.TextIndexes) == 0 {
		td.TTLAttribute = ""
	}
	return td, nil
}

func tableFromPrimaryPrefix(key []byte) string {
	base := []byte(storage.Namespace + "t/")
	if !bytes.HasPrefix(key, base) {
		return ""
	}
	rest := key[len(base):]
	pos := bytes.Index(rest, []byte(storage.SegPrimary))
	if pos < 0 {
		return ""
	}
	return string(rest[:pos])
}
