package server

import (
	"context"
	"fmt"

	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func (s *GRPCServer) queryByIndex(ctx context.Context, td types.TableDescriptor, indexName string, pkVal types.AttributeValue, opts pebble.QueryOptions) ([]types.Item, error) {
	// ScyllaDB-style GLOBAL index — partitioned by the indexed value
	// (#509). One shard owns the partition, so the query lands on
	// exactly one DB. Same RF=1 contract as #537 MV cascade.
	if hasGlobalIndex(td, indexName) {
		return s.queryByGlobalIndex(ctx, td, indexName, pkVal, opts)
	}
	if hasGSI(td, indexName) {
		dbs, err := s.readShardStores()
		if err != nil {
			return nil, err
		}
		return queryGSIAcrossShards(ctx, dbs, td, indexName, pkVal, opts)
	}
	if hasLSI(td, indexName) {
		pkBytes, err := storage.AttrCanonicalBytes(pkVal)
		if err != nil {
			return nil, fmt.Errorf("primary PK: %w", err)
		}
		db, err := s.readStorageFor(pkBytes)
		if err != nil {
			return nil, err
		}
		return db.QueryByLSICtx(ctx, td, indexName, pkVal, opts)
	}
	return nil, fmt.Errorf("table %q has no index named %q", td.Name, indexName)
}

// queryByGlobalIndex resolves the GI descriptor, routes by the
// indexed value's hash, and reads pointer rows from that single
// shard. Multi-node clients should be route-aware: a node that
// does not host the GI shard returns codes.Unavailable so the
// client retries against the leader (the route-aware-reads path
// from #421 handles this automatically).
//
// Pointer rows carry: { IndexedColumn, base_pk_column,
// projected... }. Callers that need non-projected base columns
// follow up with GetItem against the base table — one extra read
// per pointer in the worst case, documented as the GSI v1 read
// shape in ADR 0005 §3.
func (s *GRPCServer) queryByGlobalIndex(ctx context.Context, td types.TableDescriptor, indexName string, pkVal types.AttributeValue, opts pebble.QueryOptions) ([]types.Item, error) {
	if s.cat == nil {
		return nil, fmt.Errorf("catalog not attached")
	}
	gi, err := s.cat.DescribeGlobalIndex(indexName)
	if err != nil {
		return nil, err
	}
	// Use the requested base table's PK column as the synthetic GI
	// SK — same shape the write hook used to construct pointers.
	giTD := giSyntheticTableDescriptor(gi, td.KeySchema.PK)
	pkBytes, err := storage.AttrCanonicalBytes(pkVal)
	if err != nil {
		return nil, fmt.Errorf("indexed value: %w", err)
	}
	db, err := s.readStorageFor(pkBytes)
	if err != nil {
		return nil, err
	}
	return db.QueryByPKCtx(ctx, giTD.Name, giTD.KeySchema, pkVal, opts.Limit)
}

func hasGlobalIndex(td types.TableDescriptor, name string) bool {
	for _, n := range td.GlobalIndexes {
		if n == name {
			return true
		}
	}
	return false
}

func queryGSIAcrossShards(ctx context.Context, dbs []*pebble.DB, td types.TableDescriptor, indexName string, pkVal types.AttributeValue, opts pebble.QueryOptions) ([]types.Item, error) {
	var out []types.Item
	seen := make(map[string]struct{})

	// Push the caller's limit down to each shard. A GSI key uniquely
	// belongs to one shard in a steady-state placement, so a per-shard
	// limit equal to the caller's limit cannot under-fill the answer:
	// when shard S contributes K rows, the remaining shards contribute
	// the rest. The seen-map dedup still guards against transitional
	// placements where the same row is briefly visible on two shards.
	// In that rare case the cross-shard result may dip below limit;
	// that is acceptable — the previous shape returned the right count
	// only by transferring the entire partition from every shard.
	shardOpts := opts
	for _, db := range dbs {
		got, err := db.QueryByGSICtx(ctx, td, indexName, pkVal, shardOpts)
		if err != nil {
			return nil, err
		}
		for _, item := range got {
			id, err := primaryIdentity(item, td.KeySchema)
			if err != nil {
				return nil, err
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, item)
			if opts.Limit > 0 && len(out) >= opts.Limit {
				return out, nil
			}
		}
	}
	return out, nil
}

func hasGSI(td types.TableDescriptor, name string) bool {
	for _, g := range td.GSIs {
		if g.Name == name {
			return true
		}
	}
	return false
}

func hasLSI(td types.TableDescriptor, name string) bool {
	for _, l := range td.LSIs {
		if l.Name == name {
			return true
		}
	}
	return false
}

func primaryIdentity(item types.Item, ks types.KeySchema) (string, error) {
	pk, ok := item[ks.PK]
	if !ok {
		return "", fmt.Errorf("primary PK %q missing on item", ks.PK)
	}
	pkBytes, err := storage.AttrCanonicalBytes(pk)
	if err != nil {
		return "", err
	}
	var skBytes []byte
	if ks.SK != "" {
		sk, ok := item[ks.SK]
		if !ok {
			return "", fmt.Errorf("primary SK %q missing on item", ks.SK)
		}
		skBytes, err = storage.AttrCanonicalBytes(sk)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%d:%s/%d:%s", len(pkBytes), string(pkBytes), len(skBytes), string(skBytes)), nil
}
