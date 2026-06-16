package api

import (
	"fmt"

	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func (s *GRPCServer) queryByIndex(td types.TableDescriptor, indexName string, pkVal types.AttributeValue, opts storage.QueryOptions) ([]types.Item, error) {
	if hasGSI(td, indexName) {
		return queryGSIAcrossShards(s.allShards(), td, indexName, pkVal, opts)
	}
	if hasLSI(td, indexName) {
		pkBytes, err := storage.AttrCanonicalBytes(pkVal)
		if err != nil {
			return nil, fmt.Errorf("primary PK: %w", err)
		}
		return s.storageFor(pkBytes).QueryByLSI(td, indexName, pkVal, opts)
	}
	return nil, fmt.Errorf("table %q has no index named %q", td.Name, indexName)
}

func queryGSIAcrossShards(dbs []*storage.DB, td types.TableDescriptor, indexName string, pkVal types.AttributeValue, opts storage.QueryOptions) ([]types.Item, error) {
	var out []types.Item
	seen := make(map[string]struct{})
	shardOpts := opts
	shardOpts.Limit = 0
	for _, db := range dbs {
		got, err := db.QueryByGSI(td, indexName, pkVal, shardOpts)
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
