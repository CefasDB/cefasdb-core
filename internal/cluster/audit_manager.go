package cluster

import (
	"context"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/CefasDb/cefasdb/internal/placement"
	"github.com/CefasDb/cefasdb/internal/storage"
)

// AuditPlacement runs the pure catalog audit (placement-only checks)
// and then walks every shard's local primary-key keyspace to detect
// orphaned keys and missing catalog descriptors. Pure logic lives in
// internal/placement; everything below depends on the Manager's
// shard handles and storage iterators.
func (m *Manager) AuditPlacement(ctx context.Context, req placement.PlacementAuditRequest) (placement.PlacementAuditReport, error) {
	if err := m.RefreshPlacement(); err != nil {
		return placement.PlacementAuditReport{}, err
	}
	cat := m.Placement()
	report := placement.AuditPlacementCatalog(cat, req)
	if err := ctx.Err(); err != nil {
		return placement.PlacementAuditReport{}, err
	}
	catalogsByShard, descriptorSources, scannedCatalogs, err := m.auditCatalogDescriptors(ctx)
	if err != nil {
		return placement.PlacementAuditReport{}, err
	}
	report.ScannedCatalogKeys = scannedCatalogs
	scannedPrimary, checksum, truncated, err := m.auditPrimaryKeys(ctx, cat, req, catalogsByShard, descriptorSources, &report)
	if err != nil {
		return placement.PlacementAuditReport{}, err
	}
	report.ScannedPrimaryKeys = scannedPrimary
	report.PrimaryKeyChecksum = checksum
	report.Truncated = report.Truncated || truncated
	placement.FinishAuditReport(&report, req)
	return report, nil
}

func (m *Manager) auditCatalogDescriptors(ctx context.Context) (map[uint32]map[string]struct{}, map[string][]uint32, int64, error) {
	catalogsByShard := map[uint32]map[string]struct{}{}
	descriptorSources := map[string][]uint32{}
	var scanned int64
	for _, shard := range m.Shards() {
		if err := ctx.Err(); err != nil {
			return nil, nil, scanned, err
		}
		if shard == nil || shard.Storage == nil {
			continue
		}
		tables := map[string]struct{}{}
		lower, upper := storage.PrefixCatalog()
		iter, err := shard.Storage.Iter(lower, upper)
		if err != nil {
			return nil, nil, scanned, err
		}
		for valid := iter.First(); valid; valid = iter.Next() {
			table, ok := placement.AuditCatalogTableFromKey(iter.Key())
			if !ok {
				continue
			}
			tables[table] = struct{}{}
			descriptorSources[table] = append(descriptorSources[table], shard.ID)
			scanned++
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			return nil, nil, scanned, err
		}
		if err := iter.Close(); err != nil {
			return nil, nil, scanned, err
		}
		catalogsByShard[shard.ID] = tables
	}
	for table := range descriptorSources {
		sort.Slice(descriptorSources[table], func(i, j int) bool { return descriptorSources[table][i] < descriptorSources[table][j] })
	}
	return catalogsByShard, descriptorSources, scanned, nil
}

func (m *Manager) auditPrimaryKeys(ctx context.Context, cat placement.PlacementCatalog, req placement.PlacementAuditRequest, catalogsByShard map[uint32]map[string]struct{}, descriptorSources map[string][]uint32, report *placement.PlacementAuditReport) (int64, string, bool, error) {
	collector := placement.NewAuditCollector(req.MaxIssues, report.Issues, report.Truncated)
	maxPerShard := placement.AuditMaxPrimaryKeysPerShard(req.MaxPrimaryKeysPerShard)
	checksum := fnv.New64a()
	var scanned int64
	for _, shard := range m.Shards() {
		if err := ctx.Err(); err != nil {
			return scanned, "", collector.Truncated(), err
		}
		if shard == nil || shard.Storage == nil || shard.State == placement.ShardStateDecommissioned {
			continue
		}
		lower, upper := storage.PrefixTables()
		iter, err := shard.Storage.Iter(lower, upper)
		if err != nil {
			return scanned, "", collector.Truncated(), err
		}
		shardScanned := 0
		for valid := iter.First(); valid; valid = iter.Next() {
			if err := ctx.Err(); err != nil {
				_ = iter.Close()
				return scanned, "", collector.Truncated(), err
			}
			token, ok := storage.PrimaryTokenFromKey(iter.Key())
			if !ok {
				continue
			}
			shardScanned++
			scanned++
			if shardScanned > maxPerShard {
				collector.MarkTruncated()
				break
			}
			key := append([]byte(nil), iter.Key()...)
			_, _ = checksum.Write(key)
			_, _ = checksum.Write([]byte{0xff})
			table, tableOK := placement.AuditPrimaryTableFromKey(key)
			owners := placement.ActiveOwnersForToken(cat, token)
			if !placement.ContainsUint32(owners, shard.ID) {
				issue := placement.PlacementAuditIssue{
					Kind:          placement.PlacementAuditKindOrphanedPrimaryKey,
					Severity:      placement.PlacementAuditSeverityError,
					Detail:        fmt.Sprintf("primary key is stored on shard %d but token owners are %v", shard.ID, owners),
					ShardID:       placement.U32ptr(shard.ID),
					OwnerShardIDs: owners,
					KeyHex:        hex.EncodeToString(key),
					Token:         placement.U64ptr(token),
				}
				if tableOK {
					issue.Table = table
				}
				if len(owners) == 1 {
					issue.ExpectedShardID = placement.U32ptr(owners[0])
				}
				if !collector.Add(issue) {
					break
				}
			}
			if tableOK {
				if _, ok := catalogsByShard[shard.ID][table]; !ok {
					issue := placement.PlacementAuditIssue{
						Kind:                     placement.PlacementAuditKindMissingCatalogDescriptor,
						Severity:                 placement.PlacementAuditSeverityError,
						Detail:                   fmt.Sprintf("primary key for table %q exists on shard %d but that shard has no catalog descriptor", table, shard.ID),
						ShardID:                  placement.U32ptr(shard.ID),
						Table:                    table,
						KeyHex:                   hex.EncodeToString(key),
						Token:                    placement.U64ptr(token),
						DescriptorSourceShardIDs: append([]uint32(nil), descriptorSources[table]...),
					}
					if !collector.Add(issue) {
						break
					}
				}
			}
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			return scanned, "", collector.Truncated(), err
		}
		if err := iter.Close(); err != nil {
			return scanned, "", collector.Truncated(), err
		}
		if collector.Truncated() {
			break
		}
	}
	report.Issues = collector.Issues()
	report.Truncated = collector.Truncated()
	if scanned == 0 {
		return scanned, "", collector.Truncated(), nil
	}
	return scanned, fmt.Sprintf("%016x", checksum.Sum64()), collector.Truncated(), nil
}
