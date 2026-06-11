package cluster

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/osvaldoandrade/cefas/internal/storage"
)

const (
	PlacementAuditKindGap                      = "placement_gap"
	PlacementAuditKindOverlap                  = "placement_overlap"
	PlacementAuditKindOrphanedPrimaryKey       = "orphaned_primary_key"
	PlacementAuditKindMissingCatalogDescriptor = "missing_catalog_descriptor"
	PlacementAuditKindUnsupportedStrategy      = "unsupported_placement_strategy"

	PlacementAuditSeverityError   = "error"
	PlacementAuditSeverityWarning = "warning"

	defaultAuditMaxPrimaryKeysPerShard = 4096
	defaultAuditMaxIssues              = 200
)

type PlacementAuditRequest struct {
	MaxPrimaryKeysPerShard int  `json:"maxPrimaryKeysPerShard,omitempty"`
	MaxIssues              int  `json:"maxIssues,omitempty"`
	IncludeRepairPlan      bool `json:"includeRepairPlan,omitempty"`
}

type PlacementAuditReport struct {
	CheckedAtUnix          int64                 `json:"checkedAtUnix"`
	PlacementEpoch         uint64                `json:"placementEpoch"`
	PlacementVersion       uint64                `json:"placementVersion"`
	PlacementStrategy      string                `json:"placementStrategy"`
	ShardCount             int                   `json:"shardCount"`
	MaxPrimaryKeysPerShard int                   `json:"maxPrimaryKeysPerShard"`
	ScannedPrimaryKeys     int64                 `json:"scannedPrimaryKeys"`
	ScannedCatalogKeys     int64                 `json:"scannedCatalogKeys"`
	PrimaryKeyChecksum     string                `json:"primaryKeyChecksum,omitempty"`
	Truncated              bool                  `json:"truncated"`
	ConsistencyVerdict     string                `json:"consistencyVerdict"`
	Issues                 []PlacementAuditIssue `json:"issues,omitempty"`
	RepairPlan             *PlacementRepairPlan  `json:"repairPlan,omitempty"`
}

type PlacementAuditIssue struct {
	Kind                     string   `json:"kind"`
	Severity                 string   `json:"severity"`
	Detail                   string   `json:"detail"`
	ShardID                  *uint32  `json:"shardId,omitempty"`
	ExpectedShardID          *uint32  `json:"expectedShardId,omitempty"`
	OwnerShardIDs            []uint32 `json:"ownerShardIds,omitempty"`
	DescriptorSourceShardIDs []uint32 `json:"descriptorSourceShardIds,omitempty"`
	Table                    string   `json:"table,omitempty"`
	KeyHex                   string   `json:"keyHex,omitempty"`
	Token                    *uint64  `json:"token,omitempty"`
	RangeStart               *uint64  `json:"rangeStart,omitempty"`
	RangeEnd                 *uint64  `json:"rangeEnd,omitempty"`
}

type PlacementRepairPlan struct {
	ApplySupported bool                    `json:"applySupported"`
	ReviewRequired bool                    `json:"reviewRequired"`
	Summary        []string                `json:"summary,omitempty"`
	Actions        []PlacementRepairAction `json:"actions,omitempty"`
}

type PlacementRepairAction struct {
	Action        string  `json:"action"`
	Detail        string  `json:"detail"`
	ShardID       *uint32 `json:"shardId,omitempty"`
	SourceShardID *uint32 `json:"sourceShardId,omitempty"`
	TargetShardID *uint32 `json:"targetShardId,omitempty"`
	Table         string  `json:"table,omitempty"`
	KeyHex        string  `json:"keyHex,omitempty"`
	Token         *uint64 `json:"token,omitempty"`
	RangeStart    *uint64 `json:"rangeStart,omitempty"`
	RangeEnd      *uint64 `json:"rangeEnd,omitempty"`
}

type placementAuditCollector struct {
	max       int
	issues    []PlacementAuditIssue
	truncated bool
}

func (c *placementAuditCollector) add(issue PlacementAuditIssue) bool {
	if c.max <= 0 {
		c.max = defaultAuditMaxIssues
	}
	if len(c.issues) >= c.max {
		c.truncated = true
		return false
	}
	c.issues = append(c.issues, issue)
	return true
}

func (m *Manager) AuditPlacement(ctx context.Context, req PlacementAuditRequest) (PlacementAuditReport, error) {
	if err := m.RefreshPlacement(); err != nil {
		return PlacementAuditReport{}, err
	}
	cat := m.Placement()
	report := AuditPlacementCatalog(cat, req)
	if err := ctx.Err(); err != nil {
		return PlacementAuditReport{}, err
	}
	catalogsByShard, descriptorSources, scannedCatalogs, err := m.auditCatalogDescriptors(ctx)
	if err != nil {
		return PlacementAuditReport{}, err
	}
	report.ScannedCatalogKeys = scannedCatalogs
	scannedPrimary, checksum, truncated, err := m.auditPrimaryKeys(ctx, cat, req, catalogsByShard, descriptorSources, &report)
	if err != nil {
		return PlacementAuditReport{}, err
	}
	report.ScannedPrimaryKeys = scannedPrimary
	report.PrimaryKeyChecksum = checksum
	report.Truncated = report.Truncated || truncated
	report.finish(req)
	return report, nil
}

func AuditPlacementCatalog(cat PlacementCatalog, req PlacementAuditRequest) PlacementAuditReport {
	cat.normalize()
	report := PlacementAuditReport{
		CheckedAtUnix:          time.Now().Unix(),
		PlacementEpoch:         cat.Epoch,
		PlacementVersion:       cat.Version,
		PlacementStrategy:      cat.Strategy,
		ShardCount:             len(cat.Shards),
		MaxPrimaryKeysPerShard: auditMaxPrimaryKeysPerShard(req.MaxPrimaryKeysPerShard),
	}
	collector := placementAuditCollector{max: auditMaxIssues(req.MaxIssues)}
	if cat.Strategy != PlacementStrategyTokenRange {
		collector.add(PlacementAuditIssue{
			Kind:     PlacementAuditKindUnsupportedStrategy,
			Severity: PlacementAuditSeverityWarning,
			Detail:   fmt.Sprintf("placement strategy %q is not token-range; storage token ownership audit skipped", cat.Strategy),
		})
		report.Issues = collector.issues
		report.Truncated = collector.truncated
		report.finish(req)
		return report
	}
	auditTokenCoverage(cat, &collector)
	report.Issues = collector.issues
	report.Truncated = collector.truncated
	report.finish(req)
	return report
}

func (r *PlacementAuditReport) finish(req PlacementAuditRequest) {
	if len(r.Issues) == 0 {
		r.ConsistencyVerdict = "pass"
	} else {
		r.ConsistencyVerdict = "fail"
	}
	if req.IncludeRepairPlan {
		plan := BuildPlacementRepairPlan(r.Issues)
		r.RepairPlan = &plan
	}
}

func BuildPlacementRepairPlan(issues []PlacementAuditIssue) PlacementRepairPlan {
	plan := PlacementRepairPlan{
		ApplySupported: false,
		ReviewRequired: true,
	}
	counts := map[string]int{}
	for _, issue := range issues {
		counts[issue.Kind]++
		switch issue.Kind {
		case PlacementAuditKindGap:
			plan.Actions = append(plan.Actions, PlacementRepairAction{
				Action:     "review_placement_gap",
				Detail:     "create an explicit split/range-move placement plan that assigns this token range to exactly one active owner",
				RangeStart: issue.RangeStart,
				RangeEnd:   issue.RangeEnd,
			})
		case PlacementAuditKindOverlap:
			plan.Actions = append(plan.Actions, PlacementRepairAction{
				Action:     "review_placement_overlap",
				Detail:     "create an explicit placement plan that removes overlapping ownership before applying data cleanup",
				ShardID:    issue.ShardID,
				RangeStart: issue.RangeStart,
				RangeEnd:   issue.RangeEnd,
			})
		case PlacementAuditKindOrphanedPrimaryKey:
			if issue.ExpectedShardID != nil {
				plan.Actions = append(plan.Actions, PlacementRepairAction{
					Action:        "move_orphaned_primary_key",
					Detail:        "copy the primary row and maintained index entries to the expected shard, then delete the orphan from the current shard after verification",
					ShardID:       issue.ShardID,
					TargetShardID: issue.ExpectedShardID,
					Table:         issue.Table,
					KeyHex:        issue.KeyHex,
					Token:         issue.Token,
				})
			} else {
				plan.Actions = append(plan.Actions, PlacementRepairAction{
					Action:  "quarantine_orphaned_primary_key",
					Detail:  "no active token owner exists; export the row for review before deleting or reassigning it",
					ShardID: issue.ShardID,
					Table:   issue.Table,
					KeyHex:  issue.KeyHex,
					Token:   issue.Token,
				})
			}
		case PlacementAuditKindMissingCatalogDescriptor:
			action := PlacementRepairAction{
				Action:  "recreate_catalog_descriptor",
				Detail:  "no source descriptor was found on another local shard; recreate the descriptor before serving this table",
				ShardID: issue.ShardID,
				Table:   issue.Table,
			}
			if len(issue.DescriptorSourceShardIDs) > 0 {
				source := issue.DescriptorSourceShardIDs[0]
				action.Action = "copy_catalog_descriptor"
				action.Detail = "copy the table descriptor from a shard that still has it; verify schema before applying"
				action.SourceShardID = &source
			}
			plan.Actions = append(plan.Actions, action)
		}
	}
	for kind, count := range counts {
		plan.Summary = append(plan.Summary, fmt.Sprintf("%s=%d", kind, count))
	}
	sort.Strings(plan.Summary)
	return plan
}

func auditTokenCoverage(cat PlacementCatalog, collector *placementAuditCollector) {
	type ownedSegment struct {
		tokenSegment
		shardID uint32
	}
	var segs []ownedSegment
	for _, sh := range cat.Shards {
		if !sh.State.routable() {
			continue
		}
		for _, rng := range sh.Ranges {
			for _, seg := range tokenRangeSegments(rng) {
				segs = append(segs, ownedSegment{tokenSegment: seg, shardID: sh.ID})
			}
		}
	}
	if len(segs) == 0 {
		start, end := uint64(0), uint64(0)
		collector.add(PlacementAuditIssue{
			Kind:       PlacementAuditKindGap,
			Severity:   PlacementAuditSeverityError,
			Detail:     "token-range placement has no active owner for the full ring",
			RangeStart: &start,
			RangeEnd:   &end,
		})
		return
	}
	sort.Slice(segs, func(i, j int) bool {
		if cmp := segs[i].start.Cmp(segs[j].start); cmp != 0 {
			return cmp < 0
		}
		return segs[i].end.Cmp(segs[j].end) < 0
	})
	cursor := new(big.Int).Set(bigZero)
	for _, seg := range segs {
		if seg.start.Cmp(cursor) > 0 {
			if !collector.add(gapIssue(cursor, seg.start)) {
				return
			}
			cursor.Set(seg.end)
			continue
		}
		if seg.start.Cmp(cursor) < 0 {
			overlapEnd := minBigInt(cursor, seg.end)
			owners := activeOwnersAtToken(cat, midpointBigToken(seg.start, overlapEnd))
			issue := overlapIssue(seg.shardID, owners, seg.start, overlapEnd)
			if !collector.add(issue) {
				return
			}
		}
		if seg.end.Cmp(cursor) > 0 {
			cursor.Set(seg.end)
		}
	}
	if cursor.Cmp(bigTokenSpace) < 0 {
		collector.add(gapIssue(cursor, bigTokenSpace))
	}
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
			table, ok := auditCatalogTableFromKey(iter.Key())
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

func (m *Manager) auditPrimaryKeys(ctx context.Context, cat PlacementCatalog, req PlacementAuditRequest, catalogsByShard map[uint32]map[string]struct{}, descriptorSources map[string][]uint32, report *PlacementAuditReport) (int64, string, bool, error) {
	collector := placementAuditCollector{
		max:       auditMaxIssues(req.MaxIssues),
		issues:    append([]PlacementAuditIssue(nil), report.Issues...),
		truncated: report.Truncated,
	}
	maxPerShard := auditMaxPrimaryKeysPerShard(req.MaxPrimaryKeysPerShard)
	checksum := fnv.New64a()
	var scanned int64
	for _, shard := range m.Shards() {
		if err := ctx.Err(); err != nil {
			return scanned, "", collector.truncated, err
		}
		if shard == nil || shard.Storage == nil || shard.State == ShardStateDecommissioned {
			continue
		}
		lower, upper := storage.PrefixTables()
		iter, err := shard.Storage.Iter(lower, upper)
		if err != nil {
			return scanned, "", collector.truncated, err
		}
		shardScanned := 0
		for valid := iter.First(); valid; valid = iter.Next() {
			if err := ctx.Err(); err != nil {
				_ = iter.Close()
				return scanned, "", collector.truncated, err
			}
			token, ok := storage.PrimaryTokenFromKey(iter.Key())
			if !ok {
				continue
			}
			shardScanned++
			scanned++
			if shardScanned > maxPerShard {
				collector.truncated = true
				break
			}
			key := append([]byte(nil), iter.Key()...)
			_, _ = checksum.Write(key)
			_, _ = checksum.Write([]byte{0xff})
			table, tableOK := auditPrimaryTableFromKey(key)
			owners := activeOwnersForToken(cat, token)
			if !containsUint32(owners, shard.ID) {
				issue := PlacementAuditIssue{
					Kind:          PlacementAuditKindOrphanedPrimaryKey,
					Severity:      PlacementAuditSeverityError,
					Detail:        fmt.Sprintf("primary key is stored on shard %d but token owners are %v", shard.ID, owners),
					ShardID:       u32ptr(shard.ID),
					OwnerShardIDs: owners,
					KeyHex:        hex.EncodeToString(key),
					Token:         u64ptr(token),
				}
				if tableOK {
					issue.Table = table
				}
				if len(owners) == 1 {
					issue.ExpectedShardID = u32ptr(owners[0])
				}
				if !collector.add(issue) {
					break
				}
			}
			if tableOK {
				if _, ok := catalogsByShard[shard.ID][table]; !ok {
					issue := PlacementAuditIssue{
						Kind:                     PlacementAuditKindMissingCatalogDescriptor,
						Severity:                 PlacementAuditSeverityError,
						Detail:                   fmt.Sprintf("primary key for table %q exists on shard %d but that shard has no catalog descriptor", table, shard.ID),
						ShardID:                  u32ptr(shard.ID),
						Table:                    table,
						KeyHex:                   hex.EncodeToString(key),
						Token:                    u64ptr(token),
						DescriptorSourceShardIDs: append([]uint32(nil), descriptorSources[table]...),
					}
					if !collector.add(issue) {
						break
					}
				}
			}
		}
		if err := iter.Error(); err != nil {
			_ = iter.Close()
			return scanned, "", collector.truncated, err
		}
		if err := iter.Close(); err != nil {
			return scanned, "", collector.truncated, err
		}
		if collector.truncated {
			break
		}
	}
	report.Issues = collector.issues
	report.Truncated = collector.truncated
	if scanned == 0 {
		return scanned, "", collector.truncated, nil
	}
	return scanned, fmt.Sprintf("%016x", checksum.Sum64()), collector.truncated, nil
}

func gapIssue(start, end *big.Int) PlacementAuditIssue {
	rangeStart := auditTokenFromBig(start)
	rangeEnd := auditTokenFromBig(end)
	return PlacementAuditIssue{
		Kind:       PlacementAuditKindGap,
		Severity:   PlacementAuditSeverityError,
		Detail:     fmt.Sprintf("token range [%d,%d) has no active owner", rangeStart, rangeEnd),
		RangeStart: &rangeStart,
		RangeEnd:   &rangeEnd,
	}
}

func overlapIssue(shardID uint32, owners []uint32, start, end *big.Int) PlacementAuditIssue {
	rangeStart := auditTokenFromBig(start)
	rangeEnd := auditTokenFromBig(end)
	return PlacementAuditIssue{
		Kind:          PlacementAuditKindOverlap,
		Severity:      PlacementAuditSeverityError,
		Detail:        fmt.Sprintf("token range [%d,%d) has overlapping active owners %v", rangeStart, rangeEnd, owners),
		ShardID:       u32ptr(shardID),
		OwnerShardIDs: owners,
		RangeStart:    &rangeStart,
		RangeEnd:      &rangeEnd,
	}
}

func activeOwnersForToken(cat PlacementCatalog, token uint64) []uint32 {
	var owners []uint32
	for _, sh := range cat.Shards {
		if !sh.State.routable() {
			continue
		}
		for _, rng := range sh.Ranges {
			if rng.Contains(token) {
				owners = append(owners, sh.ID)
				break
			}
		}
	}
	sort.Slice(owners, func(i, j int) bool { return owners[i] < owners[j] })
	return owners
}

func activeOwnersAtToken(cat PlacementCatalog, token *big.Int) []uint32 {
	return activeOwnersForToken(cat, auditTokenFromBig(token))
}

func auditCatalogTableFromKey(key []byte) (string, bool) {
	prefix := []byte(storage.Namespace + "catalog/")
	if !bytes.HasPrefix(key, prefix) || len(key) == len(prefix) {
		return "", false
	}
	return string(key[len(prefix):]), true
}

func auditPrimaryTableFromKey(key []byte) (string, bool) {
	prefix := []byte(storage.Namespace + "t/")
	if !bytes.HasPrefix(key, prefix) {
		return "", false
	}
	rest := key[len(prefix):]
	pos := bytes.Index(rest, []byte("/p/"))
	if pos <= 0 {
		return "", false
	}
	return string(rest[:pos]), true
}

func auditTokenFromBig(v *big.Int) uint64 {
	if v.Cmp(bigTokenSpace) == 0 {
		return 0
	}
	return v.Uint64()
}

func midpointBigToken(start, end *big.Int) *big.Int {
	mid := new(big.Int).Add(start, end)
	mid.Div(mid, big.NewInt(2))
	if mid.Cmp(bigTokenSpace) >= 0 {
		mid.Sub(mid, bigTokenSpace)
	}
	return mid
}

func minBigInt(a, b *big.Int) *big.Int {
	if a.Cmp(b) <= 0 {
		return new(big.Int).Set(a)
	}
	return new(big.Int).Set(b)
}

func containsUint32(in []uint32, v uint32) bool {
	for _, existing := range in {
		if existing == v {
			return true
		}
	}
	return false
}

func auditMaxPrimaryKeysPerShard(v int) int {
	if v <= 0 {
		return defaultAuditMaxPrimaryKeysPerShard
	}
	return v
}

func auditMaxIssues(v int) int {
	if v <= 0 {
		return defaultAuditMaxIssues
	}
	return v
}

func u64ptr(v uint64) *uint64 { return &v }

func (r PlacementAuditReport) HasIssueKind(kind string) bool {
	for _, issue := range r.Issues {
		if issue.Kind == kind {
			return true
		}
	}
	return false
}

func PlacementAuditIssueKinds(issues []PlacementAuditIssue) []string {
	out := make([]string, 0, len(issues))
	seen := map[string]struct{}{}
	for _, issue := range issues {
		if _, ok := seen[issue.Kind]; ok {
			continue
		}
		seen[issue.Kind] = struct{}{}
		out = append(out, issue.Kind)
	}
	sort.Strings(out)
	return out
}

func PlacementAuditSummary(issues []PlacementAuditIssue) string {
	kinds := PlacementAuditIssueKinds(issues)
	if len(kinds) == 0 {
		return "pass"
	}
	return strings.Join(kinds, ",")
}
