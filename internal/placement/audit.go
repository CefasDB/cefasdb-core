package placement

import (
	"bytes"
	"fmt"
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

func AuditPlacementCatalog(cat PlacementCatalog, req PlacementAuditRequest) PlacementAuditReport {
	cat.Normalize()
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
		if !sh.State.Routable() {
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
		if !sh.State.Routable() {
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
