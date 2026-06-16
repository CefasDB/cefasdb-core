package cluster

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func TestAuditPlacementCatalogReportsGapsAndOverlaps(t *testing.T) {
	gapped := DefaultPlacement(2, "n1", nil, nil, NodeCapacity{}, PlacementStrategyTokenRange)
	gapped.Shards[0].Ranges = []TokenRange{{Start: 0, End: 100}}
	gapped.Shards[1].Ranges = []TokenRange{{Start: 200, End: 0}}

	gapReport := AuditPlacementCatalog(gapped, PlacementAuditRequest{IncludeRepairPlan: true})
	if !gapReport.HasIssueKind(PlacementAuditKindGap) {
		t.Fatalf("gap report kinds = %s, want %s", PlacementAuditSummary(gapReport.Issues), PlacementAuditKindGap)
	}
	if gapReport.ConsistencyVerdict != "fail" {
		t.Fatalf("gap verdict = %s, want fail", gapReport.ConsistencyVerdict)
	}
	if gapReport.RepairPlan == nil || len(gapReport.RepairPlan.Actions) == 0 || gapReport.RepairPlan.ApplySupported {
		t.Fatalf("gap repair plan = %+v, want review-only actions", gapReport.RepairPlan)
	}

	overlapping := DefaultPlacement(2, "n1", nil, nil, NodeCapacity{}, PlacementStrategyTokenRange)
	overlapping.Shards[0].Ranges = []TokenRange{{Start: 0, End: 100}}
	overlapping.Shards[1].Ranges = []TokenRange{{Start: 50, End: 0}}

	overlapReport := AuditPlacementCatalog(overlapping, PlacementAuditRequest{IncludeRepairPlan: true})
	if !overlapReport.HasIssueKind(PlacementAuditKindOverlap) {
		t.Fatalf("overlap report kinds = %s, want %s", PlacementAuditSummary(overlapReport.Issues), PlacementAuditKindOverlap)
	}
	if overlapReport.RepairPlan == nil || len(overlapReport.RepairPlan.Actions) == 0 {
		t.Fatalf("overlap repair plan missing: %+v", overlapReport.RepairPlan)
	}
}

func TestAuditPlacementDetectsOrphanedPrimaryKeys(t *testing.T) {
	mgr := openAuditTestManager(t, 2)
	defer mgr.Close()

	td := auditTableDescriptor("events")
	createAuditDescriptorOnAllShards(t, mgr, td)

	key := auditKeyForShard(t, mgr, 0)
	wrongShard, ok := mgr.Shard(1)
	if !ok {
		t.Fatal("missing shard 1")
	}
	if err := wrongShard.Storage.PutItemWith(td, types.Item{
		"id": sAttrAudit(key),
		"v":  sAttrAudit("orphan"),
	}, storage.PutOptions{}); err != nil {
		t.Fatalf("put orphan: %v", err)
	}

	report, err := mgr.AuditPlacement(context.Background(), PlacementAuditRequest{IncludeRepairPlan: true})
	if err != nil {
		t.Fatalf("audit placement: %v", err)
	}
	if !report.HasIssueKind(PlacementAuditKindOrphanedPrimaryKey) {
		t.Fatalf("issue kinds = %s, want %s", PlacementAuditSummary(report.Issues), PlacementAuditKindOrphanedPrimaryKey)
	}
	var found bool
	for _, issue := range report.Issues {
		if issue.Kind == PlacementAuditKindOrphanedPrimaryKey && issue.ExpectedShardID != nil && *issue.ExpectedShardID == 0 {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing orphan expected shard 0 in issues: %+v", report.Issues)
	}
	if report.RepairPlan == nil || !repairPlanHasAction(*report.RepairPlan, "move_orphaned_primary_key") {
		t.Fatalf("repair plan = %+v, want move_orphaned_primary_key", report.RepairPlan)
	}
}

func TestAuditPlacementDetectsMissingCatalogDescriptors(t *testing.T) {
	mgr := openAuditTestManager(t, 1)
	defer mgr.Close()

	td := auditTableDescriptor("missing_catalog")
	shard0, ok := mgr.Shard(0)
	if !ok {
		t.Fatal("missing shard 0")
	}
	if err := shard0.Storage.PutItemWith(td, types.Item{
		"id": sAttrAudit("missing-catalog-key"),
		"v":  sAttrAudit("value"),
	}, storage.PutOptions{}); err != nil {
		t.Fatalf("put row without descriptor: %v", err)
	}

	report, err := mgr.AuditPlacement(context.Background(), PlacementAuditRequest{
		MaxPrimaryKeysPerShard: 1,
		IncludeRepairPlan:      true,
	})
	if err != nil {
		t.Fatalf("audit placement: %v", err)
	}
	if !report.HasIssueKind(PlacementAuditKindMissingCatalogDescriptor) {
		t.Fatalf("issue kinds = %s, want %s", PlacementAuditSummary(report.Issues), PlacementAuditKindMissingCatalogDescriptor)
	}
	if report.ScannedPrimaryKeys != 1 {
		t.Fatalf("scanned primary keys = %d, want 1", report.ScannedPrimaryKeys)
	}
	if report.RepairPlan == nil || !repairPlanHasAction(*report.RepairPlan, "recreate_catalog_descriptor") {
		t.Fatalf("repair plan = %+v, want recreate_catalog_descriptor", report.RepairPlan)
	}
}

func openAuditTestManager(t *testing.T, shards int) *Manager {
	t.Helper()
	mgr, err := Open(context.Background(), Config{
		Root:      t.TempDir(),
		Shards:    shards,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	return mgr
}

func createAuditDescriptorOnAllShards(t *testing.T, mgr *Manager, td types.TableDescriptor) {
	t.Helper()
	for _, shard := range mgr.Shards() {
		cat, err := catalog.New(shard.Storage)
		if err != nil {
			t.Fatalf("open catalog shard %d: %v", shard.ID, err)
		}
		if err := cat.Create(td); err != nil {
			t.Fatalf("create descriptor shard %d: %v", shard.ID, err)
		}
	}
}

func auditKeyForShard(t *testing.T, mgr *Manager, shardID uint32) string {
	t.Helper()
	router := mgr.Router()
	for i := 0; i < 200_000; i++ {
		key := fmt.Sprintf("audit-key-%d", i)
		pk, err := storage.AttrCanonicalBytes(sAttrAudit(key))
		if err != nil {
			t.Fatal(err)
		}
		id, err := router.ShardForPK(pk)
		if err != nil {
			t.Fatalf("ShardForPK returned error: %v", err)
		}
		if id == shardID {
			return key
		}
	}
	t.Fatalf("could not find key for shard %d", shardID)
	return ""
}

func auditTableDescriptor(name string) types.TableDescriptor {
	return types.TableDescriptor{Name: name, KeySchema: types.KeySchema{PK: "id"}}
}

func sAttrAudit(s string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: s}
}

func repairPlanHasAction(plan PlacementRepairPlan, action string) bool {
	for _, got := range plan.Actions {
		if got.Action == action {
			return true
		}
	}
	return false
}
