package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/placement"
	"github.com/osvaldoandrade/cefas/internal/routing"
	"github.com/osvaldoandrade/cefas/internal/storage"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func TestFinalizeSplitVerificationFailsOnMismatch(t *testing.T) {
	mgr, plan := openTransitionSplitManagerForResume(t)
	defer mgr.Close()

	parent, child, td, key := seedSplitRow(t, mgr, plan, "mismatch")
	setSplitFinalizeHook(t, func(phase SplitFinalizePhase, _ SplitFinalizeState) error {
		if phase == SplitFinalizePhaseCopied {
			if err := child.Storage.DeleteItemWith(td, types.Item{"id": sAttrResume(key)}, pebble.DeleteOptions{}); err != nil {
				t.Fatalf("delete copied child row: %v", err)
			}
		}
		return nil
	})

	_, err := mgr.FinalizeSplit(context.Background(), SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if !errors.Is(err, placement.ErrInvalidPlacementPlan) {
		t.Fatalf("error = %v, want placement.ErrInvalidPlacementPlan", err)
	}
	state, ok, err := mgr.SplitFinalizeState(0, 1)
	if err != nil || !ok {
		t.Fatalf("state ok=%v err=%v", ok, err)
	}
	if state.Phase != SplitFinalizePhaseVerifyFailed || state.Verification.Verified {
		t.Fatalf("unexpected state after mismatch: %+v", state)
	}
	if _, err := parent.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttrResume(key)}); err != nil {
		t.Fatalf("parent row should remain after failed verify: %v", err)
	}
}

func TestFinalizeSplitRetriesAfterCopyFailure(t *testing.T) {
	mgr, plan := openTransitionSplitManagerForResume(t)
	defer mgr.Close()

	_, child, td, key := seedSplitRow(t, mgr, plan, "copy-retry")
	var failed bool
	setSplitFinalizeHook(t, func(phase SplitFinalizePhase, _ SplitFinalizeState) error {
		if phase == SplitFinalizePhaseCopied && !failed {
			failed = true
			return fmt.Errorf("stop after copy")
		}
		return nil
	})

	_, err := mgr.FinalizeSplit(context.Background(), SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err == nil {
		t.Fatal("first finalize unexpectedly succeeded")
	}
	splitFinalizeTestHook = nil
	result, err := mgr.FinalizeSplit(context.Background(), SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err != nil {
		t.Fatalf("retry finalize: %v", err)
	}
	if result.Phase != string(SplitFinalizePhaseDone) || !result.Verification.Verified {
		t.Fatalf("unexpected retry result: %+v", result)
	}
	if got, err := child.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttrResume(key)}); err != nil || got["v"].S != "copy-retry" {
		t.Fatalf("child row after retry = %+v err=%v", got, err)
	}
}

func TestFinalizeSplitRetriesCleanupAfterPublishFailure(t *testing.T) {
	mgr, plan := openTransitionSplitManagerForResume(t)
	defer mgr.Close()

	parent, child, td, key := seedSplitRow(t, mgr, plan, "cleanup-retry")
	var failed bool
	setSplitFinalizeHook(t, func(phase SplitFinalizePhase, _ SplitFinalizeState) error {
		if phase == SplitFinalizePhasePublished && !failed {
			failed = true
			return fmt.Errorf("stop after publish")
		}
		return nil
	})

	_, err := mgr.FinalizeSplit(context.Background(), SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err == nil {
		t.Fatal("first finalize unexpectedly succeeded")
	}
	if _, err := parent.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttrResume(key)}); err != nil {
		t.Fatalf("parent row should remain before cleanup retry: %v", err)
	}
	if _, err := mgr.RollbackSplit(context.Background(), SplitRollbackRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	}); !errors.Is(err, placement.ErrInvalidPlacementPlan) {
		t.Fatalf("rollback after publish error = %v, want placement.ErrInvalidPlacementPlan", err)
	}
	splitFinalizeTestHook = nil
	result, err := mgr.FinalizeSplit(context.Background(), SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err != nil {
		t.Fatalf("cleanup retry: %v", err)
	}
	if result.Phase != string(SplitFinalizePhaseDone) {
		t.Fatalf("retry phase = %q, want done", result.Phase)
	}
	if _, err := parent.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttrResume(key)}); !errors.Is(err, types.ErrItemNotFound) {
		t.Fatalf("parent row after cleanup retry error = %v, want ErrItemNotFound", err)
	}
	if got, err := child.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttrResume(key)}); err != nil || got["v"].S != "cleanup-retry" {
		t.Fatalf("child row after cleanup retry = %+v err=%v", got, err)
	}
}

func TestRollbackSplitBeforePublishRestoresRoutingAndData(t *testing.T) {
	mgr, plan := openTransitionSplitManagerForResume(t)
	defer mgr.Close()

	parent, child, td, key := seedSplitRow(t, mgr, plan, "rollback")
	if _, err := copyCatalogKeys(context.Background(), parent.Storage, child.Storage); err != nil {
		t.Fatalf("copy catalog: %v", err)
	}
	if err := child.Storage.PutItemWith(td, types.Item{"id": sAttrResume(key), "v": sAttrResume("rollback")}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put child copy: %v", err)
	}

	result, err := mgr.RollbackSplit(context.Background(), SplitRollbackRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if result.Phase != string(SplitFinalizePhaseRolledBack) {
		t.Fatalf("phase = %q, want rolled_back", result.Phase)
	}
	cat := mgr.Placement()
	if cat.Shards[0].State != placement.ShardStateActive || cat.Shards[1].State != placement.ShardStateDecommissioned {
		t.Fatalf("unexpected placement after rollback: %+v", cat.Shards)
	}
	got, err := mgr.Router().ShardForPK([]byte(key))
	if err != nil {
		t.Fatalf("ShardForPK returned error: %v", err)
	}
	if got != 0 {
		t.Fatalf("key routes to shard %d after rollback, want parent 0", got)
	}
	if got, err := parent.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttrResume(key)}); err != nil || got["v"].S != "rollback" {
		t.Fatalf("parent row after rollback = %+v err=%v", got, err)
	}
	if got := countKeysForResume(t, child.Storage, storage.PrefixTables); got != 0 {
		t.Fatalf("child data keys after rollback = %d, want 0", got)
	}
	if got := countKeysForResume(t, child.Storage, storage.PrefixCatalog); got != 0 {
		t.Fatalf("child catalog keys after rollback = %d, want 0", got)
	}
}

func openTransitionSplitManagerForResume(t *testing.T) (*Manager, placement.PlacementPlan) {
	t.Helper()
	root := t.TempDir()
	cat := placement.DefaultPlacement(1, "n1", nil, nil, placement.NodeCapacity{}, placement.PlacementStrategyTokenRange)
	plan, err := placement.BuildPlacementPlan(cat, placement.PlacementPlanRequest{
		Operation: placement.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := placement.SavePlacementFile(filepath.Join(root, "placement.json"), plan.After); err != nil {
		t.Fatal(err)
	}
	mgr, err := Open(context.Background(), Config{
		Root:      root,
		Shards:    2,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	return mgr, plan
}

func seedSplitRow(t *testing.T, mgr *Manager, plan placement.PlacementPlan, value string) (*Shard, *Shard, types.TableDescriptor, string) {
	t.Helper()
	parent, ok := mgr.Shard(0)
	if !ok {
		t.Fatal("missing parent shard")
	}
	child, ok := mgr.Shard(1)
	if !ok {
		t.Fatal("missing child shard")
	}
	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	parentCatalog, err := catalog.New(parent.Storage)
	if err != nil {
		t.Fatal(err)
	}
	if err := parentCatalog.Create(td); err != nil {
		t.Fatal(err)
	}
	key := keyInRangeForResume(t, plan.After.Shards[1].Ranges[0])
	if err := parent.Storage.PutItemWith(td, types.Item{"id": sAttrResume(key), "v": sAttrResume(value)}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put parent row: %v", err)
	}
	return parent, child, td, key
}

func keyInRangeForResume(t *testing.T, rng placement.TokenRange) string {
	t.Helper()
	router := routing.NewRouter(1)
	for i := 0; i < 100_000; i++ {
		key := fmt.Sprintf("resume-key-%d", i)
		if rng.Contains(router.TokenForPK([]byte(key))) {
			return key
		}
	}
	t.Fatal("could not find key in range")
	return ""
}

func sAttrResume(s string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: s}
}

func setSplitFinalizeHook(t *testing.T, hook func(SplitFinalizePhase, SplitFinalizeState) error) {
	t.Helper()
	prev := splitFinalizeTestHook
	splitFinalizeTestHook = hook
	t.Cleanup(func() { splitFinalizeTestHook = prev })
}

func countKeysForResume(t *testing.T, db *pebble.DB, bounds func() ([]byte, []byte)) int {
	t.Helper()
	lower, upper := bounds()
	iter, err := db.Iter(lower, upper)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	var count int
	for valid := iter.First(); valid; valid = iter.Next() {
		count++
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	return count
}
