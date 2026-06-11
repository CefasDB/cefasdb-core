package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

func TestFinalizeRangeMoveRetriesCleanupAfterPublishFailure(t *testing.T) {
	mgr, plan := openTransitionRangeMoveManagerForResume(t)
	defer mgr.Close()

	source, target, td, key := seedSplitRow(t, mgr, plan, "range-cleanup-retry")
	var failed bool
	setRangeMoveFinalizeHook(t, func(phase RangeMoveFinalizePhase, _ RangeMoveFinalizeState) error {
		if phase == RangeMoveFinalizePhasePublished && !failed {
			failed = true
			return fmt.Errorf("stop after publish")
		}
		return nil
	})

	_, err := mgr.FinalizeRangeMove(context.Background(), RangeMoveFinalizeRequest{
		SourceShardID: 0,
		TargetShardID: 1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err == nil {
		t.Fatal("first finalize unexpectedly succeeded")
	}
	if _, err := source.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttrResume(key)}); err != nil {
		t.Fatalf("source row should remain before cleanup retry: %v", err)
	}
	rangeMoveFinalizeTestHook = nil
	result, err := mgr.FinalizeRangeMove(context.Background(), RangeMoveFinalizeRequest{
		SourceShardID: 0,
		TargetShardID: 1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err != nil {
		t.Fatalf("cleanup retry: %v", err)
	}
	if result.Phase != string(RangeMoveFinalizePhaseDone) {
		t.Fatalf("retry phase = %q, want done", result.Phase)
	}
	if _, err := source.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttrResume(key)}); !errors.Is(err, types.ErrItemNotFound) {
		t.Fatalf("source row after cleanup retry error = %v, want not found", err)
	}
	if got, err := target.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttrResume(key)}); err != nil || got["v"].S != "range-cleanup-retry" {
		t.Fatalf("target row after cleanup retry = %+v err=%v", got, err)
	}
}

func openTransitionRangeMoveManagerForResume(t *testing.T) (*Manager, PlacementPlan) {
	t.Helper()
	root := t.TempDir()
	cat := DefaultPlacement(1, "n1", nil, nil, NodeCapacity{}, PlacementStrategyTokenRange)
	start := uint64(0)
	end := uint64(1) << 63
	plan, err := BuildPlacementPlan(cat, PlacementPlanRequest{
		Operation:  PlacementOperationRangeMove,
		ShardID:    0,
		RangeStart: &start,
		RangeEnd:   &end,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := SavePlacementFile(filepath.Join(root, "placement.json"), plan.After); err != nil {
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

func setRangeMoveFinalizeHook(t *testing.T, hook func(RangeMoveFinalizePhase, RangeMoveFinalizeState) error) {
	t.Helper()
	prev := rangeMoveFinalizeTestHook
	rangeMoveFinalizeTestHook = hook
	t.Cleanup(func() { rangeMoveFinalizeTestHook = prev })
}
