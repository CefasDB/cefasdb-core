package placement

import (
	"errors"
	"testing"
)

// TestDefaultPlanStrategiesCoversEveryOperation pins the registry's
// shape: every PlacementOperation* constant declared in planner.go
// must have an entry. A new operation that lands without a matching
// strategy fails the build here instead of returning the generic
// "unknown placement operation" error at runtime.
func TestDefaultPlanStrategiesCoversEveryOperation(t *testing.T) {
	t.Parallel()
	want := []PlacementOperation{
		PlacementOperationSplit,
		PlacementOperationMove,
		PlacementOperationRangeMove,
		PlacementOperationDrain,
		PlacementOperationDecommission,
	}
	if len(defaultPlanStrategies) != len(want) {
		t.Fatalf("registry has %d entries, want %d", len(defaultPlanStrategies), len(want))
	}
	for _, op := range want {
		if _, ok := defaultPlanStrategies[op]; !ok {
			t.Errorf("registry is missing strategy for %q", op)
		}
	}
}

// TestBuildPlacementPlanRejectsUnknownOperation pins the dispatcher
// behaviour for operations that aren't in the registry.
func TestBuildPlacementPlanRejectsUnknownOperation(t *testing.T) {
	t.Parallel()
	cat := PlacementCatalog{
		Version:  PlacementVersion,
		Epoch:    1,
		Strategy: PlacementStrategyLegacyModulo,
		Shards: []ShardPlacement{
			{ID: 0, State: ShardStateActive, Epoch: 1},
		},
	}
	_, err := BuildPlacementPlan(cat, PlacementPlanRequest{Operation: "no-such-op"})
	if err == nil {
		t.Fatal("expected error for unknown operation, got nil")
	}
	if !errors.Is(err, ErrInvalidPlacementPlan) {
		t.Fatalf("err = %v, want wraps ErrInvalidPlacementPlan", err)
	}
}
