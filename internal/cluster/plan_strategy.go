package cluster

// PlanStrategy resolves a PlacementPlanRequest of a specific
// PlacementOperation into a PlacementPlan. Every operation
// (split, move, range-move, drain, decommission) ships its own
// implementation in its own file under this package; the dispatcher
// in BuildPlacementPlan looks the strategy up by operation kind.
//
// The interface exists so each operation lives in its own file
// (Fowler: Replace Conditional with Polymorphism) and so adding a
// new operation is one new file + one registry line, not another
// branch in a giant switch.
type PlanStrategy interface {
	Plan(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error)
}

// planFunc adapts a plain func to the PlanStrategy interface so the
// per-operation files can stay as functions (no boilerplate struct)
// while still being addressable through the registry.
type planFunc func(PlacementCatalog, PlacementPlanRequest) (PlacementPlan, error)

// Plan implements PlanStrategy.
func (f planFunc) Plan(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	return f(cat, req)
}

// defaultPlanStrategies is the canonical registry consulted by
// BuildPlacementPlan. Every PlacementOperation* constant defined in
// placement.go must appear here; the registry contract test in
// plan_strategy_test.go fails the build if a new operation lands
// without a matching strategy entry.
var defaultPlanStrategies = map[PlacementOperation]PlanStrategy{
	PlacementOperationSplit:        planFunc(planSplit),
	PlacementOperationMove:         planFunc(planMove),
	PlacementOperationRangeMove:    planFunc(planRangeMove),
	PlacementOperationDrain:        planFunc(planDrain),
	PlacementOperationDecommission: planFunc(planDecommission),
}
