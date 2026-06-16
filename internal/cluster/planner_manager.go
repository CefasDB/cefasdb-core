package cluster

import "github.com/osvaldoandrade/cefas/internal/placement"

// PlanPlacement refreshes the manager's view of the catalog and then
// delegates to placement.BuildPlacementPlan to produce the plan. Pure
// plan-building logic lives in internal/placement; this method is the
// orchestration wrapper that knows about Manager state.
func (m *Manager) PlanPlacement(req placement.PlacementPlanRequest) (placement.PlacementPlan, error) {
	if err := m.RefreshPlacement(); err != nil {
		return placement.PlacementPlan{}, err
	}
	return placement.BuildPlacementPlan(m.Placement(), req)
}
