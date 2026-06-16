package cluster

import "strings"

// planDecommission marks a previously-drained node as
// decommissioned. The plan only mutates placement metadata; the
// operator runs the data-cleanup hooks on the node itself before
// stopping it for good.
func planDecommission(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	if err := validateDecommissionInputs(cat, req); err != nil {
		return PlacementPlan{}, err
	}
	after, err := applyDecommissionTransition(cat, req.NodeID)
	if err != nil {
		return PlacementPlan{}, err
	}
	return buildDecommissionPlan(cat, after, req.NodeID), nil
}

// validateDecommissionInputs enforces every precondition: nodeId
// supplied, node exists, not already decommissioned, currently
// Draining, and no active placement references remain.
func validateDecommissionInputs(cat PlacementCatalog, req PlacementPlanRequest) error {
	if req.NodeID == "" {
		return invalidPlan("decommission requires nodeId")
	}
	node, ok := cat.Nodes[req.NodeID]
	if !ok {
		return invalidPlan("node %q does not exist in placement", req.NodeID)
	}
	if node.State == NodeStateDecommissioned {
		return invalidPlan("node %q is already decommissioned", req.NodeID)
	}
	if node.State != NodeStateDraining {
		return invalidPlan("node %q must be draining before decommission; current state=%s", req.NodeID, node.State)
	}
	if blockers := placementNodeActiveReferences(cat, req.NodeID); len(blockers) > 0 {
		return invalidPlan("node %q still has active placement references: %s", req.NodeID, strings.Join(blockers, "; "))
	}
	return nil
}

// applyDecommissionTransition mutates the node state to
// NodeStateDecommissioned on a fresh next-epoch catalog.
func applyDecommissionTransition(cat PlacementCatalog, nodeID string) (PlacementCatalog, error) {
	after := nextCatalog(cat)
	node := after.Nodes[nodeID]
	node.State = NodeStateDecommissioned
	after.Nodes[nodeID] = node
	after.normalize()
	if err := ValidatePlacement(after); err != nil {
		return PlacementCatalog{}, err
	}
	return after, nil
}

// buildDecommissionPlan assembles the four-step plan and the
// operator-facing warning.
func buildDecommissionPlan(cat, after PlacementCatalog, nodeID string) PlacementPlan {
	return PlacementPlan{
		Operation:   PlacementOperationDecommission,
		BeforeEpoch: cat.Epoch,
		AfterEpoch:  after.Epoch,
		Before:      cat.Clone(),
		After:       after,
		Steps: []PlacementPlanStep{
			{Action: "verify_no_active_references", NodeID: nodeID, Detail: "drain complete: no active voter, non-voter, leader hint, or range ownership references remain"},
			{Action: "cleanup_unowned_data", NodeID: nodeID, Detail: "local data cleanup hook for ranges no longer owned by this node"},
			{Action: "compact_unowned_data", NodeID: nodeID, Detail: "local compaction hook after unowned range cleanup"},
			{Action: "decommission_node", NodeID: nodeID, Detail: "mark node decommissioned and exclude it from future placement"},
		},
		Warnings: []string{
			"decommission only marks placement metadata after drain has removed all active references; run cleanup hooks on the decommissioned node before stopping it permanently",
		},
		ApplySupported: true,
	}
}
