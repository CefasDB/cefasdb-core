package cluster

import "strings"

// planDecommission marks a previously-drained node as
// decommissioned. The plan only mutates placement metadata; the
// operator runs the data-cleanup hooks on the node itself before
// stopping it for good.
func planDecommission(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	if req.NodeID == "" {
		return PlacementPlan{}, invalidPlan("decommission requires nodeId")
	}
	node, ok := cat.Nodes[req.NodeID]
	if !ok {
		return PlacementPlan{}, invalidPlan("node %q does not exist in placement", req.NodeID)
	}
	if node.State == NodeStateDecommissioned {
		return PlacementPlan{}, invalidPlan("node %q is already decommissioned", req.NodeID)
	}
	if node.State != NodeStateDraining {
		return PlacementPlan{}, invalidPlan("node %q must be draining before decommission; current state=%s", req.NodeID, node.State)
	}
	blockers := placementNodeActiveReferences(cat, req.NodeID)
	if len(blockers) > 0 {
		return PlacementPlan{}, invalidPlan("node %q still has active placement references: %s", req.NodeID, strings.Join(blockers, "; "))
	}

	after := nextCatalog(cat)
	node = after.Nodes[req.NodeID]
	node.State = NodeStateDecommissioned
	after.Nodes[req.NodeID] = node
	after.normalize()
	if err := ValidatePlacement(after); err != nil {
		return PlacementPlan{}, err
	}

	return PlacementPlan{
		Operation:   PlacementOperationDecommission,
		BeforeEpoch: cat.Epoch,
		AfterEpoch:  after.Epoch,
		Before:      cat.Clone(),
		After:       after,
		Steps: []PlacementPlanStep{
			{Action: "verify_no_active_references", NodeID: req.NodeID, Detail: "drain complete: no active voter, non-voter, leader hint, or range ownership references remain"},
			{Action: "cleanup_unowned_data", NodeID: req.NodeID, Detail: "local data cleanup hook for ranges no longer owned by this node"},
			{Action: "compact_unowned_data", NodeID: req.NodeID, Detail: "local compaction hook after unowned range cleanup"},
			{Action: "decommission_node", NodeID: req.NodeID, Detail: "mark node decommissioned and exclude it from future placement"},
		},
		Warnings: []string{
			"decommission only marks placement metadata after drain has removed all active references; run cleanup hooks on the decommissioned node before stopping it permanently",
		},
		ApplySupported: true,
	}, nil
}
