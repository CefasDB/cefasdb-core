package placement

import (
	"strings"

	"github.com/osvaldoandrade/cefas/pkg/core/model"
)

// planDecommission marks a previously-drained node as
// decommissioned. The plan only mutates placement metadata; the
// operator runs the data-cleanup hooks on the node itself before
// stopping it for good.
func planDecommission(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
	nodeID, err := validateDecommissionInputs(cat, req)
	if err != nil {
		return PlacementPlan{}, err
	}
	after, err := applyDecommissionTransition(cat, nodeID)
	if err != nil {
		return PlacementPlan{}, err
	}
	return buildDecommissionPlan(cat, after, nodeID), nil
}

// validateDecommissionInputs enforces every precondition: nodeId
// supplied, node exists, not already decommissioned, currently
// Draining, and no active placement references remain. Returns the
// validated NodeID so the downstream helpers consume a VO rather
// than the raw request string.
func validateDecommissionInputs(cat PlacementCatalog, req PlacementPlanRequest) (model.NodeID, error) {
	nodeID, err := model.NewNodeID(req.NodeID)
	if err != nil {
		return model.NodeID{}, InvalidPlan("decommission requires nodeId")
	}
	key := nodeID.String()
	node, ok := cat.Nodes[key]
	if !ok {
		return model.NodeID{}, InvalidPlan("node %q does not exist in placement", key)
	}
	if node.State == NodeStateDecommissioned {
		return model.NodeID{}, InvalidPlan("node %q is already decommissioned", key)
	}
	if node.State != NodeStateDraining {
		return model.NodeID{}, InvalidPlan("node %q must be draining before decommission; current state=%s", key, node.State)
	}
	if blockers := PlacementNodeActiveReferences(cat, nodeID); len(blockers) > 0 {
		return model.NodeID{}, InvalidPlan("node %q still has active placement references: %s", key, strings.Join(blockers, "; "))
	}
	return nodeID, nil
}

// applyDecommissionTransition mutates the node state to
// NodeStateDecommissioned on a fresh next-epoch catalog.
func applyDecommissionTransition(cat PlacementCatalog, nodeID model.NodeID) (PlacementCatalog, error) {
	after := NextCatalog(cat)
	key := nodeID.String()
	node := after.Nodes[key]
	node.State = NodeStateDecommissioned
	after.Nodes[key] = node
	after.Normalize()
	if err := ValidatePlacement(after); err != nil {
		return PlacementCatalog{}, err
	}
	return after, nil
}

// buildDecommissionPlan assembles the four-step plan and the
// operator-facing warning.
func buildDecommissionPlan(cat, after PlacementCatalog, nodeID model.NodeID) PlacementPlan {
	key := nodeID.String()
	return PlacementPlan{
		Operation:   PlacementOperationDecommission,
		BeforeEpoch: cat.Epoch,
		AfterEpoch:  after.Epoch,
		Before:      cat.Clone(),
		After:       after,
		Steps: []PlacementPlanStep{
			{Action: "verify_no_active_references", NodeID: key, Detail: "drain complete: no active voter, non-voter, leader hint, or range ownership references remain"},
			{Action: "cleanup_unowned_data", NodeID: key, Detail: "local data cleanup hook for ranges no longer owned by this node"},
			{Action: "compact_unowned_data", NodeID: key, Detail: "local compaction hook after unowned range cleanup"},
			{Action: "decommission_node", NodeID: key, Detail: "mark node decommissioned and exclude it from future placement"},
		},
		Warnings: []string{
			"decommission only marks placement metadata after drain has removed all active references; run cleanup hooks on the decommissioned node before stopping it permanently",
		},
		ApplySupported: true,
	}
}
