package client

import (
	"context"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// PlacementPlanRequest packs the inputs PlanPlacement uses to compute
// a proposed shard / range reshape. Operation selects the planner
// algorithm; the remaining fields are interpreted per operation.
type PlacementPlanRequest struct {
	Operation     string
	ShardID       uint32
	SplitToken    *uint64
	NewShardID    *uint32
	TargetShardID *uint32
	RangeStart    *uint64
	RangeEnd      *uint64
	SourceNode    string
	TargetNode    string
	TargetNodes   []string
	TargetVoters  []string
	NodeID        string
	MinVoters     int
}

// PlacementPlanStep is one ordered action a placement plan would
// perform; ApplyPlacement executes the steps in order.
type PlacementPlanStep struct {
	Action  string
	ShardID *uint32
	NodeID  string
	Addr    string
	Detail  string
}

// PlacementPlan is the planner's diff between the current placement
// (Before) and the proposed placement (After), together with the
// ordered Steps and any warnings.
type PlacementPlan struct {
	Operation        string
	BeforeEpoch      uint64
	AfterEpoch       uint64
	Before           PlacementCatalog
	After            PlacementCatalog
	Steps            []PlacementPlanStep
	Warnings         []string
	RequiresDataCopy bool
	RequiresRestart  bool
	ApplySupported   bool
}

// PlacementApplyRequest carries a previously-planned PlacementPlan
// along with the epoch the caller expects the cluster to be at
// (ExpectedEpoch) and an apply timeout in milliseconds.
type PlacementApplyRequest struct {
	Plan          PlacementPlan
	ExpectedEpoch uint64
	TimeoutMS     int
}

// PlacementApplyStep is one executed step inside a PlacementApplyResult,
// with the per-step Status and human-readable Detail.
type PlacementApplyStep struct {
	Action  string
	ShardID *uint32
	NodeID  string
	Status  string
	Detail  string
}

// PlacementApplyResult is the outcome of ApplyPlacement: the executed
// steps and the resulting PlacementCatalog at AfterEpoch.
type PlacementApplyResult struct {
	Operation   string
	BeforeEpoch uint64
	AfterEpoch  uint64
	Steps       []PlacementApplyStep
	Placement   PlacementCatalog
}

// SplitFinalizeRequest finalises an in-progress shard split: the
// parent / child shard IDs, the epoch the caller expects, and whether
// writes have already been quiesced server-side.
type SplitFinalizeRequest struct {
	ParentShardID  uint32
	ChildShardID   uint32
	ExpectedEpoch  uint64
	TimeoutMS      int
	WritesQuiesced bool
}

// SplitFinalizeResult reports the per-shard token ranges before and
// after a split, the keys copied, and the placement at AfterEpoch.
type SplitFinalizeResult struct {
	ParentShardID     uint32
	ChildShardID      uint32
	BeforeEpoch       uint64
	AfterEpoch        uint64
	ParentRangeBefore TokenRange
	ParentRangeAfter  TokenRange
	ChildRange        TokenRange
	CopiedKeys        int64
	CopiedCatalogKeys int64
	DeletedKeys       int64
	Placement         PlacementCatalog
}

// RangeMoveFinalizeRequest finalises an in-progress range move from
// SourceShardID to TargetShardID at the given expected epoch.
type RangeMoveFinalizeRequest struct {
	SourceShardID uint32
	TargetShardID uint32
	ExpectedEpoch uint64
	TimeoutMS     int
}

// RangeMoveFinalizeResult reports the source ranges before / after
// the move, the moved range, the keys copied, the protocol Phase the
// server reached, and the resulting PlacementCatalog.
type RangeMoveFinalizeResult struct {
	SourceShardID      uint32
	TargetShardID      uint32
	BeforeEpoch        uint64
	AfterEpoch         uint64
	SourceRangesBefore []TokenRange
	SourceRangesAfter  []TokenRange
	MovedRange         TokenRange
	CopiedKeys         int64
	CopiedCatalogKeys  int64
	DeletedKeys        int64
	Phase              string
	Placement          PlacementCatalog
}

// PlanPlacement asks the server to compute a placement plan for the
// reshape described by req without applying it.
func (c *Client) PlanPlacement(ctx context.Context, req PlacementPlanRequest) (PlacementPlan, error) {
	pbReq := &cefaspb.PlanPlacementRequest{
		Operation:    req.Operation,
		ShardId:      req.ShardID,
		SourceNode:   req.SourceNode,
		TargetNode:   req.TargetNode,
		TargetNodes:  append([]string(nil), req.TargetNodes...),
		TargetVoters: append([]string(nil), req.TargetVoters...),
		NodeId:       req.NodeID,
		MinVoters:    int32(req.MinVoters),
	}
	if req.SplitToken != nil {
		pbReq.SplitToken = req.SplitToken
	}
	if req.NewShardID != nil {
		pbReq.NewShardId = req.NewShardID
	}
	if req.TargetShardID != nil {
		pbReq.TargetShardId = req.TargetShardID
	}
	if req.RangeStart != nil {
		pbReq.RangeStart = req.RangeStart
	}
	if req.RangeEnd != nil {
		pbReq.RangeEnd = req.RangeEnd
	}
	resp, err := c.stub.PlanPlacement(c.withAuth(ctx), pbReq)
	if err != nil {
		return PlacementPlan{}, err
	}
	return placementPlanFromPB(resp.GetPlan()), nil
}

// ApplyPlacement executes a previously-planned PlacementPlan against
// the cluster; ExpectedEpoch protects against concurrent reshapes.
func (c *Client) ApplyPlacement(ctx context.Context, req PlacementApplyRequest) (PlacementApplyResult, error) {
	resp, err := c.stub.ApplyPlacement(c.withAuth(ctx), &cefaspb.ApplyPlacementRequest{
		Plan:          placementPlanToPB(req.Plan),
		ExpectedEpoch: req.ExpectedEpoch,
		TimeoutMs:     int32(req.TimeoutMS),
	})
	if err != nil {
		return PlacementApplyResult{}, err
	}
	return placementApplyResultFromPB(resp.GetResult()), nil
}

// FinalizeSplit commits an in-progress shard split once the child
// shard has caught up.
func (c *Client) FinalizeSplit(ctx context.Context, req SplitFinalizeRequest) (SplitFinalizeResult, error) {
	resp, err := c.stub.FinalizeSplit(c.withAuth(ctx), &cefaspb.FinalizeSplitRequest{
		ParentShardId:  req.ParentShardID,
		ChildShardId:   req.ChildShardID,
		ExpectedEpoch:  req.ExpectedEpoch,
		TimeoutMs:      int32(req.TimeoutMS),
		WritesQuiesced: req.WritesQuiesced,
	})
	if err != nil {
		return SplitFinalizeResult{}, err
	}
	return splitFinalizeResultFromPB(resp.GetResult()), nil
}

// FinalizeRangeMove commits an in-progress range move once the target
// shard has caught up.
func (c *Client) FinalizeRangeMove(ctx context.Context, req RangeMoveFinalizeRequest) (RangeMoveFinalizeResult, error) {
	resp, err := c.stub.FinalizeRangeMove(c.withAuth(ctx), &cefaspb.FinalizeRangeMoveRequest{
		SourceShardId: req.SourceShardID,
		TargetShardId: req.TargetShardID,
		ExpectedEpoch: req.ExpectedEpoch,
		TimeoutMs:     int32(req.TimeoutMS),
	})
	if err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	return rangeMoveFinalizeResultFromPB(resp.GetResult()), nil
}

func placementPlanToPB(in PlacementPlan) *cefaspb.PlacementPlan {
	return &cefaspb.PlacementPlan{
		Operation:        in.Operation,
		BeforeEpoch:      in.BeforeEpoch,
		AfterEpoch:       in.AfterEpoch,
		Before:           placementCatalogToPB(in.Before),
		After:            placementCatalogToPB(in.After),
		Steps:            placementPlanStepsToPB(in.Steps),
		Warnings:         append([]string(nil), in.Warnings...),
		RequiresDataCopy: in.RequiresDataCopy,
		RequiresRestart:  in.RequiresRestart,
		ApplySupported:   in.ApplySupported,
	}
}

func placementPlanStepsToPB(in []PlacementPlanStep) []*cefaspb.PlacementPlanStep {
	out := make([]*cefaspb.PlacementPlanStep, 0, len(in))
	for _, step := range in {
		out = append(out, &cefaspb.PlacementPlanStep{
			Action:  step.Action,
			ShardId: step.ShardID,
			NodeId:  step.NodeID,
			Addr:    step.Addr,
			Detail:  step.Detail,
		})
	}
	return out
}

func placementPlanFromPB(in *cefaspb.PlacementPlan) PlacementPlan {
	if in == nil {
		return PlacementPlan{}
	}
	return PlacementPlan{
		Operation:        in.GetOperation(),
		BeforeEpoch:      in.GetBeforeEpoch(),
		AfterEpoch:       in.GetAfterEpoch(),
		Before:           placementCatalogFromPB(in.GetBefore()),
		After:            placementCatalogFromPB(in.GetAfter()),
		Steps:            placementPlanStepsFromPB(in.GetSteps()),
		Warnings:         append([]string(nil), in.GetWarnings()...),
		RequiresDataCopy: in.GetRequiresDataCopy(),
		RequiresRestart:  in.GetRequiresRestart(),
		ApplySupported:   in.GetApplySupported(),
	}
}

func placementPlanStepsFromPB(in []*cefaspb.PlacementPlanStep) []PlacementPlanStep {
	out := make([]PlacementPlanStep, 0, len(in))
	for _, step := range in {
		var shardID *uint32
		if step.ShardId != nil {
			id := step.GetShardId()
			shardID = &id
		}
		out = append(out, PlacementPlanStep{
			Action:  step.GetAction(),
			ShardID: shardID,
			NodeID:  step.GetNodeId(),
			Addr:    step.GetAddr(),
			Detail:  step.GetDetail(),
		})
	}
	return out
}

func placementApplyResultFromPB(in *cefaspb.PlacementApplyResult) PlacementApplyResult {
	if in == nil {
		return PlacementApplyResult{}
	}
	return PlacementApplyResult{
		Operation:   in.GetOperation(),
		BeforeEpoch: in.GetBeforeEpoch(),
		AfterEpoch:  in.GetAfterEpoch(),
		Steps:       placementApplyStepsFromPB(in.GetSteps()),
		Placement:   placementCatalogFromPB(in.GetPlacement()),
	}
}

func splitFinalizeResultFromPB(in *cefaspb.FinalizeSplitResult) SplitFinalizeResult {
	if in == nil {
		return SplitFinalizeResult{}
	}
	return SplitFinalizeResult{
		ParentShardID:     in.GetParentShardId(),
		ChildShardID:      in.GetChildShardId(),
		BeforeEpoch:       in.GetBeforeEpoch(),
		AfterEpoch:        in.GetAfterEpoch(),
		ParentRangeBefore: tokenRangeFromPB(in.GetParentRangeBefore()),
		ParentRangeAfter:  tokenRangeFromPB(in.GetParentRangeAfter()),
		ChildRange:        tokenRangeFromPB(in.GetChildRange()),
		CopiedKeys:        in.GetCopiedKeys(),
		CopiedCatalogKeys: in.GetCopiedCatalogKeys(),
		DeletedKeys:       in.GetDeletedKeys(),
		Placement:         placementCatalogFromPB(in.GetPlacement()),
	}
}

func rangeMoveFinalizeResultFromPB(in *cefaspb.FinalizeRangeMoveResult) RangeMoveFinalizeResult {
	if in == nil {
		return RangeMoveFinalizeResult{}
	}
	return RangeMoveFinalizeResult{
		SourceShardID:      in.GetSourceShardId(),
		TargetShardID:      in.GetTargetShardId(),
		BeforeEpoch:        in.GetBeforeEpoch(),
		AfterEpoch:         in.GetAfterEpoch(),
		SourceRangesBefore: tokenRangesFromPB(in.GetSourceRangesBefore()),
		SourceRangesAfter:  tokenRangesFromPB(in.GetSourceRangesAfter()),
		MovedRange:         tokenRangeFromPB(in.GetMovedRange()),
		CopiedKeys:         in.GetCopiedKeys(),
		CopiedCatalogKeys:  in.GetCopiedCatalogKeys(),
		DeletedKeys:        in.GetDeletedKeys(),
		Phase:              in.GetPhase(),
		Placement:          placementCatalogFromPB(in.GetPlacement()),
	}
}

func placementApplyStepsFromPB(in []*cefaspb.PlacementApplyStep) []PlacementApplyStep {
	out := make([]PlacementApplyStep, 0, len(in))
	for _, step := range in {
		var shardID *uint32
		if step.ShardId != nil {
			id := step.GetShardId()
			shardID = &id
		}
		out = append(out, PlacementApplyStep{
			Action:  step.GetAction(),
			ShardID: shardID,
			NodeID:  step.GetNodeId(),
			Status:  step.GetStatus(),
			Detail:  step.GetDetail(),
		})
	}
	return out
}
