package client

import (
	"context"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// PipelineStageTiming reports per-stage input / output counts and
// elapsed time for a server-side pipeline run (Recommend,
// NextBestAction, etc.).
type PipelineStageTiming struct {
	Stage       string
	InputCount  int
	OutputCount int
	ElapsedMS   float64
	ReasonCodes []string
}

// RecommendRequest packs the inputs to the server-side Recommend
// pipeline: ANN candidate retrieval, optional filter, MMR rerank,
// dedup and frequency-cap stages.
type RecommendRequest struct {
	Table                string
	Field                string
	DistanceOperator     string
	Target               types.AttributeValue
	CandidateLimit       int
	FilterExpression     string
	Limit                int
	MMRLambda            float64
	DisableDiversify     bool
	DedupScope           string
	DedupKeyField        string
	DedupTTLSeconds      int64
	FreqCapScope         string
	FreqCapKeyField      string
	FreqCapLimit         int
	FreqCapWindowSeconds int64
}

// RecommendRow is one returned recommendation, with the matching
// distance and an optional Reason annotation.
type RecommendRow struct {
	Item     types.Item
	Distance float64
	Reason   string
}

// RecommendResponse bundles the returned rows, the per-stage timings,
// and any pipeline-level reason codes.
type RecommendResponse struct {
	Rows        []RecommendRow
	Stages      []PipelineStageTiming
	ReasonCodes []string
}

// Recommend runs the server-side recommendation pipeline (ANN +
// optional rerank / dedup / freq-cap) and returns the ranked slate.
func (c *Client) Recommend(ctx context.Context, req RecommendRequest) (RecommendResponse, error) {
	resp, err := c.stub.Recommend(c.withAuth(ctx), &cefaspb.RecommendRequest{
		Table:                req.Table,
		Field:                req.Field,
		DistanceOperator:     req.DistanceOperator,
		Target:               attrToPB(req.Target),
		CandidateLimit:       int32(req.CandidateLimit),
		FilterExpression:     req.FilterExpression,
		Limit:                int32(req.Limit),
		MmrLambda:            req.MMRLambda,
		DisableDiversify:     req.DisableDiversify,
		DedupScope:           req.DedupScope,
		DedupKeyField:        req.DedupKeyField,
		DedupTtlSeconds:      req.DedupTTLSeconds,
		FreqcapScope:         req.FreqCapScope,
		FreqcapKeyField:      req.FreqCapKeyField,
		FreqcapLimit:         int32(req.FreqCapLimit),
		FreqcapWindowSeconds: req.FreqCapWindowSeconds,
	})
	if err != nil {
		return RecommendResponse{}, err
	}
	out := RecommendResponse{ReasonCodes: append([]string(nil), resp.GetReasonCodes()...)}
	for _, row := range resp.GetRows() {
		out.Rows = append(out.Rows, RecommendRow{
			Item:     itemFromPB(row.GetItem().GetAttributes()),
			Distance: row.GetDistance(),
			Reason:   row.GetReason(),
		})
	}
	out.Stages = stageTimingsFromPB(resp.GetStages())
	return out, nil
}

// NBAAction is one candidate action offered to the NextBestAction
// pipeline; Disabled flags an action that must be skipped, with
// Reason carrying the explanation.
type NBAAction struct {
	ActionID string
	Disabled bool
	Reason   string
	Context  map[string]string
}

// NextBestActionRequest packs the inputs to the NextBestAction
// bandit-backed pipeline: the bandit ID, the user, the candidate
// actions, optional fallback and capping parameters.
type NextBestActionRequest struct {
	BanditID           string
	UserID             string
	Actions            []NBAAction
	FallbackActionID   string
	Context            map[string]string
	CapScope           string
	CapLimit           int
	CapWindowSeconds   int64
	DecisionTTLSeconds int64
}

// NextBestActionResponse carries the selected action, whether the
// fallback was used, and per-stage diagnostics.
type NextBestActionResponse struct {
	DecisionID  string
	ActionID    string
	Fallback    bool
	ReasonCodes []string
	Stages      []PipelineStageTiming
}

// NextBestAction asks the server to select the next-best action for
// a user via a bandit, honouring caps and decision-TTL.
func (c *Client) NextBestAction(ctx context.Context, req NextBestActionRequest) (NextBestActionResponse, error) {
	actions := make([]*cefaspb.NBAAction, 0, len(req.Actions))
	for _, a := range req.Actions {
		actions = append(actions, &cefaspb.NBAAction{
			ActionId: a.ActionID,
			Disabled: a.Disabled,
			Reason:   a.Reason,
			Context:  copyStringMap(a.Context),
		})
	}
	resp, err := c.stub.NextBestAction(c.withAuth(ctx), &cefaspb.NextBestActionRequest{
		BanditId:           req.BanditID,
		UserId:             req.UserID,
		Actions:            actions,
		FallbackActionId:   req.FallbackActionID,
		Context:            copyStringMap(req.Context),
		CapScope:           req.CapScope,
		CapLimit:           int32(req.CapLimit),
		CapWindowSeconds:   req.CapWindowSeconds,
		DecisionTtlSeconds: req.DecisionTTLSeconds,
	})
	if err != nil {
		return NextBestActionResponse{}, err
	}
	return NextBestActionResponse{
		DecisionID:  resp.GetDecisionId(),
		ActionID:    resp.GetActionId(),
		Fallback:    resp.GetFallback(),
		ReasonCodes: append([]string(nil), resp.GetReasonCodes()...),
		Stages:      stageTimingsFromPB(resp.GetStages()),
	}, nil
}

// RecordRewardRequest packs the reward attribution payload sent to
// RecordReward; DecisionID resolves to the original bandit / action
// when set, otherwise BanditID and ActionID are used.
type RecordRewardRequest struct {
	DecisionID string
	BanditID   string
	ActionID   string
	Reward     float64
	Context    map[string]string
}

// RecordRewardResponse echoes the bandit and action the server
// attributed the reward to.
type RecordRewardResponse struct {
	BanditID string
	ActionID string
}

// RecordReward attributes a reward to a previous NextBestAction
// decision (or to an explicit bandit / action pair).
func (c *Client) RecordReward(ctx context.Context, req RecordRewardRequest) (RecordRewardResponse, error) {
	resp, err := c.stub.RecordReward(c.withAuth(ctx), &cefaspb.RecordRewardRequest{
		DecisionId: req.DecisionID,
		BanditId:   req.BanditID,
		ActionId:   req.ActionID,
		Reward:     req.Reward,
		Context:    copyStringMap(req.Context),
	})
	if err != nil {
		return RecordRewardResponse{}, err
	}
	return RecordRewardResponse{BanditID: resp.GetBanditId(), ActionID: resp.GetActionId()}, nil
}

// DecisionRecord is the persisted server-side audit trail for one
// NextBestAction decision, suitable for later reward attribution.
type DecisionRecord struct {
	DecisionID    string
	BanditID      string
	UserID        string
	ActionID      string
	Fallback      bool
	ReasonCodes   []string
	Context       map[string]string
	CreatedAtUnix int64
	ExpiresAtUnix int64
}

// GetDecision returns the stored DecisionRecord for decisionID; the
// boolean is false when the decision has expired or never existed.
func (c *Client) GetDecision(ctx context.Context, decisionID string) (DecisionRecord, bool, error) {
	resp, err := c.stub.GetDecision(c.withAuth(ctx), &cefaspb.GetDecisionRequest{DecisionId: decisionID})
	if err != nil {
		return DecisionRecord{}, false, err
	}
	if !resp.GetFound() || resp.GetDecision() == nil {
		return DecisionRecord{}, false, nil
	}
	d := resp.GetDecision()
	return DecisionRecord{
		DecisionID:    d.GetDecisionId(),
		BanditID:      d.GetBanditId(),
		UserID:        d.GetUserId(),
		ActionID:      d.GetActionId(),
		Fallback:      d.GetFallback(),
		ReasonCodes:   append([]string(nil), d.GetReasonCodes()...),
		Context:       copyStringMap(d.GetContext()),
		CreatedAtUnix: d.GetCreatedAtUnix(),
		ExpiresAtUnix: d.GetExpiresAtUnix(),
	}, true, nil
}

func stageTimingsFromPB(in []*cefaspb.PipelineStageTiming) []PipelineStageTiming {
	out := make([]PipelineStageTiming, 0, len(in))
	for _, s := range in {
		out = append(out, PipelineStageTiming{
			Stage:       s.GetStage(),
			InputCount:  int(s.GetInputCount()),
			OutputCount: int(s.GetOutputCount()),
			ElapsedMS:   s.GetElapsedMs(),
			ReasonCodes: append([]string(nil), s.GetReasonCodes()...),
		})
	}
	return out
}

func copyStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
