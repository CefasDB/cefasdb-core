package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/auth"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	"github.com/osvaldoandrade/cefas/internal/tracing"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/protocol"
	cquery "github.com/osvaldoandrade/cefas/internal/core/query"
	"github.com/osvaldoandrade/cefas/internal/core/query/mmr"
	"github.com/osvaldoandrade/cefas/pkg/plugin/audience"
	"github.com/osvaldoandrade/cefas/pkg/plugin/bandit"
	cefassql "github.com/osvaldoandrade/cefas/internal/sql"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

const (
	defaultRecommendFanout = 100
	defaultDecisionTTL     = 24 * time.Hour
	decisionLogPrefix      = "cefas/internal/__decisions__/"
)

type recommendationRow struct {
	item     types.Item
	distance float64
}

// Recommend composes retrieve -> filter -> diversify -> cap into one
// server-side call. Retrieval is the existing TopK path; filter uses
// the SQL predicate evaluator; diversify is MMR over the candidate
// set; cap optionally applies dedup/frequency gates before the final
// limit.
func (s *GRPCServer) Recommend(ctx context.Context, req *cefaspb.RecommendRequest) (*cefaspb.RecommendResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "Recommend")
	defer span.End()
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemRead, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemRead)); err != nil {
		return nil, err
	}
	if req.GetTable() == "" || req.GetField() == "" {
		return nil, status.Error(codes.InvalidArgument, "table and field required")
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		return nil, status.Error(codes.InvalidArgument, "limit must be > 0")
	}
	lambda := req.GetMmrLambda()
	if lambda < 0 || lambda > 1 {
		return nil, status.Errorf(codes.InvalidArgument, "mmr_lambda %.3f out of [0,1]", lambda)
	}
	candidateLimit := int(req.GetCandidateLimit())
	if candidateLimit <= 0 {
		candidateLimit = maxInt(defaultRecommendFanout, maxInt(limit, limit*4))
	}
	if candidateLimit < limit {
		candidateLimit = limit
	}
	target, err := pbToAttr(req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("target: %v", err))
	}
	dist, err := s.resolveTopKDistance(req.GetTable(), req.GetField(), req.GetDistanceOperator(), target)
	if err != nil {
		return nil, err
	}

	resp := &cefaspb.RecommendResponse{}
	reasons := reasonSet{}

	start := time.Now()
	topk, scanned, err := s.recommendCandidates(req.GetTable(), req.GetField(), req.GetDistanceOperator(), target, candidateLimit, dist)
	if err != nil {
		return nil, err
	}
	rows := make([]recommendationRow, 0, len(topk))
	for _, r := range topk {
		rows = append(rows, recommendationRow{item: r.Item, distance: r.Distance})
	}
	resp.Stages = append(resp.Stages, pipelineStage("retrieve", scanned, len(rows), start))

	if req.GetFilterExpression() != "" {
		start = time.Now()
		pred, err := parsePipelineFilter(req.GetTable(), req.GetFilterExpression())
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		filtered := rows[:0]
		for _, row := range rows {
			ok, err := cefassql.EvalBool(pred, row.item, nil)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "filter: %v", err)
			}
			if ok {
				filtered = append(filtered, row)
			} else {
				reasons.add("filter:predicate_false")
			}
		}
		resp.Stages = append(resp.Stages, pipelineStage("filter", len(rows), len(filtered), start, reasons.slice()...))
		rows = filtered
	}

	if !req.GetDisableDiversify() && len(rows) > 0 {
		start = time.Now()
		targetSize := minInt(limit, len(rows))
		cands := make([]mmr.Candidate, 0, len(rows))
		for _, row := range rows {
			cands = append(cands, mmr.Candidate{Item: row.item, Distance: row.distance, Vector: row.item[req.GetField()]})
		}
		slate, err := mmr.Rerank(mmr.Request{
			Candidates: cands,
			Sim:        mmr.SimilarityFromDistance(dist, req.GetField()),
			Lambda:     lambda,
			N:          targetSize,
		})
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "mmr: %v", err)
		}
		diversified := make([]recommendationRow, 0, len(slate))
		for _, pick := range slate {
			diversified = append(diversified, recommendationRow{item: pick.Item, distance: pick.Distance})
		}
		resp.Stages = append(resp.Stages, pipelineStage("diversify", len(rows), len(diversified), start))
		rows = diversified
	}

	start = time.Now()
	capped, capReasons, err := s.applyRecommendationCaps(req, rows, limit)
	if err != nil {
		return nil, err
	}
	reasons.addAll(capReasons...)
	resp.Stages = append(resp.Stages, pipelineStage("cap", len(rows), len(capped), start, capReasons...))
	for _, row := range capped {
		resp.Rows = append(resp.Rows, &cefaspb.RecommendRow{
			Item:     &cefaspb.Item{Attributes: itemToPB(row.item)},
			Distance: row.distance,
			Reason:   "selected",
		})
	}
	resp.ReasonCodes = reasons.slice()
	return resp, nil
}

func (s *GRPCServer) recommendCandidates(table, field, explicit string, target types.AttributeValue, candidateLimit int, dist cquery.DistanceOp) ([]cquery.TopKResult, int, error) {
	if ann, ok, err := s.indexedANNTopK(table, field, target, candidateLimit, explicit); err != nil {
		return nil, 0, err
	} else if ok {
		return ann.rows, ann.candidateCount, nil
	}
	if strings.TrimSpace(explicit) == "" {
		return nil, 0, status.Errorf(codes.FailedPrecondition, "no ann index for %s.%s", table, field)
	}
	rows, scanned, err := s.exactScanTopK(table, field, target, candidateLimit, dist, explicit)
	if err != nil {
		return nil, 0, err
	}
	return rows, scanned, nil
}

func parsePipelineFilter(table, expr string) (cefassql.Expr, error) {
	stmt, err := cefassql.Parse("SELECT * FROM " + table + " WHERE " + expr + " LIMIT 1")
	if err != nil {
		return nil, fmt.Errorf("filter_expression: %w", err)
	}
	sel, ok := stmt.(*cefassql.SelectStmt)
	if !ok || sel.Where == nil {
		return nil, fmt.Errorf("filter_expression: no predicate parsed")
	}
	return sel.Where, nil
}

func (s *GRPCServer) applyRecommendationCaps(req *cefaspb.RecommendRequest, rows []recommendationRow, limit int) ([]recommendationRow, []string, error) {
	needsDedup := req.GetDedupKeyField() != "" && req.GetDedupTtlSeconds() > 0
	needsFreqCap := req.GetFreqcapKeyField() != "" && req.GetFreqcapLimit() > 0 && req.GetFreqcapWindowSeconds() > 0
	if !needsDedup && !needsFreqCap {
		if len(rows) > limit {
			return rows[:limit], nil, nil
		}
		return rows, nil, nil
	}
	a, err := s.audiencePlugin()
	if err != nil {
		return nil, nil, err
	}
	reasons := reasonSet{}
	out := make([]recommendationRow, 0, minInt(limit, len(rows)))
	for _, row := range rows {
		if len(out) >= limit {
			break
		}
		if needsDedup {
			key, ok := itemString(row.item, req.GetDedupKeyField())
			if !ok {
				reasons.add("dedup:missing_key")
				continue
			}
			scope := req.GetDedupScope()
			if scope == "" {
				scope = req.GetTable()
			}
			allowed, err := a.Dedup(scope, key, time.Duration(req.GetDedupTtlSeconds())*time.Second)
			if err != nil {
				return nil, nil, status.Errorf(codes.InvalidArgument, "dedup: %v", err)
			}
			if !allowed {
				reasons.add("dedup:duplicate")
				continue
			}
		}
		if needsFreqCap {
			key, ok := itemString(row.item, req.GetFreqcapKeyField())
			if !ok {
				reasons.add("freqcap:missing_key")
				continue
			}
			scope := req.GetFreqcapScope()
			if scope == "" {
				scope = req.GetTable()
			}
			allowed, err := a.FreqCap(scope, key, int(req.GetFreqcapLimit()), time.Duration(req.GetFreqcapWindowSeconds())*time.Second)
			if err != nil {
				return nil, nil, status.Errorf(codes.InvalidArgument, "freqcap: %v", err)
			}
			if !allowed {
				reasons.add("freqcap:limit")
				continue
			}
		}
		out = append(out, row)
	}
	return out, reasons.slice(), nil
}

// NextBestAction composes eligibility -> bandit -> cap -> action and
// writes every decision to the internal __decisions__ namespace.
func (s *GRPCServer) NextBestAction(ctx context.Context, req *cefaspb.NextBestActionRequest) (*cefaspb.NextBestActionResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "NextBestAction")
	defer span.End()
	if err := requireAnyScope(ctx, auth.ScopeItemRead, auth.ScopeItemWrite); err != nil {
		return nil, err
	}
	if req.GetBanditId() == "" || req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "bandit_id and user_id required")
	}
	resp := &cefaspb.NextBestActionResponse{}
	reasons := reasonSet{}

	start := time.Now()
	eligible := make([]*cefaspb.NBAAction, 0, len(req.GetActions()))
	for _, action := range req.GetActions() {
		if action == nil || action.GetActionId() == "" {
			reasons.add("eligibility:missing_action_id")
			continue
		}
		if action.GetDisabled() {
			if action.GetReason() != "" {
				reasons.add("eligibility:" + action.GetReason())
			} else {
				reasons.add("eligibility:disabled")
			}
			continue
		}
		eligible = append(eligible, action)
	}
	resp.Stages = append(resp.Stages, pipelineStage("eligibility", len(req.GetActions()), len(eligible), start, reasons.slice()...))

	actionID := ""
	fallback := false
	if len(eligible) == 0 {
		actionID = req.GetFallbackActionId()
		fallback = true
		reasons.add("fallback:no_eligible_actions")
		if actionID == "" {
			return nil, status.Error(codes.FailedPrecondition, "no eligible actions and no fallback_action_id")
		}
	} else {
		bp, err := s.ensureBanditStore()
		if err != nil {
			return nil, err
		}
		start = time.Now()
		pick, unknown, err := bp.SampleEligible(req.GetBanditId(), req.GetContext(), eligibleBanditArmIDs(eligible))
		for _, armID := range unknown {
			reasons.add("bandit:unknown_arm:" + armID)
		}
		if errors.Is(err, bandit.ErrNoEligibleArms) {
			reasons.add("fallback:no_registered_eligible_actions")
			actionID = req.GetFallbackActionId()
			fallback = true
			resp.Stages = append(resp.Stages, pipelineStage("bandit", len(eligible), 0, start, reasons.slice()...))
			if actionID == "" {
				return nil, status.Error(codes.FailedPrecondition, "no eligible registered bandit arms and no fallback_action_id")
			}
		} else if err != nil {
			return nil, mapBanditErr(err)
		} else {
			actionID = pick
			resp.Stages = append(resp.Stages, pipelineStage("bandit", len(eligible), 1, start, "bandit:strategy"))
		}

		if !fallback {
			start = time.Now()
			capAllowed, capReason, err := s.checkNBACap(req, actionID)
			if err != nil {
				return nil, err
			}
			if !capAllowed {
				reasons.add(capReason)
				if req.GetFallbackActionId() == "" {
					return nil, status.Error(codes.FailedPrecondition, "selected action blocked by cap and no fallback_action_id")
				}
				actionID = req.GetFallbackActionId()
				fallback = true
			}
			resp.Stages = append(resp.Stages, pipelineStage("cap", 1, boolCount(capAllowed), start, capReason))
		}
	}

	decisionID, err := newDecisionID()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decision id: %v", err)
	}
	rec := &cefaspb.DecisionRecord{
		DecisionId:  decisionID,
		BanditId:    req.GetBanditId(),
		UserId:      req.GetUserId(),
		ActionId:    actionID,
		Fallback:    fallback,
		ReasonCodes: reasons.slice(),
		Context:     mergeStringMaps(req.GetContext(), actionContext(actionID, req.GetActions())),
	}
	ttl := time.Duration(req.GetDecisionTtlSeconds()) * time.Second
	if ttl <= 0 {
		ttl = defaultDecisionTTL
	}
	now := time.Now()
	rec.CreatedAtUnix = now.Unix()
	rec.ExpiresAtUnix = now.Add(ttl).Unix()
	if err := s.saveDecision(rec); err != nil {
		return nil, status.Errorf(codes.Internal, "decision log: %v", err)
	}

	resp.DecisionId = decisionID
	resp.ActionId = actionID
	resp.Fallback = fallback
	resp.ReasonCodes = reasons.slice()
	return resp, nil
}

func eligibleBanditArmIDs(eligible []*cefaspb.NBAAction) []string {
	out := make([]string, 0, len(eligible))
	for _, action := range eligible {
		out = append(out, action.GetActionId())
	}
	return out
}

func (s *GRPCServer) checkNBACap(req *cefaspb.NextBestActionRequest, actionID string) (bool, string, error) {
	if req.GetCapLimit() <= 0 || req.GetCapWindowSeconds() <= 0 {
		return true, "", nil
	}
	a, err := s.audiencePlugin()
	if err != nil {
		return false, "", err
	}
	scope := req.GetCapScope()
	if scope == "" {
		scope = req.GetBanditId() + "/" + actionID
	}
	allowed, err := a.FreqCap(scope, req.GetUserId(), int(req.GetCapLimit()), time.Duration(req.GetCapWindowSeconds())*time.Second)
	if err != nil {
		return false, "", status.Errorf(codes.InvalidArgument, "cap: %v", err)
	}
	if !allowed {
		return false, "cap:limit", nil
	}
	return true, "", nil
}

func (s *GRPCServer) RecordReward(ctx context.Context, req *cefaspb.RecordRewardRequest) (*cefaspb.RecordRewardResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "RecordReward")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeItemWrite); err != nil {
		return nil, err
	}
	banditID := req.GetBanditId()
	actionID := req.GetActionId()
	rewardCtx := req.GetContext()
	if req.GetDecisionId() != "" {
		rec, found, err := s.loadDecision(req.GetDecisionId())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "decision log: %v", err)
		}
		if !found {
			return nil, status.Error(codes.NotFound, "decision not found")
		}
		banditID = rec.GetBanditId()
		actionID = rec.GetActionId()
		rewardCtx = mergeStringMaps(rec.GetContext(), req.GetContext())
	}
	if banditID == "" || actionID == "" {
		return nil, status.Error(codes.InvalidArgument, "decision_id or bandit_id/action_id required")
	}
	bp, err := s.ensureBanditStore()
	if err != nil {
		return nil, err
	}
	if err := bp.Reward(banditID, actionID, req.GetReward(), rewardCtx); err != nil {
		return nil, mapBanditErr(err)
	}
	return &cefaspb.RecordRewardResponse{BanditId: banditID, ActionId: actionID}, nil
}

func (s *GRPCServer) GetDecision(ctx context.Context, req *cefaspb.GetDecisionRequest) (*cefaspb.GetDecisionResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "GetDecision")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeItemRead); err != nil {
		return nil, err
	}
	rec, found, err := s.loadDecision(req.GetDecisionId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decision log: %v", err)
	}
	return &cefaspb.GetDecisionResponse{Found: found, Decision: rec}, nil
}

type decisionLogRecord struct {
	DecisionID    string            `json:"decision_id"`
	BanditID      string            `json:"bandit_id"`
	UserID        string            `json:"user_id"`
	ActionID      string            `json:"action_id"`
	Fallback      bool              `json:"fallback"`
	ReasonCodes   []string          `json:"reason_codes"`
	Context       map[string]string `json:"context"`
	CreatedAtUnix int64             `json:"created_at_unix"`
	ExpiresAtUnix int64             `json:"expires_at_unix"`
}

func decisionKey(id string) []byte {
	return []byte(decisionLogPrefix + id)
}

func (s *GRPCServer) saveDecision(rec *cefaspb.DecisionRecord) error {
	wire := decisionLogRecord{
		DecisionID:    rec.GetDecisionId(),
		BanditID:      rec.GetBanditId(),
		UserID:        rec.GetUserId(),
		ActionID:      rec.GetActionId(),
		Fallback:      rec.GetFallback(),
		ReasonCodes:   append([]string(nil), rec.GetReasonCodes()...),
		Context:       mergeStringMaps(rec.GetContext(), nil),
		CreatedAtUnix: rec.GetCreatedAtUnix(),
		ExpiresAtUnix: rec.GetExpiresAtUnix(),
	}
	b, err := json.Marshal(wire)
	if err != nil {
		return err
	}
	return s.db.Set(decisionKey(rec.GetDecisionId()), b)
}

func (s *GRPCServer) loadDecision(id string) (*cefaspb.DecisionRecord, bool, error) {
	if id == "" {
		return nil, false, nil
	}
	v, err := s.db.Get(decisionKey(id))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var wire decisionLogRecord
	if err := json.Unmarshal(v, &wire); err != nil {
		return nil, false, err
	}
	if wire.ExpiresAtUnix > 0 && time.Now().Unix() > wire.ExpiresAtUnix {
		_ = s.db.Delete(decisionKey(id))
		return nil, false, nil
	}
	return &cefaspb.DecisionRecord{
		DecisionId:    wire.DecisionID,
		BanditId:      wire.BanditID,
		UserId:        wire.UserID,
		ActionId:      wire.ActionID,
		Fallback:      wire.Fallback,
		ReasonCodes:   append([]string(nil), wire.ReasonCodes...),
		Context:       mergeStringMaps(wire.Context, nil),
		CreatedAtUnix: wire.CreatedAtUnix,
		ExpiresAtUnix: wire.ExpiresAtUnix,
	}, true, nil
}

func (s *GRPCServer) audiencePlugin() (*audience.Plugin, error) {
	plug, ok := s.pluginRegistry().Lookup("audience")
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "audience plugin not registered")
	}
	a, ok := plug.(*audience.Plugin)
	if !ok {
		return nil, status.Error(codes.Internal, "registered audience plugin has unexpected type")
	}
	return a, nil
}

func itemString(item types.Item, field string) (string, bool) {
	av, ok := item[field]
	if !ok {
		return "", false
	}
	switch av.T {
	case types.AttrS:
		return av.S, true
	case types.AttrN:
		return av.N, true
	case types.AttrBOOL:
		return strconv.FormatBool(av.BOOL), true
	default:
		return "", false
	}
}

func actionContext(actionID string, actions []*cefaspb.NBAAction) map[string]string {
	for _, action := range actions {
		if action != nil && action.GetActionId() == actionID {
			return action.GetContext()
		}
	}
	return nil
}

func mergeStringMaps(a, b map[string]string) map[string]string {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func pipelineStage(name string, input, output int, start time.Time, reasons ...string) *cefaspb.PipelineStageTiming {
	return &cefaspb.PipelineStageTiming{
		Stage:       name,
		InputCount:  int32(input),
		OutputCount: int32(output),
		ElapsedMs:   float64(time.Since(start).Microseconds()) / 1000,
		ReasonCodes: cleanReasons(reasons),
	}
}

type reasonSet map[string]struct{}

func (s reasonSet) add(reason string) {
	if reason == "" {
		return
	}
	s[reason] = struct{}{}
}

func (s reasonSet) addAll(reasons ...string) {
	for _, reason := range reasons {
		s.add(reason)
	}
}

func (s reasonSet) slice() []string {
	out := make([]string, 0, len(s))
	for reason := range s {
		out = append(out, reason)
	}
	sort.Strings(out)
	return out
}

func cleanReasons(reasons []string) []string {
	set := reasonSet{}
	set.addAll(reasons...)
	return set.slice()
}

func newDecisionID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func boolCount(v bool) int {
	if v {
		return 1
	}
	return 0
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
