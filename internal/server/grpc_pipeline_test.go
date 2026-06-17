package server_test

import (
	"context"
	"testing"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	"github.com/CefasDb/cefasdb/pkg/plugin/bandit"
	_ "github.com/CefasDb/cefasdb/pkg/plugin/builtins"
)

func TestRecommendMatchesManualTopKPlusRerank(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	table := "RecDocs"
	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      table,
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
			AttributeDefinitions: []*cefaspb.AttributeDefinition{{
				Name: "emb", Type: "V", VectorDimensions: 3,
			}},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for _, row := range []struct {
		id     string
		region string
		vec    []float64
	}{
		{"a", "us", []float64{1, 0, 0}},
		{"b", "us", []float64{0.8, 0.2, 0}},
		{"c", "us", []float64{0, 1, 0}},
		{"d", "eu", []float64{0, 0, 1}},
	} {
		if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
			Table: table,
			Item: map[string]*cefaspb.AttributeValue{
				"id":     {Value: &cefaspb.AttributeValue_S{S: row.id}},
				"region": {Value: &cefaspb.AttributeValue_S{S: row.region}},
				"emb":    pbVec(row.vec...),
			},
		}); err != nil {
			t.Fatalf("put %s: %v", row.id, err)
		}
	}
	if _, err := stub.CreateIndex(ctx, &cefaspb.CreateIndexRequest{
		Descriptor_: &cefaspb.PluginIndexDescriptor{
			Table:        table,
			Name:         "emb_ann",
			PluginName:   "ann",
			PluginConfig: []byte(`{"field":"emb","dim":3,"metric":"cosine","algorithm":"lsh"}`),
			KeySchema:    &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create ann: %v", err)
	}

	topk, err := stub.TopK(ctx, &cefaspb.TopKRequest{
		Table: table, Field: "emb",
		Target: pbVec(1, 0, 0), K: 3,
	})
	if err != nil {
		t.Fatalf("topk: %v", err)
	}
	cands := make([]*cefaspb.RerankCandidate, 0, len(topk.GetRows()))
	for _, row := range topk.GetRows() {
		cands = append(cands, &cefaspb.RerankCandidate{Item: row.GetItem(), Distance: row.GetDistance()})
	}
	manual, err := stub.Rerank(ctx, &cefaspb.RerankRequest{
		Table: table, Field: "emb", DistanceOperator: "cosine",
		Lambda: 0.1, TargetSize: 2, Candidates: cands,
	})
	if err != nil {
		t.Fatalf("rerank: %v", err)
	}
	rec, err := stub.Recommend(ctx, &cefaspb.RecommendRequest{
		Table: table, Field: "emb",
		Target: pbVec(1, 0, 0), CandidateLimit: 3, Limit: 2,
		FilterExpression: "region = 'us'", MmrLambda: 0.1,
	})
	if err != nil {
		t.Fatalf("recommend: %v", err)
	}
	if got, want := recommendIDs(rec.GetRows()), rerankIDs(manual.GetSlate()); !sameStrings(got, want) {
		t.Fatalf("recommend rows = %v, manual rerank = %v", got, want)
	}
	if len(rec.GetStages()) < 4 {
		t.Fatalf("expected per-stage timings, got %+v", rec.GetStages())
	}

	plain, err := stub.Recommend(ctx, &cefaspb.RecommendRequest{
		Table: table, Field: "emb",
		Target: pbVec(1, 0, 0), CandidateLimit: 3, Limit: 2,
		DisableDiversify: true,
	})
	if err != nil {
		t.Fatalf("recommend no-diversify: %v", err)
	}
	if got := recommendIDs(plain.GetRows()); !sameStrings(got, []string{"a", "b"}) {
		t.Fatalf("no-diversify rows = %v, want [a b]", got)
	}
}

func TestNextBestActionDecisionLogAndReward(t *testing.T) {
	stub, cleanup := startPipelineFixtureWithBandit(t, 42)
	defer cleanup()
	ctx := context.Background()
	if _, err := stub.BanditCreate(ctx, &cefaspb.BanditCreateRequest{
		BanditId: "offers",
		Strategy: "thompson",
		Arms: []*cefaspb.BanditArmSpec{
			{ArmId: "A"}, {ArmId: "B"}, {ArmId: "C"},
		},
	}); err != nil {
		t.Fatalf("bandit create: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := stub.BanditReward(ctx, &cefaspb.BanditRewardRequest{BanditId: "offers", ArmId: "B", Reward: 1}); err != nil {
			t.Fatalf("reward B: %v", err)
		}
	}
	if _, err := stub.BanditReward(ctx, &cefaspb.BanditRewardRequest{BanditId: "offers", ArmId: "A", Reward: 0}); err != nil {
		t.Fatalf("reward A: %v", err)
	}

	decide, err := stub.NextBestAction(ctx, &cefaspb.NextBestActionRequest{
		BanditId: "offers",
		UserId:   "u1",
		Actions: []*cefaspb.NBAAction{
			{ActionId: "A"},
			{ActionId: "B"},
			{ActionId: "C", Disabled: true, Reason: "not_in_segment"},
		},
		FallbackActionId: "fallback",
	})
	if err != nil {
		t.Fatalf("nba decide: %v", err)
	}
	if decide.GetActionId() == "C" || decide.GetActionId() == "" || decide.GetDecisionId() == "" {
		t.Fatalf("unexpected decision: %+v", decide)
	}
	gotDecision, err := stub.GetDecision(ctx, &cefaspb.GetDecisionRequest{DecisionId: decide.GetDecisionId()})
	if err != nil {
		t.Fatalf("get decision: %v", err)
	}
	if !gotDecision.GetFound() || gotDecision.GetDecision().GetActionId() != decide.GetActionId() {
		t.Fatalf("decision log not queryable: %+v", gotDecision)
	}

	before, err := stub.BanditDescribe(ctx, &cefaspb.BanditDescribeRequest{BanditId: "offers"})
	if err != nil {
		t.Fatalf("describe before reward: %v", err)
	}
	if _, err := stub.RecordReward(ctx, &cefaspb.RecordRewardRequest{
		DecisionId: decide.GetDecisionId(),
		ActionId:   otherAction(decide.GetActionId()),
		Reward:     1,
	}); err != nil {
		t.Fatalf("record reward: %v", err)
	}
	snap, err := stub.BanditDescribe(ctx, &cefaspb.BanditDescribeRequest{BanditId: "offers"})
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	beforePulls := pullsByArm(before)
	afterPulls := pullsByArm(snap)
	if afterPulls[decide.GetActionId()] != beforePulls[decide.GetActionId()]+1 {
		t.Fatalf("reward did not update original arm: %+v", snap.GetArms())
	}

	fallback, err := stub.NextBestAction(ctx, &cefaspb.NextBestActionRequest{
		BanditId: "offers",
		UserId:   "u2",
		Actions: []*cefaspb.NBAAction{
			{ActionId: "A", Disabled: true},
			{ActionId: "B", Disabled: true},
		},
		FallbackActionId: "fallback",
	})
	if err != nil {
		t.Fatalf("nba fallback: %v", err)
	}
	if !fallback.GetFallback() || fallback.GetActionId() != "fallback" {
		t.Fatalf("unexpected fallback decision: %+v", fallback)
	}
}

func TestNextBestActionUsesUCBStrategyOverEligibleActions(t *testing.T) {
	stub, cleanup := startPipelineFixtureWithBandit(t, 7)
	defer cleanup()
	ctx := context.Background()
	if _, err := stub.BanditCreate(ctx, &cefaspb.BanditCreateRequest{
		BanditId: "offers",
		Strategy: "ucb1",
		Arms: []*cefaspb.BanditArmSpec{
			{ArmId: "A"}, {ArmId: "B"},
		},
	}); err != nil {
		t.Fatalf("bandit create: %v", err)
	}
	for i := 0; i < 5; i++ {
		if _, err := stub.BanditReward(ctx, &cefaspb.BanditRewardRequest{BanditId: "offers", ArmId: "A", Reward: 1}); err != nil {
			t.Fatalf("reward A: %v", err)
		}
	}

	decide, err := stub.NextBestAction(ctx, &cefaspb.NextBestActionRequest{
		BanditId: "offers",
		UserId:   "u-ucb",
		Actions: []*cefaspb.NBAAction{
			{ActionId: "A"},
			{ActionId: "B"},
		},
	})
	if err != nil {
		t.Fatalf("nba decide: %v", err)
	}
	if decide.GetActionId() != "B" {
		t.Fatalf("UCB NBA action = %q, want unexplored eligible arm B", decide.GetActionId())
	}
	if !hasReason(stageReasons(decide.GetStages(), "bandit"), "bandit:strategy") {
		t.Fatalf("bandit stage missing strategy reason: %+v", decide.GetStages())
	}
}

func TestNextBestActionUnknownEligibleArmFallsBackWithReason(t *testing.T) {
	stub, cleanup := startPipelineFixtureWithBandit(t, 1)
	defer cleanup()
	ctx := context.Background()
	if _, err := stub.BanditCreate(ctx, &cefaspb.BanditCreateRequest{
		BanditId: "offers",
		Strategy: "thompson",
		Arms: []*cefaspb.BanditArmSpec{
			{ArmId: "A"},
		},
	}); err != nil {
		t.Fatalf("bandit create: %v", err)
	}

	decide, err := stub.NextBestAction(ctx, &cefaspb.NextBestActionRequest{
		BanditId: "offers",
		UserId:   "u-unknown",
		Actions: []*cefaspb.NBAAction{
			{ActionId: "Z"},
		},
		FallbackActionId: "fallback",
	})
	if err != nil {
		t.Fatalf("nba decide: %v", err)
	}
	if !decide.GetFallback() || decide.GetActionId() != "fallback" {
		t.Fatalf("fallback decision = %+v, want fallback action", decide)
	}
	if !hasReason(decide.GetReasonCodes(), "bandit:unknown_arm:Z") ||
		!hasReason(decide.GetReasonCodes(), "fallback:no_registered_eligible_actions") {
		t.Fatalf("reason codes = %v, want unknown-arm and fallback reasons", decide.GetReasonCodes())
	}
	if got := stageOutput(decide.GetStages(), "bandit"); got != 0 {
		t.Fatalf("bandit stage output = %d, want 0", got)
	}
}

func startPipelineFixtureWithBandit(t *testing.T, seed int64) (cefaspb.CefasClient, func()) {
	t.Helper()
	reg := plugin.NewRegistry()
	if err := reg.Register(bandit.NewPluginWith(bandit.NewMemoryStore(), seed)); err != nil {
		t.Fatalf("register bandit: %v", err)
	}
	return fixtureWithRegistry(t, reg)
}

func recommendIDs(rows []*cefaspb.RecommendRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.GetItem().GetAttributes()["id"].GetS())
	}
	return out
}

func rerankIDs(rows []*cefaspb.RerankCandidate) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.GetItem().GetAttributes()["id"].GetS())
	}
	return out
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func otherAction(actionID string) string {
	if actionID == "A" {
		return "B"
	}
	return "A"
}

func hasReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}

func stageReasons(stages []*cefaspb.PipelineStageTiming, name string) []string {
	for _, stage := range stages {
		if stage.GetStage() == name {
			return stage.GetReasonCodes()
		}
	}
	return nil
}

func stageOutput(stages []*cefaspb.PipelineStageTiming, name string) int32 {
	for _, stage := range stages {
		if stage.GetStage() == name {
			return stage.GetOutputCount()
		}
	}
	return -1
}

func pullsByArm(resp *cefaspb.BanditDescribeResponse) map[string]int64 {
	out := map[string]int64{}
	for _, arm := range resp.GetArms() {
		out[arm.GetArmId()] = arm.GetPulls()
	}
	return out
}
