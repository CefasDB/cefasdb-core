package server_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/placement"
	"github.com/osvaldoandrade/cefas/internal/storage"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/builtins"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func TestDistributedTopKScattersAcrossActiveShards(t *testing.T) {
	stub, _, _, childKey, parentKey, cleanup := startFinalizedSplitRetrievalFixture(t, "DistributedTopK")
	defer cleanup()
	ctx := context.Background()

	putVectorDoc(t, ctx, stub, "DistributedTopK", parentKey, "us", []float64{0, 1, 0})
	putVectorDoc(t, ctx, stub, "DistributedTopK", childKey, "us", []float64{1, 0, 0})

	resp, err := stub.TopK(ctx, &cefaspb.TopKRequest{
		Table:            "DistributedTopK",
		Field:            "emb",
		DistanceOperator: "cosine",
		Target:           pbVec(1, 0, 0),
		K:                1,
	})
	if err != nil {
		t.Fatalf("topk: %v", err)
	}
	if got := topKIDs(resp.GetRows()); !sameStrings(got, []string{childKey}) {
		t.Fatalf("topk rows = %v, want [%s]", got, childKey)
	}
}

func TestDistributedSQLANNUsesGlobalPluginIndexBuild(t *testing.T) {
	stub, _, _, childKey, parentKey, cleanup := startFinalizedSplitRetrievalFixture(t, "DistributedSQLANN")
	defer cleanup()
	ctx := context.Background()

	putVectorDoc(t, ctx, stub, "DistributedSQLANN", parentKey, "us", []float64{0, 1, 0})
	putVectorDoc(t, ctx, stub, "DistributedSQLANN", childKey, "us", []float64{1, 0, 0})
	createANNIndexForRetrieval(t, ctx, stub, "DistributedSQLANN")

	resp, err := stub.Sql(ctx, &cefaspb.SqlRequest{
		Query: "SELECT id FROM DistributedSQLANN ORDER BY emb ANN OF [1,0,0] LIMIT 1",
	})
	if err != nil {
		t.Fatalf("sql ann: %v", err)
	}
	if len(resp.GetRows()) != 1 {
		t.Fatalf("sql ann rows = %d, want 1: %+v", len(resp.GetRows()), resp.GetRows())
	}
	if got := resp.GetRows()[0].GetAttributes()["id"].GetS(); got != childKey {
		t.Fatalf("sql ann id = %q, want %q", got, childKey)
	}
}

func TestDistributedRecommendFiltersAndDiversifiesAfterGlobalMerge(t *testing.T) {
	stub, _, _, childKey, parentKey, cleanup := startFinalizedSplitRetrievalFixture(t, "DistributedRecommend")
	defer cleanup()
	ctx := context.Background()

	putVectorDoc(t, ctx, stub, "DistributedRecommend", childKey, "us", []float64{1, 0, 0})
	putVectorDoc(t, ctx, stub, "DistributedRecommend", parentKey, "us", []float64{0.85, 0.15, 0})
	putVectorDoc(t, ctx, stub, "DistributedRecommend", parentKey+"-eu", "eu", []float64{0.99, 0.01, 0})

	resp, err := stub.Recommend(ctx, &cefaspb.RecommendRequest{
		Table:            "DistributedRecommend",
		Field:            "emb",
		DistanceOperator: "cosine",
		Target:           pbVec(1, 0, 0),
		CandidateLimit:   3,
		Limit:            2,
		FilterExpression: "region = 'us'",
		MmrLambda:        1,
	})
	if err != nil {
		t.Fatalf("recommend: %v", err)
	}
	if got := recommendIDs(resp.GetRows()); !sameStrings(got, []string{childKey, parentKey}) {
		t.Fatalf("recommend rows = %v, want [%s %s]", got, childKey, parentKey)
	}
	if len(resp.GetStages()) < 4 {
		t.Fatalf("recommend stages = %+v, want retrieve/filter/diversify/cap", resp.GetStages())
	}
	if retrieve := resp.GetStages()[0]; retrieve.GetStage() != "retrieve" || retrieve.GetInputCount() < 3 || retrieve.GetOutputCount() != 3 {
		t.Fatalf("retrieve stage = %+v, want global candidate counts", retrieve)
	}
}

func startFinalizedSplitRetrievalFixture(t *testing.T, table string) (cefaspb.CefasClient, *cluster.Manager, placement.PlacementPlan, string, string, func()) {
	t.Helper()
	stub, mgr, plan, cleanup := startSplitGRPCFixture(t)
	ctx := context.Background()
	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      table,
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
			AttributeDefinitions: []*cefaspb.AttributeDefinition{{
				Name: "emb", Type: "V", VectorDimensions: 3,
			}},
		},
	}); err != nil {
		cleanup()
		t.Fatalf("create table: %v", err)
	}
	childKey := childRangeKeys(t, mgr, plan, 1)[0]
	parentKey := keyInTokenRange(t, mgr, plan.After.Shards[0].Ranges[0], table+"-parent")
	if _, err := mgr.FinalizeSplit(ctx, cluster.SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	}); err != nil {
		cleanup()
		t.Fatalf("finalize split: %v", err)
	}
	return stub, mgr, plan, childKey, parentKey, cleanup
}

func keyInTokenRange(t *testing.T, mgr *cluster.Manager, rng placement.TokenRange, prefix string) string {
	t.Helper()
	for i := 0; i < 200_000; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		pkBytes, err := storage.AttrCanonicalBytes(types.AttributeValue{T: types.AttrS, S: key})
		if err != nil {
			t.Fatal(err)
		}
		if rng.Contains(mgr.Router().TokenForPK(pkBytes)) {
			return key
		}
	}
	t.Fatalf("no key found in range %+v", rng)
	return ""
}

func putVectorDoc(t *testing.T, ctx context.Context, stub cefaspb.CefasClient, table, id, region string, vec []float64) {
	t.Helper()
	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: table,
		Item: map[string]*cefaspb.AttributeValue{
			"id":     pbString(id),
			"region": pbString(region),
			"emb":    pbVec(vec...),
		},
	}); err != nil {
		t.Fatalf("put %s: %v", id, err)
	}
}

func createANNIndexForRetrieval(t *testing.T, ctx context.Context, stub cefaspb.CefasClient, table string) {
	t.Helper()
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
}

func topKIDs(rows []*cefaspb.TopKRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.GetItem().GetAttributes()["id"].GetS())
	}
	return out
}
