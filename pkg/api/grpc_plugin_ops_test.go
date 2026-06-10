package api_test

import (
	"context"
	"errors"
	"io"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
	// Side-effect imports register the builtin plugins against
	// plugin.Default so the unsecured fixture surfaces them.
	_ "github.com/osvaldoandrade/cefas/pkg/plugin/builtins"
)

func putKV(t *testing.T, stub cefaspb.CefasClient, table, pk, attr, val string) {
	t.Helper()
	if _, err := stub.PutItem(context.Background(), &cefaspb.PutItemRequest{
		Table: table,
		Item: map[string]*cefaspb.AttributeValue{
			"id":  {Value: &cefaspb.AttributeValue_S{S: pk}},
			attr:  {Value: &cefaspb.AttributeValue_S{S: val}},
		},
	}); err != nil {
		t.Fatalf("put %s: %v", pk, err)
	}
}

func putNum(t *testing.T, stub cefaspb.CefasClient, table, pk, attr, val string) {
	t.Helper()
	if _, err := stub.PutItem(context.Background(), &cefaspb.PutItemRequest{
		Table: table,
		Item: map[string]*cefaspb.AttributeValue{
			"id":  {Value: &cefaspb.AttributeValue_S{S: pk}},
			attr:  {Value: &cefaspb.AttributeValue_N{N: val}},
		},
	}); err != nil {
		t.Fatalf("put %s: %v", pk, err)
	}
}

func createTbl(t *testing.T, stub cefaspb.CefasClient, name string) {
	t.Helper()
	if _, err := stub.CreateTable(context.Background(), &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      name,
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
}

func TestCreateAndDescribeIndex(t *testing.T) {
	// Use the unsecured fixture from grpc_ttl_test.go; plugin.Default
	// is populated via the builtins blank-import above.
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTbl(t, stub, "Merchants")
	putKV(t, stub, "Merchants", "m1", "name", "habibs")
	resp, err := stub.CreateIndex(ctx, &cefaspb.CreateIndexRequest{
		Descriptor_: &cefaspb.PluginIndexDescriptor{
			Table:        "Merchants",
			Name:         "name_tri",
			PluginName:   "trigram",
			PluginConfig: []byte(`{"field":"name"}`),
			KeySchema:    &cefaspb.KeySchema{Pk: "id"},
		},
	})
	if err != nil {
		t.Fatalf("create-index: %v", err)
	}
	if resp.GetDescriptor_().GetPluginName() != "trigram" {
		t.Fatalf("plugin = %q", resp.GetDescriptor_().GetPluginName())
	}
	d, err := stub.DescribeIndex(ctx, &cefaspb.DescribeIndexRequest{Table: "Merchants", Name: "name_tri"})
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if d.GetDescriptor_().GetName() != "name_tri" {
		t.Fatalf("got %+v", d)
	}
}

func TestCreateIndexUnknownPlugin(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTbl(t, stub, "T")
	_, err := stub.CreateIndex(ctx, &cefaspb.CreateIndexRequest{
		Descriptor_: &cefaspb.PluginIndexDescriptor{
			Table: "T", Name: "x", PluginName: "ghost",
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
		},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestExplainReturnsPlan(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	resp, err := stub.Explain(context.Background(), &cefaspb.ExplainRequest{
		Table: "Users", Predicate: "levenshtein(name, 'ova') <= 1", Format: "text",
	})
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	if resp.GetPlan() == "" {
		t.Fatal("empty plan")
	}
}

func TestTopKRanksByDistance(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTbl(t, stub, "Docs")
	// Populate items with a numeric attribute we'll rank by.
	for _, p := range []struct{ id, v string }{
		{"a", "10"}, {"b", "1"}, {"c", "100"}, {"d", "11"},
	} {
		putNum(t, stub, "Docs", p.id, "score", p.v)
	}
	// We have no built-in absdiff plugin, but cosine on 1-D vectors
	// works: target [10] vs scores wrapped as [N]. Skip — use a
	// distance plugin we already shipped: hamming on string scores.
	// (hamming wants equal-length strings; pad to 3 chars.)
	for _, p := range []struct{ id, v string }{
		{"e", "abc"}, {"f", "abd"}, {"g", "xyz"}, {"h", "abe"},
	} {
		putKV(t, stub, "Docs", p.id, "code", p.v)
	}
	resp, err := stub.TopK(ctx, &cefaspb.TopKRequest{
		Table: "Docs", DistanceOperator: "hamming", Field: "code",
		Target: &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_S{S: "abc"}},
		K:      2,
	})
	if err != nil {
		t.Fatalf("topk: %v", err)
	}
	if len(resp.GetRows()) != 2 {
		t.Fatalf("rows = %d, want 2", len(resp.GetRows()))
	}
	if resp.GetRows()[0].GetDistance() != 0 {
		t.Fatalf("best distance = %g, want 0 (identical to target)", resp.GetRows()[0].GetDistance())
	}
}

func TestCohortEstimateApproximate(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTbl(t, stub, "Events")
	for i := 0; i < 50; i++ {
		putKV(t, stub, "Events", "e"+strconv.Itoa(i), "user_id", "u"+strconv.Itoa(i))
	}
	// Add some dupes — distinct count should remain ~50.
	for i := 0; i < 20; i++ {
		putKV(t, stub, "Events", "dup"+strconv.Itoa(i), "user_id", "u"+strconv.Itoa(i))
	}
	resp, err := stub.CohortEstimate(ctx, &cefaspb.CohortEstimateRequest{
		Table: "Events", Field: "user_id",
	})
	if err != nil {
		t.Fatalf("estimate: %v", err)
	}
	if resp.GetApproximateCount() < 40 || resp.GetApproximateCount() > 60 {
		t.Fatalf("estimate = %.0f, want ~50 ±20", resp.GetApproximateCount())
	}
}

func TestDedupAndFreqCap(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	r1, err := stub.Dedup(ctx, &cefaspb.DedupRequest{Scope: "camp1", Key: "u1", TtlSeconds: 60})
	if err != nil || !r1.GetAllowed() {
		t.Fatalf("first dedup: %v %v", r1, err)
	}
	r2, err := stub.Dedup(ctx, &cefaspb.DedupRequest{Scope: "camp1", Key: "u1", TtlSeconds: 60})
	if err != nil || r2.GetAllowed() {
		t.Fatalf("second dedup should be blocked: %v %v", r2, err)
	}

	for i := 0; i < 3; i++ {
		r, err := stub.FreqCap(ctx, &cefaspb.FreqCapRequest{
			Scope: "merchant1", Key: "u2", Limit: 3, WindowSeconds: int64(time.Hour / time.Second),
		})
		if err != nil || !r.GetAllowed() {
			t.Fatalf("freqcap %d: %v %v", i+1, r, err)
		}
	}
	r, err := stub.FreqCap(ctx, &cefaspb.FreqCapRequest{
		Scope: "merchant1", Key: "u2", Limit: 3, WindowSeconds: int64(time.Hour / time.Second),
	})
	if err != nil || r.GetAllowed() {
		t.Fatalf("4th freqcap should be blocked: %v %v", r, err)
	}
}

func TestAggregateMinGroupSize(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTbl(t, stub, "CampaignEvents")
	for i := 0; i < 4; i++ {
		_, _ = stub.PutItem(ctx, &cefaspb.PutItemRequest{
			Table: "CampaignEvents",
			Item: map[string]*cefaspb.AttributeValue{
				"id":          {Value: &cefaspb.AttributeValue_S{S: "e" + strconv.Itoa(i)}},
				"campaign_id": {Value: &cefaspb.AttributeValue_S{S: "c1"}},
				"imp":         {Value: &cefaspb.AttributeValue_N{N: "1"}},
			},
		})
	}
	// One small group below threshold:
	_, _ = stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "CampaignEvents",
		Item: map[string]*cefaspb.AttributeValue{
			"id":          {Value: &cefaspb.AttributeValue_S{S: "e99"}},
			"campaign_id": {Value: &cefaspb.AttributeValue_S{S: "c2"}},
			"imp":         {Value: &cefaspb.AttributeValue_N{N: "1"}},
		},
	})
	_, err := stub.Aggregate(ctx, &cefaspb.AggregateRequest{
		Table: "CampaignEvents", GroupBy: []string{"campaign_id"}, Metrics: []string{"imp"},
		MinGroupSize: 2,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", err)
	}
}

func TestGeoAudienceStreams(t *testing.T) {
	stub, cleanup := startUnsecuredFixture(t)
	defer cleanup()
	ctx := context.Background()
	createTbl(t, stub, "Stores")
	// Two stores near SP, one in Santos.
	_, _ = stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "Stores",
		Item: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "s1"}},
			"loc": {Value: &cefaspb.AttributeValue_M{M: &cefaspb.Map{Values: map[string]*cefaspb.AttributeValue{
				"lat": {Value: &cefaspb.AttributeValue_N{N: "-23.5510"}},
				"lon": {Value: &cefaspb.AttributeValue_N{N: "-46.6340"}},
			}}}},
		},
	})
	_, _ = stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "Stores",
		Item: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "s2"}},
			"loc": {Value: &cefaspb.AttributeValue_M{M: &cefaspb.Map{Values: map[string]*cefaspb.AttributeValue{
				"lat": {Value: &cefaspb.AttributeValue_N{N: "-23.9608"}},
				"lon": {Value: &cefaspb.AttributeValue_N{N: "-46.3336"}},
			}}}},
		},
	})
	// Create the geohash index first (needed by GeoAudience).
	_, err := stub.CreateIndex(ctx, &cefaspb.CreateIndexRequest{
		Descriptor_: &cefaspb.PluginIndexDescriptor{
			Table: "Stores", Name: "loc_geo", PluginName: "geohash",
			PluginConfig: []byte(`{"field":"loc","precision":5}`),
			KeySchema:    &cefaspb.KeySchema{Pk: "id"},
		},
	})
	if err != nil {
		t.Fatalf("create-index: %v", err)
	}
	stream, err := stub.GeoAudience(ctx, &cefaspb.GeoAudienceRequest{
		Table: "Stores", Index: "loc_geo",
		Lat: -23.5505, Lon: -46.6333, RadiusMeters: 2000,
	})
	if err != nil {
		t.Fatalf("geo audience: %v", err)
	}
	got := map[string]bool{}
	for {
		it, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		got[it.GetAttributes()["id"].GetS()] = true
	}
	if !got["s1"] {
		t.Errorf("s1 should be in radius")
	}
	if got["s2"] {
		t.Errorf("s2 (Santos, ~55km) should not be in 2km radius")
	}
}

// quiet unused-import compiler when we want plugin in test ranges.
var _ = plugin.Default
