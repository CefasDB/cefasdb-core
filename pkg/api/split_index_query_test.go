package api_test

import (
	"context"
	"io"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/cluster"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
)

func TestGRPCGSIQueryFindsMovedSplitRowsOnce(t *testing.T) {
	stub, mgr, plan, cleanup := startSplitGRPCFixture(t)
	defer cleanup()

	ctx := context.Background()
	const table = "GSIEvents"
	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      table,
			KeySchema: &cefaspb.KeySchema{Pk: "id"},
			Gsis: []*cefaspb.GSIDescriptor{{
				Name:      "by_event",
				KeySchema: &cefaspb.KeySchema{Pk: "event"},
			}},
		},
	}); err != nil {
		t.Fatalf("create table: %v", err)
	}

	key := childRangeKeys(t, mgr, plan, 1)[0]
	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: table,
		Item: map[string]*cefaspb.AttributeValue{
			"id":    pbString(key),
			"event": pbString("login"),
			"v":     pbString("moved"),
		},
	}); err != nil {
		t.Fatalf("put during split: %v", err)
	}
	if _, err := mgr.FinalizeSplit(ctx, cluster.SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	}); err != nil {
		t.Fatalf("finalize: %v", err)
	}

	stream, err := stub.Query(ctx, &cefaspb.QueryRequest{
		Table:     table,
		IndexName: "by_event",
		PkValue:   pbString("login"),
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	var hits []*cefaspb.Item
	for {
		item, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		hits = append(hits, item)
	}
	if len(hits) != 1 {
		t.Fatalf("GSI hits = %d, want 1: %+v", len(hits), hits)
	}
	if hits[0].GetAttributes()["id"].GetS() != key || hits[0].GetAttributes()["v"].GetS() != "moved" {
		t.Fatalf("unexpected GSI hit: %+v", hits[0].GetAttributes())
	}
}
