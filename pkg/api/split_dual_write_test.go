package api_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/api"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func TestGRPCSplitDualWriteCatchupAndFinalize(t *testing.T) {
	stub, mgr, plan, cleanup := startSplitGRPCFixture(t)
	defer cleanup()

	ctx := context.Background()
	const table = "SplitLive"
	ks := types.KeySchema{PK: "id"}
	createTable(t, stub, table)

	keys := childRangeKeys(t, mgr, plan, 8)
	putKey := keys[0]
	updateKey := keys[1]
	deleteKey := keys[2]
	batchPutKey := keys[3]
	batchDeleteKey := keys[4]
	txPutKey := keys[5]
	txDeleteKey := keys[6]

	putGRPCItem(t, ctx, stub, table, putKey, "put")
	putGRPCItem(t, ctx, stub, table, updateKey, "seed-update")
	if _, err := stub.UpdateItem(ctx, &cefaspb.UpdateItemRequest{
		Table:            table,
		Key:              map[string]*cefaspb.AttributeValue{"id": pbString(updateKey)},
		UpdateExpression: "SET v = :v",
		ExpressionAttributeValues: map[string]*cefaspb.AttributeValue{
			":v": pbString("updated"),
		},
	}); err != nil {
		t.Fatalf("update during split: %v", err)
	}

	putGRPCItem(t, ctx, stub, table, deleteKey, "delete-me")
	if _, err := stub.DeleteItem(ctx, &cefaspb.DeleteItemRequest{
		Table: table,
		Key:   map[string]*cefaspb.AttributeValue{"id": pbString(deleteKey)},
	}); err != nil {
		t.Fatalf("delete during split: %v", err)
	}

	putGRPCItem(t, ctx, stub, table, batchDeleteKey, "batch-delete-me")
	if _, err := stub.BatchWriteItem(ctx, &cefaspb.BatchWriteItemRequest{
		Table: table,
		Ops: []*cefaspb.BatchWriteOp{
			{
				Kind: cefaspb.BatchWriteOp_KIND_PUT,
				Item: map[string]*cefaspb.AttributeValue{"id": pbString(batchPutKey), "v": pbString("batch")},
			},
			{
				Kind: cefaspb.BatchWriteOp_KIND_DELETE,
				Key:  map[string]*cefaspb.AttributeValue{"id": pbString(batchDeleteKey)},
			},
		},
	}); err != nil {
		t.Fatalf("batch write during split: %v", err)
	}

	putGRPCItem(t, ctx, stub, table, txDeleteKey, "tx-delete-me")
	if _, err := stub.TransactWriteItems(ctx, &cefaspb.TransactWriteItemsRequest{
		Ops: []*cefaspb.TransactWriteOp{
			{
				Op: &cefaspb.TransactWriteOp_ConditionCheck_{
					ConditionCheck: &cefaspb.TransactWriteOp_ConditionCheck{
						Table: table,
						Key:   map[string]*cefaspb.AttributeValue{"id": pbString(putKey)},
					},
				},
				ConditionExpression: "v = :expected",
				Binds:               map[string]*cefaspb.AttributeValue{":expected": pbString("put")},
			},
			{
				Op: &cefaspb.TransactWriteOp_Put_{
					Put: &cefaspb.TransactWriteOp_Put{
						Table: table,
						Item:  map[string]*cefaspb.AttributeValue{"id": pbString(txPutKey), "v": pbString("tx")},
					},
				},
			},
			{
				Op: &cefaspb.TransactWriteOp_Delete_{
					Delete: &cefaspb.TransactWriteOp_Delete{
						Table: table,
						Key:   map[string]*cefaspb.AttributeValue{"id": pbString(txDeleteKey)},
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("transaction during split: %v", err)
	}

	_, child := splitShards(t, mgr)
	assertStoredValue(t, child.Storage, table, ks, updateKey, "updated")
	assertMissing(t, child.Storage, table, ks, deleteKey)

	result, err := mgr.FinalizeSplit(ctx, cluster.SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err != nil {
		t.Fatalf("finalize split without writesQuiesced: %v", err)
	}
	if result.AfterEpoch != plan.AfterEpoch+1 {
		t.Fatalf("after epoch = %d, want %d", result.AfterEpoch, plan.AfterEpoch+1)
	}

	assertRoutedGRPCValue(t, ctx, stub, table, putKey, "put")
	assertRoutedGRPCValue(t, ctx, stub, table, updateKey, "updated")
	assertRoutedGRPCValue(t, ctx, stub, table, batchPutKey, "batch")
	assertRoutedGRPCValue(t, ctx, stub, table, txPutKey, "tx")
	assertMissing(t, child.Storage, table, ks, deleteKey)
	assertMissing(t, child.Storage, table, ks, batchDeleteKey)
	assertMissing(t, child.Storage, table, ks, txDeleteKey)

	pkBytes, err := storage.AttrCanonicalBytes(types.AttributeValue{T: types.AttrS, S: putKey})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.RouteForPK(pkBytes, plan.AfterEpoch); !errors.Is(err, cluster.ErrStaleRoute) {
		t.Fatalf("stale route error = %v, want ErrStaleRoute", err)
	}
}

func TestGRPCRangeMoveDualWriteCatchupAndFinalize(t *testing.T) {
	stub, mgr, plan, cleanup := startRangeMoveGRPCFixture(t)
	defer cleanup()

	ctx := context.Background()
	const table = "RangeMoveLive"
	ks := types.KeySchema{PK: "id"}
	createTable(t, stub, table)

	keys := childRangeKeys(t, mgr, plan, 2)
	putKey := keys[0]
	deleteKey := keys[1]

	putGRPCItem(t, ctx, stub, table, putKey, "put")
	putGRPCItem(t, ctx, stub, table, deleteKey, "delete-me")
	if _, err := stub.DeleteItem(ctx, &cefaspb.DeleteItemRequest{
		Table: table,
		Key:   map[string]*cefaspb.AttributeValue{"id": pbString(deleteKey)},
	}); err != nil {
		t.Fatalf("delete during range move: %v", err)
	}

	source, target := splitShards(t, mgr)
	assertStoredValue(t, target.Storage, table, ks, putKey, "put")
	assertMissing(t, target.Storage, table, ks, deleteKey)

	resp, err := stub.FinalizeRangeMove(ctx, &cefaspb.FinalizeRangeMoveRequest{
		SourceShardId: 0,
		TargetShardId: 1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err != nil {
		t.Fatalf("finalize range move: %v", err)
	}
	if resp.GetResult().GetAfterEpoch() != plan.AfterEpoch+1 {
		t.Fatalf("after epoch = %d, want %d", resp.GetResult().GetAfterEpoch(), plan.AfterEpoch+1)
	}

	assertRoutedGRPCValue(t, ctx, stub, table, putKey, "put")
	assertMissing(t, source.Storage, table, ks, putKey)
	assertMissing(t, target.Storage, table, ks, deleteKey)
}

func TestGRPCRangeMoveWriteBurstDuringMovement(t *testing.T) {
	stub, mgr, plan, cleanup := startRangeMoveGRPCFixture(t)
	defer cleanup()

	ctx := context.Background()
	const table = "RangeMoveBurst"
	createTable(t, stub, table)

	keys := childRangeKeys(t, mgr, plan, 64)
	for i, key := range keys {
		putGRPCItem(t, ctx, stub, table, key, fmt.Sprintf("burst-%02d", i))
	}

	if _, err := stub.FinalizeRangeMove(ctx, &cefaspb.FinalizeRangeMoveRequest{
		SourceShardId: 0,
		TargetShardId: 1,
		ExpectedEpoch: plan.AfterEpoch,
	}); err != nil {
		t.Fatalf("finalize range move: %v", err)
	}
	for i, key := range keys {
		assertRoutedGRPCValue(t, ctx, stub, table, key, fmt.Sprintf("burst-%02d", i))
	}
}

func startSplitGRPCFixture(t *testing.T) (cefaspb.CefasClient, *cluster.Manager, cluster.PlacementPlan, func()) {
	t.Helper()
	env := transitionPlacementTestEnv(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		env.Cleanup()
		t.Fatalf("listen: %v", err)
	}
	shard0, ok := env.Manager.Shard(0)
	if !ok {
		env.Cleanup()
		t.Fatal("missing shard 0")
	}
	server := api.NewGRPCServer(shard0.Storage, env.Catalog, nil)
	server.AttachManager(env.Manager)
	gsrv := grpc.NewServer()
	cefaspb.RegisterCefasServer(gsrv, server)
	go func() { _ = gsrv.Serve(ln) }()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		gsrv.Stop()
		_ = ln.Close()
		env.Cleanup()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		gsrv.GracefulStop()
		_ = ln.Close()
		env.Cleanup()
	}
	return cefaspb.NewCefasClient(conn), env.Manager, env.Plan, cleanup
}

func startRangeMoveGRPCFixture(t *testing.T) (cefaspb.CefasClient, *cluster.Manager, cluster.PlacementPlan, func()) {
	t.Helper()
	env := transitionRangeMovePlacementTestEnv(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		env.Cleanup()
		t.Fatalf("listen: %v", err)
	}
	shard0, ok := env.Manager.Shard(0)
	if !ok {
		env.Cleanup()
		t.Fatal("missing shard 0")
	}
	server := api.NewGRPCServer(shard0.Storage, env.Catalog, nil)
	server.AttachManager(env.Manager)
	gsrv := grpc.NewServer()
	cefaspb.RegisterCefasServer(gsrv, server)
	go func() { _ = gsrv.Serve(ln) }()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		gsrv.Stop()
		_ = ln.Close()
		env.Cleanup()
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		gsrv.GracefulStop()
		_ = ln.Close()
		env.Cleanup()
	}
	return cefaspb.NewCefasClient(conn), env.Manager, env.Plan, cleanup
}

func childRangeKeys(t *testing.T, mgr *cluster.Manager, plan cluster.PlacementPlan, n int) []string {
	t.Helper()
	if len(plan.After.Shards) < 2 || len(plan.After.Shards[1].Ranges) != 1 {
		t.Fatalf("unexpected split plan: %+v", plan.After.Shards)
	}
	rng := plan.After.Shards[1].Ranges[0]
	router := mgr.Router()
	keys := make([]string, 0, n)
	for i := 0; len(keys) < n && i < 200_000; i++ {
		key := fmt.Sprintf("split-live-%d", i)
		pkBytes, err := storage.AttrCanonicalBytes(types.AttributeValue{T: types.AttrS, S: key})
		if err != nil {
			t.Fatal(err)
		}
		if rng.Contains(router.TokenForPK(pkBytes)) {
			keys = append(keys, key)
		}
	}
	if len(keys) != n {
		t.Fatalf("found %d keys in child range, want %d", len(keys), n)
	}
	return keys
}

func splitShards(t *testing.T, mgr *cluster.Manager) (*cluster.Shard, *cluster.Shard) {
	t.Helper()
	parent, ok := mgr.Shard(0)
	if !ok {
		t.Fatal("missing parent shard")
	}
	child, ok := mgr.Shard(1)
	if !ok {
		t.Fatal("missing child shard")
	}
	return parent, child
}

func putGRPCItem(t *testing.T, ctx context.Context, stub cefaspb.CefasClient, table, id, v string) {
	t.Helper()
	if _, err := stub.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: table,
		Item:  map[string]*cefaspb.AttributeValue{"id": pbString(id), "v": pbString(v)},
	}); err != nil {
		t.Fatalf("put %s: %v", id, err)
	}
}

func assertRoutedGRPCValue(t *testing.T, ctx context.Context, stub cefaspb.CefasClient, table, id, want string) {
	t.Helper()
	resp, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: table,
		Key:   map[string]*cefaspb.AttributeValue{"id": pbString(id)},
	})
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	if !resp.GetFound() {
		t.Fatalf("get %s: not found", id)
	}
	if got := resp.GetItem()["v"].GetS(); got != want {
		t.Fatalf("get %s value = %q, want %q", id, got, want)
	}
}

func assertStoredValue(t *testing.T, db *storage.DB, table string, ks types.KeySchema, id, want string) {
	t.Helper()
	item, err := db.GetItem(table, ks, types.Item{"id": {T: types.AttrS, S: id}})
	if err != nil {
		t.Fatalf("get %s: %v", id, err)
	}
	if got := item["v"].S; got != want {
		t.Fatalf("stored %s value = %q, want %q", id, got, want)
	}
}

func assertMissing(t *testing.T, db *storage.DB, table string, ks types.KeySchema, id string) {
	t.Helper()
	_, err := db.GetItem(table, ks, types.Item{"id": {T: types.AttrS, S: id}})
	if !errors.Is(err, types.ErrItemNotFound) {
		t.Fatalf("get %s error = %v, want ErrItemNotFound", id, err)
	}
}

func pbString(s string) *cefaspb.AttributeValue {
	return &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_S{S: s}}
}
