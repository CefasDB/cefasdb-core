package server_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/server"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

func startMVReplicaFixture(t *testing.T) (cefaspb.CefasClient, cefaspb.ReplicaClient, func()) {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	srv := server.NewGRPCServer(db, cat, nil)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer()
	cefaspb.RegisterCefasServer(gsrv, srv)
	cefaspb.RegisterReplicaServer(gsrv, srv)
	go func() { _ = gsrv.Serve(ln) }()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		gsrv.GracefulStop()
		_ = db.Close()
	}
	return cefaspb.NewCefasClient(conn), cefaspb.NewReplicaClient(conn), cleanup
}

func TestBatchWriteMV_WritesDirectlyToLocalPebble(t *testing.T) {
	stub, repl, cleanup := startMVReplicaFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "Base",
			KeySchema: &cefaspb.KeySchema{Pk: "pk", Sk: "sk"},
		},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	if _, err := stub.CreateMaterializedView(ctx, &cefaspb.CreateMaterializedViewRequest{
		Descriptor_: &cefaspb.MaterializedViewDescriptor{
			Name:      "Base_mv",
			BaseTable: "Base",
			KeySchema: &cefaspb.KeySchema{Pk: "sk", Sk: "pk"},
			RefreshPolicy: &cefaspb.RefreshPolicy{
				Mode: cefaspb.RefreshPolicy_EAGER,
			},
		},
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}

	if _, err := repl.BatchWriteMV(ctx, &cefaspb.BatchWriteMVRequest{
		View: "Base_mv",
		Ops: []*cefaspb.BatchWriteOp{
			{Kind: cefaspb.BatchWriteOp_KIND_PUT, Item: map[string]*cefaspb.AttributeValue{
				"sk": {Value: &cefaspb.AttributeValue_S{S: "open"}},
				"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
			}},
			{Kind: cefaspb.BatchWriteOp_KIND_PUT, Item: map[string]*cefaspb.AttributeValue{
				"sk": {Value: &cefaspb.AttributeValue_S{S: "closed"}},
				"pk": {Value: &cefaspb.AttributeValue_S{S: "p2"}},
			}},
		},
	}); err != nil {
		t.Fatalf("BatchWriteMV: %v", err)
	}

	for _, want := range [][2]string{{"open", "p1"}, {"closed", "p2"}} {
		got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
			Table: "Base_mv",
			Key: map[string]*cefaspb.AttributeValue{
				"sk": {Value: &cefaspb.AttributeValue_S{S: want[0]}},
				"pk": {Value: &cefaspb.AttributeValue_S{S: want[1]}},
			},
		})
		if err != nil || !got.GetFound() {
			t.Errorf("missing %s/%s: err=%v found=%v", want[0], want[1], err, got.GetFound())
		}
	}
}

func TestBatchWriteMV_DeleteOps(t *testing.T) {
	stub, repl, cleanup := startMVReplicaFixture(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := stub.CreateTable(ctx, &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      "DBase",
			KeySchema: &cefaspb.KeySchema{Pk: "pk", Sk: "sk"},
		},
	}); err != nil {
		t.Fatalf("create base: %v", err)
	}
	if _, err := stub.CreateMaterializedView(ctx, &cefaspb.CreateMaterializedViewRequest{
		Descriptor_: &cefaspb.MaterializedViewDescriptor{
			Name:      "DBase_mv",
			BaseTable: "DBase",
			KeySchema: &cefaspb.KeySchema{Pk: "sk", Sk: "pk"},
			RefreshPolicy: &cefaspb.RefreshPolicy{
				Mode: cefaspb.RefreshPolicy_EAGER,
			},
		},
	}); err != nil {
		t.Fatalf("create view: %v", err)
	}

	if _, err := repl.BatchWriteMV(ctx, &cefaspb.BatchWriteMVRequest{
		View: "DBase_mv",
		Ops: []*cefaspb.BatchWriteOp{
			{Kind: cefaspb.BatchWriteOp_KIND_PUT, Item: map[string]*cefaspb.AttributeValue{
				"sk": {Value: &cefaspb.AttributeValue_S{S: "x"}},
				"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
			}},
		},
	}); err != nil {
		t.Fatalf("BatchWriteMV put: %v", err)
	}
	if _, err := repl.BatchWriteMV(ctx, &cefaspb.BatchWriteMVRequest{
		View: "DBase_mv",
		Ops: []*cefaspb.BatchWriteOp{
			{Kind: cefaspb.BatchWriteOp_KIND_DELETE, Key: map[string]*cefaspb.AttributeValue{
				"sk": {Value: &cefaspb.AttributeValue_S{S: "x"}},
				"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
			}},
		},
	}); err != nil {
		t.Fatalf("BatchWriteMV delete: %v", err)
	}

	got, err := stub.GetItem(ctx, &cefaspb.GetItemRequest{
		Table: "DBase_mv",
		Key: map[string]*cefaspb.AttributeValue{
			"sk": {Value: &cefaspb.AttributeValue_S{S: "x"}},
			"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
		},
	})
	if err != nil {
		t.Fatalf("GetItem: %v", err)
	}
	if got.GetFound() {
		t.Error("row should have been deleted via BatchWriteMV")
	}
}

func TestBatchWriteMV_UnknownView(t *testing.T) {
	_, repl, cleanup := startMVReplicaFixture(t)
	defer cleanup()
	_, err := repl.BatchWriteMV(context.Background(), &cefaspb.BatchWriteMVRequest{
		View: "nope",
		Ops:  []*cefaspb.BatchWriteOp{},
	})
	if err == nil {
		t.Fatal("expected NotFound for unknown view")
	}
}
