package server_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/cluster"
	"github.com/CefasDb/cefasdb/internal/server"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

type scanShardFixture struct {
	replica cefaspb.ReplicaClient
	cefas   cefaspb.CefasClient
	manager *cluster.Manager
	cleanup func()
}

func startScanShardFixture(t *testing.T) *scanShardFixture {
	t.Helper()
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:   t.TempDir(),
		Shards: 1,
		SelfID: "n1",
	})
	if err != nil {
		t.Fatalf("cluster open: %v", err)
	}
	sh0, ok := mgr.Shard(0)
	if !ok {
		t.Fatalf("missing shard 0")
	}
	cat, err := catalog.New(sh0.Storage)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	srv := server.NewGRPCServer(sh0.Storage, cat, nil)
	srv.AttachManager(mgr)

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
	return &scanShardFixture{
		replica: cefaspb.NewReplicaClient(conn),
		cefas:   cefaspb.NewCefasClient(conn),
		manager: mgr,
		cleanup: func() {
			_ = conn.Close()
			gsrv.GracefulStop()
			_ = mgr.Close()
		},
	}
}

func (f *scanShardFixture) createTable(t *testing.T, name string) {
	t.Helper()
	_, err := f.cefas.CreateTable(context.Background(), &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      name,
			KeySchema: &cefaspb.KeySchema{Pk: "pk"},
		},
	})
	if err != nil {
		t.Fatalf("create table %s: %v", name, err)
	}
}

func (f *scanShardFixture) putItem(t *testing.T, table, pk string) {
	t.Helper()
	_, err := f.cefas.PutItem(context.Background(), &cefaspb.PutItemRequest{
		Table: table,
		Item: map[string]*cefaspb.AttributeValue{
			"pk": {Value: &cefaspb.AttributeValue_S{S: pk}},
		},
	})
	if err != nil {
		t.Fatalf("put %s/%s: %v", table, pk, err)
	}
}

func collectScanShard(t *testing.T, fx *scanShardFixture, table string, shardID uint32) ([]string, error) {
	t.Helper()
	stream, err := fx.replica.ScanShard(context.Background(), &cefaspb.ScanShardRequest{
		Table:   table,
		ShardId: shardID,
	})
	if err != nil {
		return nil, err
	}
	var pks []string
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return pks, nil
		}
		if err != nil {
			return pks, err
		}
		if pk, ok := item.GetAttributes()["pk"]; ok && pk != nil {
			pks = append(pks, pk.GetS())
		}
	}
}

func TestScanShard_StreamsLocalShardItems(t *testing.T) {
	fx := startScanShardFixture(t)
	defer fx.cleanup()

	fx.createTable(t, "Orders")
	fx.putItem(t, "Orders", "o-1")
	fx.putItem(t, "Orders", "o-2")
	fx.putItem(t, "Orders", "o-3")

	pks, err := collectScanShard(t, fx, "Orders", 0)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(pks) != 3 {
		t.Fatalf("want 3 items, got %d (%v)", len(pks), pks)
	}
}

func TestScanShard_EmptyTable(t *testing.T) {
	fx := startScanShardFixture(t)
	defer fx.cleanup()

	fx.createTable(t, "Empty")

	pks, err := collectScanShard(t, fx, "Empty", 0)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(pks) != 0 {
		t.Fatalf("want 0 items, got %d (%v)", len(pks), pks)
	}
}

func TestScanShard_UnknownShardReturnsNotFound(t *testing.T) {
	fx := startScanShardFixture(t)
	defer fx.cleanup()
	fx.createTable(t, "X")

	_, err := collectScanShard(t, fx, "X", 99)
	if code := status.Code(err); code != codes.NotFound {
		t.Fatalf("want NotFound, got %v (%v)", code, err)
	}
}

func TestScanShard_MissingTableArgReturnsInvalidArgument(t *testing.T) {
	fx := startScanShardFixture(t)
	defer fx.cleanup()

	_, err := collectScanShard(t, fx, "", 0)
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v (%v)", code, err)
	}
}

func TestScanShard_ManagerNotAttachedReturnsFailedPrecondition(t *testing.T) {
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	defer db.Close()
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	srv := server.NewGRPCServer(db, cat, nil) // no AttachManager

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer()
	cefaspb.RegisterReplicaServer(gsrv, srv)
	go func() { _ = gsrv.Serve(ln) }()
	defer gsrv.GracefulStop()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	client := cefaspb.NewReplicaClient(conn)
	stream, err := client.ScanShard(context.Background(), &cefaspb.ScanShardRequest{Table: "T", ShardId: 0})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	_, recvErr := stream.Recv()
	if code := status.Code(recvErr); code != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition, got %v (%v)", code, recvErr)
	}
}
