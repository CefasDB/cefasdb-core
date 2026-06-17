package server_test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/server"
	"github.com/CefasDb/cefasdb/internal/catalog"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func startStreamFixture(t *testing.T) (cefaspb.CefasClient, *catalog.Catalog, func()) {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	catStore, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer()
	cefaspb.RegisterCefasServer(gsrv, server.NewGRPCServer(db, catStore, nil))
	go func() { _ = gsrv.Serve(ln) }()

	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return cefaspb.NewCefasClient(conn), catStore, func() {
		_ = conn.Close()
		gsrv.GracefulStop()
		_ = db.Close()
	}
}

func createStreamTable(t *testing.T, stub cefaspb.CefasClient, table, view string) string {
	t.Helper()
	resp, err := stub.CreateTable(context.Background(), &cefaspb.CreateTableRequest{
		Descriptor_: &cefaspb.TableDescriptor{
			Name:      table,
			KeySchema: &cefaspb.KeySchema{Pk: "pk", Sk: "sk"},
			StreamSpecification: &cefaspb.StreamSpecification{
				StreamEnabled:  true,
				StreamViewType: view,
			},
		},
	})
	if err != nil {
		t.Fatalf("create %s: %v", table, err)
	}
	return resp.GetDescriptor_().GetLatestStreamArn()
}

func TestListStreamsReturnsAllAndFilteredDescriptors(t *testing.T) {
	stub, catStore, cleanup := startStreamFixture(t)
	defer cleanup()
	ctx := context.Background()

	eventsARN := createStreamTable(t, stub, "Events", types.StreamViewTypeKeysOnly)
	ordersARN := createStreamTable(t, stub, "Orders", types.StreamViewTypeNewImage)
	events, err := catStore.Describe("Events")
	if err != nil {
		t.Fatalf("describe events: %v", err)
	}
	events.StreamSpecification = nil
	if err := catStore.UpdateTable(events); err != nil {
		t.Fatalf("disable events stream: %v", err)
	}

	all, err := stub.ListStreams(ctx, &cefaspb.ListStreamsRequest{})
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all.GetStreams()) != 2 {
		t.Fatalf("stream count = %d, want 2: %+v", len(all.GetStreams()), all.GetStreams())
	}
	if got := all.GetStreams()[0].GetStreamArn(); got != eventsARN {
		t.Fatalf("first stream ARN = %q, want retained Events stream %q", got, eventsARN)
	}
	if got := all.GetStreams()[1].GetStreamArn(); got != ordersARN {
		t.Fatalf("second stream ARN = %q, want Orders stream %q", got, ordersARN)
	}

	filtered, err := stub.ListStreams(ctx, &cefaspb.ListStreamsRequest{TableName: "Events"})
	if err != nil {
		t.Fatalf("list filtered: %v", err)
	}
	if len(filtered.GetStreams()) != 1 {
		t.Fatalf("filtered stream count = %d, want 1", len(filtered.GetStreams()))
	}
	if filtered.GetStreams()[0].GetTableName() != "Events" || filtered.GetStreams()[0].GetStreamArn() != eventsARN {
		t.Fatalf("filtered stream = %+v, want Events %q", filtered.GetStreams()[0], eventsARN)
	}
}

func TestListStreamsPaginationAndInvalidToken(t *testing.T) {
	stub, _, cleanup := startStreamFixture(t)
	defer cleanup()
	ctx := context.Background()
	firstARN := createStreamTable(t, stub, "Events", types.StreamViewTypeKeysOnly)
	secondARN := createStreamTable(t, stub, "Orders", types.StreamViewTypeNewImage)

	page1, err := stub.ListStreams(ctx, &cefaspb.ListStreamsRequest{Limit: 1})
	if err != nil {
		t.Fatalf("page1: %v", err)
	}
	if len(page1.GetStreams()) != 1 || page1.GetStreams()[0].GetStreamArn() != firstARN {
		t.Fatalf("page1 = %+v, want first ARN %q", page1.GetStreams(), firstARN)
	}
	if page1.GetLastEvaluatedStreamArn() != firstARN {
		t.Fatalf("last evaluated = %q, want %q", page1.GetLastEvaluatedStreamArn(), firstARN)
	}
	page2, err := stub.ListStreams(ctx, &cefaspb.ListStreamsRequest{
		Limit:                   1,
		ExclusiveStartStreamArn: page1.GetLastEvaluatedStreamArn(),
	})
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	if len(page2.GetStreams()) != 1 || page2.GetStreams()[0].GetStreamArn() != secondARN {
		t.Fatalf("page2 = %+v, want second ARN %q", page2.GetStreams(), secondARN)
	}
	if page2.GetLastEvaluatedStreamArn() != "" {
		t.Fatalf("page2 last evaluated = %q, want empty", page2.GetLastEvaluatedStreamArn())
	}

	_, err = stub.ListStreams(ctx, &cefaspb.ListStreamsRequest{ExclusiveStartStreamArn: "arn:missing"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid token code = %v, want InvalidArgument: %v", status.Code(err), err)
	}
}

func TestDescribeStreamReturnsShardSequenceRange(t *testing.T) {
	stub, _, cleanup := startStreamFixture(t)
	defer cleanup()
	ctx := context.Background()
	arn := createStreamTable(t, stub, "Events", types.StreamViewTypeNewAndOldImages)

	resp, err := stub.DescribeStream(ctx, &cefaspb.DescribeStreamRequest{StreamArn: arn})
	if err != nil {
		t.Fatalf("describe stream: %v", err)
	}
	desc := resp.GetStreamDescription()
	if desc.GetStreamArn() != arn || desc.GetTableName() != "Events" {
		t.Fatalf("description identity = %+v", desc)
	}
	if desc.GetStreamStatus() != types.StreamStatusEnabled {
		t.Fatalf("status = %q, want %q", desc.GetStreamStatus(), types.StreamStatusEnabled)
	}
	if desc.GetStreamViewType() != types.StreamViewTypeNewAndOldImages {
		t.Fatalf("view type = %q, want %q", desc.GetStreamViewType(), types.StreamViewTypeNewAndOldImages)
	}
	if desc.GetKeySchema().GetPk() != "pk" || desc.GetKeySchema().GetSk() != "sk" {
		t.Fatalf("key schema = %+v", desc.GetKeySchema())
	}
	if len(desc.GetShards()) != 1 {
		t.Fatalf("shards = %d, want 1", len(desc.GetShards()))
	}
	shard := desc.GetShards()[0]
	if shard.GetShardId() != types.StreamShardIDSingle {
		t.Fatalf("shard id = %q, want %q", shard.GetShardId(), types.StreamShardIDSingle)
	}
	if shard.GetSequenceNumberRange().GetStartingSequenceNumber() != "1" {
		t.Fatalf("starting sequence = %q, want 1", shard.GetSequenceNumberRange().GetStartingSequenceNumber())
	}
	if shard.GetSequenceNumberRange().GetEndingSequenceNumber() != "" {
		t.Fatalf("ending sequence = %q, want empty", shard.GetSequenceNumberRange().GetEndingSequenceNumber())
	}
}

func TestDescribeStreamMissingAndInvalidShardToken(t *testing.T) {
	stub, _, cleanup := startStreamFixture(t)
	defer cleanup()
	ctx := context.Background()
	arn := createStreamTable(t, stub, "Events", types.StreamViewTypeKeysOnly)

	_, err := stub.DescribeStream(ctx, &cefaspb.DescribeStreamRequest{StreamArn: "arn:missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("missing stream code = %v, want NotFound: %v", status.Code(err), err)
	}
	_, err = stub.DescribeStream(ctx, &cefaspb.DescribeStreamRequest{
		StreamArn:             arn,
		ExclusiveStartShardId: "missing-shard",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid shard token code = %v, want InvalidArgument: %v", status.Code(err), err)
	}
}
