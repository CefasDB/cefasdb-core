package client_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/metrics"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/api"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/client"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// fixture spins up a real gRPC server backed by a temp Pebble store
// and returns a Client connected to it. No auth, no TLS — exercising
// the dev-mode path.
type fixture struct {
	server *grpc.Server
	listen net.Listener
	db     *storage.DB
}

func newFixture(t *testing.T) (*client.Client, *fixture) {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Path: dir})
	if err != nil {
		t.Fatalf("storage open: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer()
	apiSrv := api.NewGRPCServer(db, cat, nil)
	apiSrv.AttachMetrics(metrics.New())
	apiSrv.AttachBackupScheduler(storage.NewScheduledBackupRunner(db, storage.ScheduledBackupConfig{
		Enabled:      true,
		DryRun:       true,
		Interval:     time.Minute,
		NameTemplate: "sdk-{{unix}}",
	}))
	cefaspb.RegisterCefasServer(gsrv, apiSrv)
	go func() { _ = gsrv.Serve(ln) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := client.Dial(ctx, ln.Addr().String(), client.WithPlaintext())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	f := &fixture{server: gsrv, listen: ln, db: db}
	t.Cleanup(func() {
		_ = c.Close()
		gsrv.GracefulStop()
		_ = ln.Close()
		_ = db.Close()
	})
	return c, f
}

func sAttr(s string) types.AttributeValue { return types.AttributeValue{T: types.AttrS, S: s} }
func nAttr(n string) types.AttributeValue { return types.AttributeValue{T: types.AttrN, N: n} }

func TestSDKCreateTableAndPutGet(t *testing.T) {
	c, _ := newFixture(t)
	ctx := context.Background()

	if err := c.CreateTable(ctx, types.TableDescriptor{
		Name:      "events",
		KeySchema: types.KeySchema{PK: "user_id", SK: "ts"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	td, err := c.DescribeTable(ctx, "events")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if td.KeySchema.PK != "user_id" {
		t.Fatalf("descriptor.PK=%q", td.KeySchema.PK)
	}

	if err := c.PutItem(ctx, "events", types.Item{
		"user_id": sAttr("alice"),
		"ts":      nAttr("100"),
		"event":   sAttr("login"),
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	item, err := c.GetItem(ctx, "events", types.Item{
		"user_id": sAttr("alice"),
		"ts":      nAttr("100"),
	})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if item == nil || item["event"].S != "login" {
		t.Fatalf("unexpected item: %+v", item)
	}

	missing, err := c.GetItem(ctx, "events", types.Item{
		"user_id": sAttr("missing"),
		"ts":      nAttr("0"),
	})
	if err != nil {
		t.Fatalf("get missing: %v", err)
	}
	if missing != nil {
		t.Fatalf("expected nil for missing, got %+v", missing)
	}
}

func TestSDKCreateTableWithStreamSpecification(t *testing.T) {
	c, _ := newFixture(t)
	ctx := context.Background()

	created, err := c.CreateTableWithDescriptor(ctx, types.TableDescriptor{
		Name:      "streamed_events",
		KeySchema: types.KeySchema{PK: "id"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: types.StreamViewTypeKeysOnly,
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.StreamSpecification == nil || !created.StreamSpecification.StreamEnabled {
		t.Fatalf("created stream spec = %+v", created.StreamSpecification)
	}
	if created.StreamSpecification.StreamViewType != types.StreamViewTypeKeysOnly {
		t.Fatalf("created stream view = %q", created.StreamSpecification.StreamViewType)
	}
	if created.LatestStreamArn == "" || created.LatestStreamLabel == "" || created.StreamStatus != types.StreamStatusEnabled {
		t.Fatalf("created stream metadata incomplete: %+v", created)
	}

	described, err := c.DescribeTable(ctx, "streamed_events")
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if described.LatestStreamArn != created.LatestStreamArn {
		t.Fatalf("LatestStreamArn = %q, want %q", described.LatestStreamArn, created.LatestStreamArn)
	}
	if described.StreamSpecification == nil || described.StreamSpecification.StreamViewType != types.StreamViewTypeKeysOnly {
		t.Fatalf("described stream spec = %+v", described.StreamSpecification)
	}
}

func TestSDKDynamoDBStreamsAPIs(t *testing.T) {
	c, _ := newFixture(t)
	ctx := context.Background()

	created, err := c.CreateTableWithDescriptor(ctx, types.TableDescriptor{
		Name:      "streamed_events",
		KeySchema: types.KeySchema{PK: "id"},
		StreamSpecification: &types.StreamSpecification{
			StreamEnabled:  true,
			StreamViewType: types.StreamViewTypeNewAndOldImages,
		},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.LatestStreamArn == "" {
		t.Fatal("created table returned empty stream arn")
	}
	if err := c.PutItem(ctx, "streamed_events", types.Item{
		"id":     sAttr("event-1"),
		"status": sAttr("new"),
	}); err != nil {
		t.Fatalf("put first: %v", err)
	}
	if err := c.PutItem(ctx, "streamed_events", types.Item{
		"id":     sAttr("event-1"),
		"status": sAttr("updated"),
	}); err != nil {
		t.Fatalf("put update: %v", err)
	}
	if err := c.DeleteItem(ctx, "streamed_events", types.Item{"id": sAttr("event-1")}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	listed, err := c.ListStreams(ctx, client.ListStreamsOptions{
		TableName: "streamed_events",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list streams: %v", err)
	}
	if len(listed.Streams) != 1 || listed.Streams[0].StreamArn != created.LatestStreamArn {
		t.Fatalf("listed streams = %+v, want arn %q", listed, created.LatestStreamArn)
	}

	desc, err := c.DescribeStream(ctx, created.LatestStreamArn, client.DescribeStreamOptions{Limit: 1})
	if err != nil {
		t.Fatalf("describe stream: %v", err)
	}
	if desc.StreamArn != created.LatestStreamArn ||
		desc.TableName != "streamed_events" ||
		desc.StreamViewType != types.StreamViewTypeNewAndOldImages ||
		len(desc.Shards) != 1 {
		t.Fatalf("stream description = %+v", desc)
	}

	iterator, err := c.GetShardIterator(ctx, client.GetShardIteratorOptions{
		StreamArn:         desc.StreamArn,
		ShardID:           desc.Shards[0].ShardID,
		ShardIteratorType: "TRIM_HORIZON",
	})
	if err != nil {
		t.Fatalf("get shard iterator: %v", err)
	}
	if iterator == "" {
		t.Fatal("empty shard iterator")
	}

	page1, err := c.GetRecords(ctx, iterator, client.GetRecordsOptions{Limit: 2})
	if err != nil {
		t.Fatalf("get records page1: %v", err)
	}
	if len(page1.Records) != 2 || page1.NextShardIterator == "" {
		t.Fatalf("page1 = %+v", page1)
	}
	if page1.Records[0].EventName != "INSERT" ||
		page1.Records[0].DynamoDB.Keys["id"].S != "event-1" ||
		page1.Records[1].EventName != "MODIFY" ||
		page1.Records[1].DynamoDB.OldImage["status"].S != "new" ||
		page1.Records[1].DynamoDB.NewImage["status"].S != "updated" {
		t.Fatalf("unexpected page1 records = %+v", page1.Records)
	}

	page2, err := c.GetRecords(ctx, page1.NextShardIterator, client.GetRecordsOptions{Limit: 2})
	if err != nil {
		t.Fatalf("get records page2: %v", err)
	}
	if len(page2.Records) != 1 ||
		page2.Records[0].EventName != "REMOVE" ||
		page2.Records[0].DynamoDB.OldImage["status"].S != "updated" {
		t.Fatalf("page2 = %+v", page2)
	}
}

func TestSDKQueryStreaming(t *testing.T) {
	c, _ := newFixture(t)
	ctx := context.Background()
	_ = c.CreateTable(ctx, types.TableDescriptor{
		Name:      "events",
		KeySchema: types.KeySchema{PK: "user_id", SK: "ts"},
	})
	for _, ts := range []string{"001", "002", "003", "004", "005"} {
		_ = c.PutItem(ctx, "events", types.Item{
			"user_id": sAttr("bob"),
			"ts":      sAttr(ts),
			"data":    sAttr("v-" + ts),
		})
	}

	got, err := c.Query(ctx, "events").PK(sAttr("bob")).Limit(3).Run(ctx)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("limit returned %d items, want 3", len(got))
	}

	rng, err := c.Query(ctx, "events").
		PK(sAttr("bob")).
		SKBetween(sAttr("002"), sAttr("004")).
		Run(ctx)
	if err != nil {
		t.Fatalf("range query: %v", err)
	}
	if len(rng) != 2 {
		t.Fatalf("range returned %d items, want 2 (002,003)", len(rng))
	}
	if rng[0]["ts"].S != "002" || rng[1]["ts"].S != "003" {
		t.Fatalf("range order wrong: %+v", rng)
	}
}

func TestSDKConditionalWriteFailedReturnsTypedError(t *testing.T) {
	c, _ := newFixture(t)
	ctx := context.Background()
	_ = c.CreateTable(ctx, types.TableDescriptor{
		Name:      "singles",
		KeySchema: types.KeySchema{PK: "id"},
	})
	item := types.Item{"id": sAttr("k1"), "v": sAttr("hello")}
	if err := c.PutItem(ctx, "singles", item, client.PutOptions{Condition: "attribute_not_exists(id)"}); err != nil {
		t.Fatalf("first put: %v", err)
	}
	err := c.PutItem(ctx, "singles", item, client.PutOptions{Condition: "attribute_not_exists(id)"})
	if err == nil {
		t.Fatal("expected ErrConditionFailed-style error")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status = %v, want FailedPrecondition", status.Code(err))
	}
}

func TestSDKBatchWriteAndGet(t *testing.T) {
	c, _ := newFixture(t)
	ctx := context.Background()
	_ = c.CreateTable(ctx, types.TableDescriptor{Name: "t", KeySchema: types.KeySchema{PK: "id"}})

	if err := c.BatchWriteItem(ctx, "t", []client.BatchWriteOp{
		{Put: types.Item{"id": sAttr("a"), "v": sAttr("A")}},
		{Put: types.Item{"id": sAttr("b"), "v": sAttr("B")}},
		{Put: types.Item{"id": sAttr("c"), "v": sAttr("C")}},
	}); err != nil {
		t.Fatalf("batch write: %v", err)
	}
	items, err := c.BatchGetItem(ctx, "t", []types.Item{
		{"id": sAttr("a")},
		{"id": sAttr("missing")},
		{"id": sAttr("c")},
	})
	if err != nil {
		t.Fatalf("batch get: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("got %d items, want 3", len(items))
	}
	if items[0]["v"].S != "A" || items[2]["v"].S != "C" {
		t.Fatalf("batch get order wrong: %+v", items)
	}
	if items[1] != nil {
		t.Fatalf("expected nil for missing, got %+v", items[1])
	}
}

func TestSDKClusterStatusOnSingleNode(t *testing.T) {
	c, _ := newFixture(t)
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Mode != "single-node" {
		t.Fatalf("mode = %q, want single-node", st.Mode)
	}
	if st.BackupScheduler == nil || !st.BackupScheduler.Enabled || st.BackupScheduler.NameTemplate != "sdk-{{unix}}" {
		t.Fatalf("backup scheduler status = %+v", st.BackupScheduler)
	}
}

func TestSDKClusterStatusIncludesHotRanges(t *testing.T) {
	c, _ := newFixture(t)
	ctx := context.Background()
	if err := c.CreateTable(ctx, types.TableDescriptor{
		Name:      "events",
		KeySchema: types.KeySchema{PK: "id"},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.PutItem(ctx, "events", types.Item{
		"id": sAttr("hot-key"),
		"v":  sAttr("payload"),
	}); err != nil {
		t.Fatalf("put: %v", err)
	}

	st, err := c.Status(ctx)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if len(st.HotRanges) == 0 {
		t.Fatalf("expected hot range summaries")
	}
	got := st.HotRanges[0]
	if got.ShardID != "0" || got.BucketCount == 0 || got.Writes != 1 || got.Bytes == 0 {
		t.Fatalf("unexpected hot range summary: %+v", got)
	}
}
