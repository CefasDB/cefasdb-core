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
	cefaspb.RegisterCefasServer(gsrv, api.NewGRPCServer(db, cat, nil))
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
}
