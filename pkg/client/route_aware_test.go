package client

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type routeAwareTestNode struct {
	cefaspb.UnimplementedCefasServer

	id     string
	status *cefaspb.ClusterStatusResponse
	table  types.TableDescriptor
	items  map[string]types.Item

	getCalls   atomic.Int64
	batchCalls atomic.Int64
	queryCalls atomic.Int64
	failGet    atomic.Bool
}

func TestRouteAwareEventualGetItemUsesShardReplica(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, primary, replica := newRouteAwareTestClient(t)

	got, err := c.GetItem(ctx, "events", types.Item{"id": routeAwareString("a")})
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got == nil || got["v"].S != "replica-a" {
		t.Fatalf("item = %+v, want replica-a", got)
	}
	if calls := primary.getCalls.Load(); calls != 0 {
		t.Fatalf("primary get calls = %d, want 0", calls)
	}
	if calls := replica.getCalls.Load(); calls != 1 {
		t.Fatalf("replica get calls = %d, want 1", calls)
	}
	stats := c.RouteAwareStats()
	if stats.Attempts != 1 || stats.Successes != 1 || stats.Bypasses != 0 || stats.Failures != 0 {
		t.Fatalf("route-aware stats = %+v", stats)
	}
}

func TestRouteAwareGetItemKeepsStrongAndOverrideOnPrimary(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	t.Run("strong", func(t *testing.T) {
		c, primary, replica := newRouteAwareTestClient(t)
		got, err := c.GetItem(ctx, "events", types.Item{"id": routeAwareString("a")}, GetOptions{Strong: true})
		if err != nil {
			t.Fatalf("strong get: %v", err)
		}
		if got == nil || got["v"].S != "primary-a" {
			t.Fatalf("item = %+v, want primary-a", got)
		}
		if calls := primary.getCalls.Load(); calls != 1 {
			t.Fatalf("primary get calls = %d, want 1", calls)
		}
		if calls := replica.getCalls.Load(); calls != 0 {
			t.Fatalf("replica get calls = %d, want 0", calls)
		}
	})

	t.Run("override disabled", func(t *testing.T) {
		c, primary, replica := newRouteAwareTestClient(t)
		disabled := false
		got, err := c.GetItem(ctx, "events", types.Item{"id": routeAwareString("a")}, GetOptions{RouteAware: &disabled})
		if err != nil {
			t.Fatalf("eventual get: %v", err)
		}
		if got == nil || got["v"].S != "primary-a" {
			t.Fatalf("item = %+v, want primary-a", got)
		}
		if calls := primary.getCalls.Load(); calls != 1 {
			t.Fatalf("primary get calls = %d, want 1", calls)
		}
		if calls := replica.getCalls.Load(); calls != 0 {
			t.Fatalf("replica get calls = %d, want 0", calls)
		}
	})
}

func TestRouteAwareBatchGetAndQueryUseShardReplica(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, primary, replica := newRouteAwareTestClient(t)

	items, err := c.BatchGetItem(ctx, "events", []types.Item{
		{"id": routeAwareString("b")},
		{"id": routeAwareString("missing")},
		{"id": routeAwareString("a")},
	})
	if err != nil {
		t.Fatalf("batch get: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("items len = %d, want 3", len(items))
	}
	if items[0] == nil || items[0]["v"].S != "replica-b" {
		t.Fatalf("items[0] = %+v, want replica-b", items[0])
	}
	if items[1] != nil {
		t.Fatalf("items[1] = %+v, want nil", items[1])
	}
	if items[2] == nil || items[2]["v"].S != "replica-a" {
		t.Fatalf("items[2] = %+v, want replica-a", items[2])
	}
	if calls := primary.batchCalls.Load(); calls != 0 {
		t.Fatalf("primary batch calls = %d, want 0", calls)
	}
	if calls := replica.batchCalls.Load(); calls != 1 {
		t.Fatalf("replica batch calls = %d, want 1", calls)
	}

	rows, err := c.Query(ctx, "events").PK(routeAwareString("a")).Run(ctx)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(rows) != 1 || rows[0]["v"].S != "replica-a" {
		t.Fatalf("query rows = %+v, want replica-a", rows)
	}
	if calls := primary.queryCalls.Load(); calls != 0 {
		t.Fatalf("primary query calls = %d, want 0", calls)
	}
	if calls := replica.queryCalls.Load(); calls != 1 {
		t.Fatalf("replica query calls = %d, want 1", calls)
	}
}

func TestRouteAwareCandidateOrderPrefersHealthyLowerLoadThenLatency(t *testing.T) {
	r := &routeAwareReads{nodes: map[string]*routeAwareNode{
		"busy":    {id: "busy"},
		"fast":    {id: "fast"},
		"stale":   {id: "stale"},
		"unknown": {id: "unknown"},
	}}
	r.nodes["busy"].inFlight.Store(3)
	r.nodes["busy"].lastLatencyNs.Store(int64(100 * time.Millisecond))
	r.nodes["fast"].lastLatencyNs.Store(int64(time.Millisecond))
	r.nodes["stale"].lastErrorUnix.Store(time.Now().Unix())
	r.nodes["unknown"].lastLatencyNs.Store(int64(10 * time.Millisecond))

	got := r.candidateOrder(routeAwareTarget{voters: []string{"busy", "stale", "unknown", "fast"}}, []byte("pk"))
	if len(got) != 4 {
		t.Fatalf("candidate count = %d, want 4", len(got))
	}
	if got[0].id != "fast" {
		t.Fatalf("first candidate = %q, want fast", got[0].id)
	}
	if got[1].id != "unknown" {
		t.Fatalf("second candidate = %q, want unknown", got[1].id)
	}
	if got[2].id != "busy" {
		t.Fatalf("third candidate = %q, want busy", got[2].id)
	}
	if got[3].id != "stale" {
		t.Fatalf("fourth candidate = %q, want stale", got[3].id)
	}

	again := r.candidateOrder(routeAwareTarget{voters: []string{"busy", "stale", "unknown", "fast"}}, []byte("pk"))
	for i := range got {
		if got[i].id != again[i].id {
			t.Fatalf("candidate order changed at %d: %q then %q", i, got[i].id, again[i].id)
		}
	}
}

func TestRouteAwareGetItemRefreshesOnceOnRetryableReplicaError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, replica := newRouteAwareTestClient(t)
	replica.failGet.Store(true)

	got, err := c.GetItem(ctx, "events", types.Item{"id": routeAwareString("a")})
	if err != nil {
		t.Fatalf("get item: %v", err)
	}
	if got == nil || got["v"].S != "replica-a" {
		t.Fatalf("item = %+v, want replica-a", got)
	}
	if calls := replica.getCalls.Load(); calls != 2 {
		t.Fatalf("replica get calls = %d, want 2", calls)
	}
	stats := c.RouteAwareStats()
	if stats.Successes != 1 || stats.Retries != 1 || stats.Refreshes != 2 || stats.Failures != 0 {
		t.Fatalf("route-aware stats = %+v", stats)
	}
}

func newRouteAwareTestClient(t *testing.T) (*Client, *routeAwareTestNode, *routeAwareTestNode) {
	t.Helper()
	table := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	primary := &routeAwareTestNode{
		id:    "primary",
		table: table,
		items: map[string]types.Item{
			"a": routeAwareItem("a", "primary-a"),
			"b": routeAwareItem("b", "primary-b"),
		},
	}
	replica := &routeAwareTestNode{
		id:    "replica",
		table: table,
		items: map[string]types.Item{
			"a": routeAwareItem("a", "replica-a"),
			"b": routeAwareItem("b", "replica-b"),
		},
	}
	primaryAddr := startRouteAwareTestNode(t, primary)
	replicaAddr := startRouteAwareTestNode(t, replica)
	status := routeAwareTestStatus([]string{"replica"}, primaryAddr, replicaAddr)
	primary.status = status
	replica.status = status

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, primaryAddr, WithPlaintext(), WithRouteAwareReads(map[string]string{
		"primary": primaryAddr,
		"replica": replicaAddr,
	}))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, primary, replica
}

func startRouteAwareTestNode(t *testing.T, node *routeAwareTestNode) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	cefaspb.RegisterCefasServer(srv, node)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		srv.GracefulStop()
		_ = ln.Close()
	})
	return ln.Addr().String()
}

func routeAwareTestStatus(voters []string, primaryAddr, replicaAddr string) *cefaspb.ClusterStatusResponse {
	return &cefaspb.ClusterStatusResponse{
		Mode:              "raft",
		IsLeader:          true,
		SelfId:            "primary",
		RoutingEpoch:      1,
		PlacementVersion:  1,
		ShardCount:        1,
		PlacementStrategy: routeAwareStrategyTokenRange,
		Shards: []*cefaspb.ShardPlacement{{
			Id:         0,
			State:      "active",
			Epoch:      1,
			Voters:     append([]string(nil), voters...),
			LeaderHint: "primary",
			Ranges:     []*cefaspb.TokenRange{{Start: 0, End: 0}},
		}},
		Nodes: []*cefaspb.NodeDescriptor{
			{Id: "primary", RaftAddr: primaryAddr, State: "active"},
			{Id: "replica", RaftAddr: replicaAddr, State: "active"},
		},
	}
}

func (n *routeAwareTestNode) DescribeTable(context.Context, *cefaspb.DescribeTableRequest) (*cefaspb.DescribeTableResponse, error) {
	return &cefaspb.DescribeTableResponse{Descriptor_: tdToPB(n.table)}, nil
}

func (n *routeAwareTestNode) ClusterStatus(context.Context, *cefaspb.ClusterStatusRequest) (*cefaspb.ClusterStatusResponse, error) {
	return n.status, nil
}

func (n *routeAwareTestNode) GetItem(_ context.Context, req *cefaspb.GetItemRequest) (*cefaspb.GetItemResponse, error) {
	n.getCalls.Add(1)
	if n.failGet.CompareAndSwap(true, false) {
		return nil, status.Error(codes.Unavailable, "cluster: no local replica for routed shard")
	}
	id := req.GetKey()["id"].GetS()
	item := n.items[id]
	if item == nil {
		return &cefaspb.GetItemResponse{}, nil
	}
	return &cefaspb.GetItemResponse{Found: true, Item: itemAttrMap(item)}, nil
}

func (n *routeAwareTestNode) BatchGetItem(_ context.Context, req *cefaspb.BatchGetItemRequest) (*cefaspb.BatchGetItemResponse, error) {
	n.batchCalls.Add(1)
	resp := &cefaspb.BatchGetItemResponse{Items: make([]*cefaspb.Item, 0, len(req.GetKeys()))}
	for _, key := range req.GetKeys() {
		id := key.GetAttributes()["id"].GetS()
		item := n.items[id]
		if item == nil {
			resp.Items = append(resp.Items, &cefaspb.Item{})
			continue
		}
		resp.Items = append(resp.Items, &cefaspb.Item{Attributes: itemAttrMap(item)})
	}
	return resp, nil
}

func (n *routeAwareTestNode) Query(req *cefaspb.QueryRequest, stream grpc.ServerStreamingServer[cefaspb.Item]) error {
	n.queryCalls.Add(1)
	id := req.GetPkValue().GetS()
	if item := n.items[id]; item != nil {
		if err := stream.Send(&cefaspb.Item{Attributes: itemAttrMap(item)}); err != nil {
			return err
		}
	}
	return nil
}

func routeAwareItem(id, v string) types.Item {
	return types.Item{
		"id": routeAwareString(id),
		"v":  routeAwareString(v),
	}
}

func routeAwareString(s string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: s}
}
