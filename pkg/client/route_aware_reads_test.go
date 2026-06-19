package client

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type routeAwareTestServer struct {
	cefaspb.UnimplementedCefasServer

	id       string
	statuses []*cefaspb.ClusterStatusResponse

	statusCalls atomic.Int32
	getCalls    atomic.Int32
	queryCalls  atomic.Int32
	batchCalls  atomic.Int32

	getItem func(context.Context, *cefaspb.GetItemRequest) (*cefaspb.GetItemResponse, error)
	query   func(*cefaspb.QueryRequest, cefaspb.Cefas_QueryServer) error
	batch   func(context.Context, *cefaspb.BatchGetItemRequest) (*cefaspb.BatchGetItemResponse, error)
}

func (s *routeAwareTestServer) ClusterStatus(context.Context, *cefaspb.ClusterStatusRequest) (*cefaspb.ClusterStatusResponse, error) {
	call := int(s.statusCalls.Add(1)) - 1
	if len(s.statuses) == 0 {
		return nil, status.Error(codes.Unavailable, "status unavailable")
	}
	if call >= len(s.statuses) {
		call = len(s.statuses) - 1
	}
	return s.statuses[call], nil
}

func (s *routeAwareTestServer) DescribeTable(context.Context, *cefaspb.DescribeTableRequest) (*cefaspb.DescribeTableResponse, error) {
	return &cefaspb.DescribeTableResponse{Descriptor_: &cefaspb.TableDescriptor{
		Name:      "events",
		KeySchema: &cefaspb.KeySchema{Pk: "pk", Sk: "sk"},
	}}, nil
}

func (s *routeAwareTestServer) GetItem(ctx context.Context, req *cefaspb.GetItemRequest) (*cefaspb.GetItemResponse, error) {
	s.getCalls.Add(1)
	if s.getItem != nil {
		return s.getItem(ctx, req)
	}
	return &cefaspb.GetItemResponse{
		Found: true,
		Item:  map[string]*cefaspb.AttributeValue{"served": attrToPB(types.AttributeValue{T: types.AttrS, S: s.id})},
	}, nil
}

func (s *routeAwareTestServer) Query(req *cefaspb.QueryRequest, stream cefaspb.Cefas_QueryServer) error {
	s.queryCalls.Add(1)
	if s.query != nil {
		return s.query(req, stream)
	}
	return stream.Send(&cefaspb.Item{
		Attributes: map[string]*cefaspb.AttributeValue{"served": attrToPB(types.AttributeValue{T: types.AttrS, S: s.id})},
	})
}

func (s *routeAwareTestServer) BatchGetItem(ctx context.Context, req *cefaspb.BatchGetItemRequest) (*cefaspb.BatchGetItemResponse, error) {
	s.batchCalls.Add(1)
	if s.batch != nil {
		return s.batch(ctx, req)
	}
	out := make([]*cefaspb.Item, 0, len(req.GetKeys()))
	for range req.GetKeys() {
		out = append(out, &cefaspb.Item{
			Attributes: map[string]*cefaspb.AttributeValue{"served": attrToPB(types.AttributeValue{T: types.AttrS, S: s.id})},
		})
	}
	return &cefaspb.BatchGetItemResponse{Items: out}, nil
}

func startRouteAwareTestServer(t *testing.T, srv *routeAwareTestServer) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gsrv := grpc.NewServer()
	cefaspb.RegisterCefasServer(gsrv, srv)
	go func() { _ = gsrv.Serve(ln) }()
	t.Cleanup(func() {
		gsrv.GracefulStop()
		_ = ln.Close()
	})
	return ln.Addr().String()
}

func sAttr(s string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: s}
}

func routeStatus(voters []string, leader string) *cefaspb.ClusterStatusResponse {
	return &cefaspb.ClusterStatusResponse{
		Mode:              "raft",
		RoutingEpoch:      1,
		PlacementVersion:  1,
		ShardCount:        1,
		PlacementStrategy: "token-range",
		Shards: []*cefaspb.ShardPlacement{{
			Id:         1,
			Ranges:     []*cefaspb.TokenRange{{Start: 0, End: 0}},
			State:      "active",
			Epoch:      1,
			Voters:     voters,
			LeaderHint: leader,
		}},
	}
}

func TestRouteAwareRoutesTokenToShard(t *testing.T) {
	r := &routeAwareReads{}
	split := uint64(1) << 63
	r.updatePlacement(ClusterStatus{Shards: []ShardPlacement{
		{ID: 0, Ranges: []TokenRange{{Start: 0, End: split}}, Voters: []string{"n1"}, LeaderHint: "n1"},
		{ID: 1, Ranges: []TokenRange{{Start: split, End: 0}}, Voters: []string{"n2"}, LeaderHint: "n2"},
	}})

	pkBytes := []byte("USER#route")
	target, token, err := r.routeForPK(pkBytes)
	if err != nil {
		t.Fatalf("route: %v", err)
	}
	want := uint32(0)
	if xxhash.Sum64(pkBytes) >= split {
		want = 1
	}
	if token != xxhash.Sum64(pkBytes) {
		t.Fatalf("token = %d, want %d", token, xxhash.Sum64(pkBytes))
	}
	if target.shardID != want {
		t.Fatalf("shard = %d, want %d", target.shardID, want)
	}
}

func TestRouteAwareCandidateSelectionUsesInflightLatencyErrorsAndStableTie(t *testing.T) {
	r := &routeAwareReads{
		nodes: map[string]*routeNode{
			"n1": {id: "n1"},
			"n2": {id: "n2"},
			"n3": {id: "n3"},
		},
		nodeOrder: []string{"n1", "n2", "n3"},
	}
	r.nodes["n1"].inflight.Store(3)
	r.nodes["n2"].latencyNanos.Store(int64(10 * time.Millisecond))
	r.nodes["n3"].lastErrorNanos.Store(time.Now().UnixNano())

	target := routeTarget{shardID: 7, voters: []string{"n1", "n2", "n3"}}
	got, err := r.candidatesForTarget(target, 42, time.Now())
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("candidates len = %d, want 2", len(got))
	}
	if got[0].id != "n2" {
		t.Fatalf("first = %s, want n2", got[0].id)
	}

	r.nodes["n1"].inflight.Store(0)
	r.nodes["n1"].latencyNanos.Store(int64(10 * time.Millisecond))
	r.nodes["n2"].latencyNanos.Store(int64(10 * time.Millisecond))
	first, err := r.candidatesForTarget(routeTarget{shardID: 7, voters: []string{"n1", "n2"}}, 99, time.Now())
	if err != nil {
		t.Fatalf("stable first candidates: %v", err)
	}
	second, err := r.candidatesForTarget(routeTarget{shardID: 7, voters: []string{"n1", "n2"}}, 99, time.Now())
	if err != nil {
		t.Fatalf("stable second candidates: %v", err)
	}
	if first[0].id != second[0].id {
		t.Fatalf("stable tie changed: %s then %s", first[0].id, second[0].id)
	}
}

func TestRouteAwareGetItemUsesReplicaVoter(t *testing.T) {
	n1 := &routeAwareTestServer{id: "n1", statuses: []*cefaspb.ClusterStatusResponse{routeStatus([]string{"n2"}, "n1")}}
	n1.getItem = func(context.Context, *cefaspb.GetItemRequest) (*cefaspb.GetItemResponse, error) {
		return nil, status.Error(codes.Unavailable, "unexpected base node read")
	}
	n2 := &routeAwareTestServer{id: "n2", statuses: []*cefaspb.ClusterStatusResponse{routeStatus([]string{"n2"}, "n1")}}
	addr1 := startRouteAwareTestServer(t, n1)
	addr2 := startRouteAwareTestServer(t, n2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr1, WithPlaintext(), WithRouteAwareReads(map[string]string{"n1": addr1, "n2": addr2}))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	item, err := c.GetItem(ctx, "events", types.Item{"pk": sAttr("user"), "sk": sAttr("event")})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if item["served"].S != "n2" {
		t.Fatalf("served = %q, want n2", item["served"].S)
	}
	if n2.getCalls.Load() != 1 {
		t.Fatalf("n2 get calls = %d, want 1", n2.getCalls.Load())
	}
}

func TestRouteAwareGetItemRefreshesOnNoLocalReplica(t *testing.T) {
	n1Old := routeStatus([]string{"n1"}, "n1")
	n1New := routeStatus([]string{"n2"}, "n2")
	n1 := &routeAwareTestServer{id: "n1", statuses: []*cefaspb.ClusterStatusResponse{n1Old, n1New}}
	n1.getItem = func(context.Context, *cefaspb.GetItemRequest) (*cefaspb.GetItemResponse, error) {
		return nil, status.Error(codes.Unavailable, "cluster: node n1 has no local replica for shard 1")
	}
	n2 := &routeAwareTestServer{id: "n2", statuses: []*cefaspb.ClusterStatusResponse{n1New}}
	addr1 := startRouteAwareTestServer(t, n1)
	addr2 := startRouteAwareTestServer(t, n2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr1, WithPlaintext(), WithRouteAwareReads(map[string]string{"n1": addr1, "n2": addr2}))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	item, err := c.GetItem(ctx, "events", types.Item{"pk": sAttr("user"), "sk": sAttr("event")})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if item["served"].S != "n2" {
		t.Fatalf("served = %q, want n2", item["served"].S)
	}
	stats := c.RouteAwareReadStats()
	if stats.NoLocalReplica == 0 || stats.Refreshes < 2 || stats.Retries == 0 {
		t.Fatalf("stats = %+v, want no-local retry with refresh", stats)
	}
}

func TestRouteAwareStrongGetItemKeepsBasePath(t *testing.T) {
	n1 := &routeAwareTestServer{id: "n1", statuses: []*cefaspb.ClusterStatusResponse{routeStatus([]string{"n2"}, "n2")}}
	n1.getItem = func(_ context.Context, req *cefaspb.GetItemRequest) (*cefaspb.GetItemResponse, error) {
		if req.GetConsistency() != cefaspb.Consistency_CONSISTENCY_STRONG {
			return nil, status.Error(codes.InvalidArgument, "expected strong read")
		}
		return &cefaspb.GetItemResponse{
			Found: true,
			Item:  map[string]*cefaspb.AttributeValue{"served": attrToPB(types.AttributeValue{T: types.AttrS, S: "base"})},
		}, nil
	}
	n2 := &routeAwareTestServer{id: "n2", statuses: []*cefaspb.ClusterStatusResponse{routeStatus([]string{"n2"}, "n2")}}
	n2.getItem = func(context.Context, *cefaspb.GetItemRequest) (*cefaspb.GetItemResponse, error) {
		return nil, status.Error(codes.InvalidArgument, "strong read should not route to replica")
	}
	addr1 := startRouteAwareTestServer(t, n1)
	addr2 := startRouteAwareTestServer(t, n2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr1, WithPlaintext(), WithRouteAwareReads(map[string]string{"n1": addr1, "n2": addr2}))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	item, err := c.GetItem(ctx, "events", types.Item{"pk": sAttr("user"), "sk": sAttr("event")}, GetOptions{Strong: true})
	if err != nil {
		t.Fatalf("get strong: %v", err)
	}
	if item["served"].S != "base" {
		t.Fatalf("served = %q, want base", item["served"].S)
	}
	if n2.getCalls.Load() != 0 {
		t.Fatalf("n2 get calls = %d, want 0", n2.getCalls.Load())
	}
}

func TestRouteAwareQueryRunUsesReplicaForPrimaryKeyEventualRead(t *testing.T) {
	n1 := &routeAwareTestServer{id: "n1", statuses: []*cefaspb.ClusterStatusResponse{routeStatus([]string{"n2"}, "n1")}}
	n1.query = func(*cefaspb.QueryRequest, cefaspb.Cefas_QueryServer) error {
		return status.Error(codes.Unavailable, "unexpected base query")
	}
	n2 := &routeAwareTestServer{id: "n2", statuses: []*cefaspb.ClusterStatusResponse{routeStatus([]string{"n2"}, "n1")}}
	addr1 := startRouteAwareTestServer(t, n1)
	addr2 := startRouteAwareTestServer(t, n2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr1, WithPlaintext(), WithRouteAwareReads(map[string]string{"n1": addr1, "n2": addr2}))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	items, err := c.Query(ctx, "events").PK(sAttr("user")).Run(ctx)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(items) != 1 || items[0]["served"].S != "n2" {
		t.Fatalf("items = %+v, want served by n2", items)
	}
	if n2.queryCalls.Load() != 1 {
		t.Fatalf("n2 query calls = %d, want 1", n2.queryCalls.Load())
	}
}

func TestRouteAwareBatchGetUsesReplicaAndCanBeDisabledPerCall(t *testing.T) {
	n1 := &routeAwareTestServer{id: "n1", statuses: []*cefaspb.ClusterStatusResponse{routeStatus([]string{"n2"}, "n2")}}
	n2 := &routeAwareTestServer{id: "n2", statuses: []*cefaspb.ClusterStatusResponse{routeStatus([]string{"n2"}, "n2")}}
	addr1 := startRouteAwareTestServer(t, n1)
	addr2 := startRouteAwareTestServer(t, n2)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, addr1, WithPlaintext(), WithRouteAwareReads(map[string]string{"n1": addr1, "n2": addr2}))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	items, err := c.BatchGetItem(ctx, "events", []types.Item{{"pk": sAttr("user"), "sk": sAttr("event")}})
	if err != nil {
		t.Fatalf("batch route-aware: %v", err)
	}
	if len(items) != 1 || items[0]["served"].S != "n2" {
		t.Fatalf("route-aware batch items = %+v, want n2", items)
	}
	if n2.batchCalls.Load() != 1 {
		t.Fatalf("n2 batch calls = %d, want 1", n2.batchCalls.Load())
	}

	disabled := false
	items, err = c.BatchGetItem(ctx, "events", []types.Item{{"pk": sAttr("user"), "sk": sAttr("event")}}, BatchGetOptions{RouteAware: &disabled})
	if err != nil {
		t.Fatalf("batch disabled: %v", err)
	}
	if len(items) != 1 || items[0]["served"].S != "n1" {
		t.Fatalf("disabled batch items = %+v, want n1", items)
	}
	if n1.batchCalls.Load() != 1 {
		t.Fatalf("n1 batch calls = %d, want 1", n1.batchCalls.Load())
	}
}
