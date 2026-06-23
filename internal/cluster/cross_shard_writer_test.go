package cluster

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// fakeCefas captures BatchWriteItem calls so the test can assert the
// request reached the right peer with the right payload.
type fakeCefas struct {
	cefaspb.UnimplementedCefasServer
	mu       sync.Mutex
	received []*cefaspb.BatchWriteItemRequest
}

func (f *fakeCefas) BatchWriteItem(ctx context.Context, req *cefaspb.BatchWriteItemRequest) (*cefaspb.BatchWriteItemResponse, error) {
	f.mu.Lock()
	f.received = append(f.received, req)
	f.mu.Unlock()
	return &cefaspb.BatchWriteItemResponse{}, nil
}

func (f *fakeCefas) calls() []*cefaspb.BatchWriteItemRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*cefaspb.BatchWriteItemRequest, len(f.received))
	copy(out, f.received)
	return out
}

func startFakeCefasPeer(t *testing.T, addr string, fc *fakeCefas) *fakePeer {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	cefaspb.RegisterCefasServer(srv, fc)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return &fakePeer{addr: addr, server: srv, lis: lis}
}

func TestLeaderEndpoint_SelfLeader(t *testing.T) {
	m := &Manager{
		cfg:    Config{SelfID: "n1", PeerGRPCAddrs: map[string]string{"n2": "n2:9090"}},
		shards: []*Shard{{ID: 0, LeaderHint: "n1"}},
	}
	peerID, addr, isSelf, err := m.LeaderEndpoint(0)
	if err != nil {
		t.Fatalf("LeaderEndpoint: %v", err)
	}
	if !isSelf || peerID != "n1" || addr != "" {
		t.Fatalf("self-leader mismatch: peer=%q addr=%q isSelf=%v", peerID, addr, isSelf)
	}
}

func TestLeaderEndpoint_RemoteLeader(t *testing.T) {
	m := &Manager{
		cfg:    Config{SelfID: "n1", PeerGRPCAddrs: map[string]string{"n2": "n2:9090"}},
		shards: []*Shard{{ID: 0, LeaderHint: "n2"}},
	}
	peerID, addr, isSelf, err := m.LeaderEndpoint(0)
	if err != nil {
		t.Fatalf("LeaderEndpoint: %v", err)
	}
	if isSelf || peerID != "n2" || addr != "n2:9090" {
		t.Fatalf("remote-leader mismatch: peer=%q addr=%q isSelf=%v", peerID, addr, isSelf)
	}
}

func TestLeaderEndpoint_NoHint(t *testing.T) {
	m := &Manager{
		cfg:    Config{SelfID: "n1"},
		shards: []*Shard{{ID: 0}},
	}
	if _, _, _, err := m.LeaderEndpoint(0); err == nil {
		t.Fatal("expected error when LeaderHint empty")
	}
}

func TestLeaderEndpoint_UnknownPeer(t *testing.T) {
	m := &Manager{
		cfg:    Config{SelfID: "n1", PeerGRPCAddrs: map[string]string{}},
		shards: []*Shard{{ID: 0, LeaderHint: "n2"}},
	}
	if _, _, _, err := m.LeaderEndpoint(0); err == nil {
		t.Fatal("expected error when peer addr missing")
	}
}

func TestBatchWriteItemToPeer_ForwardsRequest(t *testing.T) {
	fc := &fakeCefas{}
	peer := startFakeCefasPeer(t, "n2:9090", fc)
	m := &Manager{
		cfg: Config{
			SelfID:        "n1",
			PeerGRPCAddrs: map[string]string{"n2": "n2:9090"},
		},
		shards:     []*Shard{{ID: 0, LeaderHint: "n2"}},
		peerDialer: bufconnDialer(map[string]*fakePeer{"n2:9090": peer}),
	}
	defer m.closePeerWriters()

	req := &cefaspb.BatchWriteItemRequest{
		Table: "events_mv",
		Ops: []*cefaspb.BatchWriteOp{
			{Kind: cefaspb.BatchWriteOp_KIND_PUT, Item: map[string]*cefaspb.AttributeValue{
				"pk": {Value: &cefaspb.AttributeValue_S{S: "p1"}},
			}},
		},
	}
	if err := m.BatchWriteItemToPeer(context.Background(), "n2", "n2:9090", req); err != nil {
		t.Fatalf("BatchWriteItemToPeer: %v", err)
	}
	got := fc.calls()
	if len(got) != 1 {
		t.Fatalf("expected 1 call, got %d", len(got))
	}
	if got[0].GetTable() != "events_mv" || len(got[0].GetOps()) != 1 {
		t.Fatalf("call shape mismatch: %+v", got[0])
	}
}

func TestBatchWriteItemToPeer_ReusesConnection(t *testing.T) {
	fc := &fakeCefas{}
	peer := startFakeCefasPeer(t, "n2:9090", fc)
	m := &Manager{
		cfg: Config{
			SelfID:        "n1",
			PeerGRPCAddrs: map[string]string{"n2": "n2:9090"},
		},
		shards:     []*Shard{{ID: 0, LeaderHint: "n2"}},
		peerDialer: bufconnDialer(map[string]*fakePeer{"n2:9090": peer}),
	}
	defer m.closePeerWriters()

	ctx := context.Background()
	req := &cefaspb.BatchWriteItemRequest{Table: "t"}
	for i := 0; i < 5; i++ {
		if err := m.BatchWriteItemToPeer(ctx, "n2", "n2:9090", req); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if len(fc.calls()) != 5 {
		t.Fatalf("expected 5 calls, got %d", len(fc.calls()))
	}
	// One cached conn for the peer.
	m.peerWriters.mu.RLock()
	n := len(m.peerWriters.conns)
	m.peerWriters.mu.RUnlock()
	if n != 1 {
		t.Fatalf("expected 1 cached conn, got %d", n)
	}
}
