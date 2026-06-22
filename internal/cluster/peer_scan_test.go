package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// fakeReplica is a hand-written ReplicaServer used to drive PeerScanShard
// without standing up a real cluster. Each fake serves one bufconn
// listener; callers wire several into a peerDialer to simulate peers.
type fakeReplica struct {
	cefaspb.UnimplementedReplicaServer
	items   []*cefaspb.Item
	respond func(req *cefaspb.ScanShardRequest, send func(*cefaspb.Item) error) error
}

func (f *fakeReplica) ScanShard(req *cefaspb.ScanShardRequest, stream cefaspb.Replica_ScanShardServer) error {
	if f.respond != nil {
		return f.respond(req, stream.Send)
	}
	for _, it := range f.items {
		if err := stream.Send(it); err != nil {
			return err
		}
	}
	return nil
}

type fakePeer struct {
	addr   string
	server *grpc.Server
	lis    *bufconn.Listener
}

func startFakePeer(t *testing.T, addr string, rep *fakeReplica) *fakePeer {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	cefaspb.RegisterReplicaServer(srv, rep)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return &fakePeer{addr: addr, server: srv, lis: lis}
}

func bufconnDialer(peers map[string]*fakePeer) peerDialer {
	return func(ctx context.Context, addr string) (*grpc.ClientConn, error) {
		p, ok := peers[addr]
		if !ok {
			return nil, fmt.Errorf("no fake peer at %q", addr)
		}
		return grpc.NewClient(
			"passthrough://"+addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return p.lis.DialContext(ctx)
			}),
		)
	}
}

func newPeerScanManager(t *testing.T, self string, sh *Shard, peerAddrs map[string]string, dialer peerDialer) *Manager {
	t.Helper()
	m := &Manager{
		cfg: Config{
			SelfID:        self,
			PeerGRPCAddrs: peerAddrs,
		},
		shards:     []*Shard{sh},
		peerDialer: dialer,
	}
	return m
}

func mkItem(pk string) *cefaspb.Item {
	return &cefaspb.Item{Attributes: map[string]*cefaspb.AttributeValue{
		"pk": {Value: &cefaspb.AttributeValue_S{S: pk}},
	}}
}

func collectPeerScan(t *testing.T, items <-chan *cefaspb.Item, errs <-chan error) ([]*cefaspb.Item, error) {
	t.Helper()
	var collected []*cefaspb.Item
	for it := range items {
		collected = append(collected, it)
	}
	var firstErr error
	for e := range errs {
		if firstErr == nil {
			firstErr = e
		}
	}
	return collected, firstErr
}

func TestPeerScanShard_StreamsFromLeaderHintFirst(t *testing.T) {
	leader := startFakePeer(t, "n2:9090", &fakeReplica{items: []*cefaspb.Item{mkItem("a"), mkItem("b")}})
	voter := startFakePeer(t, "n3:9090", &fakeReplica{items: []*cefaspb.Item{mkItem("WRONG")}})

	sh := &Shard{
		ID:         0,
		Voters:     []string{"n2", "n3"},
		LeaderHint: "n2",
	}
	m := newPeerScanManager(t, "n1", sh,
		map[string]string{"n2": "n2:9090", "n3": "n3:9090"},
		bufconnDialer(map[string]*fakePeer{"n2:9090": leader, "n3:9090": voter}),
	)

	items, errs := m.PeerScanShard(context.Background(), 0, "T")
	got, err := collectPeerScan(t, items, errs)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 items from leader, got %d (%+v)", len(got), got)
	}
}

func TestPeerScanShard_SkipsSelf(t *testing.T) {
	peer := startFakePeer(t, "n2:9090", &fakeReplica{items: []*cefaspb.Item{mkItem("x")}})

	sh := &Shard{
		ID:         0,
		Voters:     []string{"n1", "n2"}, // n1 is self — must be skipped
		LeaderHint: "n1",
	}
	m := newPeerScanManager(t, "n1", sh,
		map[string]string{"n1": "n1:9090", "n2": "n2:9090"},
		bufconnDialer(map[string]*fakePeer{"n2:9090": peer}),
	)

	items, errs := m.PeerScanShard(context.Background(), 0, "T")
	got, err := collectPeerScan(t, items, errs)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 item from n2, got %d", len(got))
	}
}

func TestPeerScanShard_FallsThroughOnUnavailable(t *testing.T) {
	noShard := startFakePeer(t, "n2:9090", &fakeReplica{
		respond: func(_ *cefaspb.ScanShardRequest, _ func(*cefaspb.Item) error) error {
			return status.Error(codes.Unavailable, "no local replica")
		},
	})
	holder := startFakePeer(t, "n3:9090", &fakeReplica{items: []*cefaspb.Item{mkItem("ok")}})

	sh := &Shard{
		ID:         0,
		Voters:     []string{"n2", "n3"},
		LeaderHint: "n2",
	}
	m := newPeerScanManager(t, "n1", sh,
		map[string]string{"n2": "n2:9090", "n3": "n3:9090"},
		bufconnDialer(map[string]*fakePeer{"n2:9090": noShard, "n3:9090": holder}),
	)

	items, errs := m.PeerScanShard(context.Background(), 0, "T")
	got, err := collectPeerScan(t, items, errs)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(got) != 1 || got[0].GetAttributes()["pk"].GetS() != "ok" {
		t.Fatalf("expected item from n3, got %+v", got)
	}
}

func TestPeerScanShard_AllPeersUnavailableReturnsError(t *testing.T) {
	deny := func(_ *cefaspb.ScanShardRequest, _ func(*cefaspb.Item) error) error {
		return status.Error(codes.Unavailable, "no local replica")
	}
	p2 := startFakePeer(t, "n2:9090", &fakeReplica{respond: deny})
	p3 := startFakePeer(t, "n3:9090", &fakeReplica{respond: deny})

	sh := &Shard{ID: 0, Voters: []string{"n2", "n3"}}
	m := newPeerScanManager(t, "n1", sh,
		map[string]string{"n2": "n2:9090", "n3": "n3:9090"},
		bufconnDialer(map[string]*fakePeer{"n2:9090": p2, "n3:9090": p3}),
	)

	items, errs := m.PeerScanShard(context.Background(), 0, "T")
	_, err := collectPeerScan(t, items, errs)
	if err == nil {
		t.Fatalf("want error, got nil")
	}
}

func TestPeerScanShard_NonUnavailableErrorPropagatesImmediately(t *testing.T) {
	first := startFakePeer(t, "n2:9090", &fakeReplica{
		respond: func(_ *cefaspb.ScanShardRequest, _ func(*cefaspb.Item) error) error {
			return status.Error(codes.Internal, "boom")
		},
	})
	never := startFakePeer(t, "n3:9090", &fakeReplica{items: []*cefaspb.Item{mkItem("WRONG")}})

	sh := &Shard{ID: 0, Voters: []string{"n2", "n3"}, LeaderHint: "n2"}
	m := newPeerScanManager(t, "n1", sh,
		map[string]string{"n2": "n2:9090", "n3": "n3:9090"},
		bufconnDialer(map[string]*fakePeer{"n2:9090": first, "n3:9090": never}),
	)

	items, errs := m.PeerScanShard(context.Background(), 0, "T")
	got, err := collectPeerScan(t, items, errs)
	if err == nil {
		t.Fatalf("want error, got nil (items=%+v)", got)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 items (immediate fail), got %d", len(got))
	}
}

func TestPeerScanShard_NoGRPCAddrFails(t *testing.T) {
	sh := &Shard{ID: 0, Voters: []string{"n2"}}
	m := newPeerScanManager(t, "n1", sh,
		map[string]string{}, // peer addresses unconfigured
		bufconnDialer(map[string]*fakePeer{}),
	)

	items, errs := m.PeerScanShard(context.Background(), 0, "T")
	_, err := collectPeerScan(t, items, errs)
	if err == nil {
		t.Fatalf("want error, got nil")
	}
}

func TestPeerScanShard_ContextCancelStopsStream(t *testing.T) {
	slow := startFakePeer(t, "n2:9090", &fakeReplica{
		respond: func(_ *cefaspb.ScanShardRequest, send func(*cefaspb.Item) error) error {
			if err := send(mkItem("first")); err != nil {
				return err
			}
			time.Sleep(2 * time.Second)
			return send(mkItem("late"))
		},
	})

	sh := &Shard{ID: 0, Voters: []string{"n2"}, LeaderHint: "n2"}
	m := newPeerScanManager(t, "n1", sh,
		map[string]string{"n2": "n2:9090"},
		bufconnDialer(map[string]*fakePeer{"n2:9090": slow}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	items, errs := m.PeerScanShard(ctx, 0, "T")

	first := <-items
	if first == nil || first.GetAttributes()["pk"].GetS() != "first" {
		t.Fatalf("missing first item: %+v", first)
	}
	cancel()

	for range items {
		// drain remaining items so the goroutine exits.
	}
	// errs should close (with or without an error); the goroutine must
	// not leak. drainWithDeadline guards against a stuck goroutine.
	drainWithDeadline(t, errs, time.Second)
}

func drainWithDeadline(t *testing.T, ch <-chan error, d time.Duration) {
	t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-timer.C:
			t.Fatalf("error channel did not close within %s", d)
		}
	}
}
