package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// peerDialer is overridable so tests can swap real gRPC dials for an
// in-process bufconn or a fake. Production uses dialPeerGRPC.
type peerDialer func(ctx context.Context, addr string) (*grpc.ClientConn, error)

func dialPeerGRPC(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

// PeerScanShard streams every primary item from the requested logical
// shard via a peer that hosts a replica. It tries peers in order of
// LeaderHint → voters → non-voters, skipping this node itself. An
// UNAVAILABLE response from a peer (it does not host the shard, or it
// is briefly unreachable) falls through to the next peer; any other
// gRPC error fails the whole call.
//
// The returned item channel closes when the stream completes or fails.
// On failure the error channel receives one error before being closed.
// On success the error channel is closed without sending.
//
// Cancelling ctx tears down the in-flight stream.
//
// Complexity: O(items) over the network. No retry beyond peer fallback.
func (m *Manager) PeerScanShard(ctx context.Context, shardID uint32, table string) (<-chan *cefaspb.Item, <-chan error) {
	items := make(chan *cefaspb.Item, 64)
	errs := make(chan error, 1)

	sh, ok := m.Shard(shardID)
	if !ok {
		errs <- fmt.Errorf("cluster: shard %d not in placement", shardID)
		close(errs)
		close(items)
		return items, errs
	}

	peers := m.candidatePeersForShard(sh)
	if len(peers) == 0 {
		errs <- fmt.Errorf("cluster: shard %d has no reachable peer (voters=%v nonVoters=%v)", shardID, sh.Voters, sh.NonVoters)
		close(errs)
		close(items)
		return items, errs
	}

	go m.runPeerScanShard(ctx, peers, shardID, table, items, errs)
	return items, errs
}

func (m *Manager) runPeerScanShard(ctx context.Context, peers []peerCandidate, shardID uint32, table string, items chan<- *cefaspb.Item, errs chan<- error) {
	defer close(items)
	defer close(errs)

	var lastErr error
	for _, p := range peers {
		err := m.streamScanShardFromPeer(ctx, p, shardID, table, items)
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			errs <- ctx.Err()
			return
		}
		if status.Code(err) == codes.Unavailable {
			lastErr = err
			continue
		}
		errs <- fmt.Errorf("peer %s (%s) ScanShard(%s, %d): %w", p.id, p.addr, table, shardID, err)
		return
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("cluster: all peers refused shard %d", shardID)
	}
	errs <- fmt.Errorf("cluster: shard %d unreachable: %w", shardID, lastErr)
}

func (m *Manager) streamScanShardFromPeer(ctx context.Context, p peerCandidate, shardID uint32, table string, items chan<- *cefaspb.Item) error {
	conn, err := m.dialer()(ctx, p.addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	stream, err := cefaspb.NewReplicaClient(conn).ScanShard(ctx, &cefaspb.ScanShardRequest{
		Table:   table,
		ShardId: shardID,
	})
	if err != nil {
		return err
	}
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case items <- item:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type peerCandidate struct {
	id   string
	addr string
}

// candidatePeersForShard returns the ordered set of peers worth trying
// for a ScanShard call on the given shard: leader first (if known and
// not self), then remaining voters, then non-voters. Peers without a
// known gRPC address are dropped — without an address there is no way
// to dial them.
func (m *Manager) candidatePeersForShard(sh *Shard) []peerCandidate {
	self := m.cfg.SelfID
	addrs := m.cfg.PeerGRPCAddrs

	seen := make(map[string]struct{}, len(sh.Voters)+len(sh.NonVoters))
	out := make([]peerCandidate, 0, len(sh.Voters)+len(sh.NonVoters))

	add := func(id string) {
		if id == "" || id == self {
			return
		}
		if _, dup := seen[id]; dup {
			return
		}
		addr, ok := addrs[id]
		if !ok || addr == "" {
			return
		}
		seen[id] = struct{}{}
		out = append(out, peerCandidate{id: id, addr: addr})
	}

	add(sh.LeaderHint)
	for _, v := range sh.Voters {
		add(v)
	}
	for _, nv := range sh.NonVoters {
		add(nv)
	}
	return out
}

func (m *Manager) dialer() peerDialer {
	if m.peerDialer != nil {
		return m.peerDialer
	}
	return dialPeerGRPC
}
