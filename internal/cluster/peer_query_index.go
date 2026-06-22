package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// PeerQueryIndex calls Replica.QueryIndex on a single peer node and
// streams the candidates back through the returned channel. Unlike
// PeerScanShard, the coordinator runs one PeerQueryIndex per peer in
// parallel (concurrent fanout); each call here talks to exactly one
// peer and reports its own outcome on the error channel.
//
// peerID must name a node present in PeerGRPCAddrs; otherwise the
// helper returns an error before issuing any RPC. Cancellation
// propagates from ctx into the gRPC stream.
//
// The candidate channel closes when the peer reports EOF or fails.
// The error channel surfaces one error on failure and closes after.
//
// Complexity: O(candidates) over the network. No retry: the
// coordinator's fanout owns the merge / quorum policy.
func (m *Manager) PeerQueryIndex(ctx context.Context, peerID, table, indexName string, binds map[string]*cefaspb.AttributeValue, limit int32) (<-chan *cefaspb.IndexCandidate, <-chan error) {
	out := make(chan *cefaspb.IndexCandidate, 64)
	errs := make(chan error, 1)

	addr, ok := m.cfg.PeerGRPCAddrs[peerID]
	if !ok || addr == "" {
		errs <- fmt.Errorf("cluster: peer %q has no gRPC address configured", peerID)
		close(errs)
		close(out)
		return out, errs
	}

	go m.runPeerQueryIndex(ctx, peerID, addr, table, indexName, binds, limit, out, errs)
	return out, errs
}

func (m *Manager) runPeerQueryIndex(ctx context.Context, peerID, addr, table, indexName string, binds map[string]*cefaspb.AttributeValue, limit int32, out chan<- *cefaspb.IndexCandidate, errs chan<- error) {
	defer close(out)
	defer close(errs)

	conn, err := m.dialer()(ctx, addr)
	if err != nil {
		errs <- fmt.Errorf("peer %s (%s) dial: %w", peerID, addr, err)
		return
	}
	defer conn.Close()

	stream, err := cefaspb.NewReplicaClient(conn).QueryIndex(ctx, &cefaspb.QueryIndexRequest{
		Table:     table,
		IndexName: indexName,
		Binds:     binds,
		Limit:     limit,
	})
	if err != nil {
		errs <- fmt.Errorf("peer %s QueryIndex: %w", peerID, err)
		return
	}
	for {
		c, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			if status.Code(err) == codes.NotFound {
				// Index not registered on this peer — treat as an empty
				// contribution rather than a hard failure so other peers
				// still produce candidates.
				return
			}
			errs <- fmt.Errorf("peer %s recv: %w", peerID, err)
			return
		}
		select {
		case out <- c:
		case <-ctx.Done():
			errs <- ctx.Err()
			return
		}
	}
}

// PeersForTable returns the set of peer node IDs that host any
// shard of the given table (i.e. any voter or non-voter on any
// placement-active shard), excluding this node itself. Used by the
// QueryIndex coordinator to fan out reads. Order is stable across
// calls so retry behaviour stays predictable.
func (m *Manager) PeersForTable() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	self := m.cfg.SelfID
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, sh := range m.shards {
		if sh == nil {
			continue
		}
		for _, id := range sh.Voters {
			if id == "" || id == self {
				continue
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
		for _, id := range sh.NonVoters {
			if id == "" || id == self {
				continue
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}
