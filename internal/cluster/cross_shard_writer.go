package cluster

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// peerWriterCache caches one *grpc.ClientConn per peer SelfID so the
// cross-shard write path does not pay a dial + handshake on every
// cascade bucket. Connections are lazily opened on first use and
// closed by Manager.Close().
type peerWriterCache struct {
	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

// LeaderEndpoint resolves the current leader of shardID into routing
// information the caller can act on. isSelf=true means the local node
// holds leadership and should write directly to its pebble.DB; isSelf=false
// means the caller should forward via grpcAddr.
//
// Returns an error when the shard is unknown, the leader hint is empty
// (no election yet), or the leader's gRPC address is not configured.
func (m *Manager) LeaderEndpoint(shardID uint32) (peerID, grpcAddr string, isSelf bool, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if int(shardID) >= len(m.shards) || m.shards[shardID] == nil {
		return "", "", false, fmt.Errorf("cluster: shard %d not in placement", shardID)
	}
	sh := m.shards[shardID]
	hint := sh.LeaderHint
	if hint == "" {
		return "", "", false, fmt.Errorf("cluster: shard %d has no leader hint", shardID)
	}
	if hint == m.cfg.SelfID {
		return hint, "", true, nil
	}
	addr, ok := m.cfg.PeerGRPCAddrs[hint]
	if !ok || addr == "" {
		return "", "", false, fmt.Errorf("cluster: shard %d leader %s has no gRPC address", shardID, hint)
	}
	return hint, addr, false, nil
}

// peerWriteConn returns a cached gRPC connection to peerID, dialing
// addr on first access. Subsequent calls with the same peerID reuse
// the same conn — gRPC handles per-call multiplexing internally.
func (m *Manager) peerWriteConn(ctx context.Context, peerID, addr string) (*grpc.ClientConn, error) {
	m.peerWriters.mu.RLock()
	conn := m.peerWriters.conns[peerID]
	m.peerWriters.mu.RUnlock()
	if conn != nil {
		return conn, nil
	}
	m.peerWriters.mu.Lock()
	defer m.peerWriters.mu.Unlock()
	if conn := m.peerWriters.conns[peerID]; conn != nil {
		return conn, nil
	}
	conn, err := m.dialer()(ctx, addr)
	if err != nil {
		return nil, err
	}
	if m.peerWriters.conns == nil {
		m.peerWriters.conns = make(map[string]*grpc.ClientConn)
	}
	m.peerWriters.conns[peerID] = conn
	return conn, nil
}

// BatchWriteItemToPeer forwards a BatchWriteItem call to the given peer
// over the cached gRPC connection. The caller resolved peerID + addr
// via LeaderEndpoint; this method just dials (or reuses) and invokes.
func (m *Manager) BatchWriteItemToPeer(ctx context.Context, peerID, addr string, req *cefaspb.BatchWriteItemRequest) error {
	conn, err := m.peerWriteConn(ctx, peerID, addr)
	if err != nil {
		return fmt.Errorf("dial peer %s: %w", peerID, err)
	}
	_, err = cefaspb.NewCefasClient(conn).BatchWriteItem(ctx, req)
	return err
}

// BatchWriteMVToPeer forwards an MV cascade bucket to the peer that
// owns the routed shard. The receiver applies the ops without raft —
// MVs are RF=1 by design (see Replica.BatchWriteMV in cefas.proto).
func (m *Manager) BatchWriteMVToPeer(ctx context.Context, peerID, addr string, req *cefaspb.BatchWriteMVRequest) error {
	conn, err := m.peerWriteConn(ctx, peerID, addr)
	if err != nil {
		return fmt.Errorf("dial peer %s: %w", peerID, err)
	}
	_, err = cefaspb.NewReplicaClient(conn).BatchWriteMV(ctx, req)
	return err
}

// PeerDescribeView calls DescribeMaterializedView on the named peer
// to confirm the catalog entry has propagated. Returns nil when the
// peer's catalog can resolve the view; gRPC NotFound surfaces as a
// distinct error so the caller can retry until raft catches up.
func (m *Manager) PeerDescribeView(ctx context.Context, peerID, name string) error {
	m.mu.RLock()
	addr, ok := m.cfg.PeerGRPCAddrs[peerID]
	m.mu.RUnlock()
	if !ok || addr == "" {
		return fmt.Errorf("peer %s: no gRPC address", peerID)
	}
	conn, err := m.peerWriteConn(ctx, peerID, addr)
	if err != nil {
		return fmt.Errorf("dial peer %s: %w", peerID, err)
	}
	_, err = cefaspb.NewCefasClient(conn).DescribeMaterializedView(ctx, &cefaspb.DescribeMaterializedViewRequest{Name: name})
	return err
}

// PeerIDs returns every peer's SelfID excluding the local node.
// Empty when single-node or when no PeerGRPCAddrs are configured.
func (m *Manager) PeerIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	self := m.cfg.SelfID
	out := make([]string, 0, len(m.cfg.PeerGRPCAddrs))
	for id := range m.cfg.PeerGRPCAddrs {
		if id == self {
			continue
		}
		out = append(out, id)
	}
	return out
}

// closePeerWriters tears down every cached peer connection. Called from
// Manager.Close().
func (m *Manager) closePeerWriters() {
	m.peerWriters.mu.Lock()
	defer m.peerWriters.mu.Unlock()
	for _, conn := range m.peerWriters.conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
	m.peerWriters.conns = nil
}
