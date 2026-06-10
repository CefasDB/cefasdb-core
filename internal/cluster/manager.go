package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	craft "github.com/osvaldoandrade/cefas/internal/raft"
	"github.com/osvaldoandrade/cefas/internal/storage"
)

// ShardConfig configures a single shard's storage + raft. Most fields
// derive from the cluster-wide Manager config; the manager builds one
// of these per shard at start-up.
type ShardConfig struct {
	ShardID uint32

	StoragePath  string
	FsyncOnCommit bool

	RaftPath      string
	SelfID        string
	BindAddr      string
	Bootstrap     bool
	PeerAddrs     map[string]string // raft peer ID → mux address (every peer's MuxAcceptor)
	PeerHTTPAddrs map[string]string

	StreamLayer craft.Config // partial config injected by the manager
	LogOutput   io.Writer
}

// Shard is the per-shard handle owned by Manager: one storage.DB
// (with raft attached) per shard.
type Shard struct {
	ID      uint32
	Storage *storage.DB
	Raft    *craft.DB
}

// Config bundles every input needed to bring up a multi-Raft node.
type Config struct {
	// Root is the parent directory for shard state. The manager
	// places shard N's Pebble store at Root/shards/N/state and its
	// raft state at Root/shards/N/raft.
	Root string

	// Shards is the cluster-wide shard count. Must be ≥ 1 and match
	// across every peer.
	Shards int

	// SelfID is this node's raft ServerID. Peers identify each other
	// by SelfID across every shard — the same node serves every
	// shard it hosts.
	SelfID string

	// MuxAddr is the TCP address the MuxAcceptor binds. Empty means
	// "single-Raft fallback" — the manager opens a per-shard TCP
	// transport instead. Tests with a single shard often use this
	// path to keep the surface tiny.
	MuxAddr string

	// Peers maps peer SelfID → mux address. Every peer must run the
	// same shard count and the same peer set.
	Peers map[string]string

	// PeerHTTPAddrs is the per-peer HTTP URL used to populate
	// raft.LeaderHTTPAddr() for 307 redirects.
	PeerHTTPAddrs map[string]string

	// Bootstrap is true for the first node started in a fresh
	// cluster. The manager only honours bootstrap for shards whose
	// state directories are empty (mirrors hraft's
	// HasExistingState contract).
	Bootstrap bool

	FsyncOnCommit bool

	HeartbeatMS   int
	ElectionMS    int
	LeaderLeaseMS int
	CommitMS      int

	LogOutput io.Writer
}

// Manager owns every shard's storage + raft on a single node, plus
// the shared MuxAcceptor when multi-Raft mode is enabled.
type Manager struct {
	cfg    Config
	router *Router
	mux    *craft.MuxAcceptor
	shards []*Shard
}

// Open brings up every shard. Shards are opened sequentially so a
// failure in shard k cleanly tears down shards 0..k-1 before
// returning.
func Open(ctx context.Context, cfg Config) (*Manager, error) {
	if cfg.Shards <= 0 {
		cfg.Shards = 1
	}
	if cfg.SelfID == "" {
		return nil, errors.New("cluster: SelfID is required")
	}
	if cfg.Root == "" {
		return nil, errors.New("cluster: Root is required")
	}
	if cfg.LogOutput == nil {
		cfg.LogOutput = io.Discard
	}

	mgr := &Manager{
		cfg:    cfg,
		router: NewRouter(cfg.Shards),
	}

	useMux := cfg.MuxAddr != "" && len(cfg.Peers) > 0
	if useMux {
		mux, err := craft.NewMuxAcceptor(cfg.MuxAddr, cfg.LogOutput)
		if err != nil {
			return nil, fmt.Errorf("mux: %w", err)
		}
		mgr.mux = mux
	}

	for shardID := uint32(0); shardID < uint32(cfg.Shards); shardID++ {
		shard, err := mgr.openShard(ctx, shardID)
		if err != nil {
			mgr.Close()
			return nil, fmt.Errorf("shard %d: %w", shardID, err)
		}
		mgr.shards = append(mgr.shards, shard)
	}
	return mgr, nil
}

func (m *Manager) openShard(ctx context.Context, shardID uint32) (*Shard, error) {
	shardDir := filepath.Join(m.cfg.Root, "shards", fmt.Sprintf("%d", shardID))
	stateDir := filepath.Join(shardDir, "state")
	raftDir := filepath.Join(shardDir, "raft")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", stateDir, err)
	}
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", raftDir, err)
	}

	st, err := storage.Open(storage.Options{Path: stateDir, FsyncOnCommit: m.cfg.FsyncOnCommit})
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}

	if m.mux == nil && len(m.cfg.Peers) == 0 {
		// Single-node mode (no raft). The storage.DB stands alone.
		return &Shard{ID: shardID, Storage: st}, nil
	}

	// Build the per-shard raft config. PeerAddrs in our cluster
	// config map peer-id → mux address; raft sees the SAME address
	// for every shard because mux demultiplexes by group ID, so we
	// pass cfg.Peers through verbatim.
	rcfg := craft.Config{
		Path:          raftDir,
		SelfID:        m.cfg.SelfID,
		BindAddr:      m.cfg.MuxAddr,
		Bootstrap:     m.cfg.Bootstrap,
		PeerAddrs:     m.cfg.Peers,
		PeerHTTPAddrs: m.cfg.PeerHTTPAddrs,
		HeartbeatMS:   m.cfg.HeartbeatMS,
		ElectionMS:    m.cfg.ElectionMS,
		LeaderLeaseMS: m.cfg.LeaderLeaseMS,
		CommitMS:      m.cfg.CommitMS,
		ApplyTimeout:  5 * time.Second,
		LogOutput:     m.cfg.LogOutput,
	}
	if m.mux != nil {
		sl, err := m.mux.RegisterGroup(m.router.GroupID(shardID))
		if err != nil {
			_ = st.Close()
			return nil, fmt.Errorf("mux register group: %w", err)
		}
		rcfg.StreamLayer = sl
	}

	rdb, err := craft.Open(ctx, rcfg, st.Raw())
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("raft open: %w", err)
	}
	st.AttachReplicator(rdb)
	return &Shard{ID: shardID, Storage: st, Raft: rdb}, nil
}

// Router exposes the partition router so the API layer can look up a
// shard for a request's partition key.
func (m *Manager) Router() *Router { return m.router }

// Shard returns the per-shard handle.
func (m *Manager) Shard(id uint32) (*Shard, bool) {
	if int(id) >= len(m.shards) {
		return nil, false
	}
	return m.shards[id], true
}

// Shards returns every owned shard in ID order.
func (m *Manager) Shards() []*Shard { return m.shards }

// ShardForPK selects the shard that owns a request's PK.
func (m *Manager) ShardForPK(pkBytes []byte) *Shard {
	id := m.router.ShardForPK(pkBytes)
	if int(id) >= len(m.shards) {
		return nil
	}
	return m.shards[id]
}

// Close tears every shard down. Errors from individual shards are
// logged via cfg.LogOutput; the first error encountered is returned.
func (m *Manager) Close() error {
	var firstErr error
	for _, s := range m.shards {
		if s.Raft != nil {
			if err := s.Raft.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if s.Storage != nil {
			if err := s.Storage.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if m.mux != nil {
		_ = m.mux.Close()
	}
	return firstErr
}
