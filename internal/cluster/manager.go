package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	craft "github.com/osvaldoandrade/cefas/internal/raft"
	"github.com/osvaldoandrade/cefas/internal/storage"
)

// ShardConfig configures a single shard's storage + raft. Most fields
// derive from the cluster-wide Manager config; the manager builds one
// of these per shard at start-up.
type ShardConfig struct {
	ShardID uint32

	StoragePath    string
	FsyncOnCommit  bool
	StorageProfile string
	StorageTuning  storage.PebbleTuning
	Backpressure   storage.BackpressureOptions

	RaftPath      string
	RaftStorePath string
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
	ID          uint32
	State       ShardState
	Epoch       uint64
	Ranges      []TokenRange
	Voters      []string
	NonVoters   []string
	Storage     *storage.DB
	RaftStorage *storage.DB
	Raft        *craft.DB
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

	// PlacementPath is the node-local cache of the versioned placement
	// catalog. Empty defaults to Root/placement.json.
	PlacementPath string

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

	// NodeCapacity is advisory metadata recorded in the placement
	// catalog for this node. Zero values default to weight=1.
	NodeCapacity NodeCapacity

	// Bootstrap is true for the first node started in a fresh
	// cluster. The manager only honours bootstrap for shards whose
	// state directories are empty (mirrors hraft's
	// HasExistingState contract).
	Bootstrap bool

	FsyncOnCommit  bool
	StorageProfile string
	StorageTuning  storage.PebbleTuning
	Backpressure   storage.BackpressureOptions
	RaftProfile    string
	RaftTuning     storage.PebbleTuning

	HeartbeatMS   int
	ElectionMS    int
	LeaderLeaseMS int
	CommitMS      int

	LogOutput io.Writer
}

// Manager owns every shard's storage + raft on a single node, plus
// the shared MuxAcceptor when multi-Raft mode is enabled.
type Manager struct {
	mu            sync.RWMutex
	cfg           Config
	router        *Router
	placement     PlacementCatalog
	placementPath string
	mux           *craft.MuxAcceptor
	shards        []*Shard
}

var placementSystemKey = []byte(storage.Namespace + "cluster/placement")

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

	placementPath := PlacementFilePath(cfg.Root, cfg.PlacementPath)
	placement, err := loadOrCreatePlacement(cfg, placementPath)
	if err != nil {
		return nil, err
	}
	router, err := NewRouterFromCatalog(placement)
	if err != nil {
		return nil, err
	}
	cfg.Shards = router.Count()

	mgr := &Manager{
		cfg:           cfg,
		router:        router,
		placement:     placement,
		placementPath: placementPath,
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
	if err := mgr.RefreshPlacement(); err != nil {
		fmt.Fprintf(cfg.LogOutput, "cluster: placement refresh skipped: %v\n", err)
	}
	return mgr, nil
}

func loadOrCreatePlacement(cfg Config, path string) (PlacementCatalog, error) {
	cat, err := LoadPlacementFile(path)
	if err == nil {
		return cat, nil
	}
	if !os.IsNotExist(err) {
		return PlacementCatalog{}, err
	}
	strategy := PlacementStrategyTokenRange
	if hasExistingShardState(cfg.Root) {
		strategy = PlacementStrategyLegacyModulo
	}
	cat = DefaultPlacement(cfg.Shards, cfg.SelfID, cfg.Peers, cfg.PeerHTTPAddrs, cfg.NodeCapacity, strategy)
	if err := SavePlacementFile(path, cat); err != nil {
		return PlacementCatalog{}, err
	}
	return cat, nil
}

func hasExistingShardState(root string) bool {
	entries, err := os.ReadDir(filepath.Join(root, "shards"))
	if err != nil {
		return false
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		stateDir := filepath.Join(root, "shards", entry.Name(), "state")
		files, err := os.ReadDir(stateDir)
		if err == nil && len(files) > 0 {
			return true
		}
	}
	return false
}

func (m *Manager) openShard(ctx context.Context, shardID uint32) (*Shard, error) {
	meta, _ := m.shardPlacement(shardID)
	return m.openShardWithPlacement(ctx, shardID, meta)
}

func (m *Manager) openShardWithPlacement(ctx context.Context, shardID uint32, meta ShardPlacement) (*Shard, error) {
	shardDir := filepath.Join(m.cfg.Root, "shards", fmt.Sprintf("%d", shardID))
	stateDir := filepath.Join(shardDir, "state")
	raftDir := filepath.Join(shardDir, "raft")
	raftStoreDir := filepath.Join(raftDir, "store")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", stateDir, err)
	}
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", raftDir, err)
	}

	st, err := storage.Open(storage.Options{
		Path:          stateDir,
		FsyncOnCommit: m.cfg.FsyncOnCommit,
		Profile:       m.cfg.StorageProfile,
		Tuning:        m.cfg.StorageTuning,
		Backpressure:  m.cfg.Backpressure,
	})
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}

	if m.mux == nil && len(m.cfg.Peers) == 0 {
		// Single-node mode (no raft). The storage.DB stands alone.
		return &Shard{
			ID:        shardID,
			State:     meta.State,
			Epoch:     meta.Epoch,
			Ranges:    append([]TokenRange(nil), meta.Ranges...),
			Voters:    append([]string(nil), meta.Voters...),
			NonVoters: append([]string(nil), meta.NonVoters...),
			Storage:   st,
		}, nil
	}

	raftProfile := m.cfg.RaftProfile
	if raftProfile == "" {
		raftProfile = storage.ProfileRaft
	}
	raftStore, err := storage.Open(storage.Options{
		Path:          raftStoreDir,
		FsyncOnCommit: m.cfg.FsyncOnCommit,
		Profile:       raftProfile,
		Tuning:        m.cfg.RaftTuning,
	})
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("raft storage: %w", err)
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
		sl, err := m.mux.RegisterGroup(shardID)
		if err != nil {
			_ = raftStore.Close()
			_ = st.Close()
			return nil, fmt.Errorf("mux register group: %w", err)
		}
		rcfg.StreamLayer = sl
	}

	rdb, err := craft.Open(ctx, rcfg, st.Raw(), raftStore.Raw())
	if err != nil {
		if m.mux != nil {
			_ = m.mux.UnregisterGroup(shardID)
		}
		_ = raftStore.Close()
		_ = st.Close()
		return nil, fmt.Errorf("raft open: %w", err)
	}
	st.AttachReplicator(rdb)
	return &Shard{
		ID:          shardID,
		State:       meta.State,
		Epoch:       meta.Epoch,
		Ranges:      append([]TokenRange(nil), meta.Ranges...),
		Voters:      append([]string(nil), meta.Voters...),
		NonVoters:   append([]string(nil), meta.NonVoters...),
		Storage:     st,
		RaftStorage: raftStore,
		Raft:        rdb,
	}, nil
}

func (m *Manager) shardPlacement(shardID uint32) (ShardPlacement, bool) {
	for _, sh := range m.placement.Shards {
		if sh.ID == shardID {
			return sh, true
		}
	}
	return ShardPlacement{ID: shardID, State: ShardStateActive, Epoch: m.placement.Epoch}, false
}

func (m *Manager) openMissingShardsForPlacement(ctx context.Context, cat PlacementCatalog) error {
	cat.normalize()
	if err := ValidatePlacement(cat); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(cat.Shards) < len(m.shards) {
		return fmt.Errorf("cluster: placement shard count %d is less than open shards %d", len(cat.Shards), len(m.shards))
	}
	for len(m.shards) < len(cat.Shards) {
		shardID := uint32(len(m.shards))
		meta := cat.Shards[shardID]
		if meta.ID != shardID {
			return fmt.Errorf("cluster: shard IDs must be contiguous from 0: got %d at position %d", meta.ID, shardID)
		}
		shard, err := m.openShardWithPlacement(ctx, shardID, meta)
		if err != nil {
			return fmt.Errorf("open shard %d: %w", shardID, err)
		}
		m.shards = append(m.shards, shard)
		m.cfg.Shards = len(m.shards)
	}
	return nil
}

// Router exposes the partition router so the API layer can look up a
// shard for a request's partition key.
func (m *Manager) Router() *Router {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.router
}

func (m *Manager) Placement() PlacementCatalog {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.placement.Clone()
}

func (m *Manager) RoutingEpoch() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.placement.Epoch
}

func (m *Manager) PlacementVersion() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.placement.Version
}

func (m *Manager) PlacementStrategy() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.placement.Strategy
}

func (m *Manager) Nodes() []NodeDescriptor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]NodeDescriptor, 0, len(m.placement.Nodes))
	for _, node := range m.placement.Nodes {
		node.Capacity.Tags = append([]string(nil), node.Capacity.Tags...)
		out = append(out, node)
	}
	return out
}

// Shard returns the per-shard handle.
func (m *Manager) Shard(id uint32) (*Shard, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if int(id) >= len(m.shards) {
		return nil, false
	}
	return m.shards[id], true
}

// Shards returns every owned shard in ID order.
func (m *Manager) Shards() []*Shard {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]*Shard(nil), m.shards...)
}

// ShardForPK selects the shard that owns a request's PK.
func (m *Manager) ShardForPK(pkBytes []byte) *Shard {
	sh, err := m.RouteForPK(pkBytes, 0)
	if err != nil {
		return nil
	}
	return sh
}

// RouteForPK selects the shard and verifies an optional caller routing
// epoch. epoch==0 means "caller has no cached route".
func (m *Manager) RouteForPK(pkBytes []byte, epoch uint64) (*Shard, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := checkRoutingEpoch(epoch, m.placement.Epoch); err != nil {
		return nil, err
	}
	id := m.router.ShardForPK(pkBytes)
	if int(id) >= len(m.shards) {
		return nil, fmt.Errorf("cluster: routed to missing shard %d", id)
	}
	sh := m.shards[id]
	if sh == nil {
		return nil, fmt.Errorf("cluster: routed to missing shard %d", id)
	}
	if !sh.State.routable() {
		return nil, fmt.Errorf("cluster: routed to non-routable shard %d state=%s", id, sh.State)
	}
	return sh, nil
}

func checkRoutingEpoch(clientEpoch, currentEpoch uint64) error {
	if clientEpoch == 0 || clientEpoch == currentEpoch {
		return nil
	}
	return &StaleRouteError{ClientEpoch: clientEpoch, CurrentEpoch: currentEpoch}
}

var ErrStaleRoute = errors.New("cluster: stale routing epoch")

type StaleRouteError struct {
	ClientEpoch  uint64
	CurrentEpoch uint64
}

func (e *StaleRouteError) Error() string {
	return fmt.Sprintf("cluster: stale routing epoch %d (current %d)", e.ClientEpoch, e.CurrentEpoch)
}

func (e *StaleRouteError) Is(target error) bool { return target == ErrStaleRoute }

// RefreshPlacement loads a newer placement catalog replicated through
// shard 0's data store, if one is present locally.
func (m *Manager) RefreshPlacement() error {
	shard0, ok := m.Shard(0)
	if !ok || shard0 == nil || shard0.Storage == nil {
		return nil
	}
	raw, err := shard0.Storage.Get(placementSystemKey)
	if err == storage.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	cat, err := parsePlacement(raw)
	if err != nil {
		return err
	}
	m.mu.RLock()
	currentEpoch := m.placement.Epoch
	m.mu.RUnlock()
	if cat.Epoch <= currentEpoch {
		return nil
	}
	if err := m.openMissingShardsForPlacement(context.Background(), cat); err != nil {
		return err
	}
	return m.applyPlacement(cat, true)
}

// PublishPlacement writes the current catalog through shard 0. In
// Raft mode this must be called on shard 0's leader so followers apply
// the same system key.
func (m *Manager) PublishPlacement() error {
	m.mu.RLock()
	cat := m.placement.Clone()
	m.mu.RUnlock()
	raw, err := encodePlacement(cat)
	if err != nil {
		return err
	}
	shard0, ok := m.Shard(0)
	if !ok || shard0 == nil || shard0.Storage == nil {
		return fmt.Errorf("cluster: shard 0 unavailable")
	}
	return shard0.Storage.Set(placementSystemKey, raw)
}

func (m *Manager) applyPlacement(cat PlacementCatalog, save bool) error {
	m.mu.RLock()
	openShards := len(m.shards)
	m.mu.RUnlock()
	if len(cat.Shards) > openShards {
		if err := m.openMissingShardsForPlacement(context.Background(), cat); err != nil {
			return err
		}
	}
	m.mu.RLock()
	openShards = len(m.shards)
	m.mu.RUnlock()
	if len(cat.Shards) != openShards {
		return fmt.Errorf("cluster: placement shard count %d does not match open shards %d", len(cat.Shards), openShards)
	}
	router, err := NewRouterFromCatalog(cat)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.placement = cat.Clone()
	m.router = router
	for _, meta := range m.placement.Shards {
		if int(meta.ID) >= len(m.shards) || m.shards[meta.ID] == nil {
			continue
		}
		sh := m.shards[meta.ID]
		sh.State = meta.State
		sh.Epoch = meta.Epoch
		sh.Ranges = append([]TokenRange(nil), meta.Ranges...)
		sh.Voters = append([]string(nil), meta.Voters...)
		sh.NonVoters = append([]string(nil), meta.NonVoters...)
	}
	path := m.placementPath
	snapshot := m.placement.Clone()
	m.mu.Unlock()
	if save {
		return SavePlacementFile(path, snapshot)
	}
	return nil
}

func (m *Manager) AddShardVoter(shardID uint32, id, addr string, timeout time.Duration) error {
	sh, ok := m.Shard(shardID)
	if !ok || sh == nil || sh.Raft == nil {
		return fmt.Errorf("cluster: shard %d has no raft group", shardID)
	}
	if err := sh.Raft.AddVoter(id, addr, timeout); err != nil {
		return err
	}
	return m.recordShardVoter(shardID, id, true)
}

func (m *Manager) AddVoterAllShards(id, addr string, timeout time.Duration) error {
	for _, sh := range m.Shards() {
		if sh == nil || sh.Raft == nil {
			continue
		}
		if err := m.AddShardVoter(sh.ID, id, addr, timeout); err != nil {
			return fmt.Errorf("shard %d: %w", sh.ID, err)
		}
	}
	return nil
}

func (m *Manager) RemoveShardServer(shardID uint32, id string, timeout time.Duration) error {
	sh, ok := m.Shard(shardID)
	if !ok || sh == nil || sh.Raft == nil {
		return fmt.Errorf("cluster: shard %d has no raft group", shardID)
	}
	if err := sh.Raft.RemoveServer(id, timeout); err != nil {
		return err
	}
	return m.recordShardVoter(shardID, id, false)
}

func (m *Manager) RemoveServerAllShards(id string, timeout time.Duration) error {
	for _, sh := range m.Shards() {
		if sh == nil || sh.Raft == nil {
			continue
		}
		if err := m.RemoveShardServer(sh.ID, id, timeout); err != nil {
			return fmt.Errorf("shard %d: %w", sh.ID, err)
		}
	}
	return nil
}

func (m *Manager) recordShardVoter(shardID uint32, id string, add bool) error {
	m.mu.Lock()
	for i := range m.placement.Shards {
		if m.placement.Shards[i].ID != shardID {
			continue
		}
		if add {
			m.placement.Shards[i].Voters = appendUnique(m.placement.Shards[i].Voters, id)
		} else {
			m.placement.Shards[i].Voters = removeString(m.placement.Shards[i].Voters, id)
			m.placement.Shards[i].NonVoters = removeString(m.placement.Shards[i].NonVoters, id)
		}
		m.placement.Shards[i].Epoch = m.placement.Epoch + 1
		m.placement.Epoch++
		m.placement.UpdatedAtUnix = time.Now().Unix()
		router, err := NewRouterFromCatalog(m.placement)
		if err != nil {
			m.mu.Unlock()
			return err
		}
		m.router = router
		if int(shardID) < len(m.shards) && m.shards[shardID] != nil {
			m.shards[shardID].Voters = append([]string(nil), m.placement.Shards[i].Voters...)
			m.shards[shardID].NonVoters = append([]string(nil), m.placement.Shards[i].NonVoters...)
			m.shards[shardID].Epoch = m.placement.Shards[i].Epoch
		}
		path := m.placementPath
		snapshot := m.placement.Clone()
		m.mu.Unlock()
		return m.persistPlacementSnapshot(path, snapshot)
	}
	m.mu.Unlock()
	return fmt.Errorf("cluster: shard %d not in placement", shardID)
}

func (m *Manager) persistPlacementSnapshot(path string, snapshot PlacementCatalog) error {
	if err := SavePlacementFile(path, snapshot); err != nil {
		return err
	}
	raw, err := encodePlacement(snapshot)
	if err != nil {
		return err
	}
	shard0, ok := m.Shard(0)
	if !ok || shard0 == nil || shard0.Storage == nil {
		return nil
	}
	if err := shard0.Storage.Set(placementSystemKey, raw); err != nil && !errors.Is(err, storage.ErrNotLeader) {
		return err
	}
	return nil
}

func (m *Manager) persistPlacementSnapshotStrict(path string, snapshot PlacementCatalog) error {
	raw, err := encodePlacement(snapshot)
	if err != nil {
		return err
	}
	shard0, ok := m.Shard(0)
	if !ok || shard0 == nil || shard0.Storage == nil {
		return fmt.Errorf("cluster: shard 0 unavailable")
	}
	if err := shard0.Storage.Set(placementSystemKey, raw); err != nil {
		return err
	}
	return SavePlacementFile(path, snapshot)
}

func appendUnique(in []string, v string) []string {
	for _, existing := range in {
		if existing == v {
			return in
		}
	}
	return append(in, v)
}

func removeString(in []string, v string) []string {
	out := in[:0]
	for _, existing := range in {
		if existing != v {
			out = append(out, existing)
		}
	}
	return out
}

// Close tears every shard down. Errors from individual shards are
// logged via cfg.LogOutput; the first error encountered is returned.
func (m *Manager) Close() error {
	var firstErr error
	m.mu.RLock()
	shards := append([]*Shard(nil), m.shards...)
	mux := m.mux
	m.mu.RUnlock()
	for _, s := range shards {
		if s.Raft != nil {
			if err := s.Raft.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if s.RaftStorage != nil {
			if err := s.RaftStorage.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if s.Storage != nil {
			if err := s.Storage.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	if mux != nil {
		_ = mux.Close()
	}
	return firstErr
}
