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

	"github.com/CefasDb/cefasdb/internal/placement"
	craft "github.com/CefasDb/cefasdb/internal/replication"
	"github.com/CefasDb/cefasdb/internal/routing"
	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
)

// ShardConfig configures a single shard's storage + raft. Most fields
// derive from the cluster-wide Manager config; the manager builds one
// of these per shard at start-up.
type ShardConfig struct {
	ShardID uint32

	StoragePath     string
	FsyncOnCommit   bool
	StorageProfile  string
	StorageTuning   pebble.PebbleTuning
	Backpressure    pebble.BackpressureOptions
	StreamRetention pebble.StreamRetentionOptions

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

// Shard is the per-shard handle owned by Manager: one pebble.DB
// (with raft attached) per shard.
//
// IsLocalVoter / IsLocalNonVoter snapshot the membership decision made
// when the shard is opened: do Voters / NonVoters contain this node's
// SelfID? hasLocalReplica reads these instead of doing a linear
// string-comparison scan on every scatter-read.
type Shard struct {
	ID              uint32
	State           placement.ShardState
	Epoch           uint64
	Ranges          []placement.TokenRange
	Voters          []string
	NonVoters       []string
	LeaderHint      string
	IsLocalVoter    bool
	IsLocalNonVoter bool
	Storage         *pebble.DB
	RaftStorage     *pebble.DB
	Raft            *craft.DB
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

	// ReplicationFactor limits fresh data-shard placement to this many
	// voters. Zero keeps the legacy every-peer voter set.
	ReplicationFactor int

	// PeerHTTPAddrs is the per-peer HTTP URL used to populate
	// raft.LeaderHTTPAddr() for 307 redirects.
	PeerHTTPAddrs map[string]string

	// NodeCapacity is advisory metadata recorded in the placement
	// catalog for this node. Zero values default to weight=1.
	NodeCapacity placement.NodeCapacity

	// Bootstrap is true for the first node started in a fresh
	// cluster. The manager only honours bootstrap for shards whose
	// state directories are empty (mirrors hraft's
	// HasExistingState contract).
	Bootstrap bool

	FsyncOnCommit       bool
	StorageProfile      string
	StorageTuning       pebble.PebbleTuning
	Backpressure        pebble.BackpressureOptions
	StorageLanes        pebble.LaneOptions
	StreamRetention     pebble.StreamRetentionOptions
	ChangeLogMode       string
	AdaptiveStorageMode bool
	RaftProfile         string
	RaftTuning          pebble.PebbleTuning

	HeartbeatMS                   int
	ElectionMS                    int
	LeaderLeaseMS                 int
	CommitMS                      int
	ApplyTimeout                  time.Duration
	SnapshotEntries               uint64
	LogCompression                string
	LogCompressionMinBytes        int
	LogCompressionMinSavingsRatio float64
	LogCompressionSkipCooldown    time.Duration

	LogOutput io.Writer
}

// Manager owns every shard's storage + raft on a single node, plus
// the shared MuxAcceptor when multi-Raft mode is enabled.
type Manager struct {
	mu            sync.RWMutex
	splitMu       sync.RWMutex
	cfg           Config
	router        *routing.Router
	cat           placement.PlacementCatalog
	placementPath string
	mux           *craft.MuxAcceptor
	shards        []*Shard
}

const (
	multiShardBlockCacheBudget = int64(2 << 30)
	multiShardMinBlockCache    = int64(64 << 20)
	multiShardMaxBlockCache    = int64(256 << 20)
	multiShardMemTableSize     = uint64(32 << 20)
)

// WriteTargets is the routing decision for a mutating request. Primary
// is the shard that currently owns the key; Mirrors are transition
// targets that must receive the same write before cutover can complete.
type WriteTargets struct {
	Primary *Shard
	Mirrors []*Shard

	release func()
}

// Release drops the split write fence held while the targets were
// selected. Callers must keep the fence until the write has landed on
// primary and mirrors.
func (t WriteTargets) Release() {
	if t.release != nil {
		t.release()
	}
}

var placementSystemKey = []byte(storage.Namespace + "cluster/placement")

// ErrNoLocalReplica means the request routes to a shard that this node
// does not host as a voter or non-voter replica.
var ErrNoLocalReplica = errors.New("cluster: no local replica for routed shard")

type NoLocalReplicaError struct {
	NodeID    string
	ShardID   uint32
	Voters    []string
	NonVoters []string
}

func (e *NoLocalReplicaError) Error() string {
	return fmt.Sprintf("cluster: node %s has no local replica for shard %d (voters=%v nonVoters=%v)", e.NodeID, e.ShardID, e.Voters, e.NonVoters)
}

func (e *NoLocalReplicaError) Is(target error) bool { return target == ErrNoLocalReplica }

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

	placementPath := placement.PlacementFilePath(cfg.Root, cfg.PlacementPath)
	cat, err := loadOrCreatePlacement(cfg, placementPath)
	if err != nil {
		return nil, err
	}
	router, err := routing.NewRouterFromCatalog(cat)
	if err != nil {
		return nil, err
	}
	cfg.Shards = router.Count()

	mgr := &Manager{
		cfg:           cfg,
		router:        router,
		cat:           cat,
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
	mgr.startLeaderHintReconciliation()
	return mgr, nil
}

func loadOrCreatePlacement(cfg Config, path string) (placement.PlacementCatalog, error) {
	cat, err := placement.LoadPlacementFile(path)
	if err == nil {
		return placement.BackfillLeaderHints(cat), nil
	}
	if !os.IsNotExist(err) {
		return placement.PlacementCatalog{}, err
	}
	strategy := placement.PlacementStrategyTokenRange
	if hasExistingShardState(cfg.Root) {
		strategy = placement.PlacementStrategyLegacyModulo
	}
	cat = placement.DefaultPlacementWithReplicationFactor(cfg.Shards, cfg.SelfID, cfg.Peers, cfg.PeerHTTPAddrs, cfg.NodeCapacity, strategy, cfg.ReplicationFactor)
	if err := placement.SavePlacementFile(path, cat); err != nil {
		return placement.PlacementCatalog{}, err
	}
	return placement.BackfillLeaderHints(cat), nil
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

func (m *Manager) openShardWithPlacement(ctx context.Context, shardID uint32, meta placement.ShardPlacement) (*Shard, error) {
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

	st, err := pebble.Open(pebble.Options{
		Path:            stateDir,
		FsyncOnCommit:   m.cfg.FsyncOnCommit,
		Profile:         m.cfg.StorageProfile,
		Tuning:          storageTuningForShards(m.cfg.Shards, m.cfg.StorageTuning),
		Backpressure:    m.cfg.Backpressure,
		Lanes:           pebble.DataLaneOptions(m.cfg.StorageLanes),
		StreamRetention: m.cfg.StreamRetention,
		ChangeLogMode:   m.cfg.ChangeLogMode,
		AdaptiveMode:    m.cfg.AdaptiveStorageMode,
	})
	if err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}

	if m.mux == nil && len(m.cfg.Peers) == 0 {
		// Single-node mode (no raft). The pebble.DB stands alone.
		return &Shard{
			ID:              shardID,
			State:           meta.State,
			Epoch:           meta.Epoch,
			Ranges:          append([]placement.TokenRange(nil), meta.Ranges...),
			Voters:          append([]string(nil), meta.Voters...),
			NonVoters:       append([]string(nil), meta.NonVoters...),
			LeaderHint:      meta.LeaderHint,
			IsLocalVoter:    containsString(meta.Voters, m.cfg.SelfID),
			IsLocalNonVoter: containsString(meta.NonVoters, m.cfg.SelfID),
			Storage:         st,
		}, nil
	}

	raftProfile := m.cfg.RaftProfile
	if raftProfile == "" {
		raftProfile = pebble.ProfileRaft
	}
	raftStore, err := pebble.Open(pebble.Options{
		Path:          raftStoreDir,
		FsyncOnCommit: m.cfg.FsyncOnCommit,
		Profile:       raftProfile,
		Tuning:        m.cfg.RaftTuning,
	})
	if err != nil {
		_ = st.Close()
		return nil, fmt.Errorf("raft storage: %w", err)
	}

	// Build the per-shard raft config. PeerAddrs are filtered to this
	// shard's placement voters so replication-factor changes the actual
	// Raft quorum, not only the catalog metadata.
	rcfg := craft.Config{
		Path:                          raftDir,
		SelfID:                        m.cfg.SelfID,
		BindAddr:                      m.cfg.MuxAddr,
		Bootstrap:                     m.cfg.Bootstrap,
		PeerAddrs:                     peersForVoters(m.cfg.Peers, meta.Voters),
		PeerHTTPAddrs:                 m.cfg.PeerHTTPAddrs,
		HeartbeatMS:                   m.cfg.HeartbeatMS,
		ElectionMS:                    m.cfg.ElectionMS,
		LeaderLeaseMS:                 m.cfg.LeaderLeaseMS,
		CommitMS:                      m.cfg.CommitMS,
		ApplyTimeout:                  m.cfg.ApplyTimeout,
		CommittedApplier:              st,
		SnapshotEntries:               m.cfg.SnapshotEntries,
		LogCompression:                m.cfg.LogCompression,
		LogCompressionMinBytes:        m.cfg.LogCompressionMinBytes,
		LogCompressionMinSavingsRatio: m.cfg.LogCompressionMinSavingsRatio,
		LogCompressionSkipCooldown:    m.cfg.LogCompressionSkipCooldown,
		LogOutput:                     m.cfg.LogOutput,
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
		ID:              shardID,
		State:           meta.State,
		Epoch:           meta.Epoch,
		Ranges:          append([]placement.TokenRange(nil), meta.Ranges...),
		Voters:          append([]string(nil), meta.Voters...),
		NonVoters:       append([]string(nil), meta.NonVoters...),
		LeaderHint:      meta.LeaderHint,
		IsLocalVoter:    containsString(meta.Voters, m.cfg.SelfID),
		IsLocalNonVoter: containsString(meta.NonVoters, m.cfg.SelfID),
		Storage:         st,
		RaftStorage:     raftStore,
		Raft:            rdb,
	}, nil
}

func (m *Manager) shardPlacement(shardID uint32) (placement.ShardPlacement, bool) {
	for _, sh := range m.cat.Shards {
		if sh.ID == shardID {
			return sh, true
		}
	}
	return placement.ShardPlacement{ID: shardID, State: placement.ShardStateActive, Epoch: m.cat.Epoch}, false
}

func storageTuningForShards(shards int, tuning pebble.PebbleTuning) pebble.PebbleTuning {
	if shards <= 1 {
		return tuning
	}
	if tuning.BlockCacheSizeBytes <= 0 {
		perShard := multiShardBlockCacheBudget / int64(shards)
		if perShard < multiShardMinBlockCache {
			perShard = multiShardMinBlockCache
		}
		if perShard > multiShardMaxBlockCache {
			perShard = multiShardMaxBlockCache
		}
		tuning.BlockCacheSizeBytes = perShard
	}
	if tuning.MemTableSizeBytes == 0 {
		tuning.MemTableSizeBytes = multiShardMemTableSize
	}
	if tuning.MemTableStopWrites == 0 {
		tuning.MemTableStopWrites = 4
	}
	if tuning.MaxConcurrentCompactions == 0 {
		tuning.MaxConcurrentCompactions = 2
	}
	if tuning.L0CompactionConcurrency == 0 {
		tuning.L0CompactionConcurrency = 2
	}
	if tuning.L0CompactionFileThreshold == 0 {
		tuning.L0CompactionFileThreshold = 64
	}
	if tuning.L0StopWritesThreshold == 0 {
		tuning.L0StopWritesThreshold = 128
	}
	return tuning
}

func peersForVoters(peers map[string]string, voters []string) map[string]string {
	if len(peers) == 0 || len(voters) == 0 {
		return peers
	}
	out := make(map[string]string, len(voters))
	for _, id := range voters {
		if addr, ok := peers[id]; ok {
			out[id] = addr
		}
	}
	if len(out) == 0 {
		return peers
	}
	return out
}

func (m *Manager) openMissingShardsForPlacement(ctx context.Context, cat placement.PlacementCatalog) error {
	cat.Normalize()
	if err := placement.ValidatePlacement(cat); err != nil {
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
func (m *Manager) Router() *routing.Router {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.router
}

func (m *Manager) Placement() placement.PlacementCatalog {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cat.Clone()
}

func (m *Manager) RoutingEpoch() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cat.Epoch
}

func (m *Manager) PlacementVersion() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cat.Version
}

func (m *Manager) PlacementStrategy() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cat.Strategy
}

func (m *Manager) Nodes() []placement.NodeDescriptor {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]placement.NodeDescriptor, 0, len(m.cat.Nodes))
	for _, node := range m.cat.Nodes {
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

// ReadShardForPK selects the local readable replica for a request's PK.
// It refuses to return an empty local shard when this node is not part
// of the shard placement; callers must retry against one of the shard's
// voters instead of treating the local miss as authoritative.
func (m *Manager) ReadShardForPK(pkBytes []byte, epoch uint64) (*Shard, error) {
	sh, err := m.RouteForPK(pkBytes, epoch)
	if err != nil {
		return nil, err
	}
	if !m.hasLocalReplica(sh) {
		return nil, &NoLocalReplicaError{
			NodeID:    m.cfg.SelfID,
			ShardID:   sh.ID,
			Voters:    append([]string(nil), sh.Voters...),
			NonVoters: append([]string(nil), sh.NonVoters...),
		}
	}
	return sh, nil
}

// ReadShards returns all local shard stores needed for a complete
// scatter read. If any logical shard is not replicated locally, a
// single-node scatter would return partial results, so the call fails.
func (m *Manager) ReadShards(epoch uint64) ([]*Shard, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := checkRoutingEpoch(epoch, m.cat.Epoch); err != nil {
		return nil, err
	}
	out := make([]*Shard, 0, len(m.shards))
	for _, sh := range m.shards {
		if sh == nil {
			return nil, fmt.Errorf("cluster: routed to missing shard")
		}
		if !sh.State.Routable() {
			return nil, fmt.Errorf("cluster: routed to non-routable shard %d state=%s", sh.ID, sh.State)
		}
		if !m.hasLocalReplica(sh) {
			return nil, &NoLocalReplicaError{
				NodeID:    m.cfg.SelfID,
				ShardID:   sh.ID,
				Voters:    append([]string(nil), sh.Voters...),
				NonVoters: append([]string(nil), sh.NonVoters...),
			}
		}
		out = append(out, sh)
	}
	return out, nil
}

func (m *Manager) hasLocalReplica(sh *Shard) bool {
	if sh == nil {
		return false
	}
	if len(sh.Voters) == 0 && len(sh.NonVoters) == 0 {
		return true
	}
	return sh.IsLocalVoter || sh.IsLocalNonVoter
}

// RouteForPK selects the shard and verifies an optional caller routing
// epoch. epoch==0 means "caller has no cached route".
func (m *Manager) RouteForPK(pkBytes []byte, epoch uint64) (*Shard, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := checkRoutingEpoch(epoch, m.cat.Epoch); err != nil {
		return nil, err
	}
	id, err := m.router.ShardForPK(pkBytes)
	if err != nil {
		return nil, err
	}
	if int(id) >= len(m.shards) {
		return nil, fmt.Errorf("cluster: routed to missing shard %d", id)
	}
	sh := m.shards[id]
	if sh == nil {
		return nil, fmt.Errorf("cluster: routed to missing shard %d", id)
	}
	if !sh.State.Routable() {
		return nil, fmt.Errorf("cluster: routed to non-routable shard %d state=%s", id, sh.State)
	}
	return sh, nil
}

// WriteTargetsForPK selects the primary owner for a write and any
// transition mirrors that must receive the same mutation while a
// source shard is splitting or moving.
func (m *Manager) WriteTargetsForPK(pkBytes []byte, epoch uint64) (WriteTargets, error) {
	m.splitMu.RLock()
	targets, err := m.writeTargetsForPKLocked(pkBytes, epoch)
	if err != nil {
		m.splitMu.RUnlock()
		return WriteTargets{}, err
	}
	targets.release = m.splitMu.RUnlock
	return targets, nil
}

func (m *Manager) writeTargetsForPKLocked(pkBytes []byte, epoch uint64) (WriteTargets, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if err := checkRoutingEpoch(epoch, m.cat.Epoch); err != nil {
		return WriteTargets{}, err
	}
	token := m.router.TokenForPK(pkBytes)
	id, err := m.router.ShardForUint64(token)
	if err != nil {
		return WriteTargets{}, err
	}
	if int(id) >= len(m.shards) {
		return WriteTargets{}, fmt.Errorf("cluster: routed to missing shard %d", id)
	}
	primary := m.shards[id]
	if primary == nil {
		return WriteTargets{}, fmt.Errorf("cluster: routed to missing shard %d", id)
	}
	if !primary.State.Routable() {
		return WriteTargets{}, fmt.Errorf("cluster: routed to non-routable shard %d state=%s", id, primary.State)
	}

	out := WriteTargets{Primary: primary}
	if (primary.State != placement.ShardStateSplitting && primary.State != placement.ShardStateMoving) || m.cat.Strategy != placement.PlacementStrategyTokenRange {
		return out, nil
	}
	seen := map[uint32]struct{}{primary.ID: {}}
	for _, meta := range m.cat.Shards {
		if meta.State != placement.ShardStateCreating {
			continue
		}
		for _, rng := range meta.Ranges {
			if !rng.Contains(token) {
				continue
			}
			if _, ok := seen[meta.ID]; ok {
				break
			}
			if int(meta.ID) >= len(m.shards) || m.shards[meta.ID] == nil {
				return WriteTargets{}, fmt.Errorf("cluster: transition target shard %d is not open locally", meta.ID)
			}
			out.Mirrors = append(out.Mirrors, m.shards[meta.ID])
			seen[meta.ID] = struct{}{}
			break
		}
	}
	return out, nil
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
	if err == pebble.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	cat, err := placement.ParsePlacement(raw)
	if err != nil {
		return err
	}
	m.mu.RLock()
	currentEpoch := m.cat.Epoch
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
	cat := m.cat.Clone()
	m.mu.RUnlock()
	raw, err := placement.EncodePlacement(cat)
	if err != nil {
		return err
	}
	shard0, ok := m.Shard(0)
	if !ok || shard0 == nil || shard0.Storage == nil {
		return fmt.Errorf("cluster: shard 0 unavailable")
	}
	return shard0.Storage.Set(placementSystemKey, raw)
}

func (m *Manager) applyPlacement(cat placement.PlacementCatalog, save bool) error {
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
	router, err := routing.NewRouterFromCatalog(cat)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.cat = cat.Clone()
	m.router = router
	for _, meta := range m.cat.Shards {
		if int(meta.ID) >= len(m.shards) || m.shards[meta.ID] == nil {
			continue
		}
		sh := m.shards[meta.ID]
		sh.State = meta.State
		sh.Epoch = meta.Epoch
		sh.Ranges = append([]placement.TokenRange(nil), meta.Ranges...)
		sh.Voters = append([]string(nil), meta.Voters...)
		sh.NonVoters = append([]string(nil), meta.NonVoters...)
		sh.LeaderHint = meta.LeaderHint
	}
	path := m.placementPath
	snapshot := m.cat.Clone()
	m.mu.Unlock()
	if save {
		return placement.SavePlacementFile(path, snapshot)
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
	for i := range m.cat.Shards {
		if m.cat.Shards[i].ID != shardID {
			continue
		}
		if add {
			m.cat.Shards[i].Voters = appendUnique(m.cat.Shards[i].Voters, id)
		} else {
			m.cat.Shards[i].Voters = placement.RemoveString(m.cat.Shards[i].Voters, id)
			m.cat.Shards[i].NonVoters = placement.RemoveString(m.cat.Shards[i].NonVoters, id)
		}
		m.cat.Shards[i].Epoch = m.cat.Epoch + 1
		m.cat.Epoch++
		m.cat.UpdatedAtUnix = time.Now().Unix()
		router, err := routing.NewRouterFromCatalog(m.cat)
		if err != nil {
			m.mu.Unlock()
			return err
		}
		m.router = router
		if int(shardID) < len(m.shards) && m.shards[shardID] != nil {
			m.shards[shardID].Voters = append([]string(nil), m.cat.Shards[i].Voters...)
			m.shards[shardID].NonVoters = append([]string(nil), m.cat.Shards[i].NonVoters...)
			m.shards[shardID].Epoch = m.cat.Shards[i].Epoch
		}
		path := m.placementPath
		snapshot := m.cat.Clone()
		m.mu.Unlock()
		return m.persistPlacementSnapshot(path, snapshot)
	}
	m.mu.Unlock()
	return fmt.Errorf("cluster: shard %d not in placement", shardID)
}

func (m *Manager) persistPlacementSnapshot(path string, snapshot placement.PlacementCatalog) error {
	if err := placement.SavePlacementFile(path, snapshot); err != nil {
		return err
	}
	raw, err := placement.EncodePlacement(snapshot)
	if err != nil {
		return err
	}
	shard0, ok := m.Shard(0)
	if !ok || shard0 == nil || shard0.Storage == nil {
		return nil
	}
	if err := shard0.Storage.Set(placementSystemKey, raw); err != nil && !errors.Is(err, pebble.ErrNotLeader) {
		return err
	}
	return nil
}

func (m *Manager) persistPlacementSnapshotStrict(path string, snapshot placement.PlacementCatalog) error {
	raw, err := placement.EncodePlacement(snapshot)
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
	return placement.SavePlacementFile(path, snapshot)
}

func appendUnique(in []string, v string) []string {
	for _, existing := range in {
		if existing == v {
			return in
		}
	}
	return append(in, v)
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
