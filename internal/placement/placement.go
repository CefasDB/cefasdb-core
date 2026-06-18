package placement

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	PlacementVersion uint64 = 1

	PlacementStrategyTokenRange   = "token-range-v1"
	PlacementStrategyLegacyModulo = "legacy-modulo-v1"

	defaultPlacementFileName = "placement.json"
)

var (
	bigZero       = big.NewInt(0)
	bigTokenSpace = new(big.Int).Lsh(big.NewInt(1), 64)
)

// ShardState captures the routing lifecycle for a shard. The P0
// implementation routes only to states that can safely serve traffic;
// split/move/drain workflows can advance these states in later PRs.
type ShardState string

const (
	ShardStateCreating       ShardState = "creating"
	ShardStateActive         ShardState = "active"
	ShardStateSplitting      ShardState = "splitting"
	ShardStateMoving         ShardState = "moving"
	ShardStateDraining       ShardState = "draining"
	ShardStateReadOnly       ShardState = "read_only"
	ShardStateDecommissioned ShardState = "decommissioned"
)

func (s ShardState) Routable() bool {
	switch s {
	case "", ShardStateActive, ShardStateSplitting, ShardStateMoving, ShardStateReadOnly:
		return true
	default:
		return false
	}
}

// NodeState captures whether a physical node should receive new
// placement decisions.
type NodeState string

const (
	NodeStateActive         NodeState = "active"
	NodeStateDraining       NodeState = "draining"
	NodeStateDecommissioned NodeState = "decommissioned"
)

// TokenRange is a half-open uint64 hash-token range: [start, end).
// start == end represents the full ring, and start > end represents a
// range that wraps around the end of the token space.
type TokenRange struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

func (r TokenRange) Contains(token uint64) bool {
	if r.Start == r.End {
		return true
	}
	if r.Start < r.End {
		return token >= r.Start && token < r.End
	}
	return token >= r.Start || token < r.End
}

// NodeCapacity is advisory metadata used by placement decisions. Zero
// values are accepted; the placement policy treats missing weight as 1
// and missing resource dimensions as unknown capacity.
type NodeCapacity struct {
	Weight      int      `json:"weight,omitempty"`
	CPU         int      `json:"cpu,omitempty"`
	MemoryBytes uint64   `json:"memoryBytes,omitempty"`
	DiskBytes   uint64   `json:"diskBytes,omitempty"`
	Zone        string   `json:"zone,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

// NodeDescriptor is the placement catalog's view of one physical node.
type NodeDescriptor struct {
	ID           string       `json:"id"`
	RaftAddr     string       `json:"raftAddr,omitempty"`
	HTTPAddr     string       `json:"httpAddr,omitempty"`
	State        NodeState    `json:"state"`
	Capacity     NodeCapacity `json:"capacity,omitempty"`
	LastSeenUnix int64        `json:"lastSeenUnix,omitempty"`
}

// ShardPlacement describes one logical shard's token ownership and
// Raft membership intent.
type ShardPlacement struct {
	ID         uint32       `json:"id"`
	Ranges     []TokenRange `json:"ranges,omitempty"`
	State      ShardState   `json:"state"`
	Epoch      uint64       `json:"epoch"`
	Voters     []string     `json:"voters,omitempty"`
	NonVoters  []string     `json:"nonVoters,omitempty"`
	LeaderHint string       `json:"leaderHint,omitempty"`
}

// PlacementCatalog is the cluster-wide routing contract. Epoch changes
// are how callers detect stale routing metadata.
type PlacementCatalog struct {
	Version       uint64                    `json:"version"`
	Epoch         uint64                    `json:"epoch"`
	Strategy      string                    `json:"strategy"`
	Shards        []ShardPlacement          `json:"shards"`
	Nodes         map[string]NodeDescriptor `json:"nodes,omitempty"`
	UpdatedAtUnix int64                     `json:"updatedAtUnix,omitempty"`
}

func (c PlacementCatalog) Clone() PlacementCatalog {
	out := c
	out.Shards = append([]ShardPlacement(nil), c.Shards...)
	for i := range out.Shards {
		out.Shards[i].Ranges = append([]TokenRange(nil), c.Shards[i].Ranges...)
		out.Shards[i].Voters = append([]string(nil), c.Shards[i].Voters...)
		out.Shards[i].NonVoters = append([]string(nil), c.Shards[i].NonVoters...)
	}
	if c.Nodes != nil {
		out.Nodes = make(map[string]NodeDescriptor, len(c.Nodes))
		for id, node := range c.Nodes {
			node.Capacity.Tags = append([]string(nil), node.Capacity.Tags...)
			out.Nodes[id] = node
		}
	}
	return out
}

func (c *PlacementCatalog) Normalize() {
	if c.Version == 0 {
		c.Version = PlacementVersion
	}
	if c.Epoch == 0 {
		c.Epoch = 1
	}
	if c.Strategy == "" {
		c.Strategy = PlacementStrategyTokenRange
	}
	if c.UpdatedAtUnix == 0 {
		c.UpdatedAtUnix = time.Now().Unix()
	}
	for i := range c.Shards {
		if c.Shards[i].State == "" {
			c.Shards[i].State = ShardStateActive
		}
		if c.Shards[i].Epoch == 0 {
			c.Shards[i].Epoch = c.Epoch
		}
		sort.Strings(c.Shards[i].Voters)
		sort.Strings(c.Shards[i].NonVoters)
		if c.Shards[i].LeaderHint != "" && !containsString(c.Shards[i].Voters, c.Shards[i].LeaderHint) {
			c.Shards[i].LeaderHint = ""
		}
	}
	sort.Slice(c.Shards, func(i, j int) bool { return c.Shards[i].ID < c.Shards[j].ID })
	if c.Nodes != nil {
		for id, node := range c.Nodes {
			if node.State == "" {
				node.State = NodeStateActive
			}
			if node.Capacity.Weight == 0 {
				node.Capacity.Weight = 1
			}
			sort.Strings(node.Capacity.Tags)
			c.Nodes[id] = node
		}
	}
}

// DefaultPlacement builds the deterministic placement every node can
// derive from the same static peer configuration.
func DefaultPlacement(shards int, selfID string, peers, httpPeers map[string]string, capacity NodeCapacity, strategy string) PlacementCatalog {
	return defaultPlacement(shards, selfID, peers, httpPeers, capacity, strategy, 0)
}

// DefaultPlacementWithReplicationFactor builds a deterministic placement
// with at most replicationFactor voters per shard. A zero factor keeps
// the legacy behavior where every peer votes on every shard.
func DefaultPlacementWithReplicationFactor(shards int, selfID string, peers, httpPeers map[string]string, capacity NodeCapacity, strategy string, replicationFactor int) PlacementCatalog {
	return defaultPlacement(shards, selfID, peers, httpPeers, capacity, strategy, replicationFactor)
}

func defaultPlacement(shards int, selfID string, peers, httpPeers map[string]string, capacity NodeCapacity, strategy string, replicationFactor int) PlacementCatalog {
	if shards <= 0 {
		shards = 1
	}
	if strategy == "" {
		strategy = PlacementStrategyTokenRange
	}
	if capacity.Weight == 0 {
		capacity.Weight = 1
	}
	now := time.Now().Unix()
	nodes := make(map[string]NodeDescriptor)
	for _, id := range sortedNodeIDs(selfID, peers, httpPeers) {
		node := NodeDescriptor{
			ID:           id,
			RaftAddr:     peers[id],
			HTTPAddr:     httpPeers[id],
			State:        NodeStateActive,
			LastSeenUnix: now,
		}
		if id == selfID {
			node.Capacity = capacity
		} else {
			node.Capacity = NodeCapacity{Weight: 1}
		}
		nodes[id] = node
	}
	allVoters := sortedMapKeys(peers)
	if len(allVoters) == 0 && selfID != "" {
		allVoters = []string{selfID}
	}

	ranges := splitTokenRanges(shards)
	placements := make([]ShardPlacement, 0, shards)
	for i := 0; i < shards; i++ {
		voters := defaultShardVoters(allVoters, uint32(i), replicationFactor)
		if i == 0 {
			// Shard 0 currently carries the metadata catalog. Keep it
			// everywhere so every node can resolve table descriptors.
			voters = append([]string(nil), allVoters...)
		}
		leaderHint := leaderHintForShard(allVoters, uint32(i))
		if !containsString(voters, leaderHint) {
			leaderHint = leaderHintForShard(voters, uint32(i))
		}
		sh := ShardPlacement{
			ID:         uint32(i),
			State:      ShardStateActive,
			Epoch:      1,
			Voters:     append([]string(nil), voters...),
			LeaderHint: leaderHint,
		}
		if strategy == PlacementStrategyTokenRange {
			sh.Ranges = []TokenRange{ranges[i]}
		}
		placements = append(placements, sh)
	}

	cat := PlacementCatalog{
		Version:       PlacementVersion,
		Epoch:         1,
		Strategy:      strategy,
		Shards:        placements,
		Nodes:         nodes,
		UpdatedAtUnix: now,
	}
	cat.Normalize()
	return cat
}

func sortedNodeIDs(selfID string, maps ...map[string]string) []string {
	seen := make(map[string]struct{})
	if selfID != "" {
		seen[selfID] = struct{}{}
	}
	for _, m := range maps {
		for id := range m {
			seen[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func sortedMapKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func leaderHintForShard(voters []string, shardID uint32) string {
	voters = sortedUnique(voters)
	if len(voters) == 0 {
		return ""
	}
	return voters[int(shardID)%len(voters)]
}

// BackfillLeaderHints assigns deterministic per-shard leader targets to
// catalogs produced before LeaderHint was populated.
func BackfillLeaderHints(cat PlacementCatalog) PlacementCatalog {
	cat.Normalize()
	for i := range cat.Shards {
		if cat.Shards[i].LeaderHint == "" {
			cat.Shards[i].LeaderHint = leaderHintForShard(cat.Shards[i].Voters, cat.Shards[i].ID)
		}
	}
	return cat
}

func defaultShardVoters(voters []string, shardID uint32, replicationFactor int) []string {
	voters = sortedUnique(voters)
	if len(voters) == 0 {
		return nil
	}
	if replicationFactor <= 0 || replicationFactor >= len(voters) {
		return append([]string(nil), voters...)
	}
	out := make([]string, 0, replicationFactor)
	start := int(shardID) % len(voters)
	for i := 0; i < replicationFactor; i++ {
		out = append(out, voters[(start+i)%len(voters)])
	}
	sort.Strings(out)
	return out
}

func normalizeLeaderHint(sh ShardPlacement) string {
	if sh.LeaderHint != "" && containsString(sh.Voters, sh.LeaderHint) {
		return sh.LeaderHint
	}
	return leaderHintForShard(sh.Voters, sh.ID)
}

func splitTokenRanges(n int) []TokenRange {
	if n <= 1 {
		return []TokenRange{{Start: 0, End: 0}}
	}
	total := new(big.Int).Set(bigTokenSpace)
	den := big.NewInt(int64(n))
	out := make([]TokenRange, 0, n)
	for i := 0; i < n; i++ {
		start := new(big.Int).Div(new(big.Int).Mul(total, big.NewInt(int64(i))), den)
		end := new(big.Int).Div(new(big.Int).Mul(total, big.NewInt(int64(i+1))), den)
		r := TokenRange{Start: start.Uint64()}
		if end.Cmp(total) == 0 {
			r.End = 0
		} else {
			r.End = end.Uint64()
		}
		out = append(out, r)
	}
	return out
}

// ValidatePlacement rejects catalogs that cannot drive deterministic
// routing on this node.
func ValidatePlacement(cat PlacementCatalog) error {
	cat.Normalize()
	if cat.Version != PlacementVersion {
		return fmt.Errorf("cluster: unsupported placement version %d", cat.Version)
	}
	if cat.Strategy != PlacementStrategyTokenRange && cat.Strategy != PlacementStrategyLegacyModulo {
		return fmt.Errorf("cluster: unsupported placement strategy %q", cat.Strategy)
	}
	if len(cat.Shards) == 0 {
		return errors.New("cluster: placement has no shards")
	}
	seen := make(map[uint32]struct{}, len(cat.Shards))
	for i, sh := range cat.Shards {
		if sh.ID != uint32(i) {
			return fmt.Errorf("cluster: shard IDs must be contiguous from 0: got %d at position %d", sh.ID, i)
		}
		if _, ok := seen[sh.ID]; ok {
			return fmt.Errorf("cluster: duplicate shard %d", sh.ID)
		}
		seen[sh.ID] = struct{}{}
		if sh.State == ShardStateDecommissioned {
			continue
		}
		if !sh.State.Routable() && sh.State != ShardStateDraining && sh.State != ShardStateCreating {
			return fmt.Errorf("cluster: invalid shard %d state %q", sh.ID, sh.State)
		}
		if cat.Strategy == PlacementStrategyTokenRange && sh.State.Routable() && len(sh.Ranges) == 0 {
			return fmt.Errorf("cluster: token-range shard %d has no ranges", sh.ID)
		}
	}
	if cat.Strategy == PlacementStrategyTokenRange {
		return validateTokenCoverage(cat.Shards)
	}
	return nil
}

func validateTokenCoverage(shards []ShardPlacement) error {
	var segs []tokenSegment
	for _, sh := range shards {
		if !sh.State.Routable() {
			continue
		}
		for _, r := range sh.Ranges {
			segs = append(segs, tokenRangeSegments(r)...)
		}
	}
	if len(segs) == 0 {
		return errors.New("cluster: token-range placement has no routable ranges")
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].start.Cmp(segs[j].start) < 0 })
	if segs[0].start.Cmp(bigZero) != 0 {
		return fmt.Errorf("cluster: token ranges start at %s, want 0", segs[0].start.String())
	}
	prevEnd := new(big.Int).Set(segs[0].end)
	for i := 1; i < len(segs); i++ {
		if segs[i].start.Cmp(prevEnd) != 0 {
			return fmt.Errorf("cluster: token range gap/overlap between %s and %s", prevEnd.String(), segs[i].start.String())
		}
		prevEnd.Set(segs[i].end)
	}
	if prevEnd.Cmp(bigTokenSpace) != 0 {
		return fmt.Errorf("cluster: token ranges end at %s, want %s", prevEnd.String(), bigTokenSpace.String())
	}
	return nil
}

type tokenSegment struct {
	start *big.Int
	end   *big.Int
}

func tokenRangeSegments(r TokenRange) []tokenSegment {
	start := new(big.Int).SetUint64(r.Start)
	end := new(big.Int).SetUint64(r.End)
	if r.Start == r.End {
		return []tokenSegment{{start: new(big.Int).Set(bigZero), end: new(big.Int).Set(bigTokenSpace)}}
	}
	if r.Start < r.End {
		return []tokenSegment{{start: start, end: end}}
	}
	if r.End == 0 {
		return []tokenSegment{{start: start, end: new(big.Int).Set(bigTokenSpace)}}
	}
	return []tokenSegment{
		{start: start, end: new(big.Int).Set(bigTokenSpace)},
		{start: new(big.Int).Set(bigZero), end: end},
	}
}

func PlacementFilePath(root, explicit string) string {
	if explicit != "" {
		return explicit
	}
	return filepath.Join(root, defaultPlacementFileName)
}

func LoadPlacementFile(path string) (PlacementCatalog, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return PlacementCatalog{}, err
	}
	var cat PlacementCatalog
	if err := json.Unmarshal(b, &cat); err != nil {
		return PlacementCatalog{}, fmt.Errorf("cluster: decode placement: %w", err)
	}
	cat.Normalize()
	if err := ValidatePlacement(cat); err != nil {
		return PlacementCatalog{}, err
	}
	return cat, nil
}

func SavePlacementFile(path string, cat PlacementCatalog) error {
	cat.Normalize()
	if err := ValidatePlacement(cat); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("cluster: mkdir placement dir: %w", err)
	}
	b, err := json.MarshalIndent(cat, "", "  ")
	if err != nil {
		return fmt.Errorf("cluster: encode placement: %w", err)
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}

func ParsePlacement(raw []byte) (PlacementCatalog, error) {
	var cat PlacementCatalog
	if err := json.Unmarshal(raw, &cat); err != nil {
		return PlacementCatalog{}, fmt.Errorf("cluster: decode placement: %w", err)
	}
	cat.Normalize()
	if err := ValidatePlacement(cat); err != nil {
		return PlacementCatalog{}, err
	}
	return cat, nil
}

func EncodePlacement(cat PlacementCatalog) ([]byte, error) {
	cat.Normalize()
	if err := ValidatePlacement(cat); err != nil {
		return nil, err
	}
	return json.Marshal(cat)
}
