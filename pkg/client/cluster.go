package client

import (
	"context"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// ---------- cluster ----------

// ClusterStatus returns membership and leadership info.
type ClusterStatus struct {
	Mode              string
	IsLeader          bool
	SelfID            string
	BindAddr          string
	LeaderHTTP        string
	RoutingEpoch      uint64
	PlacementVersion  uint64
	ShardCount        int
	PlacementStrategy string
	Shards            []ShardPlacement
	Nodes             []NodeDescriptor
	HotRanges         []RangeHotspotSummary
	BackupScheduler   *ScheduledBackupStatus
}

// ScheduledBackupStatus describes the cluster's automatic-backup
// scheduler: configuration, the last run's outcome, and the next
// scheduled invocation time.
type ScheduledBackupStatus struct {
	Enabled                bool
	DryRun                 bool
	IntervalSeconds        int64
	NameTemplate           string
	Tables                 []string
	RetentionKeepLatest    int
	RetentionKeepLatestSet bool
	RetentionMaxAgeSeconds int64
	RetentionMaxAgeSet     bool
	RetentionDryRun        bool
	Running                bool
	NextRunUnix            int64
	LastStartedUnix        int64
	LastFinishedUnix       int64
	LastDurationSeconds    float64
	LastStatus             string
	LastBackupName         string
	LastError              string
	LastRows               int64
	LastBytes              int64
	LastSuccessUnix        int64
	LastFailureUnix        int64
	LastRetention          *BackupRetentionResult
}

// TokenRange is a half-open [Start, End) interval of the 64-bit
// partitioning token space owned by a shard.
type TokenRange struct {
	Start uint64
	End   uint64
}

// ShardPlacement describes the token ownership and Raft membership
// for one shard.
type ShardPlacement struct {
	ID             uint32
	Ranges         []TokenRange
	State          string
	Epoch          uint64
	Voters         []string
	NonVoters      []string
	LeaderHint     string
	ActualLeader   string
	DesiredLeader  string
	LeaderMismatch bool
}

// RangeHotspotSummary is the per-bucket traffic and latency snapshot
// the server publishes for range-hotspot diagnostics.
type RangeHotspotSummary struct {
	ShardID             string
	Bucket              int
	BucketCount         int
	TokenStart          uint64
	TokenEnd            uint64
	Reads               uint64
	Writes              uint64
	Bytes               uint64
	AvgLatencySeconds   float64
	MaxLatencySeconds   float64
	CompactionDebtBytes uint64
	ThrottleState       int
	Status              string
	Reasons             []string
	WindowStartedUnix   int64
	LastSeenUnix        int64
	HotUntilUnix        int64
}

// NodeCapacity carries the placement hints a node advertises to the
// scheduler: relative weight, CPU, memory, disk, zone and free-form
// tags.
type NodeCapacity struct {
	Weight      int
	CPU         int
	MemoryBytes uint64
	DiskBytes   uint64
	Zone        string
	Tags        []string
}

// NodeDescriptor is the cluster's view of one peer, including its
// network addresses, state, advertised capacity and last-seen time.
type NodeDescriptor struct {
	ID           string
	RaftAddr     string
	HTTPAddr     string
	State        string
	Capacity     NodeCapacity
	LastSeenUnix int64
}

// MembershipOptions narrows AddVoterWithOptions / RemoveServerWithOptions
// to a specific shard or to every shard at once.
type MembershipOptions struct {
	ShardID   *uint32
	AllShards bool
}

// PlacementCatalog is the cluster-wide placement snapshot — shards,
// nodes, the placement epoch, and the strategy that produced it.
type PlacementCatalog struct {
	Version       uint64
	Epoch         uint64
	Strategy      string
	Shards        []ShardPlacement
	Nodes         []NodeDescriptor
	UpdatedAtUnix int64
}

// Status fetches the cluster status. Works without a token (public).
func (c *Client) Status(ctx context.Context) (ClusterStatus, error) {
	resp, err := c.stub.ClusterStatus(c.withAuth(ctx), &cefaspb.ClusterStatusRequest{})
	if err != nil {
		return ClusterStatus{}, err
	}
	return clusterStatusFromPB(resp), nil
}

func clusterStatusFromPB(resp *cefaspb.ClusterStatusResponse) ClusterStatus {
	if resp == nil {
		return ClusterStatus{}
	}
	return ClusterStatus{
		Mode:              resp.GetMode(),
		IsLeader:          resp.GetIsLeader(),
		SelfID:            resp.GetSelfId(),
		BindAddr:          resp.GetBindAddr(),
		LeaderHTTP:        resp.GetLeaderHttp(),
		RoutingEpoch:      resp.GetRoutingEpoch(),
		PlacementVersion:  resp.GetPlacementVersion(),
		ShardCount:        int(resp.GetShardCount()),
		PlacementStrategy: resp.GetPlacementStrategy(),
		Shards:            shardPlacementsFromPB(resp.GetShards()),
		Nodes:             nodeDescriptorsFromPB(resp.GetNodes()),
		HotRanges:         rangeHotspotsFromPB(resp.GetHotRanges()),
		BackupScheduler:   scheduledBackupStatusFromPB(resp.GetBackupScheduler()),
	}
}

func scheduledBackupStatusFromPB(in *cefaspb.ScheduledBackupStatus) *ScheduledBackupStatus {
	if in == nil {
		return nil
	}
	var retention *BackupRetentionResult
	if in.GetLastRetention() != nil {
		cp := backupRetentionFromPB(in.GetLastRetention())
		retention = &cp
	}
	return &ScheduledBackupStatus{
		Enabled:                in.GetEnabled(),
		DryRun:                 in.GetDryRun(),
		IntervalSeconds:        in.GetIntervalSeconds(),
		NameTemplate:           in.GetNameTemplate(),
		Tables:                 append([]string(nil), in.GetTables()...),
		RetentionKeepLatest:    int(in.GetRetentionKeepLatest()),
		RetentionKeepLatestSet: in.GetRetentionKeepLatestSet(),
		RetentionMaxAgeSeconds: in.GetRetentionMaxAgeSeconds(),
		RetentionMaxAgeSet:     in.GetRetentionMaxAgeSet(),
		RetentionDryRun:        in.GetRetentionDryRun(),
		Running:                in.GetRunning(),
		NextRunUnix:            in.GetNextRunUnix(),
		LastStartedUnix:        in.GetLastStartedUnix(),
		LastFinishedUnix:       in.GetLastFinishedUnix(),
		LastDurationSeconds:    in.GetLastDurationSeconds(),
		LastStatus:             in.GetLastStatus(),
		LastBackupName:         in.GetLastBackupName(),
		LastError:              in.GetLastError(),
		LastRows:               in.GetLastRows(),
		LastBytes:              in.GetLastBytes(),
		LastSuccessUnix:        in.GetLastSuccessUnix(),
		LastFailureUnix:        in.GetLastFailureUnix(),
		LastRetention:          retention,
	}
}

func shardPlacementsFromPB(in []*cefaspb.ShardPlacement) []ShardPlacement {
	out := make([]ShardPlacement, 0, len(in))
	for _, sh := range in {
		out = append(out, ShardPlacement{
			ID:             sh.GetId(),
			Ranges:         tokenRangesFromPB(sh.GetRanges()),
			State:          sh.GetState(),
			Epoch:          sh.GetEpoch(),
			Voters:         append([]string(nil), sh.GetVoters()...),
			NonVoters:      append([]string(nil), sh.GetNonVoters()...),
			LeaderHint:     sh.GetLeaderHint(),
			ActualLeader:   sh.GetActualLeader(),
			DesiredLeader:  sh.GetDesiredLeader(),
			LeaderMismatch: sh.GetLeaderMismatch(),
		})
	}
	return out
}

func rangeHotspotsFromPB(in []*cefaspb.RangeHotspotSummary) []RangeHotspotSummary {
	out := make([]RangeHotspotSummary, 0, len(in))
	for _, hs := range in {
		out = append(out, RangeHotspotSummary{
			ShardID:             hs.GetShardId(),
			Bucket:              int(hs.GetBucket()),
			BucketCount:         int(hs.GetBucketCount()),
			TokenStart:          hs.GetTokenStart(),
			TokenEnd:            hs.GetTokenEnd(),
			Reads:               hs.GetReads(),
			Writes:              hs.GetWrites(),
			Bytes:               hs.GetBytes(),
			AvgLatencySeconds:   hs.GetAvgLatencySeconds(),
			MaxLatencySeconds:   hs.GetMaxLatencySeconds(),
			CompactionDebtBytes: hs.GetCompactionDebtBytes(),
			ThrottleState:       int(hs.GetThrottleState()),
			Status:              hs.GetStatus(),
			Reasons:             append([]string(nil), hs.GetReasons()...),
			WindowStartedUnix:   hs.GetWindowStartedUnix(),
			LastSeenUnix:        hs.GetLastSeenUnix(),
			HotUntilUnix:        hs.GetHotUntilUnix(),
		})
	}
	return out
}

func tokenRangesFromPB(in []*cefaspb.TokenRange) []TokenRange {
	out := make([]TokenRange, 0, len(in))
	for _, r := range in {
		out = append(out, TokenRange{Start: r.GetStart(), End: r.GetEnd()})
	}
	return out
}

func nodeDescriptorsFromPB(in []*cefaspb.NodeDescriptor) []NodeDescriptor {
	out := make([]NodeDescriptor, 0, len(in))
	for _, node := range in {
		capacity := NodeCapacity{}
		if c := node.GetCapacity(); c != nil {
			capacity = NodeCapacity{
				Weight:      int(c.GetWeight()),
				CPU:         int(c.GetCpu()),
				MemoryBytes: c.GetMemoryBytes(),
				DiskBytes:   c.GetDiskBytes(),
				Zone:        c.GetZone(),
				Tags:        append([]string(nil), c.GetTags()...),
			}
		}
		out = append(out, NodeDescriptor{
			ID:           node.GetId(),
			RaftAddr:     node.GetRaftAddr(),
			HTTPAddr:     node.GetHttpAddr(),
			State:        node.GetState(),
			Capacity:     capacity,
			LastSeenUnix: node.GetLastSeenUnix(),
		})
	}
	return out
}

// AddVoter asks the leader to add `id` at `addr` to the cluster.
// Requires cefas:cluster:admin scope.
func (c *Client) AddVoter(ctx context.Context, id, addr string) error {
	return c.AddVoterWithOptions(ctx, id, addr, MembershipOptions{})
}

// AddVoterWithOptions is the per-shard form of AddVoter; opts targets
// either a specific ShardID or every shard.
func (c *Client) AddVoterWithOptions(ctx context.Context, id, addr string, opts MembershipOptions) error {
	req := &cefaspb.AddVoterRequest{Id: id, Addr: addr, AllShards: opts.AllShards}
	if opts.ShardID != nil {
		req.ShardId = opts.ShardID
	}
	_, err := c.stub.AddVoter(c.withAuth(ctx), req)
	return err
}

// RemoveServer evicts a peer from the cluster. Requires
// cefas:cluster:admin scope.
func (c *Client) RemoveServer(ctx context.Context, id string) error {
	return c.RemoveServerWithOptions(ctx, id, MembershipOptions{})
}

// RemoveServerWithOptions is the per-shard form of RemoveServer; opts
// targets either a specific ShardID or every shard.
func (c *Client) RemoveServerWithOptions(ctx context.Context, id string, opts MembershipOptions) error {
	req := &cefaspb.RemoveServerRequest{Id: id, AllShards: opts.AllShards}
	if opts.ShardID != nil {
		req.ShardId = opts.ShardID
	}
	_, err := c.stub.RemoveServer(c.withAuth(ctx), req)
	return err
}

func placementCatalogToPB(in PlacementCatalog) *cefaspb.PlacementCatalog {
	return &cefaspb.PlacementCatalog{
		Version:       in.Version,
		Epoch:         in.Epoch,
		Strategy:      in.Strategy,
		Shards:        shardPlacementsToPB(in.Shards),
		Nodes:         nodeDescriptorsToPB(in.Nodes),
		UpdatedAtUnix: in.UpdatedAtUnix,
	}
}

func shardPlacementsToPB(in []ShardPlacement) []*cefaspb.ShardPlacement {
	out := make([]*cefaspb.ShardPlacement, 0, len(in))
	for _, sh := range in {
		out = append(out, &cefaspb.ShardPlacement{
			Id:             sh.ID,
			Ranges:         tokenRangesToPB(sh.Ranges),
			State:          sh.State,
			Epoch:          sh.Epoch,
			Voters:         append([]string(nil), sh.Voters...),
			NonVoters:      append([]string(nil), sh.NonVoters...),
			LeaderHint:     sh.LeaderHint,
			ActualLeader:   sh.ActualLeader,
			DesiredLeader:  sh.DesiredLeader,
			LeaderMismatch: sh.LeaderMismatch,
		})
	}
	return out
}

func tokenRangesToPB(in []TokenRange) []*cefaspb.TokenRange {
	out := make([]*cefaspb.TokenRange, 0, len(in))
	for _, r := range in {
		out = append(out, tokenRangeToPB(r))
	}
	return out
}

func tokenRangeToPB(r TokenRange) *cefaspb.TokenRange {
	return &cefaspb.TokenRange{Start: r.Start, End: r.End}
}

func nodeDescriptorsToPB(in []NodeDescriptor) []*cefaspb.NodeDescriptor {
	out := make([]*cefaspb.NodeDescriptor, 0, len(in))
	for _, node := range in {
		out = append(out, &cefaspb.NodeDescriptor{
			Id:       node.ID,
			RaftAddr: node.RaftAddr,
			HttpAddr: node.HTTPAddr,
			State:    node.State,
			Capacity: &cefaspb.NodeCapacity{
				Weight:      int32(node.Capacity.Weight),
				Cpu:         int32(node.Capacity.CPU),
				MemoryBytes: node.Capacity.MemoryBytes,
				DiskBytes:   node.Capacity.DiskBytes,
				Zone:        node.Capacity.Zone,
				Tags:        append([]string(nil), node.Capacity.Tags...),
			},
			LastSeenUnix: node.LastSeenUnix,
		})
	}
	return out
}

func placementCatalogFromPB(in *cefaspb.PlacementCatalog) PlacementCatalog {
	if in == nil {
		return PlacementCatalog{}
	}
	return PlacementCatalog{
		Version:       in.GetVersion(),
		Epoch:         in.GetEpoch(),
		Strategy:      in.GetStrategy(),
		Shards:        shardPlacementsFromPB(in.GetShards()),
		Nodes:         nodeDescriptorsFromPB(in.GetNodes()),
		UpdatedAtUnix: in.GetUpdatedAtUnix(),
	}
}

func tokenRangeFromPB(in *cefaspb.TokenRange) TokenRange {
	if in == nil {
		return TokenRange{}
	}
	return TokenRange{Start: in.GetStart(), End: in.GetEnd()}
}
