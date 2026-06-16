// Package client is the typed Go SDK for cefas. It wraps the
// generated gRPC stubs with helpers that consume the public
// pkg/types.Item / AttributeValue model so application code never
// touches generated protobuf structs directly.
//
// Usage:
//
//	c, err := client.Dial(ctx, "localhost:9090", client.WithBearer("..."))
//	defer c.Close()
//
//	err = c.PutItem(ctx, "events", types.Item{
//	    "user_id": types.AttributeValue{T: types.AttrS, S: "alice"},
//	    "ts":      types.AttributeValue{T: types.AttrN, N: "100"},
//	})
//
//	items, err := c.Query(ctx, "events").
//	    PK(types.AttributeValue{T: types.AttrS, S: "alice"}).
//	    Limit(50).
//	    Run(ctx)
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
)

// Client is a typed cefas gRPC client. Safe for concurrent use.
type Client struct {
	conn   *grpc.ClientConn
	stub   cefaspb.CefasClient
	bearer string
}

// Option configures a Client at Dial time.
type Option func(*config)

type config struct {
	bearer    string
	tls       *tls.Config
	plaintext bool
	dialOpts  []grpc.DialOption
}

// WithBearer adds an "Authorization: Bearer <token>" metadata header
// to every RPC.
func WithBearer(token string) Option {
	return func(c *config) { c.bearer = token }
}

// WithTLS enables transport encryption using the supplied tls.Config.
// Pass &tls.Config{} for the system roots, or build a custom config
// for mTLS.
func WithTLS(cfg *tls.Config) Option { return func(c *config) { c.tls = cfg } }

// WithPlaintext disables transport security. Required for local dev
// against a -grpc-reflection enabled server with no TLS cert.
func WithPlaintext() Option { return func(c *config) { c.plaintext = true } }

// WithMTLSFiles wires mTLS from filesystem paths: client cert + key
// the server verifies, plus the CA bundle that signed the server's
// certificate.
func WithMTLSFiles(certPath, keyPath, serverCAPath string) Option {
	return func(c *config) {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			c.dialOpts = append(c.dialOpts, grpc.WithDisableHealthCheck()) // no-op marker
			c.tls = &tls.Config{InsecureSkipVerify: false}
			fmt.Fprintf(os.Stderr, "cefas/client: load client cert: %v\n", err)
			return
		}
		pool := x509.NewCertPool()
		if pem, err := os.ReadFile(serverCAPath); err == nil {
			pool.AppendCertsFromPEM(pem)
		}
		c.tls = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
		}
	}
}

// WithDialOption appends a raw grpc.DialOption (escape hatch for
// keepalive, retry policies, etc.).
func WithDialOption(o grpc.DialOption) Option {
	return func(c *config) { c.dialOpts = append(c.dialOpts, o) }
}

// Dial opens a connection to a cefas server.
func Dial(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}
	dialOpts := append([]grpc.DialOption{}, cfg.dialOpts...)
	switch {
	case cfg.tls != nil:
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(cfg.tls)))
	case cfg.plaintext:
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	default:
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}

	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("cefas: dial %s: %w", addr, err)
	}
	return &Client{conn: conn, stub: cefaspb.NewCefasClient(conn), bearer: cfg.bearer}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error { return c.conn.Close() }

// withAuth augments outgoing metadata with the bearer token (when set).
func (c *Client) withAuth(ctx context.Context) context.Context {
	if c.bearer == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.bearer)
}

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

type TokenRange struct {
	Start uint64
	End   uint64
}

type ShardPlacement struct {
	ID         uint32
	Ranges     []TokenRange
	State      string
	Epoch      uint64
	Voters     []string
	NonVoters  []string
	LeaderHint string
}

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

type NodeCapacity struct {
	Weight      int
	CPU         int
	MemoryBytes uint64
	DiskBytes   uint64
	Zone        string
	Tags        []string
}

type NodeDescriptor struct {
	ID           string
	RaftAddr     string
	HTTPAddr     string
	State        string
	Capacity     NodeCapacity
	LastSeenUnix int64
}

type MembershipOptions struct {
	ShardID   *uint32
	AllShards bool
}

type PlacementPlanRequest struct {
	Operation     string
	ShardID       uint32
	SplitToken    *uint64
	NewShardID    *uint32
	TargetShardID *uint32
	RangeStart    *uint64
	RangeEnd      *uint64
	SourceNode    string
	TargetNode    string
	TargetNodes   []string
	TargetVoters  []string
	NodeID        string
	MinVoters     int
}

type PlacementCatalog struct {
	Version       uint64
	Epoch         uint64
	Strategy      string
	Shards        []ShardPlacement
	Nodes         []NodeDescriptor
	UpdatedAtUnix int64
}

type PlacementPlanStep struct {
	Action  string
	ShardID *uint32
	NodeID  string
	Addr    string
	Detail  string
}

type PlacementPlan struct {
	Operation        string
	BeforeEpoch      uint64
	AfterEpoch       uint64
	Before           PlacementCatalog
	After            PlacementCatalog
	Steps            []PlacementPlanStep
	Warnings         []string
	RequiresDataCopy bool
	RequiresRestart  bool
	ApplySupported   bool
}

type PlacementApplyRequest struct {
	Plan          PlacementPlan
	ExpectedEpoch uint64
	TimeoutMS     int
}

type PlacementApplyStep struct {
	Action  string
	ShardID *uint32
	NodeID  string
	Status  string
	Detail  string
}

type PlacementApplyResult struct {
	Operation   string
	BeforeEpoch uint64
	AfterEpoch  uint64
	Steps       []PlacementApplyStep
	Placement   PlacementCatalog
}

type SplitFinalizeRequest struct {
	ParentShardID  uint32
	ChildShardID   uint32
	ExpectedEpoch  uint64
	TimeoutMS      int
	WritesQuiesced bool
}

type SplitFinalizeResult struct {
	ParentShardID     uint32
	ChildShardID      uint32
	BeforeEpoch       uint64
	AfterEpoch        uint64
	ParentRangeBefore TokenRange
	ParentRangeAfter  TokenRange
	ChildRange        TokenRange
	CopiedKeys        int64
	CopiedCatalogKeys int64
	DeletedKeys       int64
	Placement         PlacementCatalog
}

type RangeMoveFinalizeRequest struct {
	SourceShardID uint32
	TargetShardID uint32
	ExpectedEpoch uint64
	TimeoutMS     int
}

type RangeMoveFinalizeResult struct {
	SourceShardID      uint32
	TargetShardID      uint32
	BeforeEpoch        uint64
	AfterEpoch         uint64
	SourceRangesBefore []TokenRange
	SourceRangesAfter  []TokenRange
	MovedRange         TokenRange
	CopiedKeys         int64
	CopiedCatalogKeys  int64
	DeletedKeys        int64
	Phase              string
	Placement          PlacementCatalog
}

// Status fetches the cluster status. Works without a token (public).
func (c *Client) Status(ctx context.Context) (ClusterStatus, error) {
	resp, err := c.stub.ClusterStatus(c.withAuth(ctx), &cefaspb.ClusterStatusRequest{})
	if err != nil {
		return ClusterStatus{}, err
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
	}, nil
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
			ID:         sh.GetId(),
			Ranges:     tokenRangesFromPB(sh.GetRanges()),
			State:      sh.GetState(),
			Epoch:      sh.GetEpoch(),
			Voters:     append([]string(nil), sh.GetVoters()...),
			NonVoters:  append([]string(nil), sh.GetNonVoters()...),
			LeaderHint: sh.GetLeaderHint(),
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

func (c *Client) RemoveServerWithOptions(ctx context.Context, id string, opts MembershipOptions) error {
	req := &cefaspb.RemoveServerRequest{Id: id, AllShards: opts.AllShards}
	if opts.ShardID != nil {
		req.ShardId = opts.ShardID
	}
	_, err := c.stub.RemoveServer(c.withAuth(ctx), req)
	return err
}

func (c *Client) PlanPlacement(ctx context.Context, req PlacementPlanRequest) (PlacementPlan, error) {
	pbReq := &cefaspb.PlanPlacementRequest{
		Operation:    req.Operation,
		ShardId:      req.ShardID,
		SourceNode:   req.SourceNode,
		TargetNode:   req.TargetNode,
		TargetNodes:  append([]string(nil), req.TargetNodes...),
		TargetVoters: append([]string(nil), req.TargetVoters...),
		NodeId:       req.NodeID,
		MinVoters:    int32(req.MinVoters),
	}
	if req.SplitToken != nil {
		pbReq.SplitToken = req.SplitToken
	}
	if req.NewShardID != nil {
		pbReq.NewShardId = req.NewShardID
	}
	if req.TargetShardID != nil {
		pbReq.TargetShardId = req.TargetShardID
	}
	if req.RangeStart != nil {
		pbReq.RangeStart = req.RangeStart
	}
	if req.RangeEnd != nil {
		pbReq.RangeEnd = req.RangeEnd
	}
	resp, err := c.stub.PlanPlacement(c.withAuth(ctx), pbReq)
	if err != nil {
		return PlacementPlan{}, err
	}
	return placementPlanFromPB(resp.GetPlan()), nil
}

func (c *Client) ApplyPlacement(ctx context.Context, req PlacementApplyRequest) (PlacementApplyResult, error) {
	resp, err := c.stub.ApplyPlacement(c.withAuth(ctx), &cefaspb.ApplyPlacementRequest{
		Plan:          placementPlanToPB(req.Plan),
		ExpectedEpoch: req.ExpectedEpoch,
		TimeoutMs:     int32(req.TimeoutMS),
	})
	if err != nil {
		return PlacementApplyResult{}, err
	}
	return placementApplyResultFromPB(resp.GetResult()), nil
}

func (c *Client) FinalizeSplit(ctx context.Context, req SplitFinalizeRequest) (SplitFinalizeResult, error) {
	resp, err := c.stub.FinalizeSplit(c.withAuth(ctx), &cefaspb.FinalizeSplitRequest{
		ParentShardId:  req.ParentShardID,
		ChildShardId:   req.ChildShardID,
		ExpectedEpoch:  req.ExpectedEpoch,
		TimeoutMs:      int32(req.TimeoutMS),
		WritesQuiesced: req.WritesQuiesced,
	})
	if err != nil {
		return SplitFinalizeResult{}, err
	}
	return splitFinalizeResultFromPB(resp.GetResult()), nil
}

func (c *Client) FinalizeRangeMove(ctx context.Context, req RangeMoveFinalizeRequest) (RangeMoveFinalizeResult, error) {
	resp, err := c.stub.FinalizeRangeMove(c.withAuth(ctx), &cefaspb.FinalizeRangeMoveRequest{
		SourceShardId: req.SourceShardID,
		TargetShardId: req.TargetShardID,
		ExpectedEpoch: req.ExpectedEpoch,
		TimeoutMs:     int32(req.TimeoutMS),
	})
	if err != nil {
		return RangeMoveFinalizeResult{}, err
	}
	return rangeMoveFinalizeResultFromPB(resp.GetResult()), nil
}

func placementPlanToPB(in PlacementPlan) *cefaspb.PlacementPlan {
	return &cefaspb.PlacementPlan{
		Operation:        in.Operation,
		BeforeEpoch:      in.BeforeEpoch,
		AfterEpoch:       in.AfterEpoch,
		Before:           placementCatalogToPB(in.Before),
		After:            placementCatalogToPB(in.After),
		Steps:            placementPlanStepsToPB(in.Steps),
		Warnings:         append([]string(nil), in.Warnings...),
		RequiresDataCopy: in.RequiresDataCopy,
		RequiresRestart:  in.RequiresRestart,
		ApplySupported:   in.ApplySupported,
	}
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
			Id:         sh.ID,
			Ranges:     tokenRangesToPB(sh.Ranges),
			State:      sh.State,
			Epoch:      sh.Epoch,
			Voters:     append([]string(nil), sh.Voters...),
			NonVoters:  append([]string(nil), sh.NonVoters...),
			LeaderHint: sh.LeaderHint,
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

func placementPlanStepsToPB(in []PlacementPlanStep) []*cefaspb.PlacementPlanStep {
	out := make([]*cefaspb.PlacementPlanStep, 0, len(in))
	for _, step := range in {
		out = append(out, &cefaspb.PlacementPlanStep{
			Action:  step.Action,
			ShardId: step.ShardID,
			NodeId:  step.NodeID,
			Addr:    step.Addr,
			Detail:  step.Detail,
		})
	}
	return out
}

func placementPlanFromPB(in *cefaspb.PlacementPlan) PlacementPlan {
	if in == nil {
		return PlacementPlan{}
	}
	return PlacementPlan{
		Operation:        in.GetOperation(),
		BeforeEpoch:      in.GetBeforeEpoch(),
		AfterEpoch:       in.GetAfterEpoch(),
		Before:           placementCatalogFromPB(in.GetBefore()),
		After:            placementCatalogFromPB(in.GetAfter()),
		Steps:            placementPlanStepsFromPB(in.GetSteps()),
		Warnings:         append([]string(nil), in.GetWarnings()...),
		RequiresDataCopy: in.GetRequiresDataCopy(),
		RequiresRestart:  in.GetRequiresRestart(),
		ApplySupported:   in.GetApplySupported(),
	}
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

func placementPlanStepsFromPB(in []*cefaspb.PlacementPlanStep) []PlacementPlanStep {
	out := make([]PlacementPlanStep, 0, len(in))
	for _, step := range in {
		var shardID *uint32
		if step.ShardId != nil {
			id := step.GetShardId()
			shardID = &id
		}
		out = append(out, PlacementPlanStep{
			Action:  step.GetAction(),
			ShardID: shardID,
			NodeID:  step.GetNodeId(),
			Addr:    step.GetAddr(),
			Detail:  step.GetDetail(),
		})
	}
	return out
}

func placementApplyResultFromPB(in *cefaspb.PlacementApplyResult) PlacementApplyResult {
	if in == nil {
		return PlacementApplyResult{}
	}
	return PlacementApplyResult{
		Operation:   in.GetOperation(),
		BeforeEpoch: in.GetBeforeEpoch(),
		AfterEpoch:  in.GetAfterEpoch(),
		Steps:       placementApplyStepsFromPB(in.GetSteps()),
		Placement:   placementCatalogFromPB(in.GetPlacement()),
	}
}

func splitFinalizeResultFromPB(in *cefaspb.FinalizeSplitResult) SplitFinalizeResult {
	if in == nil {
		return SplitFinalizeResult{}
	}
	return SplitFinalizeResult{
		ParentShardID:     in.GetParentShardId(),
		ChildShardID:      in.GetChildShardId(),
		BeforeEpoch:       in.GetBeforeEpoch(),
		AfterEpoch:        in.GetAfterEpoch(),
		ParentRangeBefore: tokenRangeFromPB(in.GetParentRangeBefore()),
		ParentRangeAfter:  tokenRangeFromPB(in.GetParentRangeAfter()),
		ChildRange:        tokenRangeFromPB(in.GetChildRange()),
		CopiedKeys:        in.GetCopiedKeys(),
		CopiedCatalogKeys: in.GetCopiedCatalogKeys(),
		DeletedKeys:       in.GetDeletedKeys(),
		Placement:         placementCatalogFromPB(in.GetPlacement()),
	}
}

func rangeMoveFinalizeResultFromPB(in *cefaspb.FinalizeRangeMoveResult) RangeMoveFinalizeResult {
	if in == nil {
		return RangeMoveFinalizeResult{}
	}
	return RangeMoveFinalizeResult{
		SourceShardID:      in.GetSourceShardId(),
		TargetShardID:      in.GetTargetShardId(),
		BeforeEpoch:        in.GetBeforeEpoch(),
		AfterEpoch:         in.GetAfterEpoch(),
		SourceRangesBefore: tokenRangesFromPB(in.GetSourceRangesBefore()),
		SourceRangesAfter:  tokenRangesFromPB(in.GetSourceRangesAfter()),
		MovedRange:         tokenRangeFromPB(in.GetMovedRange()),
		CopiedKeys:         in.GetCopiedKeys(),
		CopiedCatalogKeys:  in.GetCopiedCatalogKeys(),
		DeletedKeys:        in.GetDeletedKeys(),
		Phase:              in.GetPhase(),
		Placement:          placementCatalogFromPB(in.GetPlacement()),
	}
}

func tokenRangeFromPB(in *cefaspb.TokenRange) TokenRange {
	if in == nil {
		return TokenRange{}
	}
	return TokenRange{Start: in.GetStart(), End: in.GetEnd()}
}

func placementApplyStepsFromPB(in []*cefaspb.PlacementApplyStep) []PlacementApplyStep {
	out := make([]PlacementApplyStep, 0, len(in))
	for _, step := range in {
		var shardID *uint32
		if step.ShardId != nil {
			id := step.GetShardId()
			shardID = &id
		}
		out = append(out, PlacementApplyStep{
			Action:  step.GetAction(),
			ShardID: shardID,
			NodeID:  step.GetNodeId(),
			Status:  step.GetStatus(),
			Detail:  step.GetDetail(),
		})
	}
	return out
}
