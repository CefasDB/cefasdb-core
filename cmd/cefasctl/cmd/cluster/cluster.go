// Package cluster hosts the `cefas cluster <verb>` subcommands —
// status, add-voter, remove-server. These wrap the SDK's cluster
// helpers and inherit every global flag from the root command.
package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/fileloader"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
	"github.com/CefasDb/cefasdb/pkg/client"
)

// Register installs the cluster subcommand tree onto root.
func Register(root *cobra.Command) {
	c := &cobra.Command{
		Use:   "cluster",
		Short: "Cluster membership and status",
	}
	c.AddCommand(statusCmd())
	c.AddCommand(addVoterCmd())
	c.AddCommand(removeServerCmd())
	c.AddCommand(rebalanceLeadersCmd())
	c.AddCommand(planCmd())
	c.AddCommand(applyCmd())
	c.AddCommand(splitCmd())
	c.AddCommand(rangeMoveCmd())
	root.AddCommand(c)
}

func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show the cluster mode, self ID, and current leader",
		Long: `Fetches the cluster status. Public — no token required.

Example:
  cefas cluster status`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			st, err := cli.Status(ctx)
			if err != nil {
				return fmt.Errorf("cluster status: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Mode":              st.Mode,
				"IsLeader":          st.IsLeader,
				"SelfID":            st.SelfID,
				"BindAddr":          st.BindAddr,
				"LeaderHTTP":        st.LeaderHTTP,
				"RoutingEpoch":      st.RoutingEpoch,
				"PlacementVersion":  st.PlacementVersion,
				"ShardCount":        st.ShardCount,
				"PlacementStrategy": st.PlacementStrategy,
				"Shards":            st.Shards,
				"Nodes":             st.Nodes,
				"HotRanges":         st.HotRanges,
				"BackupScheduler":   st.BackupScheduler,
				"DrainProgress":     clusterDrainProgress(st),
			})
		},
	}
}

func addVoterCmd() *cobra.Command {
	var (
		id        string
		addr      string
		shardID   uint32
		shardSet  bool
		allShards bool
	)
	c := &cobra.Command{
		Use:   "add-voter",
		Short: "Add a voting peer to the Raft configuration",
		Long: `Asks the leader to add a voter at the supplied Raft address.
Requires the cefas:cluster:admin scope on the bearer token.

Example:
  cefas cluster add-voter --id node-3 --addr 10.0.0.13:9000`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if id == "" {
				return fmt.Errorf("--id is required")
			}
			if addr == "" {
				return fmt.Errorf("--addr is required")
			}
			if shardSet && allShards {
				return fmt.Errorf("--shard and --all-shards are mutually exclusive")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			opts := clientMembershipOptions(shardID, shardSet, allShards)
			if err := cli.AddVoterWithOptions(ctx, id, addr, opts); err != nil {
				return fmt.Errorf("add voter: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			scope := "default"
			if allShards {
				scope = "all-shards"
			} else if shardSet {
				scope = fmt.Sprintf("shard-%d", shardID)
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Added": map[string]string{"ID": id, "Addr": addr, "Scope": scope},
			})
		},
	}
	f := c.Flags()
	f.StringVar(&id, "id", "", "Raft node ID (required)")
	f.StringVar(&addr, "addr", "", "Raft transport address host:port (required)")
	f.Uint32Var(&shardID, "shard", 0, "Apply to one shard ID instead of the representative cluster")
	f.BoolVar(&allShards, "all-shards", false, "Apply to every shard Raft group")
	c.PreRun = func(cmd *cobra.Command, _ []string) {
		shardSet = cmd.Flags().Changed("shard")
	}
	_ = c.MarkFlagRequired("id")
	_ = c.MarkFlagRequired("addr")
	return c
}

func removeServerCmd() *cobra.Command {
	var (
		id        string
		shardID   uint32
		shardSet  bool
		allShards bool
	)
	c := &cobra.Command{
		Use:   "remove-server",
		Short: "Evict a peer from the Raft configuration",
		Long: `Removes the peer with the given ID from the cluster. Requires
the cefas:cluster:admin scope on the bearer token.

Example:
  cefas cluster remove-server --id node-3`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if id == "" {
				return fmt.Errorf("--id is required")
			}
			if shardSet && allShards {
				return fmt.Errorf("--shard and --all-shards are mutually exclusive")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			opts := clientMembershipOptions(shardID, shardSet, allShards)
			if err := cli.RemoveServerWithOptions(ctx, id, opts); err != nil {
				return fmt.Errorf("remove server: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			scope := "default"
			if allShards {
				scope = "all-shards"
			} else if shardSet {
				scope = fmt.Sprintf("shard-%d", shardID)
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Removed": map[string]string{"ID": id, "Scope": scope},
			})
		},
	}
	f := c.Flags()
	f.StringVar(&id, "id", "", "Raft node ID to remove (required)")
	f.Uint32Var(&shardID, "shard", 0, "Apply to one shard ID instead of the representative cluster")
	f.BoolVar(&allShards, "all-shards", false, "Apply to every shard Raft group")
	c.PreRun = func(cmd *cobra.Command, _ []string) {
		shardSet = cmd.Flags().Changed("shard")
	}
	_ = c.MarkFlagRequired("id")
	return c
}

func rebalanceLeadersCmd() *cobra.Command {
	var (
		dryRun           bool
		includeShardZero bool
		maxConcurrent    int
		timeoutMS        int
		nodeEndpoints    string
	)
	c := &cobra.Command{
		Use:   "rebalance-leaders",
		Short: "Transfer shard leaders toward placement leader hints",
		Long: `Transfers locally-led shards whose current Raft leader differs from
the placement leader hint. Pass --node-endpoints to orchestrate the same local
operation across every node and aggregate the cluster-wide result. Shard 0 is
skipped by default because it carries metadata/catalog state.

Example:
  cefas cluster rebalance-leaders --dry-run
  cefas cluster rebalance-leaders --dry-run --node-endpoints n1=127.0.0.1:9291,n2=127.0.0.1:9292
  cefas cluster rebalance-leaders --max-concurrent 2 --timeout-ms 5000`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			req := client.RebalanceLeadersRequest{
				DryRun:           dryRun,
				IncludeShardZero: includeShardZero,
				MaxConcurrent:    maxConcurrent,
				TimeoutMS:        timeoutMS,
			}
			if strings.TrimSpace(nodeEndpoints) != "" {
				endpoints, err := parseNodeEndpointMap(nodeEndpoints)
				if err != nil {
					return err
				}
				profile, err := runtime.ResolveProfile(ctx)
				if err != nil {
					return err
				}
				result, err := rebalanceLeadersAcrossNodes(ctx, endpoints, req)
				if err != nil {
					return err
				}
				fm, err := output.Validate(profile.Output)
				if err != nil {
					return err
				}
				return output.New(cmd.OutOrStdout(), fm).Object(result)
			}
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			result, err := cli.RebalanceLeaders(ctx, req)
			if err != nil {
				return fmt.Errorf("rebalance leaders: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(result)
		},
	}
	f := c.Flags()
	f.BoolVar(&dryRun, "dry-run", false, "Plan leader transfers without applying them")
	f.BoolVar(&includeShardZero, "include-shard-zero", false, "Allow transferring metadata shard 0")
	f.IntVar(&maxConcurrent, "max-concurrent", 1, "Maximum concurrent leadership transfers")
	f.IntVar(&timeoutMS, "timeout-ms", 5000, "Per-transfer timeout in milliseconds")
	f.StringVar(&nodeEndpoints, "node-endpoints", "", "Comma-separated nodeID=gRPC endpoint map for cluster-wide orchestration")
	return c
}

type rebalanceLeadersClusterResult struct {
	DryRun           bool
	IncludeShardZero bool
	MaxConcurrent    int
	TimeoutMS        int
	NodeEndpoints    map[string]string
	NodeResults      []rebalanceLeadersNodeResult
	Before           []client.ShardLeadershipStatus
	After            []client.ShardLeadershipStatus
	BeforeCounts     []client.LeaderCount
	AfterCounts      []client.LeaderCount
	Planned          int
	Transferred      int
	Skipped          int
	Failed           int
	NodePlanned      int
	NodeTransferred  int
	NodeSkipped      int
	NodeFailed       int
}

type rebalanceLeadersNodeResult struct {
	NodeID   string
	Endpoint string
	Result   client.RebalanceLeadersResult
}

func rebalanceLeadersAcrossNodes(ctx context.Context, endpoints map[string]string, req client.RebalanceLeadersRequest) (rebalanceLeadersClusterResult, error) {
	result := rebalanceLeadersClusterResult{
		DryRun:           req.DryRun,
		IncludeShardZero: req.IncludeShardZero,
		MaxConcurrent:    req.MaxConcurrent,
		TimeoutMS:        req.TimeoutMS,
		NodeEndpoints:    copyStringMap(endpoints),
	}
	nodeIDs := sortedStringKeys(endpoints)
	for _, nodeID := range nodeIDs {
		endpoint := endpoints[nodeID]
		cli, err := dialEndpoint(ctx, endpoint)
		if err != nil {
			return result, fmt.Errorf("rebalance leaders %s=%s: %w", nodeID, endpoint, err)
		}
		nodeResult, err := cli.RebalanceLeaders(ctx, req)
		cerr := cli.Close()
		if err != nil {
			return result, fmt.Errorf("rebalance leaders %s=%s: %w", nodeID, endpoint, err)
		}
		if cerr != nil {
			return result, fmt.Errorf("rebalance leaders %s=%s close: %w", nodeID, endpoint, cerr)
		}
		result.NodeResults = append(result.NodeResults, rebalanceLeadersNodeResult{
			NodeID:   nodeID,
			Endpoint: endpoint,
			Result:   nodeResult,
		})
		result.NodePlanned += nodeResult.Planned
		result.NodeTransferred += nodeResult.Transferred
		result.NodeSkipped += nodeResult.Skipped
		result.NodeFailed += nodeResult.Failed
	}
	result.Before = mergeShardLeadershipStatuses(result.NodeResults, func(r client.RebalanceLeadersResult) []client.ShardLeadershipStatus {
		return r.Before
	})
	result.After = mergeShardLeadershipStatuses(result.NodeResults, func(r client.RebalanceLeadersResult) []client.ShardLeadershipStatus {
		return r.After
	})
	result.BeforeCounts = leaderCountsFromStatuses(result.Before)
	result.AfterCounts = leaderCountsFromStatuses(result.After)
	result.Planned, result.Transferred, result.Skipped, result.Failed = aggregateLeaderRebalanceSteps(result.NodeResults)
	return result, nil
}

func dialEndpoint(ctx context.Context, endpoint string) (*client.Client, error) {
	opts := runtime.Flags
	if s := runtime.FromContext(ctx); s != nil {
		opts = s.Options()
	}
	opts.Endpoint = endpoint
	cli, _, err := runtime.Dial(runtime.WithSession(ctx, runtime.NewSession(opts)))
	return cli, err
}

func parseNodeEndpointMap(raw string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		nodeID, endpoint, ok := strings.Cut(part, "=")
		if !ok || strings.TrimSpace(nodeID) == "" || strings.TrimSpace(endpoint) == "" {
			return nil, fmt.Errorf("bad node endpoint mapping %q", part)
		}
		out[strings.TrimSpace(nodeID)] = strings.TrimSpace(endpoint)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no node endpoints configured")
	}
	return out, nil
}

func mergeShardLeadershipStatuses(nodes []rebalanceLeadersNodeResult, selectStatuses func(client.RebalanceLeadersResult) []client.ShardLeadershipStatus) []client.ShardLeadershipStatus {
	byShard := map[uint32]client.ShardLeadershipStatus{}
	for _, node := range nodes {
		for _, st := range selectStatuses(node.Result) {
			current := byShard[st.ShardID]
			current.ShardID = st.ShardID
			if current.ActualLeader == "" && st.ActualLeader != "" {
				current.ActualLeader = st.ActualLeader
			}
			if current.DesiredLeader == "" && st.DesiredLeader != "" {
				current.DesiredLeader = st.DesiredLeader
			}
			current.LeaderMismatch = current.LeaderMismatch || st.LeaderMismatch
			byShard[st.ShardID] = current
		}
	}
	out := make([]client.ShardLeadershipStatus, 0, len(byShard))
	for _, st := range byShard {
		st.LeaderMismatch = st.LeaderMismatch || (st.ActualLeader != "" && st.DesiredLeader != "" && st.ActualLeader != st.DesiredLeader)
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ShardID < out[j].ShardID })
	return out
}

func leaderCountsFromStatuses(statuses []client.ShardLeadershipStatus) []client.LeaderCount {
	counts := map[string]int{}
	for _, st := range statuses {
		if st.ShardID == 0 || st.ActualLeader == "" {
			continue
		}
		counts[st.ActualLeader]++
	}
	out := make([]client.LeaderCount, 0, len(counts))
	for nodeID, count := range counts {
		out = append(out, client.LeaderCount{NodeID: nodeID, Count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

func aggregateLeaderRebalanceSteps(nodes []rebalanceLeadersNodeResult) (planned, transferred, skipped, failed int) {
	statusByShard := map[uint32]string{}
	for _, node := range nodes {
		for _, step := range node.Result.Steps {
			prev := statusByShard[step.ShardID]
			statusByShard[step.ShardID] = strongerLeaderRebalanceStatus(prev, step.Status)
		}
	}
	for _, status := range statusByShard {
		switch status {
		case "planned":
			planned++
		case "transferred":
			transferred++
		case "failed":
			failed++
		default:
			skipped++
		}
	}
	return planned, transferred, skipped, failed
}

func strongerLeaderRebalanceStatus(a, b string) string {
	rank := func(status string) int {
		switch status {
		case "transferred":
			return 4
		case "failed":
			return 3
		case "planned":
			return 2
		case "skipped":
			return 1
		default:
			return 0
		}
	}
	if rank(b) > rank(a) {
		return b
	}
	return a
}

func sortedStringKeys(in map[string]string) []string {
	out := make([]string, 0, len(in))
	for key := range in {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func copyStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func planCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "plan",
		Short: "Plan shard placement changes",
	}
	c.AddCommand(planSplitCmd())
	c.AddCommand(planRangeMoveCmd())
	c.AddCommand(planMoveCmd())
	c.AddCommand(planDrainCmd())
	c.AddCommand(planDecommissionCmd())
	return c
}

func planSplitCmd() *cobra.Command {
	var (
		shardID      uint32
		splitToken   uint64
		newShardID   uint32
		targetVoters []string
		minVoters    int
	)
	c := &cobra.Command{
		Use:   "split",
		Short: "Plan a token-range shard split",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !cmd.Flags().Changed("shard") {
				return fmt.Errorf("--shard is required")
			}
			req := client.PlacementPlanRequest{
				Operation:    "split",
				ShardID:      shardID,
				TargetVoters: targetVoters,
				MinVoters:    minVoters,
			}
			if cmd.Flags().Changed("split-token") {
				v := splitToken
				req.SplitToken = &v
			}
			if cmd.Flags().Changed("new-shard") {
				v := newShardID
				req.NewShardID = &v
			}
			return runPlacementPlan(cmd, req)
		},
	}
	f := c.Flags()
	f.Uint32Var(&shardID, "shard", 0, "Shard ID to split")
	f.Uint64Var(&splitToken, "split-token", 0, "Optional split token; default is midpoint")
	f.Uint32Var(&newShardID, "new-shard", 0, "Optional new shard ID; must be next contiguous ID")
	f.StringArrayVar(&targetVoters, "target-voter", nil, "Target child shard voter; repeat for multiple nodes")
	f.IntVar(&minVoters, "min-voters", 0, "Minimum voters for the child shard; default preserves current voter count")
	return c
}

func planRangeMoveCmd() *cobra.Command {
	var (
		sourceShardID uint32
		targetShardID uint32
		rangeStart    uint64
		rangeEnd      uint64
		targetVoters  []string
		minVoters     int
	)
	c := &cobra.Command{
		Use:   "range-move",
		Short: "Plan moving a token range to a new shard",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !cmd.Flags().Changed("source-shard") {
				return fmt.Errorf("--source-shard is required")
			}
			if !cmd.Flags().Changed("range-start") || !cmd.Flags().Changed("range-end") {
				return fmt.Errorf("--range-start and --range-end are required")
			}
			start := rangeStart
			end := rangeEnd
			req := client.PlacementPlanRequest{
				Operation:    "range_move",
				ShardID:      sourceShardID,
				RangeStart:   &start,
				RangeEnd:     &end,
				TargetVoters: targetVoters,
				MinVoters:    minVoters,
			}
			if cmd.Flags().Changed("target-shard") {
				v := targetShardID
				req.TargetShardID = &v
			}
			return runPlacementPlan(cmd, req)
		},
	}
	f := c.Flags()
	f.Uint32Var(&sourceShardID, "source-shard", 0, "Source shard ID that currently owns the range")
	f.Uint32Var(&targetShardID, "target-shard", 0, "Optional target shard ID; must be next contiguous ID")
	f.Uint64Var(&rangeStart, "range-start", 0, "Inclusive token range start")
	f.Uint64Var(&rangeEnd, "range-end", 0, "Exclusive token range end; equal to start means full ring")
	f.StringArrayVar(&targetVoters, "target-voter", nil, "Target shard voter; repeat for multiple nodes")
	f.IntVar(&minVoters, "min-voters", 0, "Minimum voters for the target shard; default preserves current voter count")
	return c
}

func planMoveCmd() *cobra.Command {
	var (
		shardID      uint32
		sourceNode   string
		targetNode   string
		targetVoters []string
		minVoters    int
	)
	c := &cobra.Command{
		Use:   "move",
		Short: "Plan moving a shard Raft membership placement",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !cmd.Flags().Changed("shard") {
				return fmt.Errorf("--shard is required")
			}
			if len(targetVoters) == 0 && (sourceNode == "" || targetNode == "") {
				return fmt.Errorf("--source-node and --target-node are required unless --target-voter is supplied")
			}
			req := client.PlacementPlanRequest{
				Operation:    "move",
				ShardID:      shardID,
				SourceNode:   sourceNode,
				TargetNode:   targetNode,
				TargetVoters: targetVoters,
				MinVoters:    minVoters,
			}
			return runPlacementPlan(cmd, req)
		},
	}
	f := c.Flags()
	f.Uint32Var(&shardID, "shard", 0, "Shard ID to move")
	f.StringVar(&sourceNode, "source-node", "", "Existing voter node to replace")
	f.StringVar(&targetNode, "target-node", "", "Replacement node")
	f.StringArrayVar(&targetVoters, "target-voter", nil, "Full target voter set; repeat for multiple nodes")
	f.IntVar(&minVoters, "min-voters", 1, "Minimum voters allowed after the move")
	return c
}

func planDrainCmd() *cobra.Command {
	var (
		nodeID      string
		targetNodes []string
		minVoters   int
	)
	c := &cobra.Command{
		Use:   "drain",
		Short: "Plan draining a node from shard memberships",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if nodeID == "" {
				return fmt.Errorf("--node is required")
			}
			req := client.PlacementPlanRequest{
				Operation:   "drain",
				NodeID:      nodeID,
				TargetNodes: targetNodes,
				MinVoters:   minVoters,
			}
			return runPlacementPlan(cmd, req)
		},
	}
	f := c.Flags()
	f.StringVar(&nodeID, "node", "", "Node ID to drain")
	f.StringArrayVar(&targetNodes, "target-node", nil, "Replacement node; repeat for multiple nodes")
	f.IntVar(&minVoters, "min-voters", 1, "Minimum voters allowed after drain")
	_ = c.MarkFlagRequired("node")
	return c
}

func planDecommissionCmd() *cobra.Command {
	var nodeID string
	c := &cobra.Command{
		Use:   "decommission",
		Short: "Plan final decommission after a node drain",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if nodeID == "" {
				return fmt.Errorf("--node is required")
			}
			return runPlacementPlan(cmd, client.PlacementPlanRequest{
				Operation: "decommission",
				NodeID:    nodeID,
			})
		},
	}
	f := c.Flags()
	f.StringVar(&nodeID, "node", "", "Drained node ID to mark decommissioned")
	_ = c.MarkFlagRequired("node")
	return c
}

func runPlacementPlan(cmd *cobra.Command, req client.PlacementPlanRequest) error {
	ctx := cmd.Context()
	cli, profile, err := runtime.Dial(ctx)
	if err != nil {
		return err
	}
	defer cli.Close()
	plan, err := cli.PlanPlacement(ctx, req)
	if err != nil {
		return fmt.Errorf("plan placement: %w", err)
	}
	fm, err := output.Validate(profile.Output)
	if err != nil {
		return err
	}
	return output.New(cmd.OutOrStdout(), fm).Object(plan)
}

func applyCmd() *cobra.Command {
	var (
		planArg       string
		expectedEpoch uint64
		timeoutMS     int
		yes           bool
	)
	c := &cobra.Command{
		Use:   "apply",
		Short: "Apply an approved shard placement plan",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if planArg == "" {
				return fmt.Errorf("--plan is required")
			}
			if !yes {
				return fmt.Errorf("--yes is required to apply placement changes")
			}
			raw, err := fileloader.Load(planArg)
			if err != nil {
				return err
			}
			var plan client.PlacementPlan
			if err := json.Unmarshal(raw, &plan); err != nil {
				return fmt.Errorf("decode plan: %w", err)
			}
			if expectedEpoch == 0 {
				expectedEpoch = plan.BeforeEpoch
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			result, err := cli.ApplyPlacement(ctx, client.PlacementApplyRequest{
				Plan:          plan,
				ExpectedEpoch: expectedEpoch,
				TimeoutMS:     timeoutMS,
			})
			if err != nil {
				return fmt.Errorf("apply placement: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(result)
		},
	}
	f := c.Flags()
	f.StringVar(&planArg, "plan", "", "Placement plan JSON or file://path")
	f.Uint64Var(&expectedEpoch, "expected-epoch", 0, "Expected current routing epoch; defaults to plan beforeEpoch")
	f.IntVar(&timeoutMS, "timeout-ms", 5000, "Per-step Raft timeout in milliseconds")
	f.BoolVar(&yes, "yes", false, "Confirm applying the placement plan")
	_ = c.MarkFlagRequired("plan")
	return c
}

type DrainProgress struct {
	NodeID           string
	State            string
	Status           string
	ActiveReferences int
	Blockers         []string
}

func clusterDrainProgress(st client.ClusterStatus) []DrainProgress {
	nodes := append([]client.NodeDescriptor(nil), st.Nodes...)
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	progress := make([]DrainProgress, 0)
	for _, node := range nodes {
		if node.State != "draining" && node.State != "decommissioned" {
			continue
		}
		blockers := clusterDrainBlockers(st.Shards, node.ID)
		status := "blocked"
		if node.State == "decommissioned" {
			status = "decommissioned"
			blockers = nil
		} else if len(blockers) == 0 {
			status = "ready_for_decommission"
		}
		progress = append(progress, DrainProgress{
			NodeID:           node.ID,
			State:            node.State,
			Status:           status,
			ActiveReferences: len(blockers),
			Blockers:         blockers,
		})
	}
	return progress
}

func clusterDrainBlockers(shards []client.ShardPlacement, nodeID string) []string {
	var blockers []string
	for _, shard := range shards {
		if shard.State == "decommissioned" {
			continue
		}
		if containsClientString(shard.Voters, nodeID) {
			blockers = append(blockers, fmt.Sprintf("shard %d voter state=%s ranges=%d", shard.ID, shard.State, len(shard.Ranges)))
		}
		if containsClientString(shard.NonVoters, nodeID) {
			blockers = append(blockers, fmt.Sprintf("shard %d non-voter state=%s", shard.ID, shard.State))
		}
		if shard.LeaderHint == nodeID {
			blockers = append(blockers, fmt.Sprintf("shard %d leader hint state=%s", shard.ID, shard.State))
		}
	}
	sort.Strings(blockers)
	return blockers
}

func containsClientString(in []string, v string) bool {
	for _, existing := range in {
		if existing == v {
			return true
		}
	}
	return false
}

func splitCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "split",
		Short: "Manage prepared shard splits",
	}
	c.AddCommand(splitFinalizeCmd())
	return c
}

func rangeMoveCmd() *cobra.Command {
	c := &cobra.Command{
		Use:   "range-move",
		Short: "Manage prepared token range moves",
	}
	c.AddCommand(rangeMoveFinalizeCmd())
	return c
}

func rangeMoveFinalizeCmd() *cobra.Command {
	var (
		sourceShardID uint32
		targetShardID uint32
		expectedEpoch uint64
		timeoutMS     int
		yes           bool
	)
	c := &cobra.Command{
		Use:   "finalize",
		Short: "Copy, verify, cut over, and clean up a prepared range move",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !cmd.Flags().Changed("source-shard") {
				return fmt.Errorf("--source-shard is required")
			}
			if !cmd.Flags().Changed("target-shard") {
				return fmt.Errorf("--target-shard is required")
			}
			if !cmd.Flags().Changed("expected-epoch") {
				return fmt.Errorf("--expected-epoch is required")
			}
			if !yes {
				return fmt.Errorf("--yes is required to finalize a range move")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			result, err := cli.FinalizeRangeMove(ctx, client.RangeMoveFinalizeRequest{
				SourceShardID: sourceShardID,
				TargetShardID: targetShardID,
				ExpectedEpoch: expectedEpoch,
				TimeoutMS:     timeoutMS,
			})
			if err != nil {
				return fmt.Errorf("finalize range move: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(result)
		},
	}
	f := c.Flags()
	f.Uint32Var(&sourceShardID, "source-shard", 0, "Moving source shard ID")
	f.Uint32Var(&targetShardID, "target-shard", 0, "Prepared target shard ID")
	f.Uint64Var(&expectedEpoch, "expected-epoch", 0, "Expected current routing epoch")
	f.IntVar(&timeoutMS, "timeout-ms", 5000, "Per-write timeout in milliseconds")
	f.BoolVar(&yes, "yes", false, "Confirm finalizing the range move")
	return c
}

func splitFinalizeCmd() *cobra.Command {
	var (
		parentShardID  uint32
		childShardID   uint32
		expectedEpoch  uint64
		timeoutMS      int
		writesQuiesced bool
		yes            bool
	)
	c := &cobra.Command{
		Use:   "finalize",
		Short: "Copy a prepared split range and activate the child shard",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !cmd.Flags().Changed("parent-shard") {
				return fmt.Errorf("--parent-shard is required")
			}
			if !cmd.Flags().Changed("child-shard") {
				return fmt.Errorf("--child-shard is required")
			}
			if !cmd.Flags().Changed("expected-epoch") {
				return fmt.Errorf("--expected-epoch is required")
			}
			if !writesQuiesced {
				return fmt.Errorf("--writes-quiesced is required")
			}
			if !yes {
				return fmt.Errorf("--yes is required to finalize a split")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			result, err := cli.FinalizeSplit(ctx, client.SplitFinalizeRequest{
				ParentShardID:  parentShardID,
				ChildShardID:   childShardID,
				ExpectedEpoch:  expectedEpoch,
				TimeoutMS:      timeoutMS,
				WritesQuiesced: writesQuiesced,
			})
			if err != nil {
				return fmt.Errorf("finalize split: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(result)
		},
	}
	f := c.Flags()
	f.Uint32Var(&parentShardID, "parent-shard", 0, "Splitting parent shard ID")
	f.Uint32Var(&childShardID, "child-shard", 0, "Prepared child shard ID")
	f.Uint64Var(&expectedEpoch, "expected-epoch", 0, "Expected current routing epoch")
	f.IntVar(&timeoutMS, "timeout-ms", 5000, "Per-write timeout in milliseconds")
	f.BoolVar(&writesQuiesced, "writes-quiesced", false, "Confirm writes to the split range are paused")
	f.BoolVar(&yes, "yes", false, "Confirm finalizing the split")
	return c
}

func clientMembershipOptions(shardID uint32, shardSet, allShards bool) client.MembershipOptions {
	opts := client.MembershipOptions{AllShards: allShards}
	if shardSet {
		id := shardID
		opts.ShardID = &id
	}
	return opts
}
