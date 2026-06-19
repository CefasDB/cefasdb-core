package client

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/storage"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

const (
	routeAwarePlacementTTL   = 30 * time.Second
	routeAwareRecentErrorTTL = 2 * time.Second
)

type routeAwareReads struct {
	mu        sync.RWMutex
	refreshMu sync.Mutex

	nodes       map[string]*routeNode
	nodeOrder   []string
	shards      []routeShard
	schemaCache map[string]types.KeySchema
	lastRefresh time.Time

	attempts       atomic.Uint64
	successes      atomic.Uint64
	refreshes      atomic.Uint64
	retries        atomic.Uint64
	staleRoutes    atomic.Uint64
	noLocalReplica atomic.Uint64
	leaderServed   atomic.Uint64
	followerServed atomic.Uint64
}

type routeNode struct {
	id   string
	addr string
	conn *grpc.ClientConn
	stub cefaspb.CefasClient

	inflight       atomic.Int64
	latencyNanos   atomic.Int64
	lastErrorNanos atomic.Int64
}

type routeShard struct {
	id     uint32
	ranges []TokenRange
	voters []string
	leader string
}

type routeTarget struct {
	shardID uint32
	leader  string
	voters  []string
}

// RouteAwareReadStats is a snapshot of client-side route-aware read activity.
type RouteAwareReadStats struct {
	Attempts       uint64
	Successes      uint64
	Refreshes      uint64
	Retries        uint64
	StaleRoutes    uint64
	NoLocalReplica uint64
	LeaderServed   uint64
	FollowerServed uint64
	Nodes          []RouteAwareNodeStats
}

// RouteAwareNodeStats is a per-node snapshot used to debug route selection.
type RouteAwareNodeStats struct {
	NodeID      string
	Endpoint    string
	Inflight    int64
	Latency     time.Duration
	LastErrorAt time.Time
}

func newRouteAwareReads(endpoints map[string]string, dialOpts []grpc.DialOption) (*routeAwareReads, error) {
	r := &routeAwareReads{
		nodes:       make(map[string]*routeNode, len(endpoints)),
		schemaCache: map[string]types.KeySchema{},
	}
	for id := range endpoints {
		r.nodeOrder = append(r.nodeOrder, id)
	}
	sort.Strings(r.nodeOrder)
	for _, id := range r.nodeOrder {
		addr := endpoints[id]
		conn, err := grpc.NewClient(addr, dialOpts...)
		if err != nil {
			_ = r.close()
			return nil, fmt.Errorf("cefas: route-aware read dial %s=%s: %w", id, addr, err)
		}
		r.nodes[id] = &routeNode{id: id, addr: addr, conn: conn, stub: cefaspb.NewCefasClient(conn)}
	}
	return r, nil
}

func (r *routeAwareReads) close() error {
	r.mu.RLock()
	nodes := make([]*routeNode, 0, len(r.nodes))
	for _, id := range r.nodeOrder {
		if node := r.nodes[id]; node != nil {
			nodes = append(nodes, node)
		}
	}
	r.mu.RUnlock()

	var err error
	for _, node := range nodes {
		if cerr := node.conn.Close(); err == nil {
			err = cerr
		}
	}
	return err
}

// RouteAwareReadStats returns a point-in-time snapshot. Zero values mean
// route-aware reads are disabled on this client.
func (c *Client) RouteAwareReadStats() RouteAwareReadStats {
	if c == nil || c.routeReads == nil {
		return RouteAwareReadStats{}
	}
	return c.routeReads.stats()
}

func (r *routeAwareReads) stats() RouteAwareReadStats {
	out := RouteAwareReadStats{
		Attempts:       r.attempts.Load(),
		Successes:      r.successes.Load(),
		Refreshes:      r.refreshes.Load(),
		Retries:        r.retries.Load(),
		StaleRoutes:    r.staleRoutes.Load(),
		NoLocalReplica: r.noLocalReplica.Load(),
		LeaderServed:   r.leaderServed.Load(),
		FollowerServed: r.followerServed.Load(),
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, id := range r.nodeOrder {
		node := r.nodes[id]
		if node == nil {
			continue
		}
		lastErr := time.Time{}
		if nanos := node.lastErrorNanos.Load(); nanos > 0 {
			lastErr = time.Unix(0, nanos)
		}
		out.Nodes = append(out.Nodes, RouteAwareNodeStats{
			NodeID:      node.id,
			Endpoint:    node.addr,
			Inflight:    node.inflight.Load(),
			Latency:     time.Duration(node.latencyNanos.Load()),
			LastErrorAt: lastErr,
		})
	}
	return out
}

func routeAwareOverride(enabled *bool) bool {
	if enabled == nil {
		return true
	}
	return *enabled
}

func (c *Client) ensureRoutePlacement(ctx context.Context, force bool) error {
	if c.routeReads == nil {
		return errors.New("route-aware reads are disabled")
	}
	now := time.Now()
	if !force {
		c.routeReads.mu.RLock()
		ok := len(c.routeReads.shards) > 0 && now.Sub(c.routeReads.lastRefresh) < routeAwarePlacementTTL
		c.routeReads.mu.RUnlock()
		if ok {
			return nil
		}
	}

	c.routeReads.refreshMu.Lock()
	defer c.routeReads.refreshMu.Unlock()

	now = time.Now()
	if !force {
		c.routeReads.mu.RLock()
		ok := len(c.routeReads.shards) > 0 && now.Sub(c.routeReads.lastRefresh) < routeAwarePlacementTTL
		c.routeReads.mu.RUnlock()
		if ok {
			return nil
		}
	}

	st, err := c.fetchRouteStatus(ctx)
	if err != nil {
		return err
	}
	if len(st.Shards) == 0 {
		return errors.New("route-aware reads require shard placement in cluster status")
	}
	c.routeReads.updatePlacement(st)
	c.routeReads.refreshes.Add(1)
	return nil
}

func (c *Client) fetchRouteStatus(ctx context.Context) (ClusterStatus, error) {
	resp, err := c.stub.ClusterStatus(c.withAuth(ctx), &cefaspb.ClusterStatusRequest{})
	if err == nil {
		return clusterStatusFromPB(resp), nil
	}
	last := err

	c.routeReads.mu.RLock()
	nodes := make([]*routeNode, 0, len(c.routeReads.nodes))
	for _, id := range c.routeReads.nodeOrder {
		if node := c.routeReads.nodes[id]; node != nil {
			nodes = append(nodes, node)
		}
	}
	c.routeReads.mu.RUnlock()
	for _, node := range nodes {
		resp, err := node.stub.ClusterStatus(c.withAuth(ctx), &cefaspb.ClusterStatusRequest{})
		if err == nil {
			return clusterStatusFromPB(resp), nil
		}
		last = err
	}
	return ClusterStatus{}, last
}

func (r *routeAwareReads) updatePlacement(st ClusterStatus) {
	shards := make([]routeShard, 0, len(st.Shards))
	for _, sh := range st.Shards {
		leader := sh.LeaderHint
		if leader == "" && len(sh.Voters) > 0 {
			leader = sh.Voters[0]
		}
		shards = append(shards, routeShard{
			id:     sh.ID,
			ranges: append([]TokenRange(nil), sh.Ranges...),
			voters: append([]string(nil), sh.Voters...),
			leader: leader,
		})
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i].id < shards[j].id })

	r.mu.Lock()
	r.shards = shards
	r.lastRefresh = time.Now()
	r.mu.Unlock()
}

func (c *Client) routeTableKeySchema(ctx context.Context, table string) (types.KeySchema, error) {
	r := c.routeReads
	r.mu.RLock()
	if ks, ok := r.schemaCache[table]; ok {
		r.mu.RUnlock()
		return ks, nil
	}
	r.mu.RUnlock()

	td, err := c.DescribeTable(ctx, table)
	if err != nil {
		return types.KeySchema{}, err
	}
	if td.KeySchema.PK == "" {
		return types.KeySchema{}, fmt.Errorf("table %q has no partition key", table)
	}

	r.mu.Lock()
	r.schemaCache[table] = td.KeySchema
	r.mu.Unlock()
	return td.KeySchema, nil
}

func (c *Client) pkBytesForKey(ctx context.Context, table string, key types.Item) ([]byte, error) {
	ks, err := c.routeTableKeySchema(ctx, table)
	if err != nil {
		return nil, err
	}
	pk, ok := key[ks.PK]
	if !ok {
		return nil, fmt.Errorf("key missing partition key %q", ks.PK)
	}
	return storage.AttrCanonicalBytes(pk)
}

func pkBytesForQuery(req *cefaspb.QueryRequest) ([]byte, bool, error) {
	if req.GetIndexName() != "" || req.GetConsistency() == cefaspb.Consistency_CONSISTENCY_STRONG || req.GetPkValue() == nil {
		return nil, false, nil
	}
	pkVal := attrFromPB(req.GetPkValue())
	pkBytes, err := storage.AttrCanonicalBytes(pkVal)
	return pkBytes, true, err
}

func (r *routeAwareReads) routeForPK(pkBytes []byte) (routeTarget, uint64, error) {
	token := xxhash.Sum64(pkBytes)
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, sh := range r.shards {
		for _, rng := range sh.ranges {
			if tokenRangeContains(rng, token) {
				return routeTarget{
					shardID: sh.id,
					leader:  sh.leader,
					voters:  append([]string(nil), sh.voters...),
				}, token, nil
			}
		}
	}
	return routeTarget{}, token, fmt.Errorf("route-aware reads: no shard for token %d", token)
}

func (r *routeAwareReads) candidatesForTarget(target routeTarget, token uint64, now time.Time) ([]*routeNode, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	candidates := make([]*routeNode, 0, len(target.voters))
	seen := map[string]struct{}{}
	for _, id := range target.voters {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if node := r.nodes[id]; node != nil {
			candidates = append(candidates, node)
		}
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("route-aware reads: shard %d has no configured voter endpoint", target.shardID)
	}
	healthy := candidates[:0]
	for _, node := range candidates {
		lastErr := node.lastErrorNanos.Load()
		if lastErr == 0 || now.Sub(time.Unix(0, lastErr)) >= routeAwareRecentErrorTTL {
			healthy = append(healthy, node)
		}
	}
	if len(healthy) > 0 {
		candidates = healthy
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		li := candidates[i].inflight.Load()
		lj := candidates[j].inflight.Load()
		if li != lj {
			return li < lj
		}
		di := candidates[i].latencyNanos.Load()
		dj := candidates[j].latencyNanos.Load()
		if di != dj {
			return di < dj
		}
		return stableNodeRank(candidates[i].id, token) < stableNodeRank(candidates[j].id, token)
	})
	return append([]*routeNode(nil), candidates...), nil
}

func stableNodeRank(nodeID string, token uint64) uint64 {
	return xxhash.Sum64String(fmt.Sprintf("%d/%s", token, nodeID))
}

func tokenRangeContains(r TokenRange, token uint64) bool {
	if r.Start == r.End {
		return true
	}
	if r.Start < r.End {
		return token >= r.Start && token < r.End
	}
	return token >= r.Start || token < r.End
}

func (node *routeNode) begin() time.Time {
	node.inflight.Add(1)
	return time.Now()
}

func (node *routeNode) finish(start time.Time, err error) {
	node.inflight.Add(-1)
	if err != nil {
		node.lastErrorNanos.Store(time.Now().UnixNano())
		return
	}
	elapsed := time.Since(start).Nanoseconds()
	for {
		cur := node.latencyNanos.Load()
		next := elapsed
		if cur > 0 {
			next = (cur*7 + elapsed) / 8
		}
		if node.latencyNanos.CompareAndSwap(cur, next) {
			return
		}
	}
}

func (r *routeAwareReads) observeServedBy(target routeTarget, nodeID string) {
	if target.leader != "" && target.leader == nodeID {
		r.leaderServed.Add(1)
		return
	}
	r.followerServed.Add(1)
}

func (r *routeAwareReads) observeRetry(err error) bool {
	reason, retry := routeAwareRetryReason(err)
	if !retry {
		return false
	}
	r.retries.Add(1)
	switch reason {
	case "stale_route":
		r.staleRoutes.Add(1)
	case "no_local_replica":
		r.noLocalReplica.Add(1)
	}
	return true
}

func routeAwareRetryReason(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	code := status.Code(err)
	msg := strings.ToLower(err.Error())
	switch code {
	case codes.Unavailable:
		if strings.Contains(msg, "no local replica") {
			return "no_local_replica", true
		}
		return "unavailable", true
	case codes.DeadlineExceeded:
		return "timeout", true
	case codes.Aborted:
		return "aborted", true
	case codes.FailedPrecondition:
		switch {
		case strings.Contains(msg, "stale route"):
			return "stale_route", true
		case strings.Contains(msg, "not leader"):
			return "not_leader", true
		case strings.Contains(msg, "no local replica"):
			return "no_local_replica", true
		}
	}
	return "", false
}
