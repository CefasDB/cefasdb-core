package client

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	routeAwareStrategyTokenRange   = "token-range-v1"
	routeAwareStrategyLegacyModulo = "legacy-modulo-v1"
	routeAwareErrorCooldown        = time.Second
)

type routeAwareReads struct {
	mu      sync.RWMutex
	nodes   map[string]*routeAwareNode
	status  ClusterStatus
	schemas map[string]types.KeySchema

	attempts  atomic.Uint64
	successes atomic.Uint64
	refreshes atomic.Uint64
	retries   atomic.Uint64
	bypasses  atomic.Uint64
	failures  atomic.Uint64
}

type routeAwareNode struct {
	id       string
	endpoint string
	conn     *grpc.ClientConn
	stub     cefaspb.CefasClient

	inFlight      atomic.Int64
	successes     atomic.Uint64
	failures      atomic.Uint64
	lastLatencyNs atomic.Int64
	lastErrorUnix atomic.Int64
}

// RouteAwareStats reports client-side routing counters for route-aware reads.
type RouteAwareStats struct {
	Attempts  uint64
	Successes uint64
	Refreshes uint64
	Retries   uint64
	Bypasses  uint64
	Failures  uint64
	Nodes     []RouteAwareNodeStats
}

// RouteAwareNodeStats reports per-node routing counters.
type RouteAwareNodeStats struct {
	ID            string
	Endpoint      string
	InFlight      int64
	Successes     uint64
	Failures      uint64
	LastLatency   time.Duration
	LastErrorUnix int64
}

func routeAwareEnabled(route *routeAwareReads, override *bool) bool {
	if override != nil {
		return *override && route != nil
	}
	return route != nil
}

func newRouteAwareReads(endpoints map[string]string, dialOpts []grpc.DialOption) (*routeAwareReads, error) {
	r := &routeAwareReads{
		nodes:   make(map[string]*routeAwareNode, len(endpoints)),
		schemas: make(map[string]types.KeySchema),
	}
	for id, endpoint := range endpoints {
		if id == "" || endpoint == "" {
			continue
		}
		conn, err := grpc.NewClient(endpoint, append([]grpc.DialOption(nil), dialOpts...)...)
		if err != nil {
			_ = r.close()
			return nil, fmt.Errorf("cefas: route-aware dial %s (%s): %w", id, endpoint, err)
		}
		r.nodes[id] = &routeAwareNode{
			id:       id,
			endpoint: endpoint,
			conn:     conn,
			stub:     cefaspb.NewCefasClient(conn),
		}
	}
	if len(r.nodes) == 0 {
		return nil, nil
	}
	return r, nil
}

func (r *routeAwareReads) close() error {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	nodes := make([]*routeAwareNode, 0, len(r.nodes))
	for _, n := range r.nodes {
		nodes = append(nodes, n)
	}
	r.mu.RUnlock()
	var first error
	for _, n := range nodes {
		if err := n.conn.Close(); first == nil {
			first = err
		}
	}
	return first
}

// RouteAwareStats returns a snapshot of route-aware read counters. The zero
// value means route-aware reads are disabled on this client.
func (c *Client) RouteAwareStats() RouteAwareStats {
	if c == nil || c.route == nil {
		return RouteAwareStats{}
	}
	return c.route.stats()
}

func (r *routeAwareReads) stats() RouteAwareStats {
	out := RouteAwareStats{
		Attempts:  r.attempts.Load(),
		Successes: r.successes.Load(),
		Refreshes: r.refreshes.Load(),
		Retries:   r.retries.Load(),
		Bypasses:  r.bypasses.Load(),
		Failures:  r.failures.Load(),
	}
	r.mu.RLock()
	ids := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		n := r.nodes[id]
		out.Nodes = append(out.Nodes, RouteAwareNodeStats{
			ID:            n.id,
			Endpoint:      n.endpoint,
			InFlight:      n.inFlight.Load(),
			Successes:     n.successes.Load(),
			Failures:      n.failures.Load(),
			LastLatency:   time.Duration(n.lastLatencyNs.Load()),
			LastErrorUnix: n.lastErrorUnix.Load(),
		})
	}
	r.mu.RUnlock()
	return out
}

func (r *routeAwareReads) schema(ctx context.Context, c *Client, table string) (types.KeySchema, error) {
	r.mu.RLock()
	if ks, ok := r.schemas[table]; ok {
		r.mu.RUnlock()
		return ks, nil
	}
	r.mu.RUnlock()

	td, err := c.DescribeTable(ctx, table)
	if err != nil {
		return types.KeySchema{}, err
	}
	r.mu.Lock()
	r.schemas[table] = td.KeySchema
	r.mu.Unlock()
	return td.KeySchema, nil
}

func (r *routeAwareReads) pkBytesForKey(ctx context.Context, c *Client, table string, key types.Item) ([]byte, error) {
	ks, err := r.schema(ctx, c, table)
	if err != nil {
		return nil, err
	}
	pkAttr, ok := key[ks.PK]
	if !ok {
		return nil, fmt.Errorf("%w: PK %q", types.ErrMissingKey, ks.PK)
	}
	return storage.AttrCanonicalBytes(pkAttr)
}

func (r *routeAwareReads) refresh(ctx context.Context, c *Client) error {
	st, err := c.Status(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.status = st
	r.refreshes.Add(1)
	r.mu.Unlock()
	return nil
}

func (r *routeAwareReads) ensureStatus(ctx context.Context, c *Client) error {
	r.mu.RLock()
	ok := r.status.ShardCount > 0 && len(r.status.Shards) > 0
	r.mu.RUnlock()
	if ok {
		return nil
	}
	return r.refresh(ctx, c)
}

func (r *routeAwareReads) routeForPK(ctx context.Context, c *Client, pkBytes []byte) (routeAwareTarget, error) {
	if err := r.ensureStatus(ctx, c); err != nil {
		return routeAwareTarget{}, err
	}
	token := xxhash.Sum64(pkBytes)
	r.mu.RLock()
	defer r.mu.RUnlock()
	st := r.status
	if st.ShardCount <= 0 || len(st.Shards) == 0 {
		return routeAwareTarget{}, fmt.Errorf("cefas: route-aware placement unavailable")
	}
	if st.PlacementStrategy == routeAwareStrategyLegacyModulo {
		idx := int(token % uint64(len(st.Shards)))
		if idx < len(st.Shards) {
			return targetFromShard(st.Shards[idx]), nil
		}
	}
	for _, sh := range st.Shards {
		if !routeAwareShardRoutable(sh.State) {
			continue
		}
		for _, rng := range sh.Ranges {
			if routeAwareRangeContains(rng, token) {
				return targetFromShard(sh), nil
			}
		}
	}
	return routeAwareTarget{}, fmt.Errorf("cefas: no shard owns token %d", token)
}

type routeAwareTarget struct {
	voters []string
}

func targetFromShard(sh ShardPlacement) routeAwareTarget {
	return routeAwareTarget{voters: append([]string(nil), sh.Voters...)}
}

func routeAwareShardRoutable(state string) bool {
	switch state {
	case "", "active", "splitting", "moving", "read_only":
		return true
	default:
		return false
	}
}

func routeAwareRangeContains(r TokenRange, token uint64) bool {
	if r.Start == r.End {
		return true
	}
	if r.Start < r.End {
		return token >= r.Start && token < r.End
	}
	return token >= r.Start || token < r.End
}

func (r *routeAwareReads) candidateOrder(target routeAwareTarget, pkBytes []byte) []*routeAwareNode {
	r.mu.RLock()
	defer r.mu.RUnlock()
	candidates := make([]*routeAwareNode, 0, len(target.voters))
	for _, voter := range target.voters {
		if n := r.nodes[voter]; n != nil {
			candidates = append(candidates, n)
		}
	}
	now := time.Now().Unix()
	sort.SliceStable(candidates, func(i, j int) bool {
		a := candidates[i]
		b := candidates[j]
		aHealthy := now-a.lastErrorUnix.Load() >= int64(routeAwareErrorCooldown/time.Second)
		bHealthy := now-b.lastErrorUnix.Load() >= int64(routeAwareErrorCooldown/time.Second)
		if aHealthy != bHealthy {
			return aHealthy
		}
		if ai, bi := a.inFlight.Load(), b.inFlight.Load(); ai != bi {
			return ai < bi
		}
		if al, bl := a.lastLatencyNs.Load(), b.lastLatencyNs.Load(); al != bl {
			return al < bl
		}
		return stableNodeScore(pkBytes, a.id) < stableNodeScore(pkBytes, b.id)
	})
	return candidates
}

func stableNodeScore(pkBytes []byte, nodeID string) uint64 {
	h := xxhash.New()
	_, _ = h.Write(pkBytes)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(nodeID))
	return h.Sum64()
}

func (n *routeAwareNode) start() time.Time {
	n.inFlight.Add(1)
	return time.Now()
}

func (n *routeAwareNode) finish(start time.Time, err error) {
	n.inFlight.Add(-1)
	if err == nil {
		n.successes.Add(1)
		n.lastLatencyNs.Store(time.Since(start).Nanoseconds())
		n.lastErrorUnix.Store(0)
		return
	}
	n.failures.Add(1)
	n.lastErrorUnix.Store(time.Now().Unix())
}

func routeAwareRetryable(err error) bool {
	if err == nil {
		return false
	}
	code := status.Code(err)
	switch code {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	case codes.FailedPrecondition:
		msg := strings.ToLower(status.Convert(err).Message())
		return strings.Contains(msg, "stale") || strings.Contains(msg, "not leader") || strings.Contains(msg, "retry")
	default:
		return false
	}
}

func (c *Client) routeAwareGetItem(ctx context.Context, table string, key types.Item) (types.Item, bool, error) {
	r := c.route
	if r == nil {
		return nil, false, nil
	}
	pkBytes, err := r.pkBytesForKey(ctx, c, table, key)
	if err != nil {
		r.bypasses.Add(1)
		return nil, false, nil
	}
	req := &cefaspb.GetItemRequest{
		Table:       table,
		Key:         itemAttrMap(key),
		Consistency: cefaspb.Consistency_CONSISTENCY_EVENTUAL,
	}
	call := func(node *routeAwareNode) (types.Item, error) {
		start := node.start()
		resp, err := node.stub.GetItem(c.withAuth(ctx), req)
		node.finish(start, err)
		if err != nil {
			return nil, err
		}
		if !resp.GetFound() {
			return nil, nil
		}
		return itemFromPB(resp.GetItem()), nil
	}
	item, handled, err := routeAwareUnary(ctx, c, pkBytes, call)
	return item, handled, err
}

type routeAwareUnaryFunc[T any] func(*routeAwareNode) (T, error)

func routeAwareUnary[T any](ctx context.Context, c *Client, pkBytes []byte, call routeAwareUnaryFunc[T]) (T, bool, error) {
	var zero T
	r := c.route
	if r == nil {
		return zero, false, nil
	}
	r.attempts.Add(1)
	var last error
	refreshed := false
	for {
		target, err := r.routeForPK(ctx, c, pkBytes)
		if err != nil {
			r.bypasses.Add(1)
			return zero, false, nil
		}
		candidates := r.candidateOrder(target, pkBytes)
		if len(candidates) == 0 {
			r.bypasses.Add(1)
			return zero, false, nil
		}
		sawRetryable := false
		for _, node := range candidates {
			out, err := call(node)
			if err == nil {
				r.successes.Add(1)
				return out, true, nil
			}
			last = err
			if routeAwareRetryable(err) {
				sawRetryable = true
				continue
			}
			r.failures.Add(1)
			return zero, true, err
		}
		if sawRetryable && !refreshed {
			r.retries.Add(1)
			refreshed = true
			if err := r.refresh(ctx, c); err != nil {
				r.failures.Add(1)
				if last != nil {
					return zero, true, last
				}
				return zero, true, err
			}
			continue
		}
		r.failures.Add(1)
		if last != nil {
			return zero, true, last
		}
		return zero, true, fmt.Errorf("cefas: shard has no reachable route-aware read replica")
	}
}

type routeAwareBatchGroup struct {
	node *routeAwareNode
	keys []*cefaspb.KeyMap
	idxs []int
}

func (c *Client) routeAwareBatchGetItem(ctx context.Context, table string, keys []types.Item) ([]types.Item, bool, error) {
	r := c.route
	if r == nil || len(keys) == 0 {
		return nil, false, nil
	}
	pkByIndex := make([][]byte, len(keys))
	for i, key := range keys {
		pkBytes, err := r.pkBytesForKey(ctx, c, table, key)
		if err != nil {
			r.bypasses.Add(1)
			return nil, false, nil
		}
		pkByIndex[i] = pkBytes
	}

	var last error
	refreshed := false
	for {
		groups := map[string]*routeAwareBatchGroup{}
		for i, pkBytes := range pkByIndex {
			target, err := r.routeForPK(ctx, c, pkBytes)
			if err != nil {
				r.bypasses.Add(1)
				return nil, false, nil
			}
			candidates := r.candidateOrder(target, pkBytes)
			if len(candidates) == 0 {
				r.bypasses.Add(1)
				return nil, false, nil
			}
			node := candidates[0]
			group := groups[node.id]
			if group == nil {
				group = &routeAwareBatchGroup{node: node}
				groups[node.id] = group
			}
			group.keys = append(group.keys, &cefaspb.KeyMap{Attributes: itemAttrMap(keys[i])})
			group.idxs = append(group.idxs, i)
		}

		r.attempts.Add(1)
		out := make([]types.Item, len(keys))
		sawRetryable := false
		for _, group := range groups {
			req := &cefaspb.BatchGetItemRequest{Table: table, Keys: group.keys}
			start := group.node.start()
			resp, err := group.node.stub.BatchGetItem(c.withAuth(ctx), req)
			group.node.finish(start, err)
			if err != nil {
				last = err
				if routeAwareRetryable(err) {
					sawRetryable = true
					continue
				}
				r.failures.Add(1)
				return nil, true, err
			}
			for i, pbItem := range resp.GetItems() {
				if i >= len(group.idxs) {
					break
				}
				if len(pbItem.GetAttributes()) == 0 {
					continue
				}
				out[group.idxs[i]] = itemFromPB(pbItem.GetAttributes())
			}
		}
		if !sawRetryable {
			r.successes.Add(1)
			return out, true, nil
		}
		if !refreshed {
			r.retries.Add(1)
			refreshed = true
			if err := r.refresh(ctx, c); err != nil {
				r.failures.Add(1)
				if last != nil {
					return nil, true, last
				}
				return nil, true, err
			}
			continue
		}
		r.failures.Add(1)
		return nil, true, last
	}
}

func (c *Client) routeAwareQuery(ctx context.Context, req *cefaspb.QueryRequest) (grpc.ServerStreamingClient[cefaspb.Item], bool, error) {
	r := c.route
	if r == nil || req.GetIndexName() != "" || req.GetPkValue() == nil {
		return nil, false, nil
	}
	pkBytes, err := storage.AttrCanonicalBytes(attrFromPB(req.GetPkValue()))
	if err != nil {
		r.bypasses.Add(1)
		return nil, false, nil
	}
	call := func(node *routeAwareNode) (grpc.ServerStreamingClient[cefaspb.Item], error) {
		start := node.start()
		stream, err := node.stub.Query(c.withAuth(ctx), req)
		if err != nil {
			node.finish(start, err)
			return nil, err
		}
		return &routeAwareItemStream{ServerStreamingClient: stream, node: node, start: start}, nil
	}
	return routeAwareUnary(ctx, c, pkBytes, call)
}

type routeAwareItemStream struct {
	grpc.ServerStreamingClient[cefaspb.Item]
	node  *routeAwareNode
	start time.Time
	done  atomic.Bool
}

func (s *routeAwareItemStream) Recv() (*cefaspb.Item, error) {
	item, err := s.ServerStreamingClient.Recv()
	if err != nil && s.done.CompareAndSwap(false, true) {
		if errors.Is(err, io.EOF) {
			s.node.finish(s.start, nil)
		} else {
			s.node.finish(s.start, err)
		}
	}
	return item, err
}

func (s *routeAwareItemStream) CloseSend() error {
	err := s.ServerStreamingClient.CloseSend()
	if s.done.CompareAndSwap(false, true) {
		s.node.finish(s.start, err)
	}
	return err
}
