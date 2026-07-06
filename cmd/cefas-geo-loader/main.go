package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type cfg struct {
	Nodes          string
	ConsumersTable string
	EventsTable    string
	GeoIndex       string

	ConsumerTotal   int64
	ConsumerStart   int64
	SeedConsumers   bool
	EventLoop       bool
	WarmGeoIndex    bool
	ConsumerWorkers int
	EventWorkers    int
	BatchSize       int
	ConsumerBatch   int
	EventBatch      int
	RPCTimeout      time.Duration
	Progress        time.Duration
	RunID           string

	MinLat float64
	MaxLat float64
	MinLon float64
	MaxLon float64

	GeoPrecision int
}

type tokenRange struct {
	Start uint64
	End   uint64
}

type shardRoute struct {
	ID         uint32
	Ranges     []tokenRange
	Voters     []string
	LeaderHint string
}

type routeTarget struct {
	ShardID uint32
	Leader  string
	Voters  []string
}

type router struct {
	shards []shardRoute
}

type clients struct {
	byNode map[string]*client.Client
	addrs  map[string]string
	order  []string
}

type counters struct {
	consumerWrites atomic.Int64
	eventWrites    atomic.Int64
	writeErrors    atomic.Int64
}

func main() {
	c := parseFlags()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	nodeAddrs, err := parseNodeAddrs(c.Nodes)
	if err != nil {
		log.Fatalf("parse nodes: %v", err)
	}
	cs, err := dialClients(ctx, nodeAddrs)
	if err != nil {
		log.Fatalf("dial clients: %v", err)
	}
	defer cs.close()

	st, err := fetchStatus(ctx, cs)
	if err != nil {
		log.Fatalf("cluster status: %v", err)
	}
	rt, shards, err := routerFromStatus(st)
	if err != nil {
		log.Fatalf("build router: %v", err)
	}
	log.Printf("cluster placement: shards=%d strategy=%s version=%d epoch=%d", st.ShardCount, st.PlacementStrategy, st.PlacementVersion, st.RoutingEpoch)

	if err := ensureSchema(ctx, c, cs, shards); err != nil {
		log.Fatalf("ensure schema: %v", err)
	}
	if c.WarmGeoIndex {
		if err := warmGeoIndex(ctx, c, cs); err != nil {
			log.Fatalf("warm geo index: %v", err)
		}
	}

	var ctr counters
	progressDone := startProgress(ctx, c, &ctr)
	defer progressDone()

	if c.SeedConsumers {
		log.Printf("consumer seed starting: table=%s start=%d total=%d workers=%d batch=%d", c.ConsumersTable, c.ConsumerStart, c.ConsumerTotal, c.ConsumerWorkers, c.ConsumerBatch)
		if err := seedConsumers(ctx, c, cs, rt, &ctr); err != nil {
			log.Fatalf("consumer seed failed: %v", err)
		}
		log.Printf("consumer seed complete: wrote=%d", ctr.consumerWrites.Load())
	}

	if c.EventLoop {
		log.Printf("event loop starting: table=%s index=%s workers=%d batch=%d area=[lat %.5f..%.5f lon %.5f..%.5f]",
			c.EventsTable, c.GeoIndex, c.EventWorkers, c.EventBatch, c.MinLat, c.MaxLat, c.MinLon, c.MaxLon)
		if err := runEventLoop(ctx, c, cs, rt, &ctr); err != nil && !errors.Is(err, context.Canceled) {
			log.Fatalf("event loop failed: %v", err)
		}
	}
}

func parseFlags() cfg {
	c := cfg{}
	defaultRunID := time.Now().UTC().Format("20060102T150405Z")
	flag.StringVar(&c.Nodes, "nodes", "", "comma-separated nodeID=gRPC address map")
	flag.StringVar(&c.ConsumersTable, "consumers-table", "Consumers", "consumers table name")
	flag.StringVar(&c.EventsTable, "events-table", "ConsumerGeoEvents", "geolocation events table name")
	flag.StringVar(&c.GeoIndex, "geo-index", "loc_geo", "geohash index name")
	flag.Int64Var(&c.ConsumerTotal, "consumer-total", 40_000_000, "number of consumers to seed")
	flag.Int64Var(&c.ConsumerStart, "consumer-start", 0, "first consumer numeric id")
	flag.BoolVar(&c.SeedConsumers, "seed-consumers", true, "seed the consumers table before event loop")
	flag.BoolVar(&c.EventLoop, "event-loop", true, "write geolocation events forever after setup")
	flag.BoolVar(&c.WarmGeoIndex, "warm-geo-index", true, "query the geohash index on every node before event writes")
	flag.IntVar(&c.ConsumerWorkers, "consumer-workers", 64, "consumer seed workers")
	flag.IntVar(&c.EventWorkers, "event-workers", 64, "continuous event workers")
	flag.IntVar(&c.BatchSize, "batch-size", 500, "items per generated batch before shard grouping")
	flag.IntVar(&c.ConsumerBatch, "consumer-batch-size", 0, "consumer seed batch size; 0 inherits -batch-size")
	flag.IntVar(&c.EventBatch, "event-batch-size", 0, "event loop batch size; 0 inherits -batch-size")
	flag.DurationVar(&c.RPCTimeout, "rpc-timeout", 30*time.Second, "timeout per write RPC")
	flag.DurationVar(&c.Progress, "progress", 10*time.Second, "progress interval; 0 disables")
	flag.StringVar(&c.RunID, "run-id", defaultRunID, "run id embedded in event IDs")
	flag.Float64Var(&c.MinLat, "min-lat", -24.05, "minimum latitude for simulated Grande Sao Paulo points")
	flag.Float64Var(&c.MaxLat, "max-lat", -23.30, "maximum latitude for simulated Grande Sao Paulo points")
	flag.Float64Var(&c.MinLon, "min-lon", -47.15, "minimum longitude for simulated Grande Sao Paulo points")
	flag.Float64Var(&c.MaxLon, "max-lon", -46.20, "maximum longitude for simulated Grande Sao Paulo points")
	flag.IntVar(&c.GeoPrecision, "geo-precision", 6, "geohash precision")
	flag.Parse()

	if c.Nodes == "" {
		log.Fatal("-nodes is required")
	}
	if c.ConsumerTotal < 0 || c.ConsumerStart < 0 {
		log.Fatal("-consumer-total and -consumer-start must be >= 0")
	}
	if c.ConsumerBatch == 0 {
		c.ConsumerBatch = c.BatchSize
	}
	if c.EventBatch == 0 {
		c.EventBatch = c.BatchSize
	}
	if c.ConsumerWorkers <= 0 || c.EventWorkers <= 0 || c.BatchSize <= 0 || c.ConsumerBatch <= 0 || c.EventBatch <= 0 {
		log.Fatal("-consumer-workers, -event-workers, -batch-size, -consumer-batch-size and -event-batch-size must be > 0")
	}
	if c.MinLat > c.MaxLat || c.MinLon > c.MaxLon {
		log.Fatal("invalid geo bounds")
	}
	if c.GeoPrecision < 1 || c.GeoPrecision > 12 {
		log.Fatal("-geo-precision must be in [1,12]")
	}
	return c
}

func parseNodeAddrs(raw string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok || strings.TrimSpace(k) == "" || strings.TrimSpace(v) == "" {
			return nil, fmt.Errorf("invalid node mapping %q", part)
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	if len(out) == 0 {
		return nil, errors.New("no nodes")
	}
	return out, nil
}

func dialClients(ctx context.Context, nodeAddrs map[string]string) (*clients, error) {
	ids := make([]string, 0, len(nodeAddrs))
	for id := range nodeAddrs {
		ids = append(ids, id)
	}
	sortStrings(ids)
	cs := &clients{byNode: map[string]*client.Client{}, addrs: nodeAddrs, order: ids}
	for _, id := range ids {
		cli, err := client.Dial(ctx, nodeAddrs[id], client.WithPlaintext())
		if err != nil {
			cs.close()
			return nil, fmt.Errorf("%s=%s: %w", id, nodeAddrs[id], err)
		}
		cs.byNode[id] = cli
	}
	return cs, nil
}

func (c *clients) close() {
	for _, cli := range c.byNode {
		_ = cli.Close()
	}
}

func sortStrings(v []string) {
	for i := 1; i < len(v); i++ {
		for j := i; j > 0 && v[j] < v[j-1]; j-- {
			v[j], v[j-1] = v[j-1], v[j]
		}
	}
}

func fetchStatus(ctx context.Context, cs *clients) (client.ClusterStatus, error) {
	var last error
	for _, id := range cs.order {
		rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		st, err := cs.byNode[id].Status(rpcCtx)
		cancel()
		if err == nil {
			return st, nil
		}
		last = err
	}
	return client.ClusterStatus{}, last
}

func routerFromStatus(st client.ClusterStatus) (*router, []shardRoute, error) {
	if st.ShardCount <= 0 || len(st.Shards) == 0 {
		return nil, nil, errors.New("cluster status has no shard placement")
	}
	shards := make([]shardRoute, 0, len(st.Shards))
	for _, sh := range st.Shards {
		leader := sh.LeaderHint
		if leader == "" && len(sh.Voters) > 0 {
			leader = sh.Voters[0]
		}
		if leader == "" {
			return nil, nil, fmt.Errorf("shard %d has no leader hint or voters", sh.ID)
		}
		ranges := make([]tokenRange, 0, len(sh.Ranges))
		for _, r := range sh.Ranges {
			ranges = append(ranges, tokenRange{Start: r.Start, End: r.End})
		}
		shards = append(shards, shardRoute{
			ID:         sh.ID,
			Ranges:     ranges,
			Voters:     append([]string(nil), sh.Voters...),
			LeaderHint: leader,
		})
	}
	return &router{shards: shards}, shards, nil
}

func ensureSchema(ctx context.Context, c cfg, cs *clients, shards []shardRoute) error {
	metadata := routeTarget{ShardID: shards[0].ID, Leader: shards[0].LeaderHint, Voters: append([]string(nil), shards[0].Voters...)}
	consumers := types.TableDescriptor{
		Name:      c.ConsumersTable,
		KeySchema: types.KeySchema{PK: "consumerID"},
	}
	events := types.TableDescriptor{
		Name:      c.EventsTable,
		KeySchema: types.KeySchema{PK: "consumerID", SK: "eventID"},
	}
	if node, created, err := createTableWithRetry(ctx, c, cs, metadata, consumers); err != nil {
		return fmt.Errorf("create consumers table: %w", err)
	} else if created {
		log.Printf("created table %s via %s", c.ConsumersTable, node)
	} else {
		log.Printf("using existing table %s via %s", c.ConsumersTable, node)
	}
	if node, created, err := createTableWithRetry(ctx, c, cs, metadata, events); err != nil {
		return fmt.Errorf("create events table: %w", err)
	} else if created {
		log.Printf("created table %s via %s", c.EventsTable, node)
	} else {
		log.Printf("using existing table %s via %s", c.EventsTable, node)
	}
	if node, created, err := createGeoIndexWithRetry(ctx, c, cs, metadata); err != nil {
		return fmt.Errorf("create geo index: %w", err)
	} else if created {
		log.Printf("created geohash index %s/%s precision=%d via %s", c.EventsTable, c.GeoIndex, c.GeoPrecision, node)
	} else {
		log.Printf("using existing geohash index %s/%s via %s", c.EventsTable, c.GeoIndex, node)
	}
	return nil
}

func createTableWithRetry(ctx context.Context, c cfg, cs *clients, target routeTarget, td types.TableDescriptor) (string, bool, error) {
	var last error
	deadline := time.Now().Add(c.RPCTimeout)
	for time.Now().Before(deadline) {
		for _, node := range retryOrder(target, cs.order) {
			cli := cs.byNode[node]
			if cli == nil {
				continue
			}
			rpcCtx, cancel := context.WithTimeout(ctx, minDuration(c.RPCTimeout, 10*time.Second))
			err := cli.CreateTable(rpcCtx, td)
			cancel()
			if err == nil {
				return node, true, nil
			}
			if isAlreadyExists(err) {
				return node, false, nil
			}
			last = err
		}
		if !sleepOrDone(ctx, 100*time.Millisecond) {
			break
		}
	}
	if last != nil {
		return "", false, last
	}
	return "", false, ctx.Err()
}

func createGeoIndexWithRetry(ctx context.Context, c cfg, cs *clients, target routeTarget) (string, bool, error) {
	desc := client.PluginIndex{
		Table:        c.EventsTable,
		Name:         c.GeoIndex,
		PluginName:   "geohash",
		PluginConfig: []byte(fmt.Sprintf(`{"field":"loc","precision":%d}`, c.GeoPrecision)),
		KeySchema:    types.KeySchema{PK: "consumerID", SK: "eventID"},
	}
	var last error
	deadline := time.Now().Add(c.RPCTimeout)
	for time.Now().Before(deadline) {
		for _, node := range retryOrder(target, cs.order) {
			cli := cs.byNode[node]
			if cli == nil {
				continue
			}
			rpcCtx, cancel := context.WithTimeout(ctx, minDuration(c.RPCTimeout, 10*time.Second))
			_, err := cli.CreateIndex(rpcCtx, desc)
			cancel()
			if err == nil {
				return node, true, nil
			}
			if isAlreadyExists(err) {
				return node, false, nil
			}
			last = err
		}
		if !sleepOrDone(ctx, 100*time.Millisecond) {
			break
		}
	}
	if last != nil {
		return "", false, last
	}
	return "", false, ctx.Err()
}

func warmGeoIndex(ctx context.Context, c cfg, cs *clients) error {
	for _, node := range cs.order {
		cli := cs.byNode[node]
		if cli == nil {
			continue
		}
		rpcCtx, cancel := context.WithTimeout(ctx, minDuration(c.RPCTimeout, 15*time.Second))
		_, err := cli.GeoAudience(rpcCtx, client.GeoAudienceRequest{
			Table:        c.EventsTable,
			Index:        c.GeoIndex,
			Lat:          (c.MinLat + c.MaxLat) / 2,
			Lon:          (c.MinLon + c.MaxLon) / 2,
			RadiusMeters: 1,
			Limit:        1,
		})
		cancel()
		if err != nil {
			return fmt.Errorf("%s: %w", node, err)
		}
		log.Printf("warmed geohash index %s/%s via %s", c.EventsTable, c.GeoIndex, node)
	}
	return nil
}

func seedConsumers(ctx context.Context, c cfg, cs *clients, rt *router, ctr *counters) error {
	jobs := make(chan [2]int64, c.ConsumerWorkers*2)
	var wg sync.WaitGroup
	var firstErr error
	var firstErrOnce sync.Once
	seedCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for i := 0; i < c.ConsumerWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if err := writeConsumerRange(seedCtx, c, cs, rt, ctr, job[0], job[1]); err != nil {
					ctr.writeErrors.Add(1)
					firstErrOnce.Do(func() { firstErr = err })
					cancel()
					return
				}
			}
		}()
	}
	endTotal := c.ConsumerStart + c.ConsumerTotal
	for start := c.ConsumerStart; start < endTotal; start += int64(c.ConsumerBatch) {
		end := start + int64(c.ConsumerBatch)
		if end > endTotal {
			end = endTotal
		}
		select {
		case <-seedCtx.Done():
			close(jobs)
			wg.Wait()
			return firstErr
		case jobs <- [2]int64{start, end}:
		}
	}
	close(jobs)
	wg.Wait()
	return firstErr
}

func writeConsumerRange(ctx context.Context, c cfg, cs *clients, rt *router, ctr *counters, start, end int64) error {
	groups := map[uint32]*shardBatch{}
	for id := start; id < end; id++ {
		consumerID := consumerID(id)
		target, err := rt.routeForPK(sAttr(consumerID))
		if err != nil {
			return err
		}
		group := groups[target.ShardID]
		if group == nil {
			group = &shardBatch{target: target}
			groups[target.ShardID] = group
		}
		group.ops = append(group.ops, client.BatchWriteOp{Put: types.Item{
			"consumerID": sAttr(consumerID),
			"status":     sAttr("active"),
			"createdAt":  nAttr(strconv.FormatInt(1_700_000_000+(id%31_536_000), 10)),
		}})
	}
	return writeGroups(ctx, c, cs, c.ConsumersTable, groups, func(n int) {
		ctr.consumerWrites.Add(int64(n))
	})
}

type shardBatch struct {
	target routeTarget
	ops    []client.BatchWriteOp
}

func runEventLoop(ctx context.Context, c cfg, cs *clients, rt *router, ctr *counters) error {
	var wg sync.WaitGroup
	eventCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var firstErr error
	var firstErrOnce sync.Once
	var seq atomic.Int64
	for i := 0; i < c.EventWorkers; i++ {
		workerID := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*7919))
			for eventCtx.Err() == nil {
				if err := writeEventBatch(eventCtx, c, cs, rt, ctr, rng, &seq, workerID); err != nil {
					ctr.writeErrors.Add(1)
					firstErrOnce.Do(func() { firstErr = err })
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return eventCtx.Err()
}

func writeEventBatch(ctx context.Context, c cfg, cs *clients, rt *router, ctr *counters, rng *rand.Rand, seq *atomic.Int64, workerID int) error {
	groups := map[uint32]*shardBatch{}
	nowMillis := time.Now().UnixMilli()
	for i := 0; i < c.EventBatch; i++ {
		consumerNum := c.ConsumerStart + rng.Int63n(max64(c.ConsumerTotal, 1))
		cid := consumerID(consumerNum)
		lat := c.MinLat + rng.Float64()*(c.MaxLat-c.MinLat)
		lon := c.MinLon + rng.Float64()*(c.MaxLon-c.MinLon)
		id := seq.Add(1)
		eventID := fmt.Sprintf("%s-w%02d-%020d", c.RunID, workerID, id)
		target, err := rt.routeForPK(sAttr(cid))
		if err != nil {
			return err
		}
		group := groups[target.ShardID]
		if group == nil {
			group = &shardBatch{target: target}
			groups[target.ShardID] = group
		}
		latText := strconv.FormatFloat(lat, 'f', 6, 64)
		lonText := strconv.FormatFloat(lon, 'f', 6, 64)
		group.ops = append(group.ops, client.BatchWriteOp{Put: types.Item{
			"consumerID": sAttr(cid),
			"eventID":    sAttr(eventID),
			"ts":         nAttr(strconv.FormatInt(nowMillis+int64(i), 10)),
			"Lat":        nAttr(latText),
			"Long":       nAttr(lonText),
			"loc": mAttr(map[string]types.AttributeValue{
				"lat": nAttr(latText),
				"lon": nAttr(lonText),
			}),
		}})
	}
	return writeGroups(ctx, c, cs, c.EventsTable, groups, func(n int) {
		ctr.eventWrites.Add(int64(n))
	})
}

func writeGroups(ctx context.Context, c cfg, cs *clients, table string, groups map[uint32]*shardBatch, observe func(int)) error {
	for _, group := range groups {
		rpcCtx, cancel := context.WithTimeout(ctx, c.RPCTimeout)
		err := batchWriteWithRetry(rpcCtx, cs, group.target, table, group.ops)
		cancel()
		if err != nil {
			return err
		}
		observe(len(group.ops))
	}
	return nil
}

func batchWriteWithRetry(ctx context.Context, cs *clients, target routeTarget, table string, ops []client.BatchWriteOp) error {
	var last error
	candidates := retryOrder(target, cs.order)
	for {
		for _, node := range candidates {
			cli := cs.byNode[node]
			if cli == nil {
				continue
			}
			err := cli.BatchWriteItem(ctx, table, ops)
			if err == nil {
				return nil
			}
			last = err
			if !isRetryableWrite(err) {
				return fmt.Errorf("shard %d node %s: %w", target.ShardID, node, err)
			}
		}
		select {
		case <-ctx.Done():
			if last != nil {
				return fmt.Errorf("shard %d candidates=%v: %w", target.ShardID, candidates, last)
			}
			return ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func (r *router) routeForPK(pk types.AttributeValue) (routeTarget, error) {
	pkBytes, err := storage.AttrCanonicalBytes(pk)
	if err != nil {
		return routeTarget{}, err
	}
	token := xxhash.Sum64(pkBytes)
	for _, sh := range r.shards {
		for _, rng := range sh.Ranges {
			if contains(rng, token) {
				return routeTarget{
					ShardID: sh.ID,
					Leader:  sh.LeaderHint,
					Voters:  append([]string(nil), sh.Voters...),
				}, nil
			}
		}
	}
	return routeTarget{}, fmt.Errorf("no shard for token=%d", token)
}

func contains(r tokenRange, token uint64) bool {
	if r.Start <= r.End {
		return token >= r.Start && token < r.End
	}
	return token >= r.Start || token < r.End
}

func retryOrder(target routeTarget, fallback []string) []string {
	out := make([]string, 0, 1+len(target.Voters)+len(fallback))
	seen := map[string]struct{}{}
	add := func(id string) {
		if id == "" {
			return
		}
		if _, ok := seen[id]; ok {
			return
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	add(target.Leader)
	for _, id := range target.Voters {
		add(id)
	}
	for _, id := range fallback {
		add(id)
	}
	return out
}

func isRetryableWrite(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.FailedPrecondition:
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not leader") ||
		strings.Contains(msg, "no local replica") ||
		strings.Contains(msg, "primary write shard is not open locally") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "leadership lost")
}

func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) == codes.AlreadyExists {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "already") || strings.Contains(msg, "exist")
}

func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func startProgress(ctx context.Context, c cfg, ctr *counters) func() {
	if c.Progress <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(c.Progress)
		defer t.Stop()
		var lastConsumers, lastEvents int64
		var lastAt = time.Now()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case now := <-t.C:
				consumers := ctr.consumerWrites.Load()
				events := ctr.eventWrites.Load()
				elapsed := now.Sub(lastAt).Seconds()
				consumerRate := float64(consumers-lastConsumers) / elapsed
				eventRate := float64(events-lastEvents) / elapsed
				log.Printf("progress consumers=%d (+%.0f/s) events=%d (+%.0f/s) errors=%d",
					consumers, consumerRate, events, eventRate, ctr.writeErrors.Load())
				lastConsumers, lastEvents, lastAt = consumers, events, now
			}
		}
	}()
	return func() { close(done) }
}

func consumerID(id int64) string {
	return fmt.Sprintf("consumer-%08d", id)
}

func sAttr(v string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: v}
}

func nAttr(v string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrN, N: v}
}

func mAttr(v map[string]types.AttributeValue) types.AttributeValue {
	return types.AttributeValue{T: types.AttrM, M: v}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
