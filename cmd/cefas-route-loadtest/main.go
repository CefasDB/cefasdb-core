package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/grpc"

	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type cfg struct {
	Nodes             string
	Table             string
	Items             int64
	Keyspace          int64
	MixedDuration     time.Duration
	BatchSize         int
	Workers           int
	ReadWorkers       int
	WriteRate         int64
	ReadRate          int64
	MixedWrites       bool
	MixedReads        bool
	Users             int64
	PayloadBytes      int
	RPCTimeout        time.Duration
	Progress          time.Duration
	LatencySampleRate int64
	JSONOutput        string
	Label             string
}

type tokenRange struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

type shardRoute struct {
	ID         uint32       `json:"id"`
	Ranges     []tokenRange `json:"ranges"`
	Voters     []string     `json:"voters"`
	LeaderHint string       `json:"leader_hint"`
}

type router struct {
	shards []shardRoute
}

type routeTarget struct {
	ShardID uint32
	Leader  string
	Voters  []string
}

type clients struct {
	byNode map[string]*client.Client
	addrs  map[string]string
	order  []string
}

type batchJob struct {
	start int64
	end   int64
}

type phaseRecorder struct {
	name    string
	started time.Time
	units   atomic.Int64
	rpcs    atomic.Int64
	errors  atomic.Int64

	mu        sync.Mutex
	latencies []time.Duration
}

type phaseSummary struct {
	Name             string  `json:"name"`
	Units            int64   `json:"units"`
	RPCs             int64   `json:"rpc_calls"`
	Errors           int64   `json:"errors"`
	ElapsedSeconds   float64 `json:"elapsed_seconds"`
	ThroughputPerSec float64 `json:"throughput_per_second"`
	RPCRatePerSec    float64 `json:"rpc_rate_per_second"`
	LatencySamples   int     `json:"latency_samples"`
	LatencyMinMs     float64 `json:"latency_min_ms,omitempty"`
	LatencyP50Ms     float64 `json:"latency_p50_ms,omitempty"`
	LatencyP95Ms     float64 `json:"latency_p95_ms,omitempty"`
	LatencyP99Ms     float64 `json:"latency_p99_ms,omitempty"`
	LatencyMaxMs     float64 `json:"latency_max_ms,omitempty"`
	Found            int64   `json:"found,omitempty"`
}

type report struct {
	Label     string            `json:"label"`
	StartedAt string            `json:"started_at"`
	EndedAt   string            `json:"ended_at"`
	Config    cfg               `json:"config"`
	NodeAddrs map[string]string `json:"node_addrs"`
	Shards    []shardRoute      `json:"shards"`
	Phases    []phaseSummary    `json:"phases"`
}

var log = slog.New(slog.NewTextHandler(os.Stderr, nil))

func main() {
	c := parseFlags()
	started := time.Now()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	nodeAddrs, err := parseNodeAddrs(c.Nodes)
	if err != nil {
		fatal("parse nodes", err)
	}
	cs, err := dialClients(ctx, nodeAddrs)
	if err != nil {
		fatal("dial clients", err)
	}
	defer cs.close()

	status, err := fetchStatus(ctx, cs)
	if err != nil {
		fatal("cluster status", err)
	}
	rt, shards, err := routerFromStatus(status)
	if err != nil {
		fatal("build router", err)
	}
	fmt.Printf("placement: shards=%d strategy=%s version=%d epoch=%d\n", status.ShardCount, status.PlacementStrategy, status.PlacementVersion, status.RoutingEpoch)
	for _, sh := range shards {
		fmt.Printf("shard %d leader=%s ranges=%v\n", sh.ID, sh.LeaderHint, sh.Ranges)
		if _, ok := cs.byNode[sh.LeaderHint]; !ok {
			fatal("missing node address", fmt.Errorf("leader %q has no configured endpoint", sh.LeaderHint))
		}
	}

	if err := ensureTable(ctx, c, cs, shards); err != nil {
		fatal("ensure table", err)
	}

	var phases []phaseSummary
	var runErr error
	var runErrMsg string
	if c.Items > 0 {
		stats, err := runWrite(ctx, c, cs, rt, 0, c.Items, "seed-write")
		printSummary(stats)
		phases = append(phases, stats)
		if err != nil {
			runErr = err
			runErrMsg = "seed write failed"
		}
	}
	if c.MixedDuration > 0 && runErr == nil {
		keyspace := c.Keyspace
		if keyspace == 0 {
			keyspace = c.Items
		}
		if c.MixedReads && keyspace <= 0 {
			fatal("invalid flags", errors.New("-mixed-duration with reads requires -keyspace or -items seed keyspace > 0"))
		}
		writeStats, readStats, found, err := runMixed(ctx, c, cs, rt, keyspace)
		if c.MixedWrites {
			printSummary(writeStats)
			phases = append(phases, writeStats)
		}
		if c.MixedReads {
			readStats.Found = found
			printSummary(readStats)
			fmt.Printf("mixed read found: %d/%d\n", found, readStats.Units)
			phases = append(phases, readStats)
		}
		if err != nil {
			runErr = err
			runErrMsg = "mixed load failed"
		}
	}

	if c.JSONOutput != "" {
		rep := report{
			Label:     c.Label,
			StartedAt: started.UTC().Format(time.RFC3339),
			EndedAt:   time.Now().UTC().Format(time.RFC3339),
			Config:    c,
			NodeAddrs: nodeAddrs,
			Shards:    shards,
			Phases:    phases,
		}
		raw, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			fatal("marshal report", err)
		}
		if err := os.WriteFile(c.JSONOutput, append(raw, '\n'), 0o644); err != nil {
			fatal("write report", err)
		}
		fmt.Printf("json report: %s\n", c.JSONOutput)
	}
	if runErr != nil {
		fatal(runErrMsg, runErr)
	}
}

func parseFlags() cfg {
	var c cfg
	flag.StringVar(&c.Nodes, "nodes", "n1=localhost:9191,n2=localhost:9192,n3=localhost:9193", "comma-separated nodeID=gRPC address map")
	flag.StringVar(&c.Table, "table", "MassiveRouteAwareLoad", "table name")
	flag.Int64Var(&c.Items, "items", 200000, "seed items to write before mixed phase")
	flag.Int64Var(&c.Keyspace, "keyspace", 0, "existing keyspace for read-only/mixed reads; defaults to --items")
	flag.DurationVar(&c.MixedDuration, "mixed-duration", 30*time.Minute, "run mixed reads/writes for this duration")
	flag.IntVar(&c.BatchSize, "batch-size", 500, "items per generated write batch")
	flag.IntVar(&c.Workers, "workers", 64, "concurrent write workers")
	flag.IntVar(&c.ReadWorkers, "read-workers", 64, "concurrent read workers")
	flag.Int64Var(&c.WriteRate, "write-rate", 15000, "target write units per second; 0 uncapped")
	flag.Int64Var(&c.ReadRate, "read-rate", 20000, "target read units per second; 0 uncapped")
	flag.BoolVar(&c.MixedWrites, "mixed-writes", true, "run writes during the mixed-duration phase")
	flag.BoolVar(&c.MixedReads, "mixed-reads", true, "run reads during the mixed-duration phase")
	flag.Int64Var(&c.Users, "users", 100000, "distinct partition keys")
	flag.IntVar(&c.PayloadBytes, "payload-bytes", 256, "bytes in the payload attribute")
	flag.DurationVar(&c.RPCTimeout, "rpc-timeout", 30*time.Second, "timeout per RPC")
	flag.DurationVar(&c.Progress, "progress", 30*time.Second, "progress print interval; 0 disables")
	flag.Int64Var(&c.LatencySampleRate, "latency-sample-rate", 10, "record one latency sample every N RPCs")
	flag.StringVar(&c.JSONOutput, "json-output", "", "write benchmark summary JSON to this file")
	flag.StringVar(&c.Label, "label", "", "label stored in JSON report")
	flag.Parse()

	if c.BatchSize <= 0 || c.Workers <= 0 || c.ReadWorkers <= 0 || c.Users <= 0 || c.LatencySampleRate <= 0 {
		fatal("invalid flags", errors.New("batch-size, workers, read-workers, users and latency-sample-rate must be > 0"))
	}
	if c.Items < 0 || c.WriteRate < 0 || c.ReadRate < 0 || c.PayloadBytes < 0 {
		fatal("invalid flags", errors.New("items, rates and payload-bytes must be >= 0"))
	}
	if c.MixedDuration > 0 && !c.MixedWrites && !c.MixedReads {
		fatal("invalid flags", errors.New("mixed-duration requires mixed-writes or mixed-reads"))
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
		id, addr, ok := strings.Cut(part, "=")
		if !ok || strings.TrimSpace(id) == "" || strings.TrimSpace(addr) == "" {
			return nil, fmt.Errorf("bad node mapping %q", part)
		}
		out[strings.TrimSpace(id)] = strings.TrimSpace(addr)
	}
	if len(out) == 0 {
		return nil, errors.New("no nodes configured")
	}
	return out, nil
}

func dialClients(ctx context.Context, addrs map[string]string) (*clients, error) {
	out := &clients{byNode: map[string]*client.Client{}, addrs: addrs}
	for id := range addrs {
		out.order = append(out.order, id)
	}
	sort.Strings(out.order)
	for _, id := range out.order {
		addr := addrs[id]
		cli, err := client.Dial(ctx, addr,
			client.WithPlaintext(),
			client.WithDialOption(grpc.WithDefaultCallOptions(
				grpc.MaxCallSendMsgSize(128<<20),
				grpc.MaxCallRecvMsgSize(128<<20),
			)),
		)
		if err != nil {
			out.close()
			return nil, fmt.Errorf("%s=%s: %w", id, addr, err)
		}
		out.byNode[id] = cli
	}
	return out, nil
}

func (c *clients) close() {
	for _, cli := range c.byNode {
		_ = cli.Close()
	}
}

func fetchStatus(ctx context.Context, cs *clients) (client.ClusterStatus, error) {
	ids := make([]string, 0, len(cs.byNode))
	for id := range cs.byNode {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var last error
	for _, id := range ids {
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

func ensureTable(ctx context.Context, c cfg, cs *clients, shards []shardRoute) error {
	if len(shards) == 0 {
		return errors.New("no shards")
	}
	leader := shards[0].LeaderHint
	cli := cs.byNode[leader]
	if cli == nil {
		return fmt.Errorf("metadata shard leader %q has no client", leader)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err := cli.CreateTable(rpcCtx, types.TableDescriptor{
		Name:      c.Table,
		KeySchema: types.KeySchema{PK: "pk", SK: "sk"},
	})
	if err == nil {
		fmt.Printf("created table: %s via %s\n", c.Table, leader)
		return warmTable(ctx, c, cs)
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "already") || strings.Contains(msg, "exist") {
		fmt.Printf("using existing table: %s\n", c.Table)
		return warmTable(ctx, c, cs)
	}
	return err
}

func warmTable(ctx context.Context, c cfg, cs *clients) error {
	deadline := time.Now().Add(15 * time.Second)
	for _, id := range cs.order {
		cli := cs.byNode[id]
		var last error
		for attempt := 0; time.Now().Before(deadline); attempt++ {
			rpcCtx, cancel := context.WithTimeout(ctx, c.RPCTimeout)
			_, err := cli.DescribeTable(rpcCtx, c.Table)
			cancel()
			if err == nil {
				last = nil
				break
			}
			last = err
			time.Sleep(time.Duration(100+attempt*50) * time.Millisecond)
		}
		if last != nil {
			return fmt.Errorf("warm table %s on %s: %w", c.Table, id, last)
		}
	}
	fmt.Printf("warmed table: %s on %d nodes\n", c.Table, len(cs.order))
	return nil
}

func runWrite(ctx context.Context, c cfg, cs *clients, rt *router, startID, items int64, name string) (phaseSummary, error) {
	phaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	rec := newRecorder(name)
	stopProgress := startProgress(name, c.Progress, rec, items)
	defer stopProgress()

	jobs := make(chan batchJob, c.Workers*2)
	var wg sync.WaitGroup
	var firstErr error
	var firstErrOnce sync.Once
	for i := 0; i < c.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if err := writeBatch(phaseCtx, c, cs, rt, rec, job.start, job.end); err != nil {
					rec.errors.Add(1)
					firstErrOnce.Do(func() { firstErr = err })
					cancel()
					return
				}
			}
		}()
	}

	producerStarted := time.Now()
	var emitted int64
	for offset := startID; offset < startID+items; offset += int64(c.BatchSize) {
		end := min64(offset+int64(c.BatchSize), startID+items)
		select {
		case <-phaseCtx.Done():
			break
		case jobs <- batchJob{start: offset, end: end}:
		}
		emitted += end - offset
		if !throttle(phaseCtx, producerStarted, emitted, c.WriteRate) {
			break
		}
	}
	close(jobs)
	wg.Wait()
	return rec.summary(), firstErr
}

func runMixed(ctx context.Context, c cfg, cs *clients, rt *router, keyspace int64) (phaseSummary, phaseSummary, int64, error) {
	phaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	writeRec := newRecorder("mixed-write")
	readRec := newRecorder("mixed-read")
	stopWriteProgress := func() {}
	if c.MixedWrites {
		stopWriteProgress = startProgress("mixed-write", c.Progress, writeRec, 0)
	}
	defer stopWriteProgress()
	stopReadProgress := func() {}
	if c.MixedReads {
		stopReadProgress = startProgress("mixed-read", c.Progress, readRec, 0)
	}
	defer stopReadProgress()

	writeJobs := make(chan batchJob, c.Workers*2)
	readJobs := make(chan int64, c.ReadWorkers*4)
	var writeWG sync.WaitGroup
	var readWG sync.WaitGroup
	var producers sync.WaitGroup
	var found atomic.Int64
	var firstErr error
	var firstErrOnce sync.Once
	setErr := func(err error) {
		firstErrOnce.Do(func() { firstErr = err })
		cancel()
	}

	if c.MixedWrites {
		for i := 0; i < c.Workers; i++ {
			writeWG.Add(1)
			go func() {
				defer writeWG.Done()
				for job := range writeJobs {
					if err := writeBatch(phaseCtx, c, cs, rt, writeRec, job.start, job.end); err != nil {
						writeRec.errors.Add(1)
						setErr(err)
						return
					}
				}
			}()
		}
	}
	if c.MixedReads {
		for i := 0; i < c.ReadWorkers; i++ {
			readWG.Add(1)
			go func() {
				defer readWG.Done()
				for seq := range readJobs {
					id := permute(seq, keyspace)
					target, err := rt.routeForID(id, c.Users)
					if err != nil {
						readRec.errors.Add(1)
						setErr(err)
						return
					}
					rpcCtx, cancelRPC := context.WithTimeout(phaseCtx, c.RPCTimeout)
					t0 := time.Now()
					item, err := getItemWithRetry(rpcCtx, cs, target, c.Table, keyFor(id, c.Users))
					lat := time.Since(t0)
					cancelRPC()
					if err != nil {
						readRec.errors.Add(1)
						setErr(err)
						return
					}
					if item != nil {
						found.Add(1)
					}
					readRec.record(1, lat, c.LatencySampleRate)
				}
			}()
		}
	}

	if c.MixedWrites {
		producers.Add(1)
		go func() {
			defer producers.Done()
			defer close(writeJobs)
			deadline := time.Now().Add(c.MixedDuration)
			producerStarted := time.Now()
			var emitted int64
			for offset := keyspace; time.Now().Before(deadline); offset += int64(c.BatchSize) {
				end := offset + int64(c.BatchSize)
				select {
				case <-phaseCtx.Done():
					return
				case writeJobs <- batchJob{start: offset, end: end}:
				}
				emitted += end - offset
				if !throttle(phaseCtx, producerStarted, emitted, c.WriteRate) {
					return
				}
			}
		}()
	}
	if c.MixedReads {
		producers.Add(1)
		go func() {
			defer producers.Done()
			defer close(readJobs)
			deadline := time.Now().Add(c.MixedDuration)
			producerStarted := time.Now()
			var emitted int64
			for seq := int64(0); time.Now().Before(deadline); seq++ {
				select {
				case <-phaseCtx.Done():
					return
				case readJobs <- seq:
				}
				emitted++
				if !throttle(phaseCtx, producerStarted, emitted, c.ReadRate) {
					return
				}
			}
		}()
	}

	producers.Wait()
	writeWG.Wait()
	readWG.Wait()
	return writeRec.summary(), readRec.summary(), found.Load(), firstErr
}

func writeBatch(ctx context.Context, c cfg, cs *clients, rt *router, rec *phaseRecorder, start, end int64) error {
	type shardBatch struct {
		target routeTarget
		ops    []client.BatchWriteOp
	}
	groups := map[uint32]*shardBatch{}
	payload := strings.Repeat("x", c.PayloadBytes)
	for id := start; id < end; id++ {
		target, err := rt.routeForID(id, c.Users)
		if err != nil {
			return err
		}
		group := groups[target.ShardID]
		if group == nil {
			group = &shardBatch{target: target}
			groups[target.ShardID] = group
		}
		group.ops = append(group.ops, client.BatchWriteOp{Put: makeItem(id, c.Users, payload)})
	}
	for _, group := range groups {
		rpcCtx, cancelRPC := context.WithTimeout(ctx, c.RPCTimeout)
		t0 := time.Now()
		err := batchWriteWithRetry(rpcCtx, cs, group.target, c.Table, group.ops)
		lat := time.Since(t0)
		cancelRPC()
		if err != nil {
			return err
		}
		rec.record(int64(len(group.ops)), lat, c.LatencySampleRate)
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
			if !isNotLeader(err) {
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

func getItemWithRetry(ctx context.Context, cs *clients, target routeTarget, table string, key types.Item) (types.Item, error) {
	var last error
	for _, node := range retryOrder(target, cs.order) {
		cli := cs.byNode[node]
		if cli == nil {
			continue
		}
		item, err := cli.GetItem(ctx, table, key)
		if err == nil {
			return item, nil
		}
		last = err
		if !isNotLeader(err) {
			return nil, err
		}
	}
	if last != nil {
		return nil, last
	}
	return nil, fmt.Errorf("shard %d has no reachable client", target.ShardID)
}

func retryOrder(target routeTarget, fallback []string) []string {
	out := make([]string, 0, 1+len(target.Voters))
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
	if len(out) == 0 {
		for _, id := range fallback {
			add(id)
		}
	}
	return out
}

func isNotLeader(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not leader")
}

func (r *router) routeForID(id, users int64) (routeTarget, error) {
	token := xxhash.Sum64([]byte(fmt.Sprintf("USER#%06d", id%users)))
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
	return routeTarget{}, fmt.Errorf("no shard for id=%d token=%d", id, token)
}

func contains(r tokenRange, token uint64) bool {
	if r.Start == r.End {
		return true
	}
	if r.Start < r.End {
		return token >= r.Start && token < r.End
	}
	return token >= r.Start || token < r.End
}

func newRecorder(name string) *phaseRecorder {
	return &phaseRecorder{name: name, started: time.Now()}
}

func (r *phaseRecorder) record(units int64, latency time.Duration, sampleRate int64) {
	r.units.Add(units)
	rpc := r.rpcs.Add(1)
	if sampleRate <= 1 || rpc%sampleRate == 0 {
		r.mu.Lock()
		r.latencies = append(r.latencies, latency)
		r.mu.Unlock()
	}
}

func (r *phaseRecorder) summary() phaseSummary {
	units := r.units.Load()
	rpcs := r.rpcs.Load()
	errs := r.errors.Load()
	elapsed := time.Since(r.started).Seconds()
	if elapsed <= 0 {
		elapsed = 0
	}
	out := phaseSummary{
		Name:             r.name,
		Units:            units,
		RPCs:             rpcs,
		Errors:           errs,
		ElapsedSeconds:   elapsed,
		ThroughputPerSec: rate(units, elapsed),
		RPCRatePerSec:    rate(rpcs, elapsed),
	}
	r.mu.Lock()
	lats := append([]time.Duration(nil), r.latencies...)
	r.mu.Unlock()
	if len(lats) > 0 {
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		out.LatencySamples = len(lats)
		out.LatencyMinMs = ms(lats[0])
		out.LatencyP50Ms = ms(percentile(lats, 0.50))
		out.LatencyP95Ms = ms(percentile(lats, 0.95))
		out.LatencyP99Ms = ms(percentile(lats, 0.99))
		out.LatencyMaxMs = ms(lats[len(lats)-1])
	}
	return out
}

func startProgress(name string, interval time.Duration, rec *phaseRecorder, total int64) func() {
	if interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		var last int64
		var lastAt = time.Now()
		for {
			select {
			case <-done:
				return
			case now := <-t.C:
				cur := rec.units.Load()
				delta := cur - last
				window := now.Sub(lastAt).Seconds()
				totalRate := rate(cur, now.Sub(rec.started).Seconds())
				windowRate := rate(delta, window)
				if total > 0 {
					fmt.Printf("%s progress: %d/%d total=%.0f/s window=%.0f/s\n", name, cur, total, totalRate, windowRate)
				} else {
					fmt.Printf("%s progress: %d total=%.0f/s window=%.0f/s\n", name, cur, totalRate, windowRate)
				}
				last = cur
				lastAt = now
			}
		}
	}()
	return func() { close(done) }
}

func throttle(ctx context.Context, started time.Time, emitted, targetRate int64) bool {
	if targetRate <= 0 || emitted <= 0 {
		return true
	}
	target := started.Add(time.Duration(float64(emitted) / float64(targetRate) * float64(time.Second)))
	wait := time.Until(target)
	if wait <= 0 {
		return true
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func printSummary(s phaseSummary) {
	fmt.Printf("\n%s summary\n", s.Name)
	fmt.Printf("  units:      %d\n", s.Units)
	fmt.Printf("  rpc calls:  %d\n", s.RPCs)
	fmt.Printf("  elapsed:    %.0fs\n", s.ElapsedSeconds)
	fmt.Printf("  throughput: %.0f units/s\n", s.ThroughputPerSec)
	fmt.Printf("  rpc rate:   %.0f rpc/s\n", s.RPCRatePerSec)
	fmt.Printf("  errors:     %d\n", s.Errors)
	if s.LatencySamples > 0 {
		fmt.Printf("  latency:    min=%.1fms p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms samples=%d\n",
			s.LatencyMinMs, s.LatencyP50Ms, s.LatencyP95Ms, s.LatencyP99Ms, s.LatencyMaxMs, s.LatencySamples)
	}
}

func makeItem(id, users int64, payload string) types.Item {
	user := id % users
	return types.Item{
		"pk":      sAttr(fmt.Sprintf("USER#%06d", user)),
		"sk":      sAttr(fmt.Sprintf("EVENT#%012d", id)),
		"name":    sAttr(fmt.Sprintf("user-%06d", user)),
		"city":    sAttr(cityFor(id)),
		"lat":     nAttr(strconv.FormatFloat(-23.5505+float64(id%1000)/100000, 'f', -1, 64)),
		"lon":     nAttr(strconv.FormatFloat(-46.6333+float64(id%1000)/100000, 'f', -1, 64)),
		"score":   nAttr(strconv.FormatInt(id%10000, 10)),
		"active":  {T: types.AttrBOOL, BOOL: id%2 == 0},
		"payload": sAttr(payload),
	}
}

func keyFor(id, users int64) types.Item {
	return types.Item{
		"pk": sAttr(fmt.Sprintf("USER#%06d", id%users)),
		"sk": sAttr(fmt.Sprintf("EVENT#%012d", id)),
	}
}

func cityFor(id int64) string {
	switch id % 4 {
	case 0:
		return "Sao Paulo"
	case 1:
		return "Rio de Janeiro"
	case 2:
		return "Belo Horizonte"
	default:
		return "Curitiba"
	}
}

func sAttr(value string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: value}
}

func nAttr(value string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrN, N: value}
}

func permute(seq, modulo int64) int64 {
	if modulo <= 1 {
		return 0
	}
	x := uint64(seq + 1)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return int64(x % uint64(modulo))
}

func percentile(values []time.Duration, p float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func rate(n int64, seconds float64) float64 {
	if seconds <= 0 {
		return 0
	}
	return float64(n) / seconds
}

func ms(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func fatal(msg string, err error) {
	log.Error(msg, "err", err)
	os.Exit(1)
}
