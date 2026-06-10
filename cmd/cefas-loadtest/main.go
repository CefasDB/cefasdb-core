package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/osvaldoandrade/cefas/pkg/client"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

type config struct {
	addr              string
	addrs             string
	table             string
	token             string
	plaintext         bool
	createTable       bool
	items             int64
	startID           int64
	keyspace          int64
	reads             int64
	writeDuration     time.Duration
	readDuration      time.Duration
	mixedDuration     time.Duration
	batchSize         int
	workers           int
	readWorkers       int
	writeRate         int64
	readRate          int64
	users             int64
	payloadBytes      int
	rpcTimeout        time.Duration
	progress          time.Duration
	latencySampleRate int64
	jsonOutput        string
	label             string
	strongRead        bool
}

type batchJob struct {
	start int64
	end   int64
}

type workerResult struct {
	latencies []time.Duration
}

type phaseStats struct {
	name      string
	units     int64
	rpcs      int64
	elapsed   time.Duration
	latencies []time.Duration
	errors    int64
	found     int64
}

type report struct {
	Label      string         `json:"label,omitempty"`
	Target     string         `json:"target"`
	Table      string         `json:"table"`
	StartedAt  string         `json:"started_at"`
	FinishedAt string         `json:"finished_at"`
	Config     reportConfig   `json:"config"`
	Phases     []phaseSummary `json:"phases"`
}

type reportConfig struct {
	Items             int64   `json:"items"`
	Reads             int64   `json:"reads"`
	WriteDuration     string  `json:"write_duration,omitempty"`
	ReadDuration      string  `json:"read_duration,omitempty"`
	MixedDuration     string  `json:"mixed_duration,omitempty"`
	BatchSize         int     `json:"batch_size"`
	Workers           int     `json:"workers"`
	ReadWorkers       int     `json:"read_workers"`
	WriteRate         int64   `json:"write_rate,omitempty"`
	ReadRate          int64   `json:"read_rate,omitempty"`
	Users             int64   `json:"users"`
	PayloadBytes      int     `json:"payload_bytes"`
	LatencySampleRate int64   `json:"latency_sample_rate"`
	StrongRead        bool    `json:"strong_read"`
	StartedID         int64   `json:"start_id"`
	Keyspace          int64   `json:"keyspace"`
	ApproxItemKB      float64 `json:"approx_item_kb"`
}

type phaseSummary struct {
	Name           string  `json:"name"`
	Units          int64   `json:"units"`
	RPCs           int64   `json:"rpcs"`
	ElapsedSeconds float64 `json:"elapsed_seconds"`
	Throughput     float64 `json:"throughput_units_per_second"`
	RPCRate        float64 `json:"rpc_per_second"`
	Errors         int64   `json:"errors"`
	Found          int64   `json:"found,omitempty"`
	LatencySamples int     `json:"latency_samples"`
	P50Millis      float64 `json:"p50_ms,omitempty"`
	P95Millis      float64 `json:"p95_ms,omitempty"`
	P99Millis      float64 `json:"p99_ms,omitempty"`
	MaxMillis      float64 `json:"max_ms,omitempty"`
}

func main() {
	cfg := parseFlags()
	startedAt := time.Now()
	var phases []phaseSummary

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	opts := []client.Option{
		client.WithDialOption(grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(128<<20),
			grpc.MaxCallRecvMsgSize(128<<20),
		)),
	}
	if cfg.plaintext {
		opts = append(opts, client.WithPlaintext())
	}
	if cfg.token != "" {
		opts = append(opts, client.WithBearer(cfg.token))
	}

	addr := cfg.addr
	if cfg.addrs != "" {
		var err error
		addr, err = selectLeader(ctx, cfg.addrs, opts)
		if err != nil {
			log.Fatalf("select leader: %v", err)
		}
	}
	fmt.Printf("target: %s\n", addr)

	cli, err := client.Dial(ctx, addr, opts...)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer cli.Close()

	if cfg.createTable {
		if err := ensureTable(ctx, cli, cfg.table); err != nil {
			log.Fatalf("create table: %v", err)
		}
	}

	var wrote int64
	if cfg.items > 0 || cfg.writeDuration > 0 {
		stats, err := runWritePhase(ctx, cfg, cli)
		printStats(stats)
		phases = append(phases, summarizeStats(stats))
		if err != nil {
			log.Fatalf("write load failed: %v", err)
		}
		wrote = stats.units
	}

	if cfg.reads > 0 || cfg.readDuration > 0 {
		keyspace := cfg.keyspace
		if keyspace == 0 {
			keyspace = wrote
		}
		if keyspace == 0 {
			log.Fatal("--reads requires --keyspace when --items is 0")
		}
		stats, found, err := runReadPhase(ctx, cfg, cli, keyspace)
		stats.found = found
		printStats(stats)
		fmt.Printf("read found: %d/%d\n", found, stats.units)
		phases = append(phases, summarizeStats(stats))
		if err != nil {
			log.Fatalf("read load failed: %v", err)
		}
	}

	if cfg.mixedDuration > 0 {
		keyspace := cfg.keyspace
		if keyspace == 0 {
			keyspace = wrote
		}
		if keyspace == 0 {
			log.Fatal("--mixed-duration requires --keyspace or a preceding write phase")
		}
		writeStartID := cfg.startID + keyspace
		writeStats, readStats, found, err := runMixedPhase(ctx, cfg, cli, keyspace, writeStartID)
		readStats.found = found
		printStats(writeStats)
		printStats(readStats)
		fmt.Printf("mixed read found: %d/%d\n", found, readStats.units)
		phases = append(phases, summarizeStats(writeStats), summarizeStats(readStats))
		if err != nil {
			log.Fatalf("mixed load failed: %v", err)
		}
	}

	if cfg.jsonOutput != "" {
		if err := writeReport(cfg, addr, startedAt, time.Now(), phases); err != nil {
			log.Fatalf("write json report: %v", err)
		}
		fmt.Printf("json report: %s\n", cfg.jsonOutput)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", "localhost:9090", "gRPC address")
	flag.StringVar(&cfg.addrs, "addrs", "", "comma-separated gRPC addresses; when set, the current leader is selected automatically")
	flag.StringVar(&cfg.table, "table", "MassiveLoad", "table name")
	flag.StringVar(&cfg.token, "token", "", "bearer token")
	flag.BoolVar(&cfg.plaintext, "plaintext", true, "use plaintext gRPC")
	flag.BoolVar(&cfg.createTable, "create-table", true, "create the table if it does not exist")
	flag.Int64Var(&cfg.items, "items", 1_000_000, "items to write")
	flag.Int64Var(&cfg.startID, "start-id", 0, "first numeric item id to write")
	flag.Int64Var(&cfg.keyspace, "keyspace", 0, "existing keyspace for read-only tests; defaults to --items")
	flag.Int64Var(&cfg.reads, "reads", 100_000, "get-item reads after the write phase")
	flag.DurationVar(&cfg.writeDuration, "write-duration", 0, "write for this duration instead of stopping at --items")
	flag.DurationVar(&cfg.readDuration, "read-duration", 0, "read for this duration instead of stopping at --reads")
	flag.DurationVar(&cfg.mixedDuration, "mixed-duration", 0, "run reads and writes concurrently for this duration after any write/read phases")
	flag.IntVar(&cfg.batchSize, "batch-size", 500, "items per BatchWriteItem RPC")
	flag.IntVar(&cfg.workers, "workers", 64, "concurrent write workers")
	flag.IntVar(&cfg.readWorkers, "read-workers", 64, "concurrent read workers")
	flag.Int64Var(&cfg.writeRate, "write-rate", 0, "target write units per second; 0 means uncapped")
	flag.Int64Var(&cfg.readRate, "read-rate", 0, "target read units per second; 0 means uncapped")
	flag.Int64Var(&cfg.users, "users", 100_000, "distinct partition keys")
	flag.IntVar(&cfg.payloadBytes, "payload-bytes", 256, "bytes in the payload attribute")
	flag.DurationVar(&cfg.rpcTimeout, "rpc-timeout", 30*time.Second, "timeout per RPC")
	flag.DurationVar(&cfg.progress, "progress", 5*time.Second, "progress print interval; 0 disables")
	flag.Int64Var(&cfg.latencySampleRate, "latency-sample-rate", 1, "record one latency sample every N RPCs")
	flag.StringVar(&cfg.jsonOutput, "json-output", "", "write benchmark summary to this JSON file")
	flag.StringVar(&cfg.label, "label", "", "label stored in the JSON report")
	flag.BoolVar(&cfg.strongRead, "strong-read", false, "use strong consistency for get-item reads")
	flag.Parse()

	if cfg.items < 0 || cfg.reads < 0 {
		log.Fatal("--items and --reads must be >= 0")
	}
	if cfg.writeDuration < 0 || cfg.readDuration < 0 {
		log.Fatal("--write-duration and --read-duration must be >= 0")
	}
	if cfg.mixedDuration < 0 {
		log.Fatal("--mixed-duration must be >= 0")
	}
	if cfg.startID < 0 {
		log.Fatal("--start-id must be >= 0")
	}
	if cfg.batchSize <= 0 {
		log.Fatal("--batch-size must be > 0")
	}
	if cfg.workers <= 0 || cfg.readWorkers <= 0 {
		log.Fatal("--workers and --read-workers must be > 0")
	}
	if cfg.writeRate < 0 || cfg.readRate < 0 {
		log.Fatal("--write-rate and --read-rate must be >= 0")
	}
	if cfg.users <= 0 {
		log.Fatal("--users must be > 0")
	}
	if cfg.payloadBytes < 0 {
		log.Fatal("--payload-bytes must be >= 0")
	}
	if cfg.latencySampleRate <= 0 {
		log.Fatal("--latency-sample-rate must be > 0")
	}
	return cfg
}

func selectLeader(ctx context.Context, addrs string, opts []client.Option) (string, error) {
	candidates := strings.Split(addrs, ",")
	var firstReachable string
	var lastErr error
	deadline := time.Now().Add(30 * time.Second)

	for time.Now().Before(deadline) {
		for _, raw := range candidates {
			addr := strings.TrimSpace(raw)
			if addr == "" {
				continue
			}
			probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			cli, err := client.Dial(probeCtx, addr, opts...)
			if err != nil {
				cancel()
				lastErr = err
				continue
			}
			status, err := cli.Status(probeCtx)
			_ = cli.Close()
			cancel()
			if err != nil {
				lastErr = err
				continue
			}
			if firstReachable == "" {
				firstReachable = addr
			}
			fmt.Printf("probe %s: mode=%s self=%s leader=%v\n", addr, status.Mode, status.SelfID, status.IsLeader)
			if status.IsLeader {
				return addr, nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if firstReachable != "" {
		return firstReachable, fmt.Errorf("no leader reported by %s", addrs)
	}
	return "", fmt.Errorf("no reachable node in %s: %w", addrs, lastErr)
}

func ensureTable(ctx context.Context, cli *client.Client, table string) error {
	rpcCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err := cli.CreateTable(rpcCtx, types.TableDescriptor{
		Name:      table,
		KeySchema: types.KeySchema{PK: "pk", SK: "sk"},
	})
	if err == nil {
		fmt.Printf("created table: %s\n", table)
		return nil
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "already") || strings.Contains(msg, "exist") {
		fmt.Printf("using existing table: %s\n", table)
		return nil
	}
	return err
}

func runWritePhase(parent context.Context, cfg config, cli *client.Client) (phaseStats, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	started := time.Now()
	jobs := make(chan batchJob, cfg.workers*2)
	results := make(chan workerResult, cfg.workers)
	var wg sync.WaitGroup
	var written atomic.Int64
	var rpcs atomic.Int64
	var errors atomic.Int64
	var firstErr error
	var firstErrOnce sync.Once
	payload := strings.Repeat("x", cfg.payloadBytes)
	progressTotal := cfg.items
	if cfg.writeDuration > 0 {
		progressTotal = 0
	}

	stopProgress := startProgress("write", cfg.progress, &written, progressTotal, started)
	defer stopProgress()

	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			local := make([]time.Duration, 0, max(1, int(cfg.items/int64(cfg.batchSize*cfg.workers))))
			for job := range jobs {
				ops := make([]client.BatchWriteOp, 0, job.end-job.start)
				for id := job.start; id < job.end; id++ {
					ops = append(ops, client.BatchWriteOp{Put: makeItem(id, cfg.users, payload)})
				}

				rpcCtx, cancelRPC := context.WithTimeout(ctx, cfg.rpcTimeout)
				t0 := time.Now()
				err := cli.BatchWriteItem(rpcCtx, cfg.table, ops)
				lat := time.Since(t0)
				cancelRPC()
				if err != nil {
					errors.Add(1)
					firstErrOnce.Do(func() { firstErr = err })
					cancel()
					return
				}
				written.Add(job.end - job.start)
				rpcN := rpcs.Add(1)
				if shouldSample(rpcN, cfg.latencySampleRate) {
					local = append(local, lat)
				}
			}
			results <- workerResult{latencies: local}
		}(i)
	}

	producerStarted := time.Now()
	var emitted int64
	if cfg.writeDuration > 0 {
		deadline := time.Now().Add(cfg.writeDuration)
		for offset := cfg.startID; time.Now().Before(deadline); offset += int64(cfg.batchSize) {
			end := offset + int64(cfg.batchSize)
			select {
			case <-ctx.Done():
				goto writeSubmitDone
			case jobs <- batchJob{start: offset, end: end}:
			}
			emitted += end - offset
			if !throttle(ctx, producerStarted, emitted, cfg.writeRate) {
				goto writeSubmitDone
			}
		}
	} else {
		for offset := cfg.startID; offset < cfg.startID+cfg.items; offset += int64(cfg.batchSize) {
			end := min(offset+int64(cfg.batchSize), cfg.startID+cfg.items)
			select {
			case <-ctx.Done():
				goto writeSubmitDone
			case jobs <- batchJob{start: offset, end: end}:
			}
			emitted += end - offset
			if !throttle(ctx, producerStarted, emitted, cfg.writeRate) {
				goto writeSubmitDone
			}
		}
	}
writeSubmitDone:
	close(jobs)
	wg.Wait()
	close(results)

	latencies := collectLatencies(results)
	stats := phaseStats{
		name:      "write",
		units:     written.Load(),
		rpcs:      rpcs.Load(),
		elapsed:   time.Since(started),
		latencies: latencies,
		errors:    errors.Load(),
	}
	return stats, firstErr
}

func runReadPhase(parent context.Context, cfg config, cli *client.Client, keyspace int64) (phaseStats, int64, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	started := time.Now()
	jobs := make(chan int64, cfg.readWorkers*4)
	results := make(chan workerResult, cfg.readWorkers)
	var wg sync.WaitGroup
	var read atomic.Int64
	var rpcs atomic.Int64
	var found atomic.Int64
	var errors atomic.Int64
	var firstErr error
	var firstErrOnce sync.Once
	progressTotal := cfg.reads
	if cfg.readDuration > 0 {
		progressTotal = 0
	}

	stopProgress := startProgress("read", cfg.progress, &read, progressTotal, started)
	defer stopProgress()

	for i := 0; i < cfg.readWorkers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			local := make([]time.Duration, 0, max(1, int(cfg.reads/int64(cfg.readWorkers))))
			for seq := range jobs {
				id := cfg.startID + permute(seq, keyspace)
				rpcCtx, cancelRPC := context.WithTimeout(ctx, cfg.rpcTimeout)
				t0 := time.Now()
				item, err := cli.GetItem(rpcCtx, cfg.table, keyFor(id, cfg.users), client.GetOptions{Strong: cfg.strongRead})
				lat := time.Since(t0)
				cancelRPC()
				if err != nil {
					errors.Add(1)
					firstErrOnce.Do(func() { firstErr = err })
					cancel()
					return
				}
				if item != nil {
					found.Add(1)
				}
				read.Add(1)
				rpcN := rpcs.Add(1)
				if shouldSample(rpcN, cfg.latencySampleRate) {
					local = append(local, lat)
				}
			}
			results <- workerResult{latencies: local}
		}(i)
	}

	producerStarted := time.Now()
	var emitted int64
	if cfg.readDuration > 0 {
		deadline := time.Now().Add(cfg.readDuration)
		for i := int64(0); time.Now().Before(deadline); i++ {
			select {
			case <-ctx.Done():
				goto readSubmitDone
			case jobs <- i:
			}
			emitted++
			if !throttle(ctx, producerStarted, emitted, cfg.readRate) {
				goto readSubmitDone
			}
		}
	} else {
		for i := int64(0); i < cfg.reads; i++ {
			select {
			case <-ctx.Done():
				goto readSubmitDone
			case jobs <- i:
			}
			emitted++
			if !throttle(ctx, producerStarted, emitted, cfg.readRate) {
				goto readSubmitDone
			}
		}
	}
readSubmitDone:
	close(jobs)
	wg.Wait()
	close(results)

	latencies := collectLatencies(results)
	stats := phaseStats{
		name:      "read",
		units:     read.Load(),
		rpcs:      rpcs.Load(),
		elapsed:   time.Since(started),
		latencies: latencies,
		errors:    errors.Load(),
	}
	return stats, found.Load(), firstErr
}

func runMixedPhase(parent context.Context, cfg config, cli *client.Client, keyspace, writeStartID int64) (phaseStats, phaseStats, int64, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	started := time.Now()
	writeJobs := make(chan batchJob, cfg.workers*2)
	readJobs := make(chan int64, cfg.readWorkers*4)
	writeResults := make(chan workerResult, cfg.workers)
	readResults := make(chan workerResult, cfg.readWorkers)

	var writeWG sync.WaitGroup
	var readWG sync.WaitGroup
	var producers sync.WaitGroup
	var written atomic.Int64
	var writeRPCs atomic.Int64
	var read atomic.Int64
	var readRPCs atomic.Int64
	var found atomic.Int64
	var writeErrors atomic.Int64
	var readErrors atomic.Int64
	var firstErr error
	var firstErrOnce sync.Once
	payload := strings.Repeat("x", cfg.payloadBytes)

	stopWriteProgress := startProgress("mixed-write", cfg.progress, &written, 0, started)
	defer stopWriteProgress()
	stopReadProgress := startProgress("mixed-read", cfg.progress, &read, 0, started)
	defer stopReadProgress()

	for i := 0; i < cfg.workers; i++ {
		writeWG.Add(1)
		go func() {
			defer writeWG.Done()
			local := make([]time.Duration, 0, 1024)
			for job := range writeJobs {
				ops := make([]client.BatchWriteOp, 0, job.end-job.start)
				for id := job.start; id < job.end; id++ {
					ops = append(ops, client.BatchWriteOp{Put: makeItem(id, cfg.users, payload)})
				}

				rpcCtx, cancelRPC := context.WithTimeout(ctx, cfg.rpcTimeout)
				t0 := time.Now()
				err := cli.BatchWriteItem(rpcCtx, cfg.table, ops)
				lat := time.Since(t0)
				cancelRPC()
				if err != nil {
					writeErrors.Add(1)
					firstErrOnce.Do(func() { firstErr = err })
					cancel()
					return
				}
				written.Add(job.end - job.start)
				rpcN := writeRPCs.Add(1)
				if shouldSample(rpcN, cfg.latencySampleRate) {
					local = append(local, lat)
				}
			}
			writeResults <- workerResult{latencies: local}
		}()
	}

	for i := 0; i < cfg.readWorkers; i++ {
		readWG.Add(1)
		go func() {
			defer readWG.Done()
			local := make([]time.Duration, 0, 1024)
			for seq := range readJobs {
				id := cfg.startID + permute(seq, keyspace)
				rpcCtx, cancelRPC := context.WithTimeout(ctx, cfg.rpcTimeout)
				t0 := time.Now()
				item, err := cli.GetItem(rpcCtx, cfg.table, keyFor(id, cfg.users), client.GetOptions{Strong: cfg.strongRead})
				lat := time.Since(t0)
				cancelRPC()
				if err != nil {
					readErrors.Add(1)
					firstErrOnce.Do(func() { firstErr = err })
					cancel()
					return
				}
				if item != nil {
					found.Add(1)
				}
				read.Add(1)
				rpcN := readRPCs.Add(1)
				if shouldSample(rpcN, cfg.latencySampleRate) {
					local = append(local, lat)
				}
			}
			readResults <- workerResult{latencies: local}
		}()
	}

	producers.Add(2)
	go func() {
		defer producers.Done()
		defer close(writeJobs)
		deadline := time.Now().Add(cfg.mixedDuration)
		producerStarted := time.Now()
		var emitted int64
		for offset := writeStartID; time.Now().Before(deadline); offset += int64(cfg.batchSize) {
			end := offset + int64(cfg.batchSize)
			select {
			case <-ctx.Done():
				return
			case writeJobs <- batchJob{start: offset, end: end}:
			}
			emitted += end - offset
			if !throttle(ctx, producerStarted, emitted, cfg.writeRate) {
				return
			}
		}
	}()

	go func() {
		defer producers.Done()
		defer close(readJobs)
		deadline := time.Now().Add(cfg.mixedDuration)
		producerStarted := time.Now()
		var emitted int64
		for seq := int64(0); time.Now().Before(deadline); seq++ {
			select {
			case <-ctx.Done():
				return
			case readJobs <- seq:
			}
			emitted++
			if !throttle(ctx, producerStarted, emitted, cfg.readRate) {
				return
			}
		}
	}()

	producers.Wait()
	writeWG.Wait()
	readWG.Wait()
	close(writeResults)
	close(readResults)

	writeStats := phaseStats{
		name:      "mixed-write",
		units:     written.Load(),
		rpcs:      writeRPCs.Load(),
		elapsed:   time.Since(started),
		latencies: collectLatencies(writeResults),
		errors:    writeErrors.Load(),
	}
	readStats := phaseStats{
		name:      "mixed-read",
		units:     read.Load(),
		rpcs:      readRPCs.Load(),
		elapsed:   time.Since(started),
		latencies: collectLatencies(readResults),
		errors:    readErrors.Load(),
	}
	return writeStats, readStats, found.Load(), firstErr
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

func collectLatencies(results <-chan workerResult) []time.Duration {
	var latencies []time.Duration
	for result := range results {
		latencies = append(latencies, result.latencies...)
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	return latencies
}

func shouldSample(n, rate int64) bool {
	return rate <= 1 || n%rate == 0
}

func throttle(ctx context.Context, started time.Time, emitted, rate int64) bool {
	if rate <= 0 || emitted <= 0 {
		return true
	}
	target := started.Add(time.Duration(float64(emitted) / float64(rate) * float64(time.Second)))
	sleep := time.Until(target)
	if sleep <= 0 {
		return true
	}
	timer := time.NewTimer(sleep)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func printStats(stats phaseStats) {
	summary := summarizeStats(stats)
	rate := float64(stats.units) / stats.elapsed.Seconds()
	rpcRate := float64(stats.rpcs) / stats.elapsed.Seconds()
	fmt.Printf("\n%s summary\n", stats.name)
	fmt.Printf("  units:      %d\n", stats.units)
	fmt.Printf("  rpc calls:  %d\n", stats.rpcs)
	fmt.Printf("  elapsed:    %s\n", stats.elapsed.Round(time.Millisecond))
	fmt.Printf("  throughput: %.0f units/s\n", rate)
	fmt.Printf("  rpc rate:   %.0f rpc/s\n", rpcRate)
	fmt.Printf("  errors:     %d\n", stats.errors)
	if stats.found > 0 {
		fmt.Printf("  found:      %d\n", stats.found)
	}
	if len(stats.latencies) > 0 {
		fmt.Printf("  samples:    %d\n", summary.LatencySamples)
		fmt.Printf("  rpc p50:    %s\n", percentile(stats.latencies, 50).Round(time.Microsecond))
		fmt.Printf("  rpc p95:    %s\n", percentile(stats.latencies, 95).Round(time.Microsecond))
		fmt.Printf("  rpc p99:    %s\n", percentile(stats.latencies, 99).Round(time.Microsecond))
		fmt.Printf("  rpc max:    %s\n", stats.latencies[len(stats.latencies)-1].Round(time.Microsecond))
	}
	fmt.Println()
}

func summarizeStats(stats phaseStats) phaseSummary {
	elapsed := stats.elapsed.Seconds()
	out := phaseSummary{
		Name:           stats.name,
		Units:          stats.units,
		RPCs:           stats.rpcs,
		ElapsedSeconds: elapsed,
		Errors:         stats.errors,
		Found:          stats.found,
		LatencySamples: len(stats.latencies),
	}
	if elapsed > 0 {
		out.Throughput = float64(stats.units) / elapsed
		out.RPCRate = float64(stats.rpcs) / elapsed
	}
	if len(stats.latencies) > 0 {
		out.P50Millis = durationMillis(percentile(stats.latencies, 50))
		out.P95Millis = durationMillis(percentile(stats.latencies, 95))
		out.P99Millis = durationMillis(percentile(stats.latencies, 99))
		out.MaxMillis = durationMillis(stats.latencies[len(stats.latencies)-1])
	}
	return out
}

func writeReport(cfg config, target string, startedAt, finishedAt time.Time, phases []phaseSummary) error {
	rep := report{
		Label:      cfg.label,
		Target:     target,
		Table:      cfg.table,
		StartedAt:  startedAt.Format(time.RFC3339Nano),
		FinishedAt: finishedAt.Format(time.RFC3339Nano),
		Config: reportConfig{
			Items:             cfg.items,
			Reads:             cfg.reads,
			WriteDuration:     durationString(cfg.writeDuration),
			ReadDuration:      durationString(cfg.readDuration),
			MixedDuration:     durationString(cfg.mixedDuration),
			BatchSize:         cfg.batchSize,
			Workers:           cfg.workers,
			ReadWorkers:       cfg.readWorkers,
			WriteRate:         cfg.writeRate,
			ReadRate:          cfg.readRate,
			Users:             cfg.users,
			PayloadBytes:      cfg.payloadBytes,
			LatencySampleRate: cfg.latencySampleRate,
			StrongRead:        cfg.strongRead,
			StartedID:         cfg.startID,
			Keyspace:          cfg.keyspace,
			ApproxItemKB:      approximateItemKB(cfg.payloadBytes),
		},
		Phases: phases,
	}

	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(cfg.jsonOutput); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(cfg.jsonOutput, append(data, '\n'), 0o644)
}

func durationMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func durationString(d time.Duration) string {
	if d == 0 {
		return ""
	}
	return d.String()
}

func approximateItemKB(payloadBytes int) float64 {
	// Rough payload size for comparing runs; exact storage footprint includes key
	// encoding, indexes, log records, and Pebble metadata.
	return float64(payloadBytes+220) / 1024
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	idx := int((p / 100) * float64(len(sorted)-1))
	return sorted[idx]
}

func startProgress(name string, interval time.Duration, current *atomic.Int64, total int64, started time.Time) func() {
	if interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var last int64
		var lastAt = started
		for {
			select {
			case <-done:
				return
			case now := <-ticker.C:
				cur := current.Load()
				delta := cur - last
				window := now.Sub(lastAt).Seconds()
				totalRate := float64(cur) / now.Sub(started).Seconds()
				windowRate := float64(delta) / window
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
