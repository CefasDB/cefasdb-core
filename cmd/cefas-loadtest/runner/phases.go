package runner

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CefasDb/cefasdb/pkg/client"
)

type batchJob struct {
	start int64
	end   int64
}

type workerResult struct {
	latencies []time.Duration
}

// PhaseStats is the result of a single workload phase.
type PhaseStats struct {
	Name      string
	Units     int64
	RPCs      int64
	Elapsed   time.Duration
	Latencies []time.Duration
	Errors    int64
	Found     int64
}

// RunWritePhase issues BatchWriteItem RPCs against cli for the duration or
// item count described by cfg.
func RunWritePhase(parent context.Context, cfg Config, cli *client.Client) (PhaseStats, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	started := time.Now()
	jobs := make(chan batchJob, cfg.Workers*2)
	results := make(chan workerResult, cfg.Workers)
	var wg sync.WaitGroup
	var written atomic.Int64
	var rpcs atomic.Int64
	var errors atomic.Int64
	var firstErr error
	var firstErrOnce sync.Once
	repeatPayload := repeatedPayload(cfg.PayloadBytes)
	progressTotal := cfg.Items
	if cfg.WriteDuration > 0 {
		progressTotal = 0
	}

	stopProgress := startProgress("write", cfg.Progress, &written, progressTotal, started)
	defer stopProgress()

	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]time.Duration, 0, max(1, int(cfg.Items/int64(cfg.BatchSize*cfg.Workers))))
			for job := range jobs {
				ops := make([]client.BatchWriteOp, 0, job.end-job.start)
				for id := job.start; id < job.end; id++ {
					payload := payloadFor(id, cfg.PayloadBytes, cfg.PayloadMode, repeatPayload)
					ops = append(ops, client.BatchWriteOp{Put: makeItem(id, cfg.Users, payload)})
				}

				rpcCtx, cancelRPC := context.WithTimeout(ctx, cfg.RPCTimeout)
				t0 := time.Now()
				err := cli.BatchWriteItem(rpcCtx, cfg.Table, ops)
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
				if shouldSample(rpcN, cfg.LatencySampleRate) {
					local = append(local, lat)
				}
			}
			results <- workerResult{latencies: local}
		}()
	}

	producerStarted := time.Now()
	var emitted int64
	if cfg.WriteDuration > 0 {
		deadline := time.Now().Add(cfg.WriteDuration)
		for offset := cfg.StartID; time.Now().Before(deadline); offset += int64(cfg.BatchSize) {
			end := offset + int64(cfg.BatchSize)
			select {
			case <-ctx.Done():
				goto writeSubmitDone
			case jobs <- batchJob{start: offset, end: end}:
			}
			emitted += end - offset
			if !throttle(ctx, producerStarted, emitted, cfg.WriteRate) {
				goto writeSubmitDone
			}
		}
	} else {
		for offset := cfg.StartID; offset < cfg.StartID+cfg.Items; offset += int64(cfg.BatchSize) {
			end := min(offset+int64(cfg.BatchSize), cfg.StartID+cfg.Items)
			select {
			case <-ctx.Done():
				goto writeSubmitDone
			case jobs <- batchJob{start: offset, end: end}:
			}
			emitted += end - offset
			if !throttle(ctx, producerStarted, emitted, cfg.WriteRate) {
				goto writeSubmitDone
			}
		}
	}
writeSubmitDone:
	close(jobs)
	wg.Wait()
	close(results)

	latencies := collectLatencies(results)
	stats := PhaseStats{
		Name:      "write",
		Units:     written.Load(),
		RPCs:      rpcs.Load(),
		Elapsed:   time.Since(started),
		Latencies: latencies,
		Errors:    errors.Load(),
	}
	return stats, firstErr
}

// RunReadPhase issues GetItem RPCs across keyspace IDs.
func RunReadPhase(parent context.Context, cfg Config, cli *client.Client, keyspace int64) (PhaseStats, int64, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	started := time.Now()
	jobs := make(chan int64, cfg.ReadWorkers*4)
	results := make(chan workerResult, cfg.ReadWorkers)
	var wg sync.WaitGroup
	var read atomic.Int64
	var rpcs atomic.Int64
	var found atomic.Int64
	var errors atomic.Int64
	var firstErr error
	var firstErrOnce sync.Once
	progressTotal := cfg.Reads
	if cfg.ReadDuration > 0 {
		progressTotal = 0
	}

	stopProgress := startProgress("read", cfg.Progress, &read, progressTotal, started)
	defer stopProgress()

	for i := 0; i < cfg.ReadWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]time.Duration, 0, max(1, int(cfg.Reads/int64(cfg.ReadWorkers))))
			for seq := range jobs {
				id := cfg.StartID + permute(seq, keyspace)
				rpcCtx, cancelRPC := context.WithTimeout(ctx, cfg.RPCTimeout)
				t0 := time.Now()
				item, err := cli.GetItem(rpcCtx, cfg.Table, keyFor(id, cfg.Users), client.GetOptions{Strong: cfg.StrongRead})
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
				if shouldSample(rpcN, cfg.LatencySampleRate) {
					local = append(local, lat)
				}
			}
			results <- workerResult{latencies: local}
		}()
	}

	producerStarted := time.Now()
	var emitted int64
	if cfg.ReadDuration > 0 {
		deadline := time.Now().Add(cfg.ReadDuration)
		for i := int64(0); time.Now().Before(deadline); i++ {
			select {
			case <-ctx.Done():
				goto readSubmitDone
			case jobs <- i:
			}
			emitted++
			if !throttle(ctx, producerStarted, emitted, cfg.ReadRate) {
				goto readSubmitDone
			}
		}
	} else {
		for i := int64(0); i < cfg.Reads; i++ {
			select {
			case <-ctx.Done():
				goto readSubmitDone
			case jobs <- i:
			}
			emitted++
			if !throttle(ctx, producerStarted, emitted, cfg.ReadRate) {
				goto readSubmitDone
			}
		}
	}
readSubmitDone:
	close(jobs)
	wg.Wait()
	close(results)

	latencies := collectLatencies(results)
	stats := PhaseStats{
		Name:      "read",
		Units:     read.Load(),
		RPCs:      rpcs.Load(),
		Elapsed:   time.Since(started),
		Latencies: latencies,
		Errors:    errors.Load(),
	}
	return stats, found.Load(), firstErr
}

// RunMixedPhase runs reads and writes concurrently for cfg.MixedDuration.
func RunMixedPhase(parent context.Context, cfg Config, cli *client.Client, keyspace, writeStartID int64) (PhaseStats, PhaseStats, int64, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	started := time.Now()
	writeJobs := make(chan batchJob, cfg.Workers*2)
	readJobs := make(chan int64, cfg.ReadWorkers*4)
	writeResults := make(chan workerResult, cfg.Workers)
	readResults := make(chan workerResult, cfg.ReadWorkers)

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
	repeatPayload := repeatedPayload(cfg.PayloadBytes)

	stopWriteProgress := startProgress("mixed-write", cfg.Progress, &written, 0, started)
	defer stopWriteProgress()
	stopReadProgress := startProgress("mixed-read", cfg.Progress, &read, 0, started)
	defer stopReadProgress()

	for i := 0; i < cfg.Workers; i++ {
		writeWG.Add(1)
		go func() {
			defer writeWG.Done()
			local := make([]time.Duration, 0, 1024)
			for job := range writeJobs {
				ops := make([]client.BatchWriteOp, 0, job.end-job.start)
				for id := job.start; id < job.end; id++ {
					payload := payloadFor(id, cfg.PayloadBytes, cfg.PayloadMode, repeatPayload)
					ops = append(ops, client.BatchWriteOp{Put: makeItem(id, cfg.Users, payload)})
				}

				rpcCtx, cancelRPC := context.WithTimeout(ctx, cfg.RPCTimeout)
				t0 := time.Now()
				err := cli.BatchWriteItem(rpcCtx, cfg.Table, ops)
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
				if shouldSample(rpcN, cfg.LatencySampleRate) {
					local = append(local, lat)
				}
			}
			writeResults <- workerResult{latencies: local}
		}()
	}

	for i := 0; i < cfg.ReadWorkers; i++ {
		readWG.Add(1)
		go func() {
			defer readWG.Done()
			local := make([]time.Duration, 0, 1024)
			for seq := range readJobs {
				id := cfg.StartID + permute(seq, keyspace)
				rpcCtx, cancelRPC := context.WithTimeout(ctx, cfg.RPCTimeout)
				t0 := time.Now()
				item, err := cli.GetItem(rpcCtx, cfg.Table, keyFor(id, cfg.Users), client.GetOptions{Strong: cfg.StrongRead})
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
				if shouldSample(rpcN, cfg.LatencySampleRate) {
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
		deadline := time.Now().Add(cfg.MixedDuration)
		producerStarted := time.Now()
		var emitted int64
		for offset := writeStartID; time.Now().Before(deadline); offset += int64(cfg.BatchSize) {
			end := offset + int64(cfg.BatchSize)
			select {
			case <-ctx.Done():
				return
			case writeJobs <- batchJob{start: offset, end: end}:
			}
			emitted += end - offset
			if !throttle(ctx, producerStarted, emitted, cfg.WriteRate) {
				return
			}
		}
	}()

	go func() {
		defer producers.Done()
		defer close(readJobs)
		deadline := time.Now().Add(cfg.MixedDuration)
		producerStarted := time.Now()
		var emitted int64
		for seq := int64(0); time.Now().Before(deadline); seq++ {
			select {
			case <-ctx.Done():
				return
			case readJobs <- seq:
			}
			emitted++
			if !throttle(ctx, producerStarted, emitted, cfg.ReadRate) {
				return
			}
		}
	}()

	producers.Wait()
	writeWG.Wait()
	readWG.Wait()
	close(writeResults)
	close(readResults)

	writeStats := PhaseStats{
		Name:      "mixed-write",
		Units:     written.Load(),
		RPCs:      writeRPCs.Load(),
		Elapsed:   time.Since(started),
		Latencies: collectLatencies(writeResults),
		Errors:    writeErrors.Load(),
	}
	readStats := PhaseStats{
		Name:      "mixed-read",
		Units:     read.Load(),
		RPCs:      readRPCs.Load(),
		Elapsed:   time.Since(started),
		Latencies: collectLatencies(readResults),
		Errors:    readErrors.Load(),
	}
	return writeStats, readStats, found.Load(), firstErr
}
