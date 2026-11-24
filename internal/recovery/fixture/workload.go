package fixture

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/bits"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/status"

	recoverymodel "github.com/CefasDb/cefasdb/internal/recovery/model"
	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/pkg/types"
)

const (
	defaultWorkloadWorkers     = 4
	defaultWorkloadQueue       = 64
	maxWorkloadWorkers         = 256
	maxWorkloadQueue           = 65536
	maxWorkloadOperations      = uint64(10_000_000)
	maxWorkloadDuration        = 24 * time.Hour
	workloadOperationAttribute = "fixture_workload_operation"
)

// WorkloadRequest defines a bounded deterministic fixture workload.
type WorkloadRequest struct {
	Manifest    FixtureManifest
	Contract    recoverymodel.RecoveryContract
	ContractRef recoverymodel.ContractRef
	Phase       string
	Duration    time.Duration
	Workers     int
	QueueDepth  int
	RPCTimeout  time.Duration

	scale scalePlan
}

// LatencyPercentiles contains sorted nearest-rank latency boundaries.
type LatencyPercentiles struct {
	P50 time.Duration `json:"p50"`
	P95 time.Duration `json:"p95"`
	P99 time.Duration `json:"p99"`
	Max time.Duration `json:"max"`
}

// WorkloadReport contains counts and timing only; it is not source evidence.
type WorkloadReport struct {
	FixtureID            string             `json:"fixtureId"`
	ManifestSHA256       string             `json:"manifestSha256"`
	Phase                string             `json:"phase"`
	PlannedReads         uint64             `json:"plannedReads"`
	PlannedWrites        uint64             `json:"plannedWrites"`
	AttemptedReads       uint64             `json:"attemptedReads"`
	AcknowledgedReads    uint64             `json:"acknowledgedReads"`
	AttemptedWrites      uint64             `json:"attemptedWrites"`
	AcknowledgedWrites   uint64             `json:"acknowledgedWrites"`
	AchievedReadsPerSec  float64            `json:"achievedReadsPerSecond"`
	AchievedWritesPerSec float64            `json:"achievedWritesPerSecond"`
	ErrorCounts          map[string]uint64  `json:"errorCounts"`
	Elapsed              time.Duration      `json:"elapsed"`
	Latency              LatencyPercentiles `json:"latency"`
	OperationIDsSHA256   string             `json:"operationIdsSha256"`
}

type workloadClock interface {
	Now() time.Time
	NewTimer(time.Duration) workloadTimer
}

type workloadTimer interface {
	C() <-chan time.Time
	Stop() bool
}

type realWorkloadClock struct{}

func (realWorkloadClock) Now() time.Time { return time.Now() }

func (realWorkloadClock) NewTimer(duration time.Duration) workloadTimer {
	return realWorkloadTimer{timer: time.NewTimer(duration)}
}

type realWorkloadTimer struct{ timer *time.Timer }

func (timer realWorkloadTimer) C() <-chan time.Time { return timer.timer.C }
func (timer realWorkloadTimer) Stop() bool          { return timer.timer.Stop() }

type workloadKind uint8

const (
	workloadRead workloadKind = iota
	workloadWrite
)

type workloadJob struct {
	kind      workloadKind
	ordinal   uint64
	global    uint64
	due       time.Duration
	operation string
}

type workloadResult struct {
	job     workloadJob
	latency time.Duration
	err     error
}

type workloadPlan struct {
	reads      uint64
	writes     uint64
	readRate   uint64
	writeRate  uint64
	readNext   uint64
	writeNext  uint64
	globalNext uint64
}

// RunWorkload executes the deterministic workload through a concrete typed client.
func RunWorkload(ctx context.Context, cli *client.Client, req WorkloadRequest) (WorkloadReport, error) {
	if cli == nil {
		return WorkloadReport{}, failure(CodeWorkloadFailed, "RunWorkload", "client", "NIL_CLIENT", nil)
	}
	return runWorkload(ctx, clientAdapter{cli: cli}, realWorkloadClock{}, req)
}

func runWorkload(
	ctx context.Context,
	items itemPort,
	clock workloadClock,
	req WorkloadRequest,
) (WorkloadReport, error) {
	plan, normalized, err := prepareWorkload(req)
	if err != nil {
		return WorkloadReport{}, err
	}
	if clock == nil {
		return WorkloadReport{}, failure(CodeWorkloadFailed, "RunWorkload", "clock", "NIL_CLOCK", nil)
	}
	operationDigest, err := digestOperationIDs(plan, req.Manifest.SHA256, normalized.Phase)
	if err != nil {
		return WorkloadReport{}, err
	}
	report := WorkloadReport{
		FixtureID: req.Manifest.FixtureID, ManifestSHA256: req.Manifest.SHA256,
		Phase: normalized.Phase, PlannedReads: plan.reads, PlannedWrites: plan.writes,
		ErrorCounts: make(map[string]uint64), OperationIDsSHA256: operationDigest,
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan workloadJob, normalized.QueueDepth)
	results := make(chan workloadResult, normalized.QueueDepth)
	start := clock.Now()

	var workers sync.WaitGroup
	workers.Add(normalized.Workers)
	for worker := 0; worker < normalized.Workers; worker++ {
		go func() {
			defer workers.Done()
			// Termination is bounded by runCtx cancellation or the closed jobs input.
			for {
				select {
				case <-runCtx.Done():
					return
				default:
				}
				select {
				case <-runCtx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					result := executeWorkloadJob(runCtx, items, clock, normalized, job)
					// The collector remains live until every worker exits, so every attempted
					// operation is reported even when another worker cancels the run.
					results <- result
				}
			}
		}()
	}

	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		defer close(jobs)
		producerPlan := plan
		// Termination is bounded by runCtx cancellation or exhaustion of the finite plan,
		// after which this goroutine closes its output.
		for {
			job, ok, nextErr := producerPlan.next(req.Manifest.SHA256, normalized.Phase)
			if nextErr != nil {
				select {
				case results <- workloadResult{err: nextErr}:
				case <-runCtx.Done():
				}
				return
			}
			if !ok {
				_ = waitForWorkload(runCtx, clock, start.Add(normalized.Duration))
				return
			}
			if waitErr := waitForWorkload(runCtx, clock, start.Add(job.due)); waitErr != nil {
				return
			}
			select {
			case jobs <- job:
			case <-runCtx.Done():
				return
			}
		}
	}()

	workersDone := make(chan struct{})
	go func() {
		// Termination follows worker completion; workers stop only on cancellation or closed input.
		workers.Wait()
		close(results)
		close(workersDone)
	}()

	var latencies []time.Duration
	failed := false
	for result := range results {
		if result.job.operation != "" {
			latencies = append(latencies, result.latency)
			switch result.job.kind {
			case workloadRead:
				report.AttemptedReads++
			case workloadWrite:
				report.AttemptedWrites++
			}
		}
		if result.err != nil {
			report.ErrorCounts[classifyWorkloadError(result.err)]++
			if !failed {
				failed = true
				cancel()
			}
			continue
		}
		switch result.job.kind {
		case workloadRead:
			report.AcknowledgedReads++
		case workloadWrite:
			report.AcknowledgedWrites++
		}
	}
	<-workersDone
	<-producerDone
	report.Elapsed = clock.Now().Sub(start)
	if report.Elapsed < 0 {
		return report, failure(CodeWorkloadFailed, "RunWorkload", "clock", "CLOCK_MOVED_BACKWARD", nil)
	}
	report.Latency = latencyPercentiles(latencies)
	if report.Elapsed > 0 {
		seconds := report.Elapsed.Seconds()
		report.AchievedReadsPerSec = float64(report.AcknowledgedReads) / seconds
		report.AchievedWritesPerSec = float64(report.AcknowledgedWrites) / seconds
	}
	if failed {
		return report, failure(CodeWorkloadFailed, "RunWorkload", "operations", "OPERATION_FAILED", nil)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		report.ErrorCounts[classifyWorkloadError(contextErr)]++
		return report, failure(CodeWorkloadFailed, "RunWorkload", "context", "CONTEXT_ENDED", contextErr)
	}
	return report, nil
}

func prepareWorkload(req WorkloadRequest) (workloadPlan, WorkloadRequest, error) {
	seedReq := SeedRequest{
		Manifest: req.Manifest, Seed: req.Manifest.Seed, Contract: req.Contract, ContractRef: req.ContractRef,
	}
	if err := validateSeedRequest(seedReq); err != nil {
		return workloadPlan{}, WorkloadRequest{}, failure(CodeWorkloadFailed, "RunWorkload", "request", "INVALID_CONTRACT", err)
	}
	req.Phase = strings.TrimSpace(req.Phase)
	switch {
	case !validID(req.Phase):
		return workloadPlan{}, WorkloadRequest{}, failure(CodeWorkloadFailed, "RunWorkload", "phase", "INVALID_IDENTITY", nil)
	case req.Duration <= 0 || req.Duration > maxWorkloadDuration:
		return workloadPlan{}, WorkloadRequest{}, failure(CodeWorkloadFailed, "RunWorkload", "duration", "OUT_OF_RANGE", nil)
	case req.RPCTimeout <= 0 || req.RPCTimeout > req.Duration:
		return workloadPlan{}, WorkloadRequest{}, failure(CodeWorkloadFailed, "RunWorkload", "rpcTimeout", "OUT_OF_RANGE", nil)
	}
	if req.Workers == 0 {
		req.Workers = defaultWorkloadWorkers
	}
	if req.QueueDepth == 0 {
		req.QueueDepth = defaultWorkloadQueue
	}
	if req.Workers < 1 || req.Workers > maxWorkloadWorkers {
		return workloadPlan{}, WorkloadRequest{}, failure(CodeWorkloadFailed, "RunWorkload", "workers", "OUT_OF_RANGE", nil)
	}
	if req.QueueDepth < 1 || req.QueueDepth > maxWorkloadQueue {
		return workloadPlan{}, WorkloadRequest{}, failure(CodeWorkloadFailed, "RunWorkload", "queueDepth", "OUT_OF_RANGE", nil)
	}
	reads, err := operationCount(req.Duration, req.Contract.Workload.ReadsPerSecond)
	if err != nil {
		return workloadPlan{}, WorkloadRequest{}, err
	}
	writes, err := operationCount(req.Duration, req.Contract.Workload.WritesPerSecond)
	if err != nil {
		return workloadPlan{}, WorkloadRequest{}, err
	}
	if reads > maxWorkloadOperations || writes > maxWorkloadOperations ||
		reads > maxWorkloadOperations-writes {
		return workloadPlan{}, WorkloadRequest{}, failure(CodeWorkloadFailed, "RunWorkload", "duration", "TOO_MANY_OPERATIONS", nil)
	}
	scale, err := planScale(req.Manifest, req.Contract.Workload.LogicalBytes)
	if err != nil {
		return workloadPlan{}, WorkloadRequest{}, failure(CodeWorkloadFailed, "RunWorkload", "logicalBytes", "INVALID_SCALE", err)
	}
	req.scale = scale
	return workloadPlan{
		reads: reads, writes: writes, readRate: req.Contract.Workload.ReadsPerSecond,
		writeRate: req.Contract.Workload.WritesPerSecond,
	}, req, nil
}

func operationCount(duration time.Duration, rate uint64) (uint64, error) {
	if duration < 0 || rate == 0 {
		return 0, failure(CodeWorkloadFailed, "RunWorkload", "rate", "INVALID_RATE", nil)
	}
	high, low := bits.Mul64(uint64(duration), rate)
	if high >= uint64(time.Second) {
		return 0, failure(CodeWorkloadFailed, "RunWorkload", "rate", "OPERATION_COUNT_OVERFLOW", nil)
	}
	count, _ := bits.Div64(high, low, uint64(time.Second))
	return count, nil
}

func (plan *workloadPlan) next(manifestDigest, phase string) (workloadJob, bool, error) {
	readAvailable := plan.readNext < plan.reads
	writeAvailable := plan.writeNext < plan.writes
	if !readAvailable && !writeAvailable {
		return workloadJob{}, false, nil
	}
	readDue := time.Duration(1<<63 - 1)
	writeDue := time.Duration(1<<63 - 1)
	if readAvailable {
		value, err := pacedOffset(plan.readNext+1, plan.readRate)
		if err != nil {
			return workloadJob{}, false, err
		}
		readDue = value
	}
	if writeAvailable {
		value, err := pacedOffset(plan.writeNext+1, plan.writeRate)
		if err != nil {
			return workloadJob{}, false, err
		}
		writeDue = value
	}
	job := workloadJob{global: plan.globalNext}
	if writeAvailable && writeDue <= readDue {
		job.kind = workloadWrite
		job.ordinal = plan.writeNext
		job.due = writeDue
		plan.writeNext++
	} else {
		job.kind = workloadRead
		job.ordinal = plan.readNext
		job.due = readDue
		plan.readNext++
	}
	job.operation = operationID(manifestDigest, phase, job.global)
	plan.globalNext++
	return job, true, nil
}

func pacedOffset(ordinal, rate uint64) (time.Duration, error) {
	if rate == 0 {
		return 0, failure(CodeWorkloadFailed, "RunWorkload", "rate", "INVALID_RATE", nil)
	}
	high, low := bits.Mul64(ordinal, uint64(time.Second))
	if high >= rate {
		return 0, failure(CodeWorkloadFailed, "RunWorkload", "rate", "PACE_OVERFLOW", nil)
	}
	value, _ := bits.Div64(high, low, rate)
	if value > uint64(1<<63-1) {
		return 0, failure(CodeWorkloadFailed, "RunWorkload", "rate", "PACE_OVERFLOW", nil)
	}
	return time.Duration(value), nil
}

func operationID(manifestDigest, phase string, ordinal uint64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\n%s\n%d\n", manifestDigest, phase, ordinal)))
	return "op-" + hex.EncodeToString(sum[:16])
}

func digestOperationIDs(plan workloadPlan, manifestDigest, phase string) (string, error) {
	hasher := sha256.New()
	for {
		job, ok, err := plan.next(manifestDigest, phase)
		if err != nil {
			return "", err
		}
		if !ok {
			break
		}
		writeDigestFrame(hasher, []byte(job.operation))
	}
	return "sha256:" + hex.EncodeToString(hasher.Sum(nil)), nil
}

func waitForWorkload(ctx context.Context, clock workloadClock, target time.Time) error {
	delay := target.Sub(clock.Now())
	if delay <= 0 {
		return nil
	}
	timer := clock.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C():
		return nil
	}
}

func executeWorkloadJob(
	ctx context.Context,
	items itemPort,
	clock workloadClock,
	req WorkloadRequest,
	job workloadJob,
) workloadResult {
	result := workloadResult{job: job}
	started := clock.Now()
	rpcCtx, cancel := workloadRPCContext(ctx, clock.Now(), req.RPCTimeout)
	defer cancel()

	table, _ := manifestTable(req.Manifest, "recovery_events_v1")
	rowOrdinal := job.ordinal % req.scale.EventRows
	baseRow := generateRow(req.Manifest, table.ID, rowOrdinal, req.scale.PayloadBytes)
	switch job.kind {
	case workloadWrite:
		row := generateRow(req.Manifest, table.ID, rowOrdinal, req.scale.PayloadBytes)
		row[workloadOperationAttribute] = stringValue(job.operation)
		result.err = writeWorkloadItemReconciled(rpcCtx, items, table, baseRow, row, job.operation)
	case workloadRead:
		var key types.Item
		key, result.err = keyForItem(table.KeySchema, baseRow)
		if result.err == nil {
			var got types.Item
			got, result.err = items.StrongGet(rpcCtx, table.Name, key)
			if result.err == nil {
				var equal bool
				equal, result.err = equalWorkloadRow(got, baseRow)
				if result.err == nil && !equal {
					result.err = failure(CodeWorkloadFailed, "RunWorkload", job.operation, "READ_MISMATCH", nil)
				}
			}
		}
	default:
		result.err = failure(CodeWorkloadFailed, "RunWorkload", job.operation, "UNKNOWN_OPERATION", nil)
	}
	result.latency = clock.Now().Sub(started)
	if result.latency < 0 && result.err == nil {
		result.err = failure(CodeWorkloadFailed, "RunWorkload", "clock", "CLOCK_MOVED_BACKWARD", nil)
	}
	return result
}

func writeWorkloadItemReconciled(
	ctx context.Context,
	items itemPort,
	table FixtureTable,
	baseRow types.Item,
	desired types.Item,
	operation string,
) error {
	key, err := keyForItem(table.KeySchema, desired)
	if err != nil {
		return err
	}
	before, err := items.StrongGet(ctx, table.Name, key)
	if err != nil {
		return failure(CodeWorkloadFailed, "RunWorkload", operation, "WRITE_PREFLIGHT_FAILED", err)
	}
	compatible, err := equalWorkloadRow(before, baseRow)
	if err != nil {
		return err
	}
	if !compatible {
		return failure(CodeWorkloadFailed, "RunWorkload", operation, "WRITE_PRECONDITION_FAILED", nil)
	}
	alreadyApplied, err := equalItem(before, desired)
	if err != nil {
		return err
	}
	if alreadyApplied {
		return nil
	}
	write := func() error {
		return items.WriteBatch(ctx, table.Name, []client.BatchWriteOp{{Put: desired}})
	}
	writeErr := write()
	if writeErr == nil {
		return nil
	}
	if !ambiguousMutation(writeErr) {
		return failure(CodeWorkloadFailed, "RunWorkload", operation, "WRITE_REJECTED", writeErr)
	}
	reconcileCtx, reconcileCancel := mutationReconcileContext(ctx)
	state, err := reconcileWorkloadItem(reconcileCtx, items, table, before, desired, operation)
	reconcileCancel()
	if err != nil {
		return err
	}
	if state == batchCommitted {
		return nil
	}
	if state != batchAbsent {
		return failure(CodeWorkloadFailed, "RunWorkload", operation, "WRITE_STATE_CONFLICT", writeErr)
	}
	if canceledMutation(writeErr) || ctx.Err() != nil {
		return failure(CodeWorkloadFailed, "RunWorkload", operation, "WRITE_CANCELED_UNCOMMITTED", writeErr)
	}
	retryErr := write()
	if retryErr == nil {
		return nil
	}
	if !ambiguousMutation(retryErr) {
		return failure(CodeWorkloadFailed, "RunWorkload", operation, "WRITE_RETRY_REJECTED", retryErr)
	}
	retryCtx, retryCancel := mutationReconcileContext(ctx)
	state, err = reconcileWorkloadItem(retryCtx, items, table, before, desired, operation)
	retryCancel()
	if err != nil {
		return err
	}
	if state == batchCommitted {
		return nil
	}
	if state == batchAbsent {
		return failure(CodeWorkloadFailed, "RunWorkload", operation, "WRITE_RETRY_UNACKNOWLEDGED", retryErr)
	}
	return failure(CodeWorkloadFailed, "RunWorkload", operation, "WRITE_STATE_CONFLICT", retryErr)
}

func reconcileWorkloadItem(
	ctx context.Context,
	items itemPort,
	table FixtureTable,
	before types.Item,
	desired types.Item,
	operation string,
) (batchState, error) {
	key, err := keyForItem(table.KeySchema, desired)
	if err != nil {
		return batchConflict, err
	}
	got, err := items.StrongGet(ctx, table.Name, key)
	if err != nil {
		return batchConflict, failure(CodeWorkloadFailed, "RunWorkload", operation, "WRITE_RECONCILE_FAILED", err)
	}
	committed, err := equalItem(got, desired)
	if err != nil {
		return batchConflict, err
	}
	if committed {
		return batchCommitted, nil
	}
	unchanged, err := equalItem(got, before)
	if err != nil {
		return batchConflict, err
	}
	if unchanged {
		return batchAbsent, nil
	}
	return batchConflict, nil
}

func equalWorkloadRow(actual, base types.Item) (bool, error) {
	if actual == nil {
		return false, nil
	}
	candidate := make(types.Item, len(actual))
	for key, value := range actual {
		candidate[key] = value
	}
	if operation, ok := candidate[workloadOperationAttribute]; ok {
		if operation.T != types.AttrS || !validWorkloadOperation(operation.S) {
			return false, failure(CodeWorkloadFailed, "RunWorkload", workloadOperationAttribute, "INVALID_OPERATION_ID", nil)
		}
		delete(candidate, workloadOperationAttribute)
	}
	return equalItem(candidate, base)
}

func validWorkloadOperation(value string) bool {
	if len(value) != 35 || !strings.HasPrefix(value, "op-") {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(value, "op-"))
	return err == nil
}

func workloadRPCContext(parent context.Context, now time.Time, timeout time.Duration) (context.Context, context.CancelFunc) {
	deadline := now.Add(timeout)
	if parentDeadline, ok := parent.Deadline(); ok && parentDeadline.Before(deadline) {
		deadline = parentDeadline
	}
	return context.WithDeadline(parent, deadline)
}

func latencyPercentiles(samples []time.Duration) LatencyPercentiles {
	if len(samples) == 0 {
		return LatencyPercentiles{}
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return LatencyPercentiles{
		P50: nearestRank(sorted, 50), P95: nearestRank(sorted, 95),
		P99: nearestRank(sorted, 99), Max: sorted[len(sorted)-1],
	}
}

func nearestRank(sorted []time.Duration, percentile int) time.Duration {
	index := (percentile*len(sorted) + 99) / 100
	if index < 1 {
		index = 1
	}
	return sorted[index-1]
}

func classifyWorkloadError(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "CONTEXT_CANCELED"
	case errors.Is(err, context.DeadlineExceeded):
		return "DEADLINE_EXCEEDED"
	}
	var fixtureErr *Error
	if errors.As(err, &fixtureErr) {
		return string(fixtureErr.Code) + "." + fixtureErr.Reason
	}
	if code := status.Code(err); code.String() != "Unknown" {
		return "RPC_" + strings.ToUpper(code.String())
	}
	return "RPC_ERROR"
}
