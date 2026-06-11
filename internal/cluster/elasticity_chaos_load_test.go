package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

const (
	elasticityChaosTable = "ElasticityChaos"
	elasticityChaosIndex = "by_bucket"
)

type elasticityChaosReport struct {
	GeneratedAt string                     `json:"generated_at"`
	Verdict     string                     `json:"verdict"`
	Scenarios   []elasticityScenarioReport `json:"scenarios"`
}

type elasticityScenarioReport struct {
	Name                 string  `json:"name"`
	Operation            string  `json:"operation"`
	InjectedFailurePhase string  `json:"injected_failure_phase,omitempty"`
	BeforeEpoch          uint64  `json:"before_epoch"`
	AfterEpoch           uint64  `json:"after_epoch"`
	Restarts             int     `json:"restarts"`
	WriteCount           int     `json:"write_count"`
	Throughput           float64 `json:"throughput_writes_per_second"`
	P99Millis            float64 `json:"p99_ms"`
	Errors               int     `json:"errors"`
	ExpectedItems        int     `json:"expected_items"`
	RoutedItems          int     `json:"routed_items"`
	GSIItems             int     `json:"gsi_items"`
	Checksum             string  `json:"checksum"`
	ConsistencyVerdict   string  `json:"consistency_verdict"`
}

type elasticityWriteRecord struct {
	ID     string
	Value  string
	Bucket string
}

type elasticityRecorder struct {
	next int64

	mu        sync.Mutex
	startedAt time.Time
	endedAt   time.Time
	records   []elasticityWriteRecord
	latencies []time.Duration
	errors    []string
}

func TestElasticityChaosLoadSuite(t *testing.T) {
	report := elasticityChaosReport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Verdict:     "pass",
	}

	scenarios := []struct {
		name      string
		operation PlacementOperation
		phase     string
		run       func(*testing.T, string) elasticityScenarioReport
	}{
		{
			name:      "split-copy-crash",
			operation: PlacementOperationSplit,
			phase:     "copy",
			run: func(t *testing.T, bucket string) elasticityScenarioReport {
				return runSplitChaosScenario(t, bucket, "copy", SplitFinalizePhaseCopied)
			},
		},
		{
			name:      "split-catalog-publish-crash",
			operation: PlacementOperationSplit,
			phase:     "catalog_publish",
			run: func(t *testing.T, bucket string) elasticityScenarioReport {
				return runSplitChaosScenario(t, bucket, "catalog_publish", SplitFinalizePhasePublishing)
			},
		},
		{
			name:      "range-move-catchup-crash",
			operation: PlacementOperationRangeMove,
			phase:     "catch_up",
			run: func(t *testing.T, bucket string) elasticityScenarioReport {
				return runRangeMoveChaosScenario(t, bucket, "catch_up", RangeMoveFinalizePhaseVerified)
			},
		},
		{
			name:      "range-move-cleanup-crash",
			operation: PlacementOperationRangeMove,
			phase:     "cleanup",
			run: func(t *testing.T, bucket string) elasticityScenarioReport {
				return runRangeMoveChaosScenario(t, bucket, "cleanup", RangeMoveFinalizePhasePublished)
			},
		},
		{
			name:      "drain-live-load",
			operation: PlacementOperationDrain,
			run: func(t *testing.T, bucket string) elasticityScenarioReport {
				return runDrainLoadScenario(t, bucket)
			},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			bucket := scenario.name
			result := scenario.run(t, bucket)
			result.Name = scenario.name
			result.Operation = string(scenario.operation)
			if result.InjectedFailurePhase == "" {
				result.InjectedFailurePhase = scenario.phase
			}
			if result.ConsistencyVerdict != "pass" || result.Errors != 0 {
				report.Verdict = "fail"
			}
			report.Scenarios = append(report.Scenarios, result)
		})
	}
	requireElasticityPhaseCoverage(t, report)

	emitElasticityReport(t, report)
	if report.Verdict != "pass" {
		t.Fatalf("elasticity chaos/load verdict = %s", report.Verdict)
	}
}

func runSplitChaosScenario(t *testing.T, bucket, phaseName string, crashPhase SplitFinalizePhase) elasticityScenarioReport {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	td := elasticityChaosTableDescriptor()
	mgr := openElasticityManager(t, root, 1)
	createElasticityTable(t, mgr, td)

	plan, err := mgr.PlanPlacement(PlacementPlanRequest{
		Operation: PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatalf("plan split: %v", err)
	}
	if _, err := mgr.ApplyPlacement(ctx, PlacementApplyRequest{
		Plan:          plan,
		ExpectedEpoch: plan.BeforeEpoch,
	}); err != nil {
		t.Fatalf("apply split transition: %v", err)
	}

	rec := &elasticityRecorder{}
	targetRange := plan.After.Shards[1].Ranges[0]
	stop := startElasticityWriteLoad(t, mgr, td, &targetRange, bucket, rec)
	waitForElasticityWrites(t, rec, 32)

	restoreHook := installSplitCrashHook(crashPhase)
	_, err = mgr.FinalizeSplit(ctx, SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	restoreHook()
	if err == nil {
		stop()
		_ = mgr.Close()
		t.Fatalf("split finalize unexpectedly succeeded during injected %s crash", phaseName)
	}
	stop()
	if err := mgr.Close(); err != nil {
		t.Fatalf("close split manager after injected crash: %v", err)
	}

	mgr = openElasticityManager(t, root, 2)
	defer mgr.Close()
	stop = startElasticityWriteLoad(t, mgr, td, &targetRange, bucket, rec)
	waitForElasticityWrites(t, rec, 64)
	result, err := mgr.FinalizeSplit(ctx, SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err != nil {
		stop()
		t.Fatalf("retry split finalize after %s crash: %v", phaseName, err)
	}
	waitForElasticityWrites(t, rec, 96)
	stop()
	if result.Phase != string(SplitFinalizePhaseDone) || !result.Verification.Verified {
		t.Fatalf("split retry result = %+v", result)
	}
	if _, err := mgr.RouteForPK(pkBytes(t, rec.successes()[0].ID), plan.AfterEpoch); !errors.Is(err, ErrStaleRoute) {
		t.Fatalf("split stale route error = %v, want ErrStaleRoute", err)
	}

	report := buildElasticityScenarioReport(t, mgr, td, bucket, rec, plan.BeforeEpoch, mgr.RoutingEpoch(), phaseName)
	report.Restarts = 1
	return report
}

func runRangeMoveChaosScenario(t *testing.T, bucket, phaseName string, crashPhase RangeMoveFinalizePhase) elasticityScenarioReport {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	td := elasticityChaosTableDescriptor()
	mgr := openElasticityManager(t, root, 1)
	createElasticityTable(t, mgr, td)

	start := uint64(0)
	end := uint64(1) << 63
	plan, err := mgr.PlanPlacement(PlacementPlanRequest{
		Operation:  PlacementOperationRangeMove,
		ShardID:    0,
		RangeStart: &start,
		RangeEnd:   &end,
	})
	if err != nil {
		t.Fatalf("plan range move: %v", err)
	}
	if _, err := mgr.ApplyPlacement(ctx, PlacementApplyRequest{
		Plan:          plan,
		ExpectedEpoch: plan.BeforeEpoch,
	}); err != nil {
		t.Fatalf("apply range move transition: %v", err)
	}

	rec := &elasticityRecorder{}
	targetRange := plan.After.Shards[1].Ranges[0]
	stop := startElasticityWriteLoad(t, mgr, td, &targetRange, bucket, rec)
	waitForElasticityWrites(t, rec, 32)

	restoreHook := installRangeMoveCrashHook(crashPhase)
	_, err = mgr.FinalizeRangeMove(ctx, RangeMoveFinalizeRequest{
		SourceShardID: 0,
		TargetShardID: 1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	restoreHook()
	if err == nil {
		stop()
		_ = mgr.Close()
		t.Fatalf("range move finalize unexpectedly succeeded during injected %s crash", phaseName)
	}
	stop()
	if err := mgr.Close(); err != nil {
		t.Fatalf("close range move manager after injected crash: %v", err)
	}

	mgr = openElasticityManager(t, root, 2)
	defer mgr.Close()
	stop = startElasticityWriteLoad(t, mgr, td, &targetRange, bucket, rec)
	waitForElasticityWrites(t, rec, 64)
	result, err := mgr.FinalizeRangeMove(ctx, RangeMoveFinalizeRequest{
		SourceShardID: 0,
		TargetShardID: 1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err != nil {
		stop()
		t.Fatalf("retry range move finalize after %s crash: %v", phaseName, err)
	}
	waitForElasticityWrites(t, rec, 96)
	stop()
	if result.Phase != string(RangeMoveFinalizePhaseDone) || !result.Verification.Verified {
		t.Fatalf("range move retry result = %+v", result)
	}
	if _, err := mgr.RouteForPK(pkBytes(t, rec.successes()[0].ID), plan.AfterEpoch); !errors.Is(err, ErrStaleRoute) {
		t.Fatalf("range move stale route error = %v, want ErrStaleRoute", err)
	}

	report := buildElasticityScenarioReport(t, mgr, td, bucket, rec, plan.BeforeEpoch, mgr.RoutingEpoch(), phaseName)
	report.Restarts = 1
	return report
}

func runDrainLoadScenario(t *testing.T, bucket string) elasticityScenarioReport {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	td := elasticityChaosTableDescriptor()
	writeDrainPlacement(t, root)
	mgr := openElasticityManager(t, root, 1)
	defer mgr.Close()
	createElasticityTable(t, mgr, td)

	rec := &elasticityRecorder{}
	stop := startElasticityWriteLoad(t, mgr, td, nil, bucket, rec)
	waitForElasticityWrites(t, rec, 32)

	plan, err := mgr.PlanPlacement(PlacementPlanRequest{
		Operation:   PlacementOperationDrain,
		NodeID:      "n1",
		TargetNodes: []string{"n2"},
		MinVoters:   1,
	})
	if err != nil {
		stop()
		t.Fatalf("plan drain: %v", err)
	}
	localPlan := plan
	localPlan.Steps = nil // local no-Raft harness applies the drain metadata without external peers.
	if _, err := mgr.ApplyPlacement(ctx, PlacementApplyRequest{
		Plan:          localPlan,
		ExpectedEpoch: plan.BeforeEpoch,
	}); err != nil {
		stop()
		t.Fatalf("apply drain metadata: %v", err)
	}
	waitForElasticityWrites(t, rec, 96)
	stop()

	placement := mgr.Placement()
	if placement.Nodes["n1"].State != NodeStateDraining {
		t.Fatalf("n1 state = %s, want draining", placement.Nodes["n1"].State)
	}
	if containsString(placement.Shards[0].Voters, "n1") || !containsString(placement.Shards[0].Voters, "n2") {
		t.Fatalf("drain voters = %v, want n1 removed and n2 present", placement.Shards[0].Voters)
	}

	return buildElasticityScenarioReport(t, mgr, td, bucket, rec, plan.BeforeEpoch, mgr.RoutingEpoch(), "metadata_apply")
}

func openElasticityManager(t *testing.T, root string, shards int) *Manager {
	t.Helper()
	mgr, err := Open(context.Background(), Config{
		Root:      root,
		Shards:    shards,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("open manager: %v", err)
	}
	return mgr
}

func createElasticityTable(t *testing.T, mgr *Manager, td types.TableDescriptor) {
	t.Helper()
	shard0, ok := mgr.Shard(0)
	if !ok || shard0 == nil || shard0.Storage == nil {
		t.Fatal("missing shard 0 storage")
	}
	cat, err := catalog.New(shard0.Storage)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	if err := cat.Create(td); err != nil && !errors.Is(err, types.ErrTableAlreadyExists) {
		t.Fatalf("create table: %v", err)
	}
}

func elasticityChaosTableDescriptor() types.TableDescriptor {
	return types.TableDescriptor{
		Name:      elasticityChaosTable,
		KeySchema: types.KeySchema{PK: "id"},
		GSIs: []types.GSIDescriptor{{
			Name: elasticityChaosIndex,
			KeySchema: types.KeySchema{
				PK: "bucket",
				SK: "id",
			},
			Projection: types.IndexProjection{Mode: "ALL"},
		}},
	}
}

func writeDrainPlacement(t *testing.T, root string) {
	t.Helper()
	cat := DefaultPlacement(1, "n1", nil, nil, NodeCapacity{}, PlacementStrategyTokenRange)
	cat.Nodes["n2"] = NodeDescriptor{ID: "n2", RaftAddr: "127.0.0.1:9202", State: NodeStateActive, Capacity: NodeCapacity{Weight: 1}}
	cat.Nodes["n3"] = NodeDescriptor{ID: "n3", RaftAddr: "127.0.0.1:9203", State: NodeStateActive, Capacity: NodeCapacity{Weight: 1}}
	cat.normalize()
	if err := SavePlacementFile(filepath.Join(root, defaultPlacementFileName), cat); err != nil {
		t.Fatalf("save drain placement: %v", err)
	}
}

func startElasticityWriteLoad(t *testing.T, mgr *Manager, td types.TableDescriptor, rng *TokenRange, bucket string, rec *elasticityRecorder) func() {
	t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	const workers = 4
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func(worker int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				seq := atomic.AddInt64(&rec.next, 1)
				key, err := nextElasticityKey(mgr.Router(), rng, bucket, seq)
				if err != nil {
					rec.recordError(err)
					time.Sleep(time.Millisecond)
					continue
				}
				value := fmt.Sprintf("v-%06d", seq)
				item := types.Item{
					"id":     sAttrElasticity(key),
					"bucket": sAttrElasticity(bucket),
					"v":      sAttrElasticity(value),
					"worker": sAttrElasticity(fmt.Sprintf("%d", worker)),
				}
				started := time.Now()
				if err := putElasticityItem(mgr, td, item); err != nil {
					rec.recordError(err)
					time.Sleep(time.Millisecond)
					continue
				}
				rec.recordSuccess(elasticityWriteRecord{ID: key, Value: value, Bucket: bucket}, time.Since(started))
			}
		}(worker)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	return func() {
		close(stop)
		<-done
		rec.finish()
	}
}

func putElasticityItem(mgr *Manager, td types.TableDescriptor, item types.Item) error {
	pk, err := storage.AttrCanonicalBytes(item[td.KeySchema.PK])
	if err != nil {
		return err
	}
	targets, err := mgr.WriteTargetsForPK(pk, 0)
	if err != nil {
		return err
	}
	defer targets.Release()
	if targets.Primary == nil || targets.Primary.Storage == nil {
		return fmt.Errorf("missing primary target")
	}
	if err := targets.Primary.Storage.PutItemWith(td, item, storage.PutOptions{}); err != nil {
		return err
	}
	for _, mirror := range targets.Mirrors {
		if mirror == nil || mirror.Storage == nil {
			return fmt.Errorf("missing mirror target")
		}
		if err := mirror.Storage.PutItemWith(td, item, storage.PutOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func nextElasticityKey(router *Router, rng *TokenRange, bucket string, seq int64) (string, error) {
	for attempt := int64(0); attempt < 200_000; attempt++ {
		key := fmt.Sprintf("%s-key-%06d-%06d", bucket, seq, attempt)
		if rng == nil {
			return key, nil
		}
		pk, err := pkBytesForKey(key)
		if err != nil {
			return "", err
		}
		if rng.Contains(router.TokenForPK(pk)) {
			return key, nil
		}
	}
	return "", fmt.Errorf("could not find key in range %+v", rng)
}

func waitForElasticityWrites(t *testing.T, rec *elasticityRecorder, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if rec.successCount() >= want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d writes, got %d", want, rec.successCount())
}

func (r *elasticityRecorder) recordSuccess(record elasticityWriteRecord, latency time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.startedAt.IsZero() {
		r.startedAt = time.Now()
	}
	r.endedAt = time.Now()
	r.records = append(r.records, record)
	r.latencies = append(r.latencies, latency)
}

func (r *elasticityRecorder) recordError(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.startedAt.IsZero() {
		r.startedAt = time.Now()
	}
	r.endedAt = time.Now()
	r.errors = append(r.errors, err.Error())
}

func (r *elasticityRecorder) finish() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.startedAt.IsZero() {
		r.endedAt = time.Now()
	}
}

func (r *elasticityRecorder) successCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.records)
}

func (r *elasticityRecorder) successes() []elasticityWriteRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]elasticityWriteRecord(nil), r.records...)
}

func (r *elasticityRecorder) stats() (int, int, float64, float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	elapsed := r.endedAt.Sub(r.startedAt).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	latencies := append([]time.Duration(nil), r.latencies...)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	var p99 time.Duration
	if len(latencies) > 0 {
		idx := int(float64(len(latencies)-1) * 0.99)
		p99 = latencies[idx]
	}
	return len(r.records), len(r.errors), float64(len(r.records)) / elapsed, float64(p99.Microseconds()) / 1000
}

func installSplitCrashHook(crashPhase SplitFinalizePhase) func() {
	prev := splitFinalizeTestHook
	var fired bool
	splitFinalizeTestHook = func(phase SplitFinalizePhase, state SplitFinalizeState) error {
		if phase == crashPhase && !fired {
			fired = true
			return fmt.Errorf("injected split crash at %s epoch=%d", phase, state.BeforeEpoch)
		}
		return nil
	}
	return func() { splitFinalizeTestHook = prev }
}

func installRangeMoveCrashHook(crashPhase RangeMoveFinalizePhase) func() {
	prev := rangeMoveFinalizeTestHook
	var fired bool
	rangeMoveFinalizeTestHook = func(phase RangeMoveFinalizePhase, state RangeMoveFinalizeState) error {
		if phase == crashPhase && !fired {
			fired = true
			return fmt.Errorf("injected range move crash at %s epoch=%d", phase, state.BeforeEpoch)
		}
		return nil
	}
	return func() { rangeMoveFinalizeTestHook = prev }
}

func buildElasticityScenarioReport(t *testing.T, mgr *Manager, td types.TableDescriptor, bucket string, rec *elasticityRecorder, beforeEpoch, afterEpoch uint64, phase string) elasticityScenarioReport {
	t.Helper()
	records := rec.successes()
	consistency := verifyElasticityConsistency(t, mgr, td, bucket, records)
	writes, errors, throughput, p99 := rec.stats()
	result := elasticityScenarioReport{
		InjectedFailurePhase: phase,
		BeforeEpoch:          beforeEpoch,
		AfterEpoch:           afterEpoch,
		WriteCount:           writes,
		Throughput:           throughput,
		P99Millis:            p99,
		Errors:               errors,
		ExpectedItems:        len(records),
		RoutedItems:          consistency.routedItems,
		GSIItems:             consistency.gsiItems,
		Checksum:             consistency.checksum,
		ConsistencyVerdict:   consistency.verdict,
	}
	if result.ConsistencyVerdict != "pass" {
		t.Fatalf("consistency failed for bucket %s: %+v", bucket, consistency)
	}
	return result
}

type elasticityConsistency struct {
	routedItems int
	gsiItems    int
	checksum    string
	verdict     string
	problems    []string
}

func verifyElasticityConsistency(t *testing.T, mgr *Manager, td types.TableDescriptor, bucket string, records []elasticityWriteRecord) elasticityConsistency {
	t.Helper()
	expected := make(map[string]elasticityWriteRecord, len(records))
	for _, record := range records {
		expected[record.ID] = record
	}

	seenRouted := make(map[string]string, len(expected))
	for id, record := range expected {
		shard, err := mgr.RouteForPK(pkBytes(t, id), 0)
		if err != nil {
			return elasticityConsistency{verdict: "fail", problems: []string{fmt.Sprintf("route %s: %v", id, err)}}
		}
		item, err := shard.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttrElasticity(id)})
		if err != nil {
			return elasticityConsistency{verdict: "fail", problems: []string{fmt.Sprintf("get %s: %v", id, err)}}
		}
		if got := item["v"].S; got != record.Value {
			return elasticityConsistency{verdict: "fail", problems: []string{fmt.Sprintf("value %s = %s, want %s", id, got, record.Value)}}
		}
		seenRouted[id] = item["v"].S
	}

	occurrences := make(map[string]int, len(expected))
	for _, shard := range mgr.Shards() {
		if shard == nil || shard.Storage == nil || shard.State == ShardStateDecommissioned {
			continue
		}
		items, err := shard.Storage.ScanTable(td.Name, 0)
		if err != nil {
			return elasticityConsistency{verdict: "fail", problems: []string{fmt.Sprintf("scan shard %d: %v", shard.ID, err)}}
		}
		for _, item := range items {
			if item["bucket"].S != bucket {
				continue
			}
			occurrences[item["id"].S]++
		}
	}
	for id := range expected {
		if occurrences[id] != 1 {
			return elasticityConsistency{verdict: "fail", problems: []string{fmt.Sprintf("primary occurrences %s = %d, want 1", id, occurrences[id])}}
		}
	}

	gsiOccurrences := make(map[string]int, len(expected))
	for _, shard := range mgr.Shards() {
		if shard == nil || shard.Storage == nil || shard.State == ShardStateDecommissioned {
			continue
		}
		items, err := shard.Storage.QueryByGSI(td, elasticityChaosIndex, sAttrElasticity(bucket), storage.QueryOptions{})
		if err != nil {
			return elasticityConsistency{verdict: "fail", problems: []string{fmt.Sprintf("gsi shard %d: %v", shard.ID, err)}}
		}
		for _, item := range items {
			id := item["id"].S
			if _, ok := expected[id]; ok {
				gsiOccurrences[id]++
			}
		}
	}
	for id := range expected {
		if gsiOccurrences[id] != 1 {
			return elasticityConsistency{verdict: "fail", problems: []string{fmt.Sprintf("gsi occurrences %s = %d, want 1", id, gsiOccurrences[id])}}
		}
	}

	return elasticityConsistency{
		routedItems: len(seenRouted),
		gsiItems:    len(gsiOccurrences),
		checksum:    checksumElasticityRecords(records),
		verdict:     "pass",
	}
}

func checksumElasticityRecords(records []elasticityWriteRecord) string {
	sorted := append([]elasticityWriteRecord(nil), records...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	h := fnv.New64a()
	for _, record := range sorted {
		_, _ = h.Write([]byte(record.ID))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(record.Value))
		_, _ = h.Write([]byte{0xff})
	}
	return fmt.Sprintf("%016x", h.Sum64())
}

func emitElasticityReport(t *testing.T, report elasticityChaosReport) {
	t.Helper()
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshal elasticity report: %v", err)
	}
	t.Logf("elasticity chaos/load report:\n%s", raw)
	if path := os.Getenv("CEFAS_ELASTICITY_REPORT"); path != "" {
		reportPath, err := resolveElasticityReportPath(path)
		if err != nil {
			t.Fatalf("resolve elasticity report path: %v", err)
		}
		if err := os.MkdirAll(filepath.Dir(reportPath), 0o755); err != nil {
			t.Fatalf("create report dir: %v", err)
		}
		if err := os.WriteFile(reportPath, append(raw, '\n'), 0o644); err != nil {
			t.Fatalf("write elasticity report: %v", err)
		}
	}
}

func resolveElasticityReportPath(path string) (string, error) {
	if filepath.IsAbs(path) {
		return path, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for dir := cwd; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return filepath.Join(dir, path), nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return filepath.Join(cwd, path), nil
		}
	}
}

func requireElasticityPhaseCoverage(t *testing.T, report elasticityChaosReport) {
	t.Helper()
	covered := make(map[string]bool)
	for _, scenario := range report.Scenarios {
		covered[scenario.InjectedFailurePhase] = true
	}
	for _, phase := range []string{"copy", "catch_up", "catalog_publish", "cleanup"} {
		if !covered[phase] {
			t.Fatalf("elasticity chaos/load suite did not cover injected failure phase %q", phase)
		}
	}
}

func sAttrElasticity(s string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: s}
}

func pkBytes(t *testing.T, key string) []byte {
	t.Helper()
	pk, err := pkBytesForKey(key)
	if err != nil {
		t.Fatalf("canonical pk %q: %v", key, err)
	}
	return pk
}

func pkBytesForKey(key string) ([]byte, error) {
	return storage.AttrCanonicalBytes(sAttrElasticity(key))
}
