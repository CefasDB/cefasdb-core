package cluster_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/metrics"
	"github.com/osvaldoandrade/cefas/internal/rebalance"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

const (
	rebalancerGateTable = "RebalancerGate"
)

type rebalancerSkewReport struct {
	GeneratedAt           string         `json:"generated_at"`
	Verdict               string         `json:"verdict"`
	Scenario              string         `json:"scenario"`
	Operation             string         `json:"operation"`
	DecisionStatus        string         `json:"decision_status"`
	DecisionReason        string         `json:"decision_reason"`
	BeforeEpoch           uint64         `json:"before_epoch"`
	AfterEpoch            uint64         `json:"after_epoch"`
	FinalizePhase         string         `json:"finalize_phase"`
	WorkloadWrites        int            `json:"workload_writes"`
	BeforeDistribution    map[string]int `json:"before_distribution"`
	AfterDistribution     map[string]int `json:"after_distribution"`
	BeforeMaxShardShare   float64        `json:"before_max_shard_share"`
	AfterMaxShardShare    float64        `json:"after_max_shard_share"`
	ShareReduction        float64        `json:"share_reduction"`
	BeforeThroughput      float64        `json:"before_throughput_writes_per_second"`
	AfterThroughput       float64        `json:"after_throughput_writes_per_second"`
	BeforeP99Millis       float64        `json:"before_p99_ms"`
	AfterP99Millis        float64        `json:"after_p99_ms"`
	ConsistencyVerdict    string         `json:"consistency_verdict"`
	ConsistencyItemsRead  int            `json:"consistency_items_read"`
	ConsistencyItemErrors int            `json:"consistency_item_errors"`
}

type fixedLoadRecord struct {
	ID     string
	Value  string
	Region string
}

type fixedLoadStats struct {
	Distribution map[string]int
	WriteCount   int
	Throughput   float64
	P99Millis    float64
}

type staticHotspots []metrics.RangeHotspotSummary

func (s staticHotspots) RangeHotspotSummaries(max int) []metrics.RangeHotspotSummary {
	out := append([]metrics.RangeHotspotSummary(nil), s...)
	if max > 0 && len(out) > max {
		return out[:max]
	}
	return out
}

func TestAutonomousRebalancerSkewReductionGate(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	mgr, err := cluster.Open(ctx, cluster.Config{
		Root:      root,
		Shards:    2,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatalf("open cluster manager: %v", err)
	}
	defer mgr.Close()

	td := rebalancerGateDescriptor()
	createTableOnShard(t, mgr, 0, td)

	quarter := uint64(1) << 62
	half := uint64(1) << 63
	firstQuarter := cluster.TokenRange{Start: 0, End: quarter}
	secondQuarter := cluster.TokenRange{Start: quarter, End: half}

	beforeRecords := append(
		fixedRecordsInRange(t, mgr.Router(), firstQuarter, "before-a", 64),
		fixedRecordsInRange(t, mgr.Router(), secondQuarter, "before-b", 64)...,
	)
	beforeStats := writeFixedLoad(t, mgr, td, beforeRecords)

	ctrl := rebalance.NewController(rebalance.Config{
		Mode:                    rebalance.ModeAuto,
		MinInterval:             time.Millisecond,
		MaxConcurrentOperations: 1,
		MaxHotspots:             1,
		MinVoters:               1,
	}, mgr, staticHotspots{{
		ShardID:     "0",
		Bucket:      0,
		BucketCount: 2,
		TokenStart:  0,
		TokenEnd:    half,
		Writes:      uint64(len(beforeRecords)),
		Bytes:       uint64(len(beforeRecords) * 256),
		Status:      "hot",
		Reasons:     []string{"write_threshold", "fixed_skew_workload"},
	}}, nil)
	decisions, err := ctrl.Tick(ctx)
	if err != nil {
		t.Fatalf("rebalancer tick: %v", err)
	}
	if len(decisions) != 1 {
		t.Fatalf("decisions len = %d, want 1: %+v", len(decisions), decisions)
	}
	decision := decisions[0]
	if decision.Status != "applied" || decision.Plan.Operation != cluster.PlacementOperationSplit {
		t.Fatalf("decision = %+v, want applied split", decision)
	}

	finalized, err := mgr.FinalizeSplit(ctx, cluster.SplitFinalizeRequest{
		ParentShardID:  0,
		ChildShardID:   2,
		ExpectedEpoch:  decision.Plan.AfterEpoch,
		WritesQuiesced: true,
	})
	if err != nil {
		t.Fatalf("finalize split after rebalancer decision: %v", err)
	}
	if finalized.Phase != string(cluster.SplitFinalizePhaseDone) || !finalized.Verification.Verified {
		t.Fatalf("finalize result = %+v", finalized)
	}

	afterRecords := append(
		fixedRecordsInRange(t, mgr.Router(), firstQuarter, "after-a", 64),
		fixedRecordsInRange(t, mgr.Router(), secondQuarter, "after-b", 64)...,
	)
	afterStats := writeFixedLoad(t, mgr, td, afterRecords)

	allRecords := append(append([]fixedLoadRecord(nil), beforeRecords...), afterRecords...)
	consistencyReads, consistencyErrors := verifyFixedRecords(t, mgr, td, allRecords)

	beforeShare := maxShardShare(beforeStats.Distribution)
	afterShare := maxShardShare(afterStats.Distribution)
	report := rebalancerSkewReport{
		GeneratedAt:           time.Now().UTC().Format(time.RFC3339),
		Verdict:               "pass",
		Scenario:              "autonomous-rebalancer-fixed-skew",
		Operation:             string(decision.Plan.Operation),
		DecisionStatus:        decision.Status,
		DecisionReason:        decision.Reason,
		BeforeEpoch:           decision.Plan.BeforeEpoch,
		AfterEpoch:            mgr.RoutingEpoch(),
		FinalizePhase:         finalized.Phase,
		WorkloadWrites:        len(beforeRecords),
		BeforeDistribution:    beforeStats.Distribution,
		AfterDistribution:     afterStats.Distribution,
		BeforeMaxShardShare:   beforeShare,
		AfterMaxShardShare:    afterShare,
		ShareReduction:        beforeShare - afterShare,
		BeforeThroughput:      beforeStats.Throughput,
		AfterThroughput:       afterStats.Throughput,
		BeforeP99Millis:       beforeStats.P99Millis,
		AfterP99Millis:        afterStats.P99Millis,
		ConsistencyVerdict:    "pass",
		ConsistencyItemsRead:  consistencyReads,
		ConsistencyItemErrors: consistencyErrors,
	}
	if consistencyErrors != 0 {
		report.Verdict = "fail"
		report.ConsistencyVerdict = "fail"
	}
	if beforeShare < 0.99 || afterShare > 0.60 || report.ShareReduction < 0.39 {
		report.Verdict = "fail"
	}
	emitRebalancerSkewReport(t, report)
	if report.Verdict != "pass" {
		t.Fatalf("rebalancer skew gate failed: %+v", report)
	}
}

func rebalancerGateDescriptor() types.TableDescriptor {
	return types.TableDescriptor{
		Name:      rebalancerGateTable,
		KeySchema: types.KeySchema{PK: "id"},
	}
}

func createTableOnShard(t *testing.T, mgr *cluster.Manager, shardID uint32, td types.TableDescriptor) {
	t.Helper()
	shard, ok := mgr.Shard(shardID)
	if !ok || shard == nil || shard.Storage == nil {
		t.Fatalf("missing shard %d storage", shardID)
	}
	cat, err := catalog.New(shard.Storage)
	if err != nil {
		t.Fatalf("open catalog: %v", err)
	}
	if err := cat.Create(td); err != nil {
		t.Fatalf("create table: %v", err)
	}
}

func fixedRecordsInRange(t *testing.T, router *cluster.Router, rng cluster.TokenRange, prefix string, count int) []fixedLoadRecord {
	t.Helper()
	records := make([]fixedLoadRecord, 0, count)
	for i := 0; len(records) < count && i < count*5000; i++ {
		id := fmt.Sprintf("%s-key-%06d", prefix, i)
		pk, err := storage.AttrCanonicalBytes(sAttr(id))
		if err != nil {
			t.Fatalf("canonical key %q: %v", id, err)
		}
		if !rng.Contains(router.TokenForPK(pk)) {
			continue
		}
		records = append(records, fixedLoadRecord{
			ID:     id,
			Value:  fmt.Sprintf("%s-value-%06d", prefix, len(records)),
			Region: prefix,
		})
	}
	if len(records) != count {
		t.Fatalf("generated %d records in range %+v, want %d", len(records), rng, count)
	}
	return records
}

func writeFixedLoad(t *testing.T, mgr *cluster.Manager, td types.TableDescriptor, records []fixedLoadRecord) fixedLoadStats {
	t.Helper()
	started := time.Now()
	latencies := make([]time.Duration, 0, len(records))
	distribution := activeShardDistribution(mgr.Placement())
	for _, record := range records {
		pk, item := fixedItem(t, record)
		shard, err := mgr.RouteForPK(pk, 0)
		if err != nil {
			t.Fatalf("route %s: %v", record.ID, err)
		}
		distribution[fmt.Sprintf("%d", shard.ID)]++

		begin := time.Now()
		targets, err := mgr.WriteTargetsForPK(pk, 0)
		if err != nil {
			t.Fatalf("write targets %s: %v", record.ID, err)
		}
		if targets.Primary == nil || targets.Primary.Storage == nil {
			targets.Release()
			t.Fatalf("missing primary target for %s", record.ID)
		}
		if err := targets.Primary.Storage.PutItemWith(td, item, storage.PutOptions{}); err != nil {
			targets.Release()
			t.Fatalf("put primary %s: %v", record.ID, err)
		}
		for _, mirror := range targets.Mirrors {
			if mirror == nil || mirror.Storage == nil {
				targets.Release()
				t.Fatalf("missing mirror target for %s", record.ID)
			}
			if err := mirror.Storage.PutItemWith(td, item, storage.PutOptions{}); err != nil {
				targets.Release()
				t.Fatalf("put mirror %s: %v", record.ID, err)
			}
		}
		targets.Release()
		latencies = append(latencies, time.Since(begin))
	}
	elapsed := time.Since(started).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	return fixedLoadStats{
		Distribution: distribution,
		WriteCount:   len(records),
		Throughput:   float64(len(records)) / elapsed,
		P99Millis:    p99Millis(latencies),
	}
}

func activeShardDistribution(cat cluster.PlacementCatalog) map[string]int {
	out := make(map[string]int)
	for _, shard := range cat.Shards {
		if shard.State == cluster.ShardStateActive {
			out[fmt.Sprintf("%d", shard.ID)] = 0
		}
	}
	return out
}

func verifyFixedRecords(t *testing.T, mgr *cluster.Manager, td types.TableDescriptor, records []fixedLoadRecord) (int, int) {
	t.Helper()
	var reads, errors int
	for _, record := range records {
		pk, _ := fixedItem(t, record)
		shard, err := mgr.RouteForPK(pk, 0)
		if err != nil {
			errors++
			continue
		}
		item, err := shard.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": sAttr(record.ID)})
		if err != nil {
			errors++
			continue
		}
		if item["v"].S != record.Value || item["region"].S != record.Region {
			errors++
			continue
		}
		reads++
	}
	return reads, errors
}

func fixedItem(t *testing.T, record fixedLoadRecord) ([]byte, types.Item) {
	t.Helper()
	item := types.Item{
		"id":     sAttr(record.ID),
		"v":      sAttr(record.Value),
		"region": sAttr(record.Region),
	}
	pk, err := storage.AttrCanonicalBytes(item["id"])
	if err != nil {
		t.Fatalf("canonical key %q: %v", record.ID, err)
	}
	return pk, item
}

func p99Millis(latencies []time.Duration) float64 {
	if len(latencies) == 0 {
		return 0
	}
	cp := append([]time.Duration(nil), latencies...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(float64(len(cp)-1) * 0.99)
	return float64(cp[idx].Microseconds()) / 1000
}

func maxShardShare(distribution map[string]int) float64 {
	var total, max int
	for _, count := range distribution {
		total += count
		if count > max {
			max = count
		}
	}
	if total == 0 {
		return 0
	}
	return float64(max) / float64(total)
}

func emitRebalancerSkewReport(t *testing.T, report rebalancerSkewReport) {
	t.Helper()
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatalf("marshal rebalancer skew report: %v", err)
	}
	t.Logf("rebalancer skew report:\n%s", raw)
	if path := os.Getenv("CEFAS_REBALANCER_REPORT"); path != "" {
		reportPath, err := resolveReportPath(path)
		if err != nil {
			t.Fatalf("resolve rebalancer report path: %v", err)
		}
		if err := os.MkdirAll(filepath.Dir(reportPath), 0o755); err != nil {
			t.Fatalf("create rebalancer report dir: %v", err)
		}
		if err := os.WriteFile(reportPath, append(raw, '\n'), 0o644); err != nil {
			t.Fatalf("write rebalancer report: %v", err)
		}
	}
}

func resolveReportPath(path string) (string, error) {
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

func sAttr(v string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: v}
}
