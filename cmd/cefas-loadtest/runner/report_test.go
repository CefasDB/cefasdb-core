package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSummarizeStatsRoundTrip(t *testing.T) {
	t.Parallel()
	stats := PhaseStats{
		Name:    "write",
		Units:   1_000,
		RPCs:    10,
		Elapsed: 2 * time.Second,
		Latencies: []time.Duration{
			1 * time.Millisecond,
			2 * time.Millisecond,
			3 * time.Millisecond,
			4 * time.Millisecond,
			5 * time.Millisecond,
		},
		Errors: 1,
		Found:  900,
	}
	got := SummarizeStats(stats)
	if got.Name != "write" {
		t.Fatalf("name = %q", got.Name)
	}
	if got.Units != 1_000 || got.RPCs != 10 {
		t.Fatalf("counts off: %+v", got)
	}
	if got.ElapsedSeconds != 2 {
		t.Fatalf("elapsed = %v", got.ElapsedSeconds)
	}
	if got.Throughput != 500 {
		t.Fatalf("throughput = %v, want 500", got.Throughput)
	}
	if got.RPCRate != 5 {
		t.Fatalf("rpc rate = %v, want 5", got.RPCRate)
	}
	if got.Errors != 1 || got.Found != 900 {
		t.Fatalf("errors/found off: %+v", got)
	}
	if got.LatencySamples != 5 {
		t.Fatalf("latency samples = %d", got.LatencySamples)
	}
	// p50 of [1..5]ms = 3ms = 3.0 ms.
	if got.P50Millis != 3 {
		t.Fatalf("p50 = %v, want 3", got.P50Millis)
	}
	if got.MaxMillis != 5 {
		t.Fatalf("max = %v, want 5", got.MaxMillis)
	}
}

func TestSummarizeStatsZeroElapsed(t *testing.T) {
	t.Parallel()
	got := SummarizeStats(PhaseStats{Name: "noop"})
	if got.Throughput != 0 || got.RPCRate != 0 {
		t.Fatalf("zero-elapsed should yield zero rates, got %+v", got)
	}
	if got.LatencySamples != 0 {
		t.Fatalf("nil latencies should give 0 samples, got %d", got.LatencySamples)
	}
}

func TestWriteReportProducesParsableJSON(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "report.json")

	cfg := Config{
		Table:             "T",
		Items:             10,
		Reads:             5,
		WriteDuration:     time.Second,
		BatchSize:         1,
		Workers:           1,
		ReadWorkers:       1,
		Users:             1,
		PayloadBytes:      8,
		LatencySampleRate: 1,
		JSONOutput:        path,
		Label:             "ut",
		StartID:           7,
		Keyspace:          0,
	}
	phases := []PhaseSummary{{Name: "write", Units: 10, RPCs: 1, ElapsedSeconds: 1}}
	started := time.Now().Add(-time.Second)
	finished := time.Now()

	if err := WriteReport(cfg, "localhost:9090", started, finished, phases); err != nil {
		t.Fatalf("WriteReport: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	var decoded report
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal report: %v", err)
	}
	if decoded.Label != "ut" {
		t.Fatalf("label = %q", decoded.Label)
	}
	if decoded.Target != "localhost:9090" {
		t.Fatalf("target = %q", decoded.Target)
	}
	if decoded.Table != "T" {
		t.Fatalf("table = %q", decoded.Table)
	}
	if decoded.Config.Items != 10 {
		t.Fatalf("config.items = %d", decoded.Config.Items)
	}
	if decoded.Config.WriteDuration != "1s" {
		t.Fatalf("config.write_duration = %q", decoded.Config.WriteDuration)
	}
	if len(decoded.Phases) != 1 || decoded.Phases[0].Name != "write" {
		t.Fatalf("phases off: %+v", decoded.Phases)
	}
}
