package runner

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/CefasDb/cefasdb/pkg/client"
)

// PhaseSummary is the JSON-serializable per-phase record written to the
// benchmark report.
type PhaseSummary struct {
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

type report struct {
	Label      string                `json:"label,omitempty"`
	Target     string                `json:"target"`
	Table      string                `json:"table"`
	StartedAt  string                `json:"started_at"`
	FinishedAt string                `json:"finished_at"`
	Config     reportConfig          `json:"config"`
	Phases     []PhaseSummary        `json:"phases"`
	RouteAware *routeAwareReadReport `json:"route_aware_reads,omitempty"`
}

type reportConfig struct {
	Items               int64   `json:"items"`
	Reads               int64   `json:"reads"`
	WriteDuration       string  `json:"write_duration,omitempty"`
	ReadDuration        string  `json:"read_duration,omitempty"`
	MixedDuration       string  `json:"mixed_duration,omitempty"`
	BatchSize           int     `json:"batch_size"`
	Workers             int     `json:"workers"`
	ReadWorkers         int     `json:"read_workers"`
	WriteRate           int64   `json:"write_rate,omitempty"`
	ReadRate            int64   `json:"read_rate,omitempty"`
	Users               int64   `json:"users"`
	PayloadBytes        int     `json:"payload_bytes"`
	PayloadMode         string  `json:"payload_mode"`
	LatencySampleRate   int64   `json:"latency_sample_rate"`
	StrongRead          bool    `json:"strong_read"`
	RouteAwareReads     bool    `json:"route_aware_reads"`
	RouteAwareReadNodes int     `json:"route_aware_read_nodes,omitempty"`
	StartedID           int64   `json:"start_id"`
	Keyspace            int64   `json:"keyspace"`
	ApproxItemKB        float64 `json:"approx_item_kb"`
}

type routeAwareReadReport struct {
	Attempts       uint64                     `json:"attempts"`
	Successes      uint64                     `json:"successes"`
	Refreshes      uint64                     `json:"refreshes"`
	Retries        uint64                     `json:"retries"`
	StaleRoutes    uint64                     `json:"stale_routes"`
	NoLocalReplica uint64                     `json:"no_local_replica"`
	LeaderServed   uint64                     `json:"leader_served"`
	FollowerServed uint64                     `json:"follower_served"`
	Nodes          []routeAwareReadNodeReport `json:"nodes,omitempty"`
}

type routeAwareReadNodeReport struct {
	NodeID            string  `json:"node_id"`
	Endpoint          string  `json:"endpoint"`
	Inflight          int64   `json:"inflight"`
	LatencyMillis     float64 `json:"latency_ms,omitempty"`
	LastErrorUnixNano int64   `json:"last_error_unix_nano,omitempty"`
}

// PrintStats writes a human-readable phase summary to stdout.
func PrintStats(stats PhaseStats) {
	summary := SummarizeStats(stats)
	rate := float64(stats.Units) / stats.Elapsed.Seconds()
	rpcRate := float64(stats.RPCs) / stats.Elapsed.Seconds()
	fmt.Printf("\n%s summary\n", stats.Name)
	fmt.Printf("  units:      %d\n", stats.Units)
	fmt.Printf("  rpc calls:  %d\n", stats.RPCs)
	fmt.Printf("  elapsed:    %s\n", stats.Elapsed.Round(time.Millisecond))
	fmt.Printf("  throughput: %.0f units/s\n", rate)
	fmt.Printf("  rpc rate:   %.0f rpc/s\n", rpcRate)
	fmt.Printf("  errors:     %d\n", stats.Errors)
	if stats.Found > 0 {
		fmt.Printf("  found:      %d\n", stats.Found)
	}
	if len(stats.Latencies) > 0 {
		fmt.Printf("  samples:    %d\n", summary.LatencySamples)
		fmt.Printf("  rpc p50:    %s\n", percentile(stats.Latencies, 50).Round(time.Microsecond))
		fmt.Printf("  rpc p95:    %s\n", percentile(stats.Latencies, 95).Round(time.Microsecond))
		fmt.Printf("  rpc p99:    %s\n", percentile(stats.Latencies, 99).Round(time.Microsecond))
		fmt.Printf("  rpc max:    %s\n", stats.Latencies[len(stats.Latencies)-1].Round(time.Microsecond))
	}
	fmt.Println()
}

// SummarizeStats converts a PhaseStats into the JSON-serializable PhaseSummary.
func SummarizeStats(stats PhaseStats) PhaseSummary {
	elapsed := stats.Elapsed.Seconds()
	out := PhaseSummary{
		Name:           stats.Name,
		Units:          stats.Units,
		RPCs:           stats.RPCs,
		ElapsedSeconds: elapsed,
		Errors:         stats.Errors,
		Found:          stats.Found,
		LatencySamples: len(stats.Latencies),
	}
	if elapsed > 0 {
		out.Throughput = float64(stats.Units) / elapsed
		out.RPCRate = float64(stats.RPCs) / elapsed
	}
	if len(stats.Latencies) > 0 {
		out.P50Millis = durationMillis(percentile(stats.Latencies, 50))
		out.P95Millis = durationMillis(percentile(stats.Latencies, 95))
		out.P99Millis = durationMillis(percentile(stats.Latencies, 99))
		out.MaxMillis = durationMillis(stats.Latencies[len(stats.Latencies)-1])
	}
	return out
}

// WriteReport serializes the full benchmark configuration plus phase summaries
// into cfg.JSONOutput.
func WriteReport(cfg Config, target string, startedAt, finishedAt time.Time, phases []PhaseSummary, routeStats ...client.RouteAwareReadStats) error {
	rep := report{
		Label:      cfg.Label,
		Target:     target,
		Table:      cfg.Table,
		StartedAt:  startedAt.Format(time.RFC3339Nano),
		FinishedAt: finishedAt.Format(time.RFC3339Nano),
		Config: reportConfig{
			Items:               cfg.Items,
			Reads:               cfg.Reads,
			WriteDuration:       durationString(cfg.WriteDuration),
			ReadDuration:        durationString(cfg.ReadDuration),
			MixedDuration:       durationString(cfg.MixedDuration),
			BatchSize:           cfg.BatchSize,
			Workers:             cfg.Workers,
			ReadWorkers:         cfg.ReadWorkers,
			WriteRate:           cfg.WriteRate,
			ReadRate:            cfg.ReadRate,
			Users:               cfg.Users,
			PayloadBytes:        cfg.PayloadBytes,
			PayloadMode:         cfg.PayloadMode,
			LatencySampleRate:   cfg.LatencySampleRate,
			StrongRead:          cfg.StrongRead,
			RouteAwareReads:     cfg.RouteAwareReads,
			RouteAwareReadNodes: cfg.RouteAwareReadNodes,
			StartedID:           cfg.StartID,
			Keyspace:            cfg.Keyspace,
			ApproxItemKB:        approximateItemKB(cfg.PayloadBytes),
		},
		Phases: phases,
	}
	if len(routeStats) > 0 && (cfg.RouteAwareReads || routeStats[0].Attempts > 0) {
		rep.RouteAware = summarizeRouteAwareReads(routeStats[0])
	}

	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(cfg.JSONOutput); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return os.WriteFile(cfg.JSONOutput, append(data, '\n'), 0o644)
}

func summarizeRouteAwareReads(stats client.RouteAwareReadStats) *routeAwareReadReport {
	out := &routeAwareReadReport{
		Attempts:       stats.Attempts,
		Successes:      stats.Successes,
		Refreshes:      stats.Refreshes,
		Retries:        stats.Retries,
		StaleRoutes:    stats.StaleRoutes,
		NoLocalReplica: stats.NoLocalReplica,
		LeaderServed:   stats.LeaderServed,
		FollowerServed: stats.FollowerServed,
	}
	for _, node := range stats.Nodes {
		item := routeAwareReadNodeReport{
			NodeID:        node.NodeID,
			Endpoint:      node.Endpoint,
			Inflight:      node.Inflight,
			LatencyMillis: durationMillis(node.Latency),
		}
		if !node.LastErrorAt.IsZero() {
			item.LastErrorUnixNano = node.LastErrorAt.UnixNano()
		}
		out.Nodes = append(out.Nodes, item)
	}
	return out
}
