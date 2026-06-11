package metrics

import (
	"math/bits"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	defaultHotspotBuckets              = 64
	defaultHotspotWindow               = time.Minute
	defaultHotspotCoolingWindow        = time.Minute
	defaultHotspotMaxSummaries         = 8
	defaultHotspotReadThreshold        = uint64(10_000)
	defaultHotspotWriteThreshold       = uint64(5_000)
	defaultHotspotBytesThreshold       = uint64(64 << 20)
	defaultHotspotLatencyThreshold     = 0.050
	defaultHotspotCompactionDebtThresh = uint64(1 << 30)
	defaultHotspotThrottleState        = 1
	maxHotspotBuckets                  = 4096
)

// RangeHotspotConfig controls bounded token-range bucketing and the
// thresholds used to mark a bucket hot in cluster status.
type RangeHotspotConfig struct {
	Buckets                      int
	Window                       time.Duration
	CoolingWindow                time.Duration
	MaxSummaries                 int
	ReadThreshold                uint64
	WriteThreshold               uint64
	BytesThreshold               uint64
	LatencyThresholdSeconds      float64
	CompactionDebtThresholdBytes uint64
	ThrottleStateThreshold       int
}

// DefaultRangeHotspotConfig returns production-safe defaults that keep
// cardinality bounded by shard_count * bucket_count.
func DefaultRangeHotspotConfig() RangeHotspotConfig {
	return RangeHotspotConfig{
		Buckets:                      defaultHotspotBuckets,
		Window:                       defaultHotspotWindow,
		CoolingWindow:                defaultHotspotCoolingWindow,
		MaxSummaries:                 defaultHotspotMaxSummaries,
		ReadThreshold:                defaultHotspotReadThreshold,
		WriteThreshold:               defaultHotspotWriteThreshold,
		BytesThreshold:               defaultHotspotBytesThreshold,
		LatencyThresholdSeconds:      defaultHotspotLatencyThreshold,
		CompactionDebtThresholdBytes: defaultHotspotCompactionDebtThresh,
		ThrottleStateThreshold:       defaultHotspotThrottleState,
	}
}

func normalizeRangeHotspotConfig(cfg RangeHotspotConfig) RangeHotspotConfig {
	def := DefaultRangeHotspotConfig()
	if cfg.Buckets <= 0 {
		cfg.Buckets = def.Buckets
	}
	if cfg.Buckets > maxHotspotBuckets {
		cfg.Buckets = maxHotspotBuckets
	}
	if cfg.Window <= 0 {
		cfg.Window = def.Window
	}
	if cfg.CoolingWindow <= 0 {
		cfg.CoolingWindow = def.CoolingWindow
	}
	if cfg.MaxSummaries <= 0 {
		cfg.MaxSummaries = def.MaxSummaries
	}
	if cfg.ReadThreshold == 0 {
		cfg.ReadThreshold = def.ReadThreshold
	}
	if cfg.WriteThreshold == 0 {
		cfg.WriteThreshold = def.WriteThreshold
	}
	if cfg.BytesThreshold == 0 {
		cfg.BytesThreshold = def.BytesThreshold
	}
	if cfg.LatencyThresholdSeconds <= 0 {
		cfg.LatencyThresholdSeconds = def.LatencyThresholdSeconds
	}
	if cfg.CompactionDebtThresholdBytes == 0 {
		cfg.CompactionDebtThresholdBytes = def.CompactionDebtThresholdBytes
	}
	if cfg.ThrottleStateThreshold <= 0 {
		cfg.ThrottleStateThreshold = def.ThrottleStateThreshold
	}
	return cfg
}

// RangeHotspotSummary is the compact status shape exposed through
// cluster status. Counters are scoped to the current rolling window.
type RangeHotspotSummary struct {
	ShardID             string   `json:"shardId"`
	Bucket              int      `json:"bucket"`
	BucketCount         int      `json:"bucketCount"`
	TokenStart          uint64   `json:"tokenStart"`
	TokenEnd            uint64   `json:"tokenEnd"`
	Reads               uint64   `json:"reads"`
	Writes              uint64   `json:"writes"`
	Bytes               uint64   `json:"bytes"`
	AvgLatencySeconds   float64  `json:"avgLatencySeconds,omitempty"`
	MaxLatencySeconds   float64  `json:"maxLatencySeconds,omitempty"`
	CompactionDebtBytes uint64   `json:"compactionDebtBytes,omitempty"`
	ThrottleState       int      `json:"throttleState,omitempty"`
	Status              string   `json:"status"`
	Reasons             []string `json:"reasons,omitempty"`
	WindowStartedUnix   int64    `json:"windowStartedUnix"`
	LastSeenUnix        int64    `json:"lastSeenUnix,omitempty"`
	HotUntilUnix        int64    `json:"hotUntilUnix,omitempty"`
}

type rangeBucketKey struct {
	shard  string
	bucket int
}

type rangeBucketStats struct {
	windowStarted  time.Time
	reads          uint64
	writes         uint64
	bytes          uint64
	latencySum     float64
	latencyCount   uint64
	maxLatency     float64
	compactionDebt uint64
	throttleState  int
	lastSeen       time.Time
	hotUntil       time.Time
	reasons        []string
}

// RangeHotspotTracker owns bounded per-token-bucket state. It is safe
// for concurrent API handlers and collectors.
type RangeHotspotTracker struct {
	cfg RangeHotspotConfig

	mu      sync.Mutex
	buckets map[rangeBucketKey]*rangeBucketStats
}

func NewRangeHotspotTracker(cfg RangeHotspotConfig) *RangeHotspotTracker {
	cfg = normalizeRangeHotspotConfig(cfg)
	return &RangeHotspotTracker{
		cfg:     cfg,
		buckets: make(map[rangeBucketKey]*rangeBucketStats),
	}
}

func (t *RangeHotspotTracker) Config() RangeHotspotConfig {
	if t == nil {
		return RangeHotspotConfig{}
	}
	return t.cfg
}

func (t *RangeHotspotTracker) BucketForToken(token uint64) int {
	if t == nil || t.cfg.Buckets <= 1 {
		return 0
	}
	hi, _ := bits.Mul64(token, uint64(t.cfg.Buckets))
	return int(hi)
}

func (t *RangeHotspotTracker) RecordOperation(shardID string, token uint64, op string, bytes uint64, latency time.Duration) {
	if t == nil {
		return
	}
	if shardID == "" {
		shardID = "0"
	}
	bucket := t.BucketForToken(token)
	now := time.Now()

	t.mu.Lock()
	defer t.mu.Unlock()
	stats := t.bucketLocked(shardID, bucket, now)
	stats.rollWindow(t.cfg, now)
	switch op {
	case "write":
		stats.writes++
	default:
		stats.reads++
	}
	stats.bytes += bytes
	if latency > 0 {
		seconds := latency.Seconds()
		stats.latencySum += seconds
		stats.latencyCount++
		if seconds > stats.maxLatency {
			stats.maxLatency = seconds
		}
	}
	stats.lastSeen = now
	t.evaluateLocked(stats, now)
}

func (t *RangeHotspotTracker) RecordShardPressure(shardID string, compactionDebtBytes uint64, throttleState int) {
	if t == nil {
		return
	}
	if shardID == "" {
		shardID = "0"
	}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	for bucket := 0; bucket < t.cfg.Buckets; bucket++ {
		stats := t.bucketLocked(shardID, bucket, now)
		stats.rollWindow(t.cfg, now)
		stats.compactionDebt = compactionDebtBytes
		stats.throttleState = throttleState
		t.evaluateLocked(stats, now)
	}
}

func (t *RangeHotspotTracker) Snapshot(max int) []RangeHotspotSummary {
	if t == nil {
		return nil
	}
	if max <= 0 {
		max = t.cfg.MaxSummaries
	}
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([]RangeHotspotSummary, 0, len(t.buckets))
	for key, stats := range t.buckets {
		stats.rollWindow(t.cfg, now)
		if stats.reads == 0 && stats.writes == 0 && stats.bytes == 0 && stats.compactionDebt == 0 && stats.throttleState == 0 && now.After(stats.hotUntil) {
			continue
		}
		out = append(out, t.summaryLocked(key, stats, now))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Status != out[j].Status {
			return statusRank(out[i].Status) < statusRank(out[j].Status)
		}
		opsI := out[i].Reads + out[i].Writes
		opsJ := out[j].Reads + out[j].Writes
		if opsI != opsJ {
			return opsI > opsJ
		}
		if out[i].Bytes != out[j].Bytes {
			return out[i].Bytes > out[j].Bytes
		}
		if out[i].MaxLatencySeconds != out[j].MaxLatencySeconds {
			return out[i].MaxLatencySeconds > out[j].MaxLatencySeconds
		}
		if out[i].ShardID != out[j].ShardID {
			return out[i].ShardID < out[j].ShardID
		}
		return out[i].Bucket < out[j].Bucket
	})
	if len(out) > max {
		out = out[:max]
	}
	return out
}

func (t *RangeHotspotTracker) bucketLocked(shardID string, bucket int, now time.Time) *rangeBucketStats {
	key := rangeBucketKey{shard: shardID, bucket: bucket}
	stats := t.buckets[key]
	if stats == nil {
		stats = &rangeBucketStats{windowStarted: now}
		t.buckets[key] = stats
	}
	return stats
}

func (s *rangeBucketStats) rollWindow(cfg RangeHotspotConfig, now time.Time) {
	if s.windowStarted.IsZero() {
		s.windowStarted = now
		return
	}
	if now.Sub(s.windowStarted) < cfg.Window {
		return
	}
	s.windowStarted = now
	s.reads = 0
	s.writes = 0
	s.bytes = 0
	s.latencySum = 0
	s.latencyCount = 0
	s.maxLatency = 0
	s.reasons = nil
}

func (t *RangeHotspotTracker) evaluateLocked(stats *rangeBucketStats, now time.Time) {
	reasons := hotspotReasons(t.cfg, stats)
	if len(reasons) == 0 {
		stats.reasons = nil
		return
	}
	stats.reasons = reasons
	stats.hotUntil = now.Add(t.cfg.CoolingWindow)
}

func hotspotReasons(cfg RangeHotspotConfig, stats *rangeBucketStats) []string {
	var reasons []string
	if stats.reads >= cfg.ReadThreshold {
		reasons = append(reasons, "read_threshold")
	}
	if stats.writes >= cfg.WriteThreshold {
		reasons = append(reasons, "write_threshold")
	}
	if stats.bytes >= cfg.BytesThreshold {
		reasons = append(reasons, "bytes_threshold")
	}
	if stats.maxLatency >= cfg.LatencyThresholdSeconds {
		reasons = append(reasons, "latency_threshold")
	}
	if stats.compactionDebt >= cfg.CompactionDebtThresholdBytes {
		reasons = append(reasons, "compaction_debt")
	}
	if stats.throttleState >= cfg.ThrottleStateThreshold {
		reasons = append(reasons, "throttling")
	}
	return reasons
}

func (t *RangeHotspotTracker) summaryLocked(key rangeBucketKey, stats *rangeBucketStats, now time.Time) RangeHotspotSummary {
	start, end := bucketBounds(key.bucket, t.cfg.Buckets)
	status := "normal"
	reasons := append([]string(nil), stats.reasons...)
	if len(reasons) > 0 {
		status = "hot"
	} else if now.Before(stats.hotUntil) {
		status = "cooling"
		reasons = []string{"cooling_window"}
	}
	avg := 0.0
	if stats.latencyCount > 0 {
		avg = stats.latencySum / float64(stats.latencyCount)
	}
	lastSeenUnix := int64(0)
	if !stats.lastSeen.IsZero() {
		lastSeenUnix = stats.lastSeen.Unix()
	}
	hotUntilUnix := int64(0)
	if !stats.hotUntil.IsZero() {
		hotUntilUnix = stats.hotUntil.Unix()
	}
	return RangeHotspotSummary{
		ShardID:             key.shard,
		Bucket:              key.bucket,
		BucketCount:         t.cfg.Buckets,
		TokenStart:          start,
		TokenEnd:            end,
		Reads:               stats.reads,
		Writes:              stats.writes,
		Bytes:               stats.bytes,
		AvgLatencySeconds:   avg,
		MaxLatencySeconds:   stats.maxLatency,
		CompactionDebtBytes: stats.compactionDebt,
		ThrottleState:       stats.throttleState,
		Status:              status,
		Reasons:             reasons,
		WindowStartedUnix:   stats.windowStarted.Unix(),
		LastSeenUnix:        lastSeenUnix,
		HotUntilUnix:        hotUntilUnix,
	}
}

func bucketBounds(bucket, buckets int) (uint64, uint64) {
	if buckets <= 1 {
		return 0, 0
	}
	start, _ := bits.Div64(uint64(bucket), 0, uint64(buckets))
	if bucket >= buckets-1 {
		return start, 0
	}
	end, _ := bits.Div64(uint64(bucket+1), 0, uint64(buckets))
	return start, end
}

func bucketLabel(bucket int) string {
	return strconv.Itoa(bucket)
}

func statusRank(status string) int {
	switch status {
	case "hot":
		return 0
	case "cooling":
		return 1
	default:
		return 2
	}
}
