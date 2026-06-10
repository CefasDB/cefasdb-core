// Package config is the cefas-server configuration loader. Precedence:
// flag > env > yaml file > default. The pattern mirrors codeq's
// loader so operators carry intuition between the two.
//
// Env variable names are derived from the flag/yaml path by
// upper-snake-casing and prefixing with CEFAS_, e.g.
// `http.addr` → `CEFAS_HTTP_ADDR`.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the deserialised cefas-server configuration. Every field
// has a sensible zero-value default so callers can construct an empty
// Config and still get a working single-node server.
type Config struct {
	Data string `yaml:"data"`
	HTTP struct {
		Addr string `yaml:"addr"`
	} `yaml:"http"`
	GRPC struct {
		Addr        string `yaml:"addr"`
		Reflection  bool   `yaml:"reflection"`
		TLSCertPath string `yaml:"tlsCertPath"`
		TLSKeyPath  string `yaml:"tlsKeyPath"`
		MTLSCAPath  string `yaml:"mtlsCaPath"`
	} `yaml:"grpc"`
	Storage struct {
		FsyncOnCommit               bool          `yaml:"fsyncOnCommit"`
		Profile                     string        `yaml:"profile"`
		RaftProfile                 string        `yaml:"raftProfile"`
		BlockCacheSizeBytes         int64         `yaml:"blockCacheSizeBytes"`
		MemTableSizeBytes           uint64        `yaml:"memTableSizeBytes"`
		MemTableStopWritesThreshold int           `yaml:"memTableStopWritesThreshold"`
		MaxConcurrentCompactions    int           `yaml:"maxConcurrentCompactions"`
		L0CompactionConcurrency     int           `yaml:"l0CompactionConcurrency"`
		L0CompactionThreshold       int           `yaml:"l0CompactionThreshold"`
		L0CompactionFileThreshold   int           `yaml:"l0CompactionFileThreshold"`
		L0StopWritesThreshold       int           `yaml:"l0StopWritesThreshold"`
		BytesPerSync                int           `yaml:"bytesPerSync"`
		WALBytesPerSync             int           `yaml:"walBytesPerSync"`
		BackpressureEnabled         bool          `yaml:"backpressureEnabled"`
		BackpressureRejectCritical  bool          `yaml:"backpressureRejectCritical"`
		BackpressureWarningL0Files  int64         `yaml:"backpressureWarningL0Files"`
		BackpressureCriticalL0Files int64         `yaml:"backpressureCriticalL0Files"`
		BackpressureWarningDebt     uint64        `yaml:"backpressureWarningDebtBytes"`
		BackpressureCriticalDebt    uint64        `yaml:"backpressureCriticalDebtBytes"`
		BackpressureWarningReadAmp  int           `yaml:"backpressureWarningReadAmp"`
		BackpressureCriticalReadAmp int           `yaml:"backpressureCriticalReadAmp"`
		BackpressureWarningDelay    time.Duration `yaml:"backpressureWarningDelay"`
		BackpressureCriticalDelay   time.Duration `yaml:"backpressureCriticalDelay"`
	} `yaml:"storage"`
	Cluster struct {
		Shards    int               `yaml:"shards"`
		MuxAddr   string            `yaml:"muxAddr"`
		SelfID    string            `yaml:"selfId"`
		Bootstrap bool              `yaml:"bootstrap"`
		Peers     map[string]string `yaml:"peers"`
		HTTPPeers map[string]string `yaml:"httpPeers"`
	} `yaml:"cluster"`
	Raft struct {
		Bind      string `yaml:"bind"`
		Path      string `yaml:"path"`
		StorePath string `yaml:"storePath"`
	} `yaml:"raft"`
	Identity struct {
		JwksURL   string        `yaml:"jwksUrl"`
		Issuer    string        `yaml:"issuer"`
		Audience  string        `yaml:"audience"`
		ClockSkew time.Duration `yaml:"clockSkew"`
	} `yaml:"identity"`
	Tracing struct {
		Endpoint   string  `yaml:"endpoint"`
		Insecure   bool    `yaml:"insecure"`
		SampleRate float64 `yaml:"sampleRate"`
	} `yaml:"tracing"`
	Metrics struct {
		Enabled bool `yaml:"enabled"`
	} `yaml:"metrics"`
}

// Defaults returns a Config populated with the same fallbacks every
// flag carries.
func Defaults() Config {
	var c Config
	c.Data = "./cefas-data"
	c.HTTP.Addr = ":8080"
	c.Identity.ClockSkew = 30 * time.Second
	c.Metrics.Enabled = true
	c.Tracing.SampleRate = 1.0
	return c
}

// LoadFile reads YAML from `path` over Defaults(). Errors only on
// IO or YAML failures — missing path is not an error (returns
// Defaults).
func LoadFile(path string) (Config, error) {
	cfg := Defaults()
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	return cfg, nil
}

// ApplyEnv overlays CEFAS_* environment variables onto cfg. The
// mapping mirrors the YAML hierarchy: cluster.muxAddr →
// CEFAS_CLUSTER_MUX_ADDR.
func ApplyEnv(cfg *Config) error {
	str := func(key, current string) string {
		if v, ok := os.LookupEnv("CEFAS_" + key); ok {
			return v
		}
		return current
	}
	boolean := func(key string, current bool) bool {
		if v, ok := os.LookupEnv("CEFAS_" + key); ok {
			parsed, err := strconv.ParseBool(v)
			if err == nil {
				return parsed
			}
		}
		return current
	}
	integer := func(key string, current int) int {
		if v, ok := os.LookupEnv("CEFAS_" + key); ok {
			parsed, err := strconv.Atoi(v)
			if err == nil {
				return parsed
			}
		}
		return current
	}
	integer64 := func(key string, current int64) int64 {
		if v, ok := os.LookupEnv("CEFAS_" + key); ok {
			parsed, err := strconv.ParseInt(v, 10, 64)
			if err == nil {
				return parsed
			}
		}
		return current
	}
	unsigned64 := func(key string, current uint64) uint64 {
		if v, ok := os.LookupEnv("CEFAS_" + key); ok {
			parsed, err := strconv.ParseUint(v, 10, 64)
			if err == nil {
				return parsed
			}
		}
		return current
	}
	dur := func(key string, current time.Duration) time.Duration {
		if v, ok := os.LookupEnv("CEFAS_" + key); ok {
			parsed, err := time.ParseDuration(v)
			if err == nil {
				return parsed
			}
		}
		return current
	}
	flt := func(key string, current float64) float64 {
		if v, ok := os.LookupEnv("CEFAS_" + key); ok {
			parsed, err := strconv.ParseFloat(v, 64)
			if err == nil {
				return parsed
			}
		}
		return current
	}

	cfg.Data = str("DATA", cfg.Data)
	cfg.HTTP.Addr = str("HTTP_ADDR", cfg.HTTP.Addr)
	cfg.GRPC.Addr = str("GRPC_ADDR", cfg.GRPC.Addr)
	cfg.GRPC.Reflection = boolean("GRPC_REFLECTION", cfg.GRPC.Reflection)
	cfg.GRPC.TLSCertPath = str("GRPC_TLS_CERT", cfg.GRPC.TLSCertPath)
	cfg.GRPC.TLSKeyPath = str("GRPC_TLS_KEY", cfg.GRPC.TLSKeyPath)
	cfg.GRPC.MTLSCAPath = str("GRPC_MTLS_CA", cfg.GRPC.MTLSCAPath)

	cfg.Storage.FsyncOnCommit = boolean("STORAGE_FSYNC", cfg.Storage.FsyncOnCommit)
	cfg.Storage.Profile = str("STORAGE_PROFILE", cfg.Storage.Profile)
	cfg.Storage.RaftProfile = str("STORAGE_RAFT_PROFILE", cfg.Storage.RaftProfile)
	cfg.Storage.BlockCacheSizeBytes = integer64("STORAGE_BLOCK_CACHE_SIZE_BYTES", cfg.Storage.BlockCacheSizeBytes)
	cfg.Storage.MemTableSizeBytes = unsigned64("STORAGE_MEMTABLE_SIZE_BYTES", cfg.Storage.MemTableSizeBytes)
	cfg.Storage.MemTableStopWritesThreshold = integer("STORAGE_MEMTABLE_STOP_WRITES_THRESHOLD", cfg.Storage.MemTableStopWritesThreshold)
	cfg.Storage.MaxConcurrentCompactions = integer("STORAGE_MAX_CONCURRENT_COMPACTIONS", cfg.Storage.MaxConcurrentCompactions)
	cfg.Storage.L0CompactionConcurrency = integer("STORAGE_L0_COMPACTION_CONCURRENCY", cfg.Storage.L0CompactionConcurrency)
	cfg.Storage.L0CompactionThreshold = integer("STORAGE_L0_COMPACTION_THRESHOLD", cfg.Storage.L0CompactionThreshold)
	cfg.Storage.L0CompactionFileThreshold = integer("STORAGE_L0_COMPACTION_FILE_THRESHOLD", cfg.Storage.L0CompactionFileThreshold)
	cfg.Storage.L0StopWritesThreshold = integer("STORAGE_L0_STOP_WRITES_THRESHOLD", cfg.Storage.L0StopWritesThreshold)
	cfg.Storage.BytesPerSync = integer("STORAGE_BYTES_PER_SYNC", cfg.Storage.BytesPerSync)
	cfg.Storage.WALBytesPerSync = integer("STORAGE_WAL_BYTES_PER_SYNC", cfg.Storage.WALBytesPerSync)
	cfg.Storage.BackpressureEnabled = boolean("STORAGE_BACKPRESSURE_ENABLED", cfg.Storage.BackpressureEnabled)
	cfg.Storage.BackpressureRejectCritical = boolean("STORAGE_BACKPRESSURE_REJECT_CRITICAL", cfg.Storage.BackpressureRejectCritical)
	cfg.Storage.BackpressureWarningL0Files = integer64("STORAGE_BACKPRESSURE_WARNING_L0_FILES", cfg.Storage.BackpressureWarningL0Files)
	cfg.Storage.BackpressureCriticalL0Files = integer64("STORAGE_BACKPRESSURE_CRITICAL_L0_FILES", cfg.Storage.BackpressureCriticalL0Files)
	cfg.Storage.BackpressureWarningDebt = unsigned64("STORAGE_BACKPRESSURE_WARNING_DEBT_BYTES", cfg.Storage.BackpressureWarningDebt)
	cfg.Storage.BackpressureCriticalDebt = unsigned64("STORAGE_BACKPRESSURE_CRITICAL_DEBT_BYTES", cfg.Storage.BackpressureCriticalDebt)
	cfg.Storage.BackpressureWarningReadAmp = integer("STORAGE_BACKPRESSURE_WARNING_READ_AMP", cfg.Storage.BackpressureWarningReadAmp)
	cfg.Storage.BackpressureCriticalReadAmp = integer("STORAGE_BACKPRESSURE_CRITICAL_READ_AMP", cfg.Storage.BackpressureCriticalReadAmp)
	cfg.Storage.BackpressureWarningDelay = dur("STORAGE_BACKPRESSURE_WARNING_DELAY", cfg.Storage.BackpressureWarningDelay)
	cfg.Storage.BackpressureCriticalDelay = dur("STORAGE_BACKPRESSURE_CRITICAL_DELAY", cfg.Storage.BackpressureCriticalDelay)

	cfg.Cluster.Shards = integer("CLUSTER_SHARDS", cfg.Cluster.Shards)
	cfg.Cluster.MuxAddr = str("CLUSTER_MUX_ADDR", cfg.Cluster.MuxAddr)
	cfg.Cluster.SelfID = str("CLUSTER_SELF_ID", cfg.Cluster.SelfID)
	cfg.Cluster.Bootstrap = boolean("CLUSTER_BOOTSTRAP", cfg.Cluster.Bootstrap)
	cfg.Cluster.Peers = mergeKV(cfg.Cluster.Peers, str("CLUSTER_PEERS", ""))
	cfg.Cluster.HTTPPeers = mergeKV(cfg.Cluster.HTTPPeers, str("CLUSTER_HTTP_PEERS", ""))

	cfg.Raft.Bind = str("RAFT_BIND", cfg.Raft.Bind)
	cfg.Raft.Path = str("RAFT_PATH", cfg.Raft.Path)
	cfg.Raft.StorePath = str("RAFT_STORE_PATH", cfg.Raft.StorePath)

	cfg.Identity.JwksURL = str("IDENTITY_JWKS_URL", cfg.Identity.JwksURL)
	cfg.Identity.Issuer = str("IDENTITY_ISSUER", cfg.Identity.Issuer)
	cfg.Identity.Audience = str("IDENTITY_AUDIENCE", cfg.Identity.Audience)
	cfg.Identity.ClockSkew = dur("IDENTITY_CLOCK_SKEW", cfg.Identity.ClockSkew)

	cfg.Tracing.Endpoint = str("TRACING_ENDPOINT", cfg.Tracing.Endpoint)
	cfg.Tracing.Insecure = boolean("TRACING_INSECURE", cfg.Tracing.Insecure)
	cfg.Tracing.SampleRate = flt("TRACING_SAMPLE", cfg.Tracing.SampleRate)

	cfg.Metrics.Enabled = boolean("METRICS_ENABLED", cfg.Metrics.Enabled)
	return nil
}

// ParsePeers turns a "id1=addr1,id2=addr2" string into the
// canonical map. Exposed so cefas-server can reuse it for the
// -raft-peers / -raft-http-peers flag conversions.
func ParsePeers(s string) (map[string]string, error) {
	out := make(map[string]string)
	if strings.TrimSpace(s) == "" {
		return out, nil
	}
	for _, entry := range strings.Split(s, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		i := strings.IndexByte(entry, '=')
		if i <= 0 || i == len(entry)-1 {
			return nil, fmt.Errorf("bad peer %q: expected id=addr", entry)
		}
		out[strings.TrimSpace(entry[:i])] = strings.TrimSpace(entry[i+1:])
	}
	return out, nil
}

// mergeKV merges a comma-separated id=addr override into base. Empty
// override leaves base intact.
func mergeKV(base map[string]string, override string) map[string]string {
	if override == "" {
		return base
	}
	parsed, err := ParsePeers(override)
	if err != nil {
		return base
	}
	if base == nil {
		base = make(map[string]string)
	}
	for k, v := range parsed {
		base[k] = v
	}
	return base
}
