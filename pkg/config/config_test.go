package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/osvaldoandrade/cefas/pkg/config"
)

func TestDefaultsPopulated(t *testing.T) {
	d := config.Defaults()
	if d.HTTP.Addr == "" {
		t.Errorf("HTTP.Addr default missing")
	}
	if d.Identity.ClockSkew != 30*time.Second {
		t.Errorf("clock skew default = %v", d.Identity.ClockSkew)
	}
	if !d.Metrics.Enabled {
		t.Errorf("metrics should default on")
	}
	if d.Metrics.HotspotBuckets != 64 || d.Metrics.HotspotCoolingWindow != time.Minute {
		t.Errorf("hotspot defaults not populated: %+v", d.Metrics)
	}
	if d.Rebalancer.Mode != "dry-run" || d.Rebalancer.Interval != 30*time.Second || d.Rebalancer.MaxConcurrentOperations != 1 {
		t.Errorf("rebalancer defaults not populated: %+v", d.Rebalancer)
	}
}

func TestLoadFileMissingReturnsDefaults(t *testing.T) {
	cfg, err := config.LoadFile(filepath.Join(t.TempDir(), "no-such-file"))
	if err != nil {
		t.Fatalf("missing file should not error: %v", err)
	}
	if cfg.HTTP.Addr == "" {
		t.Errorf("defaults lost")
	}
}

func TestLoadFileYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cefas.yaml")
	yamlSrc := `
data: /var/lib/cefas-test
http:
  addr: ":18080"
cluster:
  shards: 3
  bootstrap: true
  peers:
    n1: 10.0.0.1:9100
    n2: 10.0.0.2:9100
identity:
  jwksUrl: https://tikti.example.com/jwks.json
  clockSkew: 45s
metrics:
  hotspotBuckets: 16
  hotspotWriteThreshold: 42
  hotspotLatencyThreshold: 75ms
rebalancer:
  enabled: true
  mode: manual
  interval: 10s
  manualPlanDir: /tmp/cefas-rebalance
`
	if err := os.WriteFile(path, []byte(yamlSrc), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.Addr != ":18080" || cfg.Cluster.Shards != 3 || !cfg.Cluster.Bootstrap {
		t.Fatalf("YAML did not override: %+v", cfg)
	}
	if cfg.Cluster.Peers["n2"] != "10.0.0.2:9100" {
		t.Fatalf("peer map lost: %+v", cfg.Cluster.Peers)
	}
	if cfg.Identity.ClockSkew != 45*time.Second {
		t.Fatalf("clock skew = %v", cfg.Identity.ClockSkew)
	}
	if cfg.Metrics.HotspotBuckets != 16 || cfg.Metrics.HotspotWriteThreshold != 42 || cfg.Metrics.HotspotLatencyThreshold != 75*time.Millisecond {
		t.Fatalf("hotspot metrics config not loaded: %+v", cfg.Metrics)
	}
	if !cfg.Rebalancer.Enabled || cfg.Rebalancer.Mode != "manual" || cfg.Rebalancer.Interval != 10*time.Second || cfg.Rebalancer.ManualPlanDir != "/tmp/cefas-rebalance" {
		t.Fatalf("rebalancer config not loaded: %+v", cfg.Rebalancer)
	}
}

func TestApplyEnv(t *testing.T) {
	t.Setenv("CEFAS_HTTP_ADDR", ":19090")
	t.Setenv("CEFAS_CLUSTER_SHARDS", "4")
	t.Setenv("CEFAS_METRICS_ENABLED", "false")
	t.Setenv("CEFAS_METRICS_HOTSPOT_BUCKETS", "32")
	t.Setenv("CEFAS_METRICS_HOTSPOT_WRITE_THRESHOLD", "99")
	t.Setenv("CEFAS_METRICS_HOTSPOT_LATENCY_THRESHOLD", "25ms")
	t.Setenv("CEFAS_REBALANCER_ENABLED", "true")
	t.Setenv("CEFAS_REBALANCER_MODE", "auto")
	t.Setenv("CEFAS_REBALANCER_MAX_CONCURRENT_OPERATIONS", "2")
	t.Setenv("CEFAS_REBALANCER_MIN_INTERVAL", "30s")
	t.Setenv("CEFAS_IDENTITY_CLOCK_SKEW", "1m")

	cfg := config.Defaults()
	if err := config.ApplyEnv(&cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.Addr != ":19090" {
		t.Errorf("http addr override: %q", cfg.HTTP.Addr)
	}
	if cfg.Cluster.Shards != 4 {
		t.Errorf("shards override: %d", cfg.Cluster.Shards)
	}
	if cfg.Metrics.Enabled {
		t.Errorf("metrics disable not applied")
	}
	if cfg.Metrics.HotspotBuckets != 32 || cfg.Metrics.HotspotWriteThreshold != 99 || cfg.Metrics.HotspotLatencyThreshold != 25*time.Millisecond {
		t.Errorf("hotspot env not applied: %+v", cfg.Metrics)
	}
	if !cfg.Rebalancer.Enabled || cfg.Rebalancer.Mode != "auto" || cfg.Rebalancer.MaxConcurrentOperations != 2 || cfg.Rebalancer.MinInterval != 30*time.Second {
		t.Errorf("rebalancer env not applied: %+v", cfg.Rebalancer)
	}
	if cfg.Identity.ClockSkew != time.Minute {
		t.Errorf("clock skew: %v", cfg.Identity.ClockSkew)
	}
}

func TestParsePeers(t *testing.T) {
	good := []struct {
		in   string
		want map[string]string
	}{
		{"", map[string]string{}},
		{"n1=127.0.0.1:9100", map[string]string{"n1": "127.0.0.1:9100"}},
		{"n1=a:1,n2=b:2", map[string]string{"n1": "a:1", "n2": "b:2"}},
		{"  n1 = a:1 ,  n2 = b:2 ", map[string]string{"n1": "a:1", "n2": "b:2"}},
	}
	for _, g := range good {
		got, err := config.ParsePeers(g.in)
		if err != nil {
			t.Fatalf("%q: %v", g.in, err)
		}
		if len(got) != len(g.want) {
			t.Fatalf("%q: got %v, want %v", g.in, got, g.want)
		}
	}
	if _, err := config.ParsePeers("nope"); err == nil {
		t.Errorf("missing = should error")
	}
}
