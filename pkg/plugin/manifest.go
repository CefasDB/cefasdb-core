// Package plugin defines the registry and interfaces every CEFAS
// plugin honours. Built-in plugins (Bloom, HLL, Trigram, MinHash,
// Cosine, Haversine, Geohash, …) compile into the binary and
// register themselves via init() against the default Registry. v1 is
// strictly in-process; out-of-process / dlopen-based loading is out
// of scope.
//
// The boundary rule: nothing in pkg/plugin (or any pkg/plugin/*) may
// import pkg/api, pkg/sql, pkg/client, or any internal/* package.
// The same coregraph-style guard that protects pkg/core protects
// pkg/plugin.
package plugin

import (
	"encoding/json"
	"fmt"
)

// Kind classifies what a plugin provides. The registry uses Kind to
// route lookups + to dispatch query-planner operators.
type Kind uint8

const (
	KindUnspecified Kind = iota
	// KindIndex backs a secondary index — Trigram, Trie/Radix,
	// Bloom, Cuckoo, Roaring, MinHash, SimHash, VectorLSH, Geohash.
	KindIndex
	// KindDistance is a named distance / similarity operator —
	// Hamming, Levenshtein, Cosine, Haversine, …
	KindDistance
	// KindEstimator is an approximate aggregate — HyperLogLog,
	// Count-Min Sketch.
	KindEstimator
	// KindAudience packs geo + dedup + freqcap + aggregation for
	// the ads workloads (Epic 6 / #102).
	KindAudience
	// KindBandit backs multi-armed bandit operators (Thompson
	// sampling, UCB1, epsilon-greedy) used by next-best-action
	// workloads (issue #246).
	KindBandit
)

// String returns the canonical lower-case spelling used in manifests
// and JSON output.
func (k Kind) String() string {
	switch k {
	case KindIndex:
		return "index"
	case KindDistance:
		return "distance"
	case KindEstimator:
		return "estimator"
	case KindAudience:
		return "audience"
	case KindBandit:
		return "bandit"
	}
	return "unspecified"
}

// Manifest is the metadata every registered plugin carries.
// ConfigSchema is opaque to the registry — the plugin parses it when
// the engine hands it a fresh Descriptor.
type Manifest struct {
	Name         string          `json:"name"`
	Kind         Kind            `json:"kind"`
	Version      string          `json:"version"`
	Description  string          `json:"description,omitempty"`
	ConfigSchema json.RawMessage `json:"configSchema,omitempty"`
	// NeedsOldItem signals that the plugin's update path differentiates
	// behaviour based on the row's prior state (e.g. to undo a previous
	// indexing). When false, the mutation hook skips the snapshot read
	// before the write — material on the hot path (#487 fix #2). All
	// built-in plugins set this to false today; plugins that genuinely
	// need it (e.g. a counter index that decrements on prior values)
	// must opt in.
	NeedsOldItem bool `json:"needsOldItem,omitempty"`
}

// Validate fails fast on manifests that would break the registry.
// Called by Registry.Register; plugin authors don't usually invoke
// it directly.
func (m Manifest) Validate() error {
	if m.Name == "" {
		return fmt.Errorf("plugin manifest: name required")
	}
	if m.Kind == KindUnspecified {
		return fmt.Errorf("plugin manifest %q: kind required", m.Name)
	}
	if m.Version == "" {
		return fmt.Errorf("plugin manifest %q: version required", m.Name)
	}
	return nil
}
