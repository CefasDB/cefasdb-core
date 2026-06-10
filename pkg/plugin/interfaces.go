package plugin

import (
	"time"

	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/core/query"
)

// Plugin is the type all four concrete plugin interfaces embed. The
// registry stores Plugins; callers cast to the specific interface
// after looking up by Kind.
type Plugin interface {
	Manifest() Manifest
}

// Candidate is one entry in a plugin-produced candidate set. The
// planner post-filters the candidate stream using a DistancePlugin.
type Candidate struct {
	Key   model.Item
	Score float64 // optional; 0 when the plugin can't estimate
}

// CandidateSet is the lazy stream of candidates. Implementations
// honour cancellation through ctx in their internal iteration; the
// Next contract surfaces (Candidate, true) until exhaustion, then
// (zero, false).
type CandidateSet interface {
	Next() (Candidate, bool)
	Err() error
	Close() error
}

// IndexPlugin backs secondary indexes (Trigram, Bloom, Trie, MinHash,
// SimHash, VectorLSH, Geohash, Roaring, Cuckoo).
type IndexPlugin interface {
	Plugin
	// Build seeds the index from an initial item stream — used by
	// CreateIndex on a pre-populated table.
	Build(desc index.Descriptor, items func(yield func(model.Item) bool)) error
	// Update reflects a row mutation (old may be nil on insert).
	Update(desc index.Descriptor, oldItem, newItem model.Item) error
	// Delete removes any pointers the plugin maintains for key.
	Delete(desc index.Descriptor, key model.Item) error
	// Query produces a candidate set for the request.
	Query(desc index.Descriptor, req IndexQuery) (CandidateSet, error)
	// Estimate returns the candidate-count the planner uses for
	// cost-model decisions.
	Estimate(desc index.Descriptor, req IndexQuery) (int, error)
}

// IndexQuery describes what the planner is asking an index plugin for.
// Fields are advisory; plugins ignore what they don't support.
type IndexQuery struct {
	// Predicate is the textual predicate the operator parsed — e.g.
	// `levenshtein(name, 'habibs') <= 2`. Plugins that translate
	// predicates to an internal form (trigram shingles, geohash
	// prefix) parse it themselves.
	Predicate string
	// Binds resolves :name placeholders.
	Binds map[string]model.AttributeValue
	// Limit caps the candidate set size. 0 means "no cap".
	Limit int
}

// DistancePlugin satisfies the same shape as query.DistanceOp; it
// wraps it so the registry can carry distance functions as Plugins.
type DistancePlugin interface {
	Plugin
	query.DistanceOp
}

// EstimatorPlugin backs cardinality / frequency estimators.
type EstimatorPlugin interface {
	Plugin
	Observe(stream string, value model.AttributeValue) error
	Estimate(stream string) (float64, error)
	// Merge folds another estimator's serialized state into this one.
	// Implementations must be associative and commutative.
	Merge(stream string, other []byte) error
}

// AudienceRequest packs the geo + targeting inputs for an audience
// query. Plugins ignore fields they don't need.
type AudienceRequest struct {
	Lat, Lon      float64
	Radius        float64 // meters
	ActiveWithin  time.Duration
	Extra         map[string]model.AttributeValue
}

// AudiencePlugin backs the ads workloads (Epic 6 / #102).
type AudiencePlugin interface {
	Plugin
	Select(req AudienceRequest) (CandidateSet, error)
	Estimate(req AudienceRequest) (int, error)
	Dedup(scope, key string, ttl time.Duration) (allowed bool, err error)
	FreqCap(scope, key string, limit int, window time.Duration) (allowed bool, err error)
}
