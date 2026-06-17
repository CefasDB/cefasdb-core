package plugin

import (
	"time"

	"github.com/CefasDb/cefasdb/internal/core/index"
	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/internal/core/query"
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
	Lat, Lon     float64
	Radius       float64 // meters
	ActiveWithin time.Duration
	Extra        map[string]model.AttributeValue
}

// AudiencePlugin backs the ads workloads (Epic 6 / #102).
type AudiencePlugin interface {
	Plugin
	Select(req AudienceRequest) (CandidateSet, error)
	Estimate(req AudienceRequest) (int, error)
	Dedup(scope, key string, ttl time.Duration) (allowed bool, err error)
	FreqCap(scope, key string, limit int, window time.Duration) (allowed bool, err error)
}

// ===== Bandit (issue #246) =====

// BanditArmSpec describes one arm at registration time. Family selects
// the posterior model: "beta-bernoulli" (default) for click/no-click
// rewards, "gaussian" for continuous bounded rewards. Prior gives the
// initial parameters; for Beta-Bernoulli (Alpha, Beta) default to
// (1, 1); for Gaussian (Mu, Sigma) default to (0, 1). UCB1 and
// epsilon-greedy ignore Family but still use Alpha/Beta to seed pull
// counts and reward sums when warm-starting from a snapshot.
type BanditArmSpec struct {
	ArmID  string
	Family string // "beta-bernoulli" | "gaussian" | ""
	Alpha  float64
	Beta   float64
	Mu     float64
	Sigma  float64
}

// BanditSpec is the one-shot Init payload. Strategy is "thompson",
// "ucb1", or "epsilon-greedy". Epsilon applies to epsilon-greedy
// only; UCB1 uses C (exploration constant; default sqrt(2)).
type BanditSpec struct {
	BanditID string
	Strategy string
	Arms     []BanditArmSpec
	Epsilon  float64
	C        float64
}

// BanditArmStats is the posterior snapshot for one arm. Pulls /
// Rewards are cumulative since Init. For Beta-Bernoulli Alpha/Beta are
// the posterior parameters; Mean = Alpha / (Alpha + Beta). For
// Gaussian, Mu/Sigma describe the posterior; Mean = Mu.
type BanditArmStats struct {
	ArmID   string
	Family  string
	Alpha   float64
	Beta    float64
	Mu      float64
	Sigma   float64
	Pulls   int64
	Rewards float64
	Mean    float64
}

// BanditSnapshot is the per-bandit posterior payload returned by
// Snapshot. Strategy mirrors the value Init was called with.
type BanditSnapshot struct {
	BanditID string
	Strategy string
	Arms     []BanditArmStats
}

// BanditPlugin is the operator face every bandit strategy implements.
// Init must be called once per bandit-id before Sample / Reward.
// Posterior state lives in storage so updates survive restart; the
// plugin is otherwise free of in-process state for a given bandit-id.
// Context is an opaque map the caller threads through for future
// contextual variants — v1 implementations ignore it.
type BanditPlugin interface {
	Plugin
	Init(spec BanditSpec) error
	Sample(banditID string, context map[string]string) (armID string, err error)
	BatchSample(banditID string, context map[string]string, n int) ([]string, error)
	// SampleEligible applies the configured bandit strategy to a
	// caller-supplied eligible arm subset. Unknown arm IDs are returned
	// separately so orchestration layers can emit reason codes while
	// still sampling known eligible arms.
	SampleEligible(banditID string, context map[string]string, eligibleArmIDs []string) (armID string, unknownArmIDs []string, err error)
	Reward(banditID, armID string, reward float64, context map[string]string) error
	Snapshot(banditID string) (BanditSnapshot, error)
}
