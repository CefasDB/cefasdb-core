// Package bandit is the built-in BanditPlugin (issue #246). One
// plugin instance multiplexes any number of bandits identified by
// BanditID and dispatches Sample / Reward to the strategy the bandit
// was Init'd with (thompson, ucb1, epsilon-greedy).
//
// Posterior state for each (banditID, armID) is stored as opaque
// records through the Store interface. The default store is in-memory
// and intended only for tests + the no-storage fast path; the cefas
// server injects a storage-backed Store via Bind so posteriors
// survive node restart.
//
// IMPORTANT — concurrency model. The atomic read-modify-write
// primitive (#242) is being built in a sibling branch and is NOT
// available here. Reward therefore loops on a ConditionExpression
// retry: read the current arm row + its version, compute the new
// posterior, write back guarded by `version = :v`; on
// ErrConditionFailed re-read and retry. This is correct (each commit
// is linearizable against the storage layer) but trades a small
// amount of write amplification for the missing primitive. Swap the
// loop for one atomic call once #242 lands.
package bandit

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/osvaldoandrade/cefas/pkg/plugin"
)

// Storage families and strategy names — exported so callers (gRPC
// handler, CLI) can validate without re-spelling string literals.
const (
	FamilyBetaBernoulli = "beta-bernoulli"
	FamilyGaussian      = "gaussian"

	StrategyThompson = "thompson"
	StrategyUCB1     = "ucb1"
	StrategyEpsilon  = "epsilon-greedy"

	// DefaultBanditTable is the table namespace the storage-backed
	// Store uses. Callers may pick another name via Plugin.Bind.
	DefaultBanditTable = "cefas_bandits"
)

// Errors surfaced to callers — wrapped so handlers can map to
// codes.NotFound / codes.FailedPrecondition cleanly.
var (
	ErrUnknownBandit   = errors.New("bandit: unknown banditID")
	ErrUnknownArm      = errors.New("bandit: unknown armID")
	ErrNoArms          = errors.New("bandit: arm spec required")
	ErrBadStrategy     = errors.New("bandit: unsupported strategy")
	ErrNoEligibleArms  = errors.New("bandit: no eligible registered arms")
	ErrConditionFailed = errors.New("bandit: posterior version mismatch")
	ErrTooManyRetries  = errors.New("bandit: gave up after retry budget")
)

// Store abstracts the persistent posterior state. The default
// in-memory implementation lives in this package; the cefas server
// injects a storage-backed Store via Bind so posteriors survive a
// restart.
//
// The contract:
//   - GetMeta returns the bandit's strategy + arm list. (nil, false, nil)
//     when the bandit has never been Init'd.
//   - PutMeta installs (or overwrites) the bandit metadata.
//   - GetArm reads one arm's posterior state. (nil, false, nil) on miss.
//   - PutArm writes the arm record. When expectedVersion >= 0 the write
//     must fail with ErrConditionFailed if the on-disk row's version
//     does not match. expectedVersion == -1 means unconditional.
//   - ListArms enumerates every arm record under banditID.
type Store interface {
	GetMeta(banditID string) (*MetaRecord, bool, error)
	PutMeta(rec MetaRecord) error
	GetArm(banditID, armID string) (*ArmRecord, bool, error)
	PutArm(rec ArmRecord, expectedVersion int64) error
	ListArms(banditID string) ([]ArmRecord, error)
}

// MetaRecord is what GetMeta returns. Strategy / Epsilon / C come
// from Init; ArmIDs lets ListArms degrade gracefully on stores that
// can't efficiently enumerate.
type MetaRecord struct {
	BanditID  string
	Strategy  string
	Epsilon   float64
	C         float64
	ArmIDs    []string
	CreatedAt time.Time
}

// ArmRecord is the on-disk shape of one arm's posterior. Version is
// the optimistic-lock counter Reward bumps on every successful write.
// Family selects how Alpha/Beta vs Mu/Sigma are interpreted.
type ArmRecord struct {
	BanditID string
	ArmID    string
	Family   string
	Alpha    float64
	Beta     float64
	Mu       float64
	Sigma    float64
	Pulls    int64
	Rewards  float64
	Version  int64
}

// Plugin is the built-in BanditPlugin. Strategy state is fully
// derived from the persisted posterior so multiple plugin instances
// pointing at the same Store stay in sync.
type Plugin struct {
	mu sync.Mutex

	store Store
	rng   *rand.Rand
	now   func() time.Time

	// retryBudget caps the optimistic-lock loop inside Reward. Tests
	// can shrink it. Zero means "use defaultRetryBudget".
	retryBudget int
}

const defaultRetryBudget = 64

// NewPlugin returns a plugin backed by the in-memory store. The
// server wires a storage-backed Store via Bind before registering
// against the plugin registry.
func NewPlugin() *Plugin {
	return &Plugin{
		store:       NewMemoryStore(),
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
		now:         time.Now,
		retryBudget: defaultRetryBudget,
	}
}

// NewPluginWith is the test-friendly constructor — pass an explicit
// Store and rng seed so behaviour is deterministic.
func NewPluginWith(store Store, seed int64) *Plugin {
	if store == nil {
		store = NewMemoryStore()
	}
	return &Plugin{
		store:       store,
		rng:         rand.New(rand.NewSource(seed)),
		now:         time.Now,
		retryBudget: defaultRetryBudget,
	}
}

// Bind swaps the active Store. Callers do this once at startup before
// any Init / Sample / Reward call.
func (p *Plugin) Bind(s Store) {
	if s == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.store = s
}

// SetRetryBudget overrides the optimistic-lock retry cap. Useful in
// tests that want to exercise ErrTooManyRetries.
func (p *Plugin) SetRetryBudget(n int) {
	if n <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.retryBudget = n
}

func (p *Plugin) Manifest() plugin.Manifest {
	return plugin.Manifest{
		Name:        "bandit",
		Kind:        plugin.KindBandit,
		Version:     "1",
		Description: "Thompson sampling / UCB1 / epsilon-greedy multi-armed bandit operators (issue #246)",
	}
}

// Init registers a bandit-id + its arms. Strategy is one of
// StrategyThompson / StrategyUCB1 / StrategyEpsilon. Calling Init
// twice on the same banditID is allowed and replaces the metadata
// without touching arm posteriors that already exist — useful for
// upgrading strategy on a warm bandit.
func (p *Plugin) Init(spec plugin.BanditSpec) error {
	if spec.BanditID == "" {
		return fmt.Errorf("bandit: BanditID required")
	}
	if len(spec.Arms) == 0 {
		return ErrNoArms
	}
	strat, err := normalizeStrategy(spec.Strategy)
	if err != nil {
		return err
	}
	c := spec.C
	if strat == StrategyUCB1 && c <= 0 {
		c = math.Sqrt(2)
	}
	eps := spec.Epsilon
	if strat == StrategyEpsilon {
		if eps <= 0 || eps >= 1 {
			eps = 0.1
		}
	}
	armIDs := make([]string, 0, len(spec.Arms))
	for _, arm := range spec.Arms {
		if arm.ArmID == "" {
			return fmt.Errorf("bandit: arm ID required")
		}
		armIDs = append(armIDs, arm.ArmID)
	}
	sort.Strings(armIDs)

	p.mu.Lock()
	store := p.store
	p.mu.Unlock()
	if err := store.PutMeta(MetaRecord{
		BanditID:  spec.BanditID,
		Strategy:  strat,
		Epsilon:   eps,
		C:         c,
		ArmIDs:    armIDs,
		CreatedAt: p.now(),
	}); err != nil {
		return fmt.Errorf("bandit: put meta: %w", err)
	}
	for _, arm := range spec.Arms {
		family := normalizeFamily(arm.Family)
		alpha, beta := arm.Alpha, arm.Beta
		if family == FamilyBetaBernoulli {
			if alpha <= 0 {
				alpha = 1
			}
			if beta <= 0 {
				beta = 1
			}
		}
		mu, sigma := arm.Mu, arm.Sigma
		if family == FamilyGaussian && sigma <= 0 {
			sigma = 1
		}
		existing, ok, err := store.GetArm(spec.BanditID, arm.ArmID)
		if err != nil {
			return fmt.Errorf("bandit: get arm: %w", err)
		}
		if ok {
			// Preserve posterior; only refresh static fields if changed.
			if existing.Family == family {
				continue
			}
			// Family mismatch — overwrite from scratch so behaviour stays defined.
		}
		if err := store.PutArm(ArmRecord{
			BanditID: spec.BanditID,
			ArmID:    arm.ArmID,
			Family:   family,
			Alpha:    alpha,
			Beta:     beta,
			Mu:       mu,
			Sigma:    sigma,
		}, -1); err != nil {
			return fmt.Errorf("bandit: seed arm: %w", err)
		}
	}
	return nil
}

// Sample returns the chosen arm. Strategy comes from the meta
// record; the rng is seeded once at construction so callers can make
// the run deterministic by injecting a fixed seed via NewPluginWith.
func (p *Plugin) Sample(banditID string, _ map[string]string) (string, error) {
	meta, arms, err := p.snapshotInternal(banditID)
	if err != nil {
		return "", err
	}
	if len(arms) == 0 {
		return "", ErrNoArms
	}
	return p.sampleArm(meta, arms)
}

// BatchSample returns n independent samples — each call to Sample
// uses the live (unchanged) posterior since no Reward has happened
// in between. Useful for batch ranking / fan-out scoring.
func (p *Plugin) BatchSample(banditID string, ctx map[string]string, n int) ([]string, error) {
	if n <= 0 {
		return nil, fmt.Errorf("bandit: BatchSample n must be > 0")
	}
	meta, arms, err := p.snapshotInternal(banditID)
	if err != nil {
		return nil, err
	}
	if len(arms) == 0 {
		return nil, ErrNoArms
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		pick, err := p.sampleArm(meta, arms)
		if err != nil {
			return nil, err
		}
		out = append(out, pick)
	}
	_ = ctx
	return out, nil
}

// SampleEligible applies the configured strategy to the intersection
// of registered arms and the caller-provided eligible set.
func (p *Plugin) SampleEligible(banditID string, _ map[string]string, eligibleArmIDs []string) (string, []string, error) {
	meta, arms, err := p.snapshotInternal(banditID)
	if err != nil {
		return "", nil, err
	}
	if len(arms) == 0 {
		return "", nil, ErrNoArms
	}
	filtered, unknown := restrictArms(arms, eligibleArmIDs)
	if len(filtered) == 0 {
		return "", unknown, ErrNoEligibleArms
	}
	pick, err := p.sampleArm(meta, filtered)
	return pick, unknown, err
}

// Reward updates the posterior for (banditID, armID) by `reward`.
// Beta-Bernoulli treats values <= 0.5 as 0 and > 0.5 as 1 for the
// alpha/beta update; the raw value is still summed into Rewards for
// observability. Gaussian arms accumulate Mu via a running mean.
//
// Concurrency: optimistic-lock retry loop on the posterior version.
// On ErrConditionFailed we re-read and recompute; on a fresh family
// mismatch we error. The retry budget is bounded by retryBudget.
func (p *Plugin) Reward(banditID, armID string, reward float64, _ map[string]string) error {
	if banditID == "" || armID == "" {
		return fmt.Errorf("bandit: banditID + armID required")
	}
	p.mu.Lock()
	store := p.store
	budget := p.retryBudget
	p.mu.Unlock()
	if budget <= 0 {
		budget = defaultRetryBudget
	}
	for attempt := 0; attempt < budget; attempt++ {
		rec, ok, err := store.GetArm(banditID, armID)
		if err != nil {
			return fmt.Errorf("bandit: get arm: %w", err)
		}
		if !ok {
			return ErrUnknownArm
		}
		next := *rec
		next.Pulls++
		next.Rewards += reward
		switch next.Family {
		case FamilyBetaBernoulli:
			if reward > 0.5 {
				next.Alpha++
			} else {
				next.Beta++
			}
		case FamilyGaussian:
			// Online mean / std update (Welford).
			n := float64(next.Pulls)
			delta := reward - next.Mu
			next.Mu += delta / n
			if n > 1 {
				next.Sigma = math.Sqrt(((n-2)/(n-1))*next.Sigma*next.Sigma + (delta*delta)/n)
			}
		default:
			// Unknown family — treat reward as Bernoulli for safety.
			if reward > 0.5 {
				next.Alpha++
			} else {
				next.Beta++
			}
		}
		next.Version = rec.Version + 1
		err = store.PutArm(next, rec.Version)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrConditionFailed) {
			return fmt.Errorf("bandit: put arm: %w", err)
		}
		// retry
	}
	return ErrTooManyRetries
}

// Snapshot returns the live posterior for every arm under banditID.
// Used by `bandit describe` for observability.
func (p *Plugin) Snapshot(banditID string) (plugin.BanditSnapshot, error) {
	meta, arms, err := p.snapshotInternal(banditID)
	if err != nil {
		return plugin.BanditSnapshot{}, err
	}
	out := plugin.BanditSnapshot{
		BanditID: banditID,
		Strategy: meta.Strategy,
		Arms:     make([]plugin.BanditArmStats, 0, len(arms)),
	}
	for _, a := range arms {
		out.Arms = append(out.Arms, plugin.BanditArmStats{
			ArmID:   a.ArmID,
			Family:  a.Family,
			Alpha:   a.Alpha,
			Beta:    a.Beta,
			Mu:      a.Mu,
			Sigma:   a.Sigma,
			Pulls:   a.Pulls,
			Rewards: a.Rewards,
			Mean:    posteriorMean(a),
		})
	}
	sort.Slice(out.Arms, func(i, j int) bool { return out.Arms[i].ArmID < out.Arms[j].ArmID })
	return out, nil
}

// ---------- helpers ----------

func (p *Plugin) snapshotInternal(banditID string) (MetaRecord, []ArmRecord, error) {
	if banditID == "" {
		return MetaRecord{}, nil, fmt.Errorf("bandit: banditID required")
	}
	p.mu.Lock()
	store := p.store
	p.mu.Unlock()
	meta, ok, err := store.GetMeta(banditID)
	if err != nil {
		return MetaRecord{}, nil, fmt.Errorf("bandit: get meta: %w", err)
	}
	if !ok {
		return MetaRecord{}, nil, ErrUnknownBandit
	}
	arms, err := store.ListArms(banditID)
	if err != nil {
		return MetaRecord{}, nil, fmt.Errorf("bandit: list arms: %w", err)
	}
	return *meta, arms, nil
}

func (p *Plugin) sampleArm(meta MetaRecord, arms []ArmRecord) (string, error) {
	if len(arms) == 0 {
		return "", ErrNoArms
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	switch meta.Strategy {
	case StrategyThompson:
		return sampleThompson(p.rng, arms), nil
	case StrategyUCB1:
		return sampleUCB1(arms, meta.C), nil
	case StrategyEpsilon:
		return sampleEpsilon(p.rng, arms, meta.Epsilon), nil
	}
	return "", ErrBadStrategy
}

func restrictArms(arms []ArmRecord, eligibleArmIDs []string) ([]ArmRecord, []string) {
	byID := make(map[string]ArmRecord, len(arms))
	for _, arm := range arms {
		byID[arm.ArmID] = arm
	}
	seen := make(map[string]struct{}, len(eligibleArmIDs))
	filtered := make([]ArmRecord, 0, len(eligibleArmIDs))
	unknown := make([]string, 0)
	for _, id := range eligibleArmIDs {
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		arm, ok := byID[id]
		if !ok {
			unknown = append(unknown, id)
			continue
		}
		filtered = append(filtered, arm)
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].ArmID < filtered[j].ArmID })
	sort.Strings(unknown)
	return filtered, unknown
}

func normalizeFamily(f string) string {
	switch f {
	case "", FamilyBetaBernoulli:
		return FamilyBetaBernoulli
	case FamilyGaussian:
		return FamilyGaussian
	}
	return FamilyBetaBernoulli
}

func normalizeStrategy(s string) (string, error) {
	switch s {
	case "", StrategyThompson:
		return StrategyThompson, nil
	case StrategyUCB1:
		return StrategyUCB1, nil
	case StrategyEpsilon, "epsilon", "eps":
		return StrategyEpsilon, nil
	}
	return "", fmt.Errorf("%w: %q", ErrBadStrategy, s)
}

func posteriorMean(a ArmRecord) float64 {
	switch a.Family {
	case FamilyGaussian:
		return a.Mu
	}
	d := a.Alpha + a.Beta
	if d == 0 {
		return 0
	}
	return a.Alpha / d
}

// ---------- serialization helpers ----------

// EncodeArm / DecodeArm + EncodeMeta / DecodeMeta give external Store
// implementations (the storage-backed one in pkg/api) a stable wire
// format without re-spelling field tags.
func EncodeArm(a ArmRecord) ([]byte, error) { return json.Marshal(a) }
func DecodeArm(b []byte) (ArmRecord, error) {
	var a ArmRecord
	err := json.Unmarshal(b, &a)
	return a, err
}
func EncodeMeta(m MetaRecord) ([]byte, error) { return json.Marshal(m) }
func DecodeMeta(b []byte) (MetaRecord, error) {
	var m MetaRecord
	err := json.Unmarshal(b, &m)
	return m, err
}

func init() { plugin.Default.MustRegister(NewPlugin()) }
