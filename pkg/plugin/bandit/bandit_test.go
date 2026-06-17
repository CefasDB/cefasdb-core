package bandit

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/plugin"
)

// initBernoulli wires three Beta-Bernoulli arms with the given true
// click-through rates and returns a plugin pointing at a fresh
// in-memory store.
func initBernoulli(t *testing.T, strategy string, seed int64) (*Plugin, []float64) {
	t.Helper()
	p := NewPluginWith(NewMemoryStore(), seed)
	rates := []float64{0.10, 0.55, 0.30}
	spec := plugin.BanditSpec{
		BanditID: "ctr",
		Strategy: strategy,
	}
	for i, r := range rates {
		_ = r
		spec.Arms = append(spec.Arms, plugin.BanditArmSpec{
			ArmID:  armName(i),
			Family: FamilyBetaBernoulli,
		})
	}
	if err := p.Init(spec); err != nil {
		t.Fatalf("Init: %v", err)
	}
	return p, rates
}

func armName(i int) string {
	return string(rune('A' + i))
}

// TestThompsonConcentratesOnBest is the acceptance test from the
// issue: 10k pulls on a 3-arm Bernoulli bandit with known rates
// should put >90% of late pulls on the best arm.
func TestThompsonConcentratesOnBest(t *testing.T) {
	p, rates := initBernoulli(t, StrategyThompson, 42)
	rng := rand.New(rand.NewSource(1))

	const total = 10_000
	const tailFraction = 0.2
	tailStart := int(float64(total) * (1 - tailFraction))
	pullsByArm := map[string]int{}
	for i := 0; i < total; i++ {
		arm, err := p.Sample("ctr", nil)
		if err != nil {
			t.Fatalf("Sample: %v", err)
		}
		idx := int(arm[0] - 'A')
		reward := 0.0
		if rng.Float64() < rates[idx] {
			reward = 1.0
		}
		if err := p.Reward("ctr", arm, reward, nil); err != nil {
			t.Fatalf("Reward: %v", err)
		}
		if i >= tailStart {
			pullsByArm[arm]++
		}
	}
	tailTotal := total - tailStart
	bestArm := armName(1) // 0.55 is highest
	got := float64(pullsByArm[bestArm]) / float64(tailTotal)
	if got < 0.90 {
		t.Fatalf("Thompson did not concentrate on best arm: got %.2f%% of tail %d pulls, want >= 90%%", got*100, tailTotal)
	}
}

// TestUCB1ConvergesToBest exercises the UCB1 path. The bound is
// looser than Thompson so we accept >= 70% on the best arm.
func TestUCB1ConvergesToBest(t *testing.T) {
	p, rates := initBernoulli(t, StrategyUCB1, 7)
	rng := rand.New(rand.NewSource(2))
	const total = 10_000
	tailStart := int(float64(total) * 0.8)
	pullsByArm := map[string]int{}
	for i := 0; i < total; i++ {
		arm, err := p.Sample("ctr", nil)
		if err != nil {
			t.Fatalf("Sample: %v", err)
		}
		idx := int(arm[0] - 'A')
		reward := 0.0
		if rng.Float64() < rates[idx] {
			reward = 1.0
		}
		if err := p.Reward("ctr", arm, reward, nil); err != nil {
			t.Fatalf("Reward: %v", err)
		}
		if i >= tailStart {
			pullsByArm[arm]++
		}
	}
	tailTotal := total - tailStart
	got := float64(pullsByArm[armName(1)]) / float64(tailTotal)
	if got < 0.70 {
		t.Fatalf("UCB1 did not converge on best arm: got %.2f%%, want >= 70%%", got*100)
	}
}

// TestEpsilonExploresAndExploits checks the baseline learns the best
// arm with eps=0.1. Bar is lower because 10% of pulls are uniform.
func TestEpsilonExploresAndExploits(t *testing.T) {
	p := NewPluginWith(NewMemoryStore(), 99)
	rates := []float64{0.05, 0.7, 0.2}
	spec := plugin.BanditSpec{
		BanditID: "ctr",
		Strategy: StrategyEpsilon,
		Epsilon:  0.1,
	}
	for i := range rates {
		spec.Arms = append(spec.Arms, plugin.BanditArmSpec{ArmID: armName(i), Family: FamilyBetaBernoulli})
	}
	if err := p.Init(spec); err != nil {
		t.Fatalf("Init: %v", err)
	}
	rng := rand.New(rand.NewSource(3))
	const total = 5000
	tailStart := total * 8 / 10
	hits := 0
	tail := 0
	for i := 0; i < total; i++ {
		arm, err := p.Sample("ctr", nil)
		if err != nil {
			t.Fatalf("Sample: %v", err)
		}
		idx := int(arm[0] - 'A')
		reward := 0.0
		if rng.Float64() < rates[idx] {
			reward = 1.0
		}
		if err := p.Reward("ctr", arm, reward, nil); err != nil {
			t.Fatalf("Reward: %v", err)
		}
		if i >= tailStart {
			tail++
			if arm == armName(1) {
				hits++
			}
		}
	}
	frac := float64(hits) / float64(tail)
	// With eps=0.1, optimal arm should be picked ~90% of exploit calls + 1/3 of explore.
	if frac < 0.70 {
		t.Fatalf("epsilon-greedy hit %.2f%% on best arm; want >= 70%%", frac*100)
	}
}

func TestSampleEligibleThompsonCanPickNonMeanBestArm(t *testing.T) {
	for seed := int64(1); seed < 500; seed++ {
		p := NewPluginWith(NewMemoryStore(), seed)
		if err := p.Init(plugin.BanditSpec{
			BanditID: "restricted",
			Strategy: StrategyThompson,
			Arms: []plugin.BanditArmSpec{
				{ArmID: "A", Family: FamilyBetaBernoulli, Alpha: 6, Beta: 4},
				{ArmID: "B", Family: FamilyBetaBernoulli, Alpha: 5, Beta: 5},
			},
		}); err != nil {
			t.Fatalf("Init: %v", err)
		}
		arm, unknown, err := p.SampleEligible("restricted", nil, []string{"A", "B"})
		if err != nil {
			t.Fatalf("SampleEligible: %v", err)
		}
		if len(unknown) != 0 {
			t.Fatalf("unknown = %v, want none", unknown)
		}
		if arm == "B" {
			return
		}
	}
	t.Fatal("no deterministic Thompson seed selected the lower-mean eligible arm")
}

func TestSampleEligibleUCB1ExploresWithinEligibleSet(t *testing.T) {
	p := NewPluginWith(NewMemoryStore(), 7)
	if err := p.Init(plugin.BanditSpec{
		BanditID: "restricted",
		Strategy: StrategyUCB1,
		Arms: []plugin.BanditArmSpec{
			{ArmID: "A", Family: FamilyBetaBernoulli},
			{ArmID: "B", Family: FamilyBetaBernoulli},
			{ArmID: "C", Family: FamilyBetaBernoulli},
		},
	}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := p.Reward("restricted", "A", 1, nil); err != nil {
			t.Fatalf("Reward A: %v", err)
		}
	}
	arm, unknown, err := p.SampleEligible("restricted", nil, []string{"A", "B"})
	if err != nil {
		t.Fatalf("SampleEligible: %v", err)
	}
	if len(unknown) != 0 {
		t.Fatalf("unknown = %v, want none", unknown)
	}
	if arm != "B" {
		t.Fatalf("SampleEligible UCB1 = %q, want unexplored eligible arm B", arm)
	}
}

func TestSampleEligibleEpsilonRestrictsAndReportsUnknown(t *testing.T) {
	p := NewPluginWith(NewMemoryStore(), 99)
	if err := p.Init(plugin.BanditSpec{
		BanditID: "restricted",
		Strategy: StrategyEpsilon,
		Epsilon:  0.1,
		Arms: []plugin.BanditArmSpec{
			{ArmID: "A", Family: FamilyBetaBernoulli},
			{ArmID: "B", Family: FamilyBetaBernoulli},
			{ArmID: "C", Family: FamilyBetaBernoulli},
		},
	}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := p.Reward("restricted", "C", 1, nil); err != nil {
			t.Fatalf("Reward C: %v", err)
		}
	}
	arm, unknown, err := p.SampleEligible("restricted", nil, []string{"B", "Z"})
	if err != nil {
		t.Fatalf("SampleEligible: %v", err)
	}
	if arm != "B" {
		t.Fatalf("SampleEligible epsilon = %q, want only registered eligible arm B", arm)
	}
	if len(unknown) != 1 || unknown[0] != "Z" {
		t.Fatalf("unknown = %v, want [Z]", unknown)
	}
}

func TestSampleEligibleEpsilonCanExploreWithinEligibleSet(t *testing.T) {
	for seed := int64(1); seed < 500; seed++ {
		p := NewPluginWith(NewMemoryStore(), seed)
		if err := p.Init(plugin.BanditSpec{
			BanditID: "restricted",
			Strategy: StrategyEpsilon,
			Epsilon:  0.9,
			Arms: []plugin.BanditArmSpec{
				{ArmID: "A", Family: FamilyBetaBernoulli},
				{ArmID: "B", Family: FamilyBetaBernoulli},
			},
		}); err != nil {
			t.Fatalf("Init: %v", err)
		}
		for i := 0; i < 5; i++ {
			if err := p.Reward("restricted", "A", 1, nil); err != nil {
				t.Fatalf("Reward A: %v", err)
			}
		}
		arm, unknown, err := p.SampleEligible("restricted", nil, []string{"A", "B"})
		if err != nil {
			t.Fatalf("SampleEligible: %v", err)
		}
		if len(unknown) != 0 {
			t.Fatalf("unknown = %v, want none", unknown)
		}
		if arm == "B" {
			return
		}
	}
	t.Fatal("no deterministic epsilon seed explored the lower-mean eligible arm")
}

func TestSampleEligibleNoRegisteredEligibleArms(t *testing.T) {
	p := NewPluginWith(NewMemoryStore(), 1)
	if err := p.Init(plugin.BanditSpec{
		BanditID: "restricted",
		Strategy: StrategyThompson,
		Arms: []plugin.BanditArmSpec{
			{ArmID: "A", Family: FamilyBetaBernoulli},
		},
	}); err != nil {
		t.Fatalf("Init: %v", err)
	}
	_, unknown, err := p.SampleEligible("restricted", nil, []string{"Z"})
	if !errors.Is(err, ErrNoEligibleArms) {
		t.Fatalf("SampleEligible err = %v, want ErrNoEligibleArms", err)
	}
	if len(unknown) != 1 || unknown[0] != "Z" {
		t.Fatalf("unknown = %v, want [Z]", unknown)
	}
}

// TestPosteriorPersistsAcrossPluginInstances simulates a node restart
// by handing the same in-memory store to two plugin instances. The
// second instance must observe the posterior the first one built.
func TestPosteriorPersistsAcrossPluginInstances(t *testing.T) {
	store := NewMemoryStore()
	p1 := NewPluginWith(store, 1)
	spec := plugin.BanditSpec{BanditID: "b", Strategy: StrategyThompson}
	for i := 0; i < 2; i++ {
		spec.Arms = append(spec.Arms, plugin.BanditArmSpec{ArmID: armName(i), Family: FamilyBetaBernoulli})
	}
	if err := p1.Init(spec); err != nil {
		t.Fatalf("Init: %v", err)
	}
	for i := 0; i < 100; i++ {
		if err := p1.Reward("b", armName(0), 1, nil); err != nil {
			t.Fatalf("Reward: %v", err)
		}
	}
	// "Restart" by creating a fresh plugin against the same store.
	p2 := NewPluginWith(store, 2)
	snap, err := p2.Snapshot("b")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	for _, a := range snap.Arms {
		if a.ArmID == armName(0) {
			if a.Pulls != 100 {
				t.Fatalf("expected 100 pulls preserved, got %d", a.Pulls)
			}
			if a.Alpha < 100 {
				t.Fatalf("expected alpha to absorb rewards, got %.0f", a.Alpha)
			}
			return
		}
	}
	t.Fatal("arm A not in snapshot")
}

// TestRewardOptimisticLockRetry forces a version conflict on the
// first attempt; the plugin must retry and succeed.
func TestRewardOptimisticLockRetry(t *testing.T) {
	store := NewMemoryStore()
	p := NewPluginWith(store, 5)
	spec := plugin.BanditSpec{BanditID: "b", Strategy: StrategyThompson,
		Arms: []plugin.BanditArmSpec{{ArmID: "A", Family: FamilyBetaBernoulli}}}
	if err := p.Init(spec); err != nil {
		t.Fatalf("Init: %v", err)
	}
	// Wrap the store so the first PutArm call fails with
	// ErrConditionFailed; subsequent calls go through.
	racing := &racingStore{Store: store}
	p.Bind(racing)
	if err := p.Reward("b", "A", 1, nil); err != nil {
		t.Fatalf("Reward: %v", err)
	}
	if got := racing.attempts.Load(); got < 2 {
		t.Fatalf("expected at least 2 PutArm attempts; got %d", got)
	}
}

// TestMultiNodeConvergeOnSharedPosterior models a multi-node bandit:
// three plugin instances all hit the same Store concurrently; once
// the dust settles the shared posterior reflects every reward.
func TestMultiNodeConvergeOnSharedPosterior(t *testing.T) {
	store := NewMemoryStore()
	p1 := NewPluginWith(store, 11)
	spec := plugin.BanditSpec{BanditID: "shared", Strategy: StrategyThompson,
		Arms: []plugin.BanditArmSpec{
			{ArmID: "A", Family: FamilyBetaBernoulli},
			{ArmID: "B", Family: FamilyBetaBernoulli},
		}}
	if err := p1.Init(spec); err != nil {
		t.Fatalf("Init: %v", err)
	}
	p2 := NewPluginWith(store, 12)
	p3 := NewPluginWith(store, 13)
	nodes := []*Plugin{p1, p2, p3}

	const perNode = 200
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(p *Plugin) {
			defer wg.Done()
			for i := 0; i < perNode; i++ {
				arm := "A"
				if i%2 == 0 {
					arm = "B"
				}
				if err := p.Reward("shared", arm, 1, nil); err != nil {
					t.Errorf("Reward: %v", err)
					return
				}
			}
		}(n)
	}
	wg.Wait()
	snap, err := p1.Snapshot("shared")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	total := int64(0)
	for _, a := range snap.Arms {
		total += a.Pulls
	}
	want := int64(perNode * len(nodes))
	if total != want {
		t.Fatalf("posterior pulls drifted: got %d, want %d", total, want)
	}
}

// TestUnknownBanditError surfaces the right sentinel.
func TestUnknownBanditError(t *testing.T) {
	p := NewPluginWith(NewMemoryStore(), 0)
	if _, err := p.Sample("missing", nil); !errors.Is(err, ErrUnknownBandit) {
		t.Fatalf("expected ErrUnknownBandit, got %v", err)
	}
}

// racingStore wraps a Store and fails the first PutArm with
// ErrConditionFailed to exercise the retry path.
type racingStore struct {
	Store
	attempts atomic.Int64
}

func (r *racingStore) PutArm(rec ArmRecord, expectedVersion int64) error {
	n := r.attempts.Add(1)
	if n == 1 {
		return ErrConditionFailed
	}
	return r.Store.PutArm(rec, expectedVersion)
}

// Spot-check that the registry picks up the plugin via the package
// init() — this catches a missing blank-import in builtins.
func TestRegisteredInDefault(t *testing.T) {
	_, ok := plugin.Default.Lookup("bandit")
	if !ok {
		t.Fatal("bandit plugin not registered in plugin.Default")
	}
}
