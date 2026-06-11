package bandit

import (
	"math"
	"math/rand"
)

// sampleEpsilon: with probability eps pick a uniformly random arm;
// otherwise exploit the arm with the highest empirical mean. Ties go
// to the first arm by sort order so behaviour is deterministic given
// the same rng state.
func sampleEpsilon(rng *rand.Rand, arms []ArmRecord, eps float64) string {
	if eps <= 0 {
		eps = 0.1
	}
	if rng.Float64() < eps {
		return arms[rng.Intn(len(arms))].ArmID
	}
	best := arms[0].ArmID
	bestMean := math.Inf(-1)
	for _, a := range arms {
		m := posteriorMean(a)
		if a.Pulls > 0 {
			emp := a.Rewards / float64(a.Pulls)
			m = emp
		}
		if m > bestMean {
			bestMean = m
			best = a.ArmID
		}
	}
	return best
}
