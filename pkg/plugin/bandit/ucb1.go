package bandit

import "math"

// sampleUCB1 returns argmax_i (mean_i + c * sqrt(2 * ln(N) / n_i)).
// Any arm that has not yet been pulled is selected first so every arm
// gets at least one observation before the bound kicks in.
func sampleUCB1(arms []ArmRecord, c float64) string {
	if c <= 0 {
		c = math.Sqrt(2)
	}
	total := int64(0)
	for _, a := range arms {
		total += a.Pulls
		if a.Pulls == 0 {
			return a.ArmID
		}
	}
	if total == 0 {
		return arms[0].ArmID
	}
	bestScore := math.Inf(-1)
	best := arms[0].ArmID
	logTotal := math.Log(float64(total))
	for _, a := range arms {
		mean := posteriorMean(a)
		// For UCB1 prefer the empirical mean (rewards / pulls) when
		// available so it works for non-Bernoulli families.
		if a.Pulls > 0 {
			emp := a.Rewards / float64(a.Pulls)
			// Clamp to [0,1] for the bandit-bound assumption; bounded
			// rewards are part of the UCB1 contract.
			switch {
			case emp < 0:
				emp = 0
			case emp > 1:
				emp = 1
			}
			mean = emp
		}
		bonus := c * math.Sqrt(2*logTotal/float64(a.Pulls))
		score := mean + bonus
		if score > bestScore {
			bestScore = score
			best = a.ArmID
		}
	}
	return best
}
