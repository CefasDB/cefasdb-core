package bandit

import (
	"math"
	"math/rand"
)

// sampleThompson draws one Beta(alpha, beta) sample per arm and
// returns the arm with the highest draw. For Gaussian arms the draw
// is a Normal(mu, sigma) sample instead; that lets a mixed-family
// bandit still produce comparable scores.
func sampleThompson(rng *rand.Rand, arms []ArmRecord) string {
	best := ""
	bestScore := math.Inf(-1)
	for _, a := range arms {
		var score float64
		switch a.Family {
		case FamilyGaussian:
			sigma := a.Sigma
			if sigma <= 0 {
				sigma = 1
			}
			score = rng.NormFloat64()*sigma + a.Mu
		default:
			alpha := a.Alpha
			beta := a.Beta
			if alpha <= 0 {
				alpha = 1
			}
			if beta <= 0 {
				beta = 1
			}
			score = sampleBeta(rng, alpha, beta)
		}
		if score > bestScore {
			bestScore = score
			best = a.ArmID
		}
	}
	if best == "" && len(arms) > 0 {
		// Fallback in case every score was -Inf (shouldn't happen).
		best = arms[0].ArmID
	}
	return best
}

// sampleBeta uses the Gamma trick: X = G1 / (G1 + G2) where Gi ~
// Gamma(alpha_i, 1). The Marsaglia / Tsang rejection sampler handles
// alpha >= 1; for alpha < 1 we recurse via the boosting identity
// Gamma(alpha) = Gamma(alpha+1) * U^(1/alpha).
func sampleBeta(rng *rand.Rand, alpha, beta float64) float64 {
	g1 := sampleGamma(rng, alpha)
	g2 := sampleGamma(rng, beta)
	sum := g1 + g2
	if sum <= 0 {
		return 0
	}
	return g1 / sum
}

func sampleGamma(rng *rand.Rand, shape float64) float64 {
	if shape <= 0 {
		return 0
	}
	if shape < 1 {
		// Boosting: Gamma(shape) = Gamma(shape+1) * U^(1/shape).
		u := rng.Float64()
		if u <= 0 {
			u = 1e-300
		}
		return sampleGamma(rng, shape+1) * math.Pow(u, 1.0/shape)
	}
	// Marsaglia + Tsang.
	d := shape - 1.0/3.0
	c := 1.0 / math.Sqrt(9*d)
	for {
		x := rng.NormFloat64()
		v := 1 + c*x
		if v <= 0 {
			continue
		}
		v = v * v * v
		u := rng.Float64()
		if u < 1-0.0331*x*x*x*x {
			return d * v
		}
		if math.Log(u) < 0.5*x*x+d*(1-v+math.Log(v)) {
			return d * v
		}
	}
}
