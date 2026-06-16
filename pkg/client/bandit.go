package client

import (
	"context"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
)

// BanditArmSpec is the typed form of cefaspb.BanditArmSpec. Family is
// "beta-bernoulli" (default) or "gaussian"; Alpha/Beta/Mu/Sigma are
// optional priors with sensible defaults applied server-side.
type BanditArmSpec struct {
	ArmID  string
	Family string
	Alpha  float64
	Beta   float64
	Mu     float64
	Sigma  float64
}

// BanditCreateRequest packs the BanditCreate inputs.
type BanditCreateRequest struct {
	BanditID string
	Strategy string // "thompson" | "ucb1" | "epsilon-greedy"
	Arms     []BanditArmSpec
	Epsilon  float64 // epsilon-greedy only
	C        float64 // UCB1 only
}

// BanditCreate registers a bandit with the server. Strategy defaults
// to thompson when empty.
func (c *Client) BanditCreate(ctx context.Context, req BanditCreateRequest) error {
	arms := make([]*cefaspb.BanditArmSpec, 0, len(req.Arms))
	for _, a := range req.Arms {
		arms = append(arms, &cefaspb.BanditArmSpec{
			ArmId:  a.ArmID,
			Family: a.Family,
			Alpha:  a.Alpha,
			Beta:   a.Beta,
			Mu:     a.Mu,
			Sigma:  a.Sigma,
		})
	}
	_, err := c.stub.BanditCreate(c.withAuth(ctx), &cefaspb.BanditCreateRequest{
		BanditId: req.BanditID,
		Strategy: req.Strategy,
		Arms:     arms,
		Epsilon:  req.Epsilon,
		C:        req.C,
	})
	return err
}

// BanditSample returns one arm ID. Context is opaque to v1
// implementations; reserved for contextual bandits.
func (c *Client) BanditSample(ctx context.Context, banditID string, context map[string]string) (string, error) {
	resp, err := c.stub.BanditSample(c.withAuth(ctx), &cefaspb.BanditSampleRequest{
		BanditId: banditID,
		Context:  context,
		N:        1,
	})
	if err != nil {
		return "", err
	}
	arms := resp.GetArmId()
	if len(arms) == 0 {
		return "", nil
	}
	return arms[0], nil
}

// BanditBatchSample returns n arm IDs. Useful for fan-out scoring.
func (c *Client) BanditBatchSample(ctx context.Context, banditID string, context map[string]string, n int) ([]string, error) {
	resp, err := c.stub.BanditSample(c.withAuth(ctx), &cefaspb.BanditSampleRequest{
		BanditId: banditID,
		Context:  context,
		N:        int32(n),
	})
	if err != nil {
		return nil, err
	}
	return resp.GetArmId(), nil
}

// BanditReward records a reward observation for (banditID, armID).
// Beta-Bernoulli treats reward > 0.5 as a positive outcome; Gaussian
// arms accumulate via a Welford running mean.
func (c *Client) BanditReward(ctx context.Context, banditID, armID string, reward float64, context map[string]string) error {
	_, err := c.stub.BanditReward(c.withAuth(ctx), &cefaspb.BanditRewardRequest{
		BanditId: banditID,
		ArmId:    armID,
		Reward:   reward,
		Context:  context,
	})
	return err
}

// BanditArmStats is the client-side mirror of the proto message.
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

// BanditDescribeResult bundles the posterior snapshot for one bandit.
type BanditDescribeResult struct {
	BanditID string
	Strategy string
	Arms     []BanditArmStats
}

// BanditDescribe returns the live posterior for every arm under
// banditID — used by the CLI for observability.
func (c *Client) BanditDescribe(ctx context.Context, banditID string) (BanditDescribeResult, error) {
	resp, err := c.stub.BanditDescribe(c.withAuth(ctx), &cefaspb.BanditDescribeRequest{
		BanditId: banditID,
	})
	if err != nil {
		return BanditDescribeResult{}, err
	}
	out := BanditDescribeResult{
		BanditID: resp.GetBanditId(),
		Strategy: resp.GetStrategy(),
	}
	for _, a := range resp.GetArms() {
		out.Arms = append(out.Arms, BanditArmStats{
			ArmID:   a.GetArmId(),
			Family:  a.GetFamily(),
			Alpha:   a.GetAlpha(),
			Beta:    a.GetBeta(),
			Mu:      a.GetMu(),
			Sigma:   a.GetSigma(),
			Pulls:   a.GetPulls(),
			Rewards: a.GetRewards(),
			Mean:    a.GetMean(),
		})
	}
	return out, nil
}
