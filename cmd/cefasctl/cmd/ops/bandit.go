// CLI command tree for the bandit operator (issue #246). Mirrors the
// gRPC surface: `cefas bandit create | sample | reward | describe`.
package ops

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/client"
)

func registerBandit(root *cobra.Command) {
	grp := &cobra.Command{
		Use:   "bandit",
		Short: "Multi-armed bandit operators (Thompson sampling / UCB1 / epsilon-greedy)",
		Long: `Bandit operators learn online which action wins for a given
context. Each bandit holds a posterior per arm; Sample returns the
arm the strategy picks, Reward updates the posterior, Describe shows
the current state.`,
	}
	grp.AddCommand(banditCreateCmd())
	grp.AddCommand(banditSampleCmd())
	grp.AddCommand(banditRewardCmd())
	grp.AddCommand(banditDescribeCmd())
	root.AddCommand(grp)
}

func banditCreateCmd() *cobra.Command {
	var (
		banditID, strategy, armsArg, family string
		epsilon, c                          float64
	)
	cmd := &cobra.Command{
		Use:   "create",
		Short: "Register a bandit with its arms",
		Long: `Register a new bandit. --arms is a comma-separated list of arm IDs
(simple form), or a JSON array of {"arm_id","family","alpha","beta",
"mu","sigma"} for full control.

Examples:
  cefas bandit create --bandit-id offers --arms A,B,C
  cefas bandit create --bandit-id offers --strategy ucb1 --arms A,B,C --c 1.41
  cefas bandit create --bandit-id offers --strategy epsilon-greedy --arms A,B,C --epsilon 0.1
  cefas bandit create --bandit-id mix --arms '[{"arm_id":"A","family":"gaussian","sigma":1}]'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if banditID == "" || armsArg == "" {
				return fmt.Errorf("--bandit-id and --arms are required")
			}
			arms, err := parseArmsArg(armsArg, family)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			if err := cli.BanditCreate(ctx, client.BanditCreateRequest{
				BanditID: banditID,
				Strategy: strategy,
				Arms:     arms,
				Epsilon:  epsilon,
				C:        c,
			}); err != nil {
				return fmt.Errorf("bandit create: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"BanditID": banditID,
				"Strategy": strategy,
				"Arms":     armIDsOf(arms),
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&banditID, "bandit-id", "", "Bandit identifier (required)")
	f.StringVar(&strategy, "strategy", "thompson", "thompson | ucb1 | epsilon-greedy")
	f.StringVar(&armsArg, "arms", "", "Comma-separated arm IDs, or JSON array of arm specs (required)")
	f.StringVar(&family, "family", "beta-bernoulli", "Default family applied to comma-separated arms")
	f.Float64Var(&epsilon, "epsilon", 0, "Epsilon for epsilon-greedy (defaults to 0.1)")
	f.Float64Var(&c, "c", 0, "Exploration constant for UCB1 (defaults to sqrt(2))")
	return cmd
}

func banditSampleCmd() *cobra.Command {
	var (
		banditID, contextArg string
		n                    int
	)
	cmd := &cobra.Command{
		Use:   "sample",
		Short: "Sample one arm (or a batch with --n)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if banditID == "" {
				return fmt.Errorf("--bandit-id is required")
			}
			ctxMap, err := parseContextArg(contextArg)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			if n <= 1 {
				arm, err := cli.BanditSample(ctx, banditID, ctxMap)
				if err != nil {
					return fmt.Errorf("bandit sample: %w", err)
				}
				return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
					"BanditID": banditID,
					"ArmID":    arm,
				})
			}
			arms, err := cli.BanditBatchSample(ctx, banditID, ctxMap, n)
			if err != nil {
				return fmt.Errorf("bandit batch sample: %w", err)
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"BanditID": banditID,
				"Arms":     arms,
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&banditID, "bandit-id", "", "Bandit identifier (required)")
	f.StringVar(&contextArg, "context", "", "Optional context as JSON object {\"k\":\"v\"}")
	f.IntVar(&n, "n", 1, "Number of arm IDs to sample (>=1)")
	return cmd
}

func banditRewardCmd() *cobra.Command {
	var (
		banditID, armID, contextArg string
		reward                      float64
	)
	cmd := &cobra.Command{
		Use:   "reward",
		Short: "Record a reward observation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if banditID == "" || armID == "" {
				return fmt.Errorf("--bandit-id and --arm-id are required")
			}
			ctxMap, err := parseContextArg(contextArg)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			if err := cli.BanditReward(ctx, banditID, armID, reward, ctxMap); err != nil {
				return fmt.Errorf("bandit reward: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"BanditID": banditID,
				"ArmID":    armID,
				"Reward":   reward,
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&banditID, "bandit-id", "", "Bandit identifier (required)")
	f.StringVar(&armID, "arm-id", "", "Arm identifier (required)")
	f.Float64Var(&reward, "reward", 0, "Reward value (typically 0/1 for Bernoulli)")
	f.StringVar(&contextArg, "context", "", "Optional context as JSON object")
	return cmd
}

func banditDescribeCmd() *cobra.Command {
	var banditID string
	cmd := &cobra.Command{
		Use:   "describe",
		Short: "Show current arm posteriors",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if banditID == "" {
				return fmt.Errorf("--bandit-id is required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			snap, err := cli.BanditDescribe(ctx, banditID)
			if err != nil {
				return fmt.Errorf("bandit describe: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			rows := make([]map[string]any, 0, len(snap.Arms))
			for _, a := range snap.Arms {
				rows = append(rows, map[string]any{
					"ArmID":   a.ArmID,
					"Family":  a.Family,
					"Alpha":   a.Alpha,
					"Beta":    a.Beta,
					"Mu":      a.Mu,
					"Sigma":   a.Sigma,
					"Pulls":   a.Pulls,
					"Rewards": a.Rewards,
					"Mean":    a.Mean,
				})
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"BanditID": snap.BanditID,
				"Strategy": snap.Strategy,
				"Arms":     rows,
			})
		},
	}
	cmd.Flags().StringVar(&banditID, "bandit-id", "", "Bandit identifier (required)")
	return cmd
}

// ---------- helpers ----------

func parseArmsArg(arg, family string) ([]client.BanditArmSpec, error) {
	arg = strings.TrimSpace(arg)
	if strings.HasPrefix(arg, "[") {
		var raw []client.BanditArmSpec
		if err := json.Unmarshal([]byte(arg), &raw); err != nil {
			return nil, fmt.Errorf("--arms JSON: %w", err)
		}
		if len(raw) == 0 {
			return nil, fmt.Errorf("--arms: empty array")
		}
		for i := range raw {
			if raw[i].Family == "" {
				raw[i].Family = family
			}
		}
		return raw, nil
	}
	parts := strings.Split(arg, ",")
	out := make([]client.BanditArmSpec, 0, len(parts))
	for _, p := range parts {
		id := strings.TrimSpace(p)
		if id == "" {
			continue
		}
		out = append(out, client.BanditArmSpec{ArmID: id, Family: family})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("--arms: no arm IDs")
	}
	return out, nil
}

func parseContextArg(arg string) (map[string]string, error) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return nil, nil
	}
	var out map[string]string
	if err := json.Unmarshal([]byte(arg), &out); err != nil {
		return nil, fmt.Errorf("--context: %w", err)
	}
	return out, nil
}

func armIDsOf(arms []client.BanditArmSpec) []string {
	out := make([]string, 0, len(arms))
	for _, a := range arms {
		out = append(out, a.ArmID)
	}
	return out
}
