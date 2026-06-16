package ops

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
)

func registerFreqCap(root *cobra.Command) {
	grp := &cobra.Command{
		Use:   "freqcap",
		Short: "Sliding-window frequency cap",
	}
	grp.AddCommand(freqCapCheckCmd())
	root.AddCommand(grp)
}

func freqCapCheckCmd() *cobra.Command {
	var (
		scope, key string
		limit      int
		window     time.Duration
	)
	c := &cobra.Command{
		Use:   "check",
		Short: "Increment + check the freq counter for (scope, key) in window",
		Long: `Records one hit against (scope, key) and reports whether the
cumulative count inside --window stayed at or below --limit. Returns
true when the call is allowed.

Example:
  cefas freqcap check \
    --scope merchant-456 \
    --key USER#1 \
    --limit 3 \
    --window 7d`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if scope == "" || key == "" || limit <= 0 || window <= 0 {
				return fmt.Errorf("--scope, --key, --limit > 0, --window > 0 are required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			ok, err := cli.FreqCap(ctx, scope, key, limit, window)
			if err != nil {
				return fmt.Errorf("freqcap: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{"Allowed": ok})
		},
	}
	f := c.Flags()
	f.StringVar(&scope, "scope", "", "Cap scope (e.g. merchant-id) (required)")
	f.StringVar(&key, "key", "", "Cap key (e.g. user id) (required)")
	f.IntVar(&limit, "limit", 0, "Maximum allowed hits inside the window (required)")
	f.DurationVar(&window, "window", 0, "Sliding window duration (e.g. 7d, 24h, 30m)")
	return c
}
