package ops

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
)

func registerDedup(root *cobra.Command) {
	grp := &cobra.Command{
		Use:   "dedup",
		Short: "Audience-level dedup (TTL-bucketed)",
	}
	grp.AddCommand(dedupPutCmd())
	root.AddCommand(grp)
}

func dedupPutCmd() *cobra.Command {
	var (
		scope, key string
		ttl        time.Duration
	)
	c := &cobra.Command{
		Use:   "put",
		Short: "Record (scope, key) with a TTL — true if first hit in window",
		Long: `Records (scope, key) for the given TTL and returns whether the
caller is allowed to proceed (true on first hit inside the window,
false on a duplicate).

Example:
  cefas dedup put --scope campaign-123 --key USER#1 --ttl 7d`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if scope == "" || key == "" || ttl <= 0 {
				return fmt.Errorf("--scope, --key, --ttl > 0 are required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			ok, err := cli.Dedup(ctx, scope, key, ttl)
			if err != nil {
				return fmt.Errorf("dedup: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{"Allowed": ok})
		},
	}
	f := c.Flags()
	f.StringVar(&scope, "scope", "", "Dedup scope (e.g. campaign-id) (required)")
	f.StringVar(&key, "key", "", "Dedup key (e.g. user id) (required)")
	f.DurationVar(&ttl, "ttl", 0, "TTL with unit suffix (e.g. 7d, 24h, 30m). Note: 7d = 7*24h.")
	return c
}
