package ops

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/fileloader"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/ddbjson"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func registerCohort(root *cobra.Command) {
	grp := &cobra.Command{
		Use:   "cohort",
		Short: "Roaring-bitmap cohorts + HyperLogLog estimates",
	}
	grp.AddCommand(cohortCreateCmd())
	grp.AddCommand(cohortEstimateCmd())
	root.AddCommand(grp)
}

func cohortCreateCmd() *cobra.Command {
	var (
		table, cohort, field, where, bindsArg string
	)
	c := &cobra.Command{
		Use:   "create",
		Short: "Build a Roaring-bitmap cohort over a numeric attribute",
		Long: `Mirrors aws dynamodb create-cohort. Scans the table, filters with
--where, indexes the configured numeric field into a Roaring bitmap.

Example:
  cefas cohort create \
    --table Users \
    --cohort high_value \
    --field user_id \
    --where "spend >= :floor" \
    --binds '{":floor":{"N":"1000"}}'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" || cohort == "" || field == "" {
				return fmt.Errorf("--table, --cohort, --field are required")
			}
			binds, err := loadBinds(bindsArg)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			n, err := cli.CohortCreate(ctx, table, cohort, field, where, binds)
			if err != nil {
				return fmt.Errorf("cohort create: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"CohortName": cohort,
				"Members":    n,
			})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Target table (required)")
	f.StringVar(&cohort, "cohort", "", "Cohort name (required)")
	f.StringVar(&field, "field", "", "Numeric attribute that identifies cohort members (required)")
	f.StringVar(&where, "where", "", "Optional filter (cefas condition subset)")
	f.StringVar(&bindsArg, "binds", "", "DynamoDB-JSON bind map for --where")
	return c
}

func cohortEstimateCmd() *cobra.Command {
	var (
		table, field, where, bindsArg string
	)
	c := &cobra.Command{
		Use:   "estimate",
		Short: "Approximate distinct count over an attribute (HLL)",
		Long: `Mirrors aws dynamodb cohort-estimate. Streams the table's items
into a HyperLogLog stream keyed by --field and returns the estimate.

Example:
  cefas cohort estimate --table Events --field user_id
  cefas cohort estimate \
    --table Events --field user_id \
    --where "campaign_id = :c" --binds '{":c":{"S":"camp1"}}'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" || field == "" {
				return fmt.Errorf("--table and --field are required")
			}
			binds, err := loadBinds(bindsArg)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			est, err := cli.CohortEstimate(ctx, table, field, where, binds)
			if err != nil {
				return fmt.Errorf("cohort estimate: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"ApproximateCount": est,
			})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Target table (required)")
	f.StringVar(&field, "field", "", "Attribute to count distinct values of (required)")
	f.StringVar(&where, "where", "", "Optional filter (cefas condition subset)")
	f.StringVar(&bindsArg, "binds", "", "DynamoDB-JSON bind map for --where")
	return c
}

func loadBinds(arg string) (map[string]types.AttributeValue, error) {
	if arg == "" {
		return nil, nil
	}
	raw, err := fileloader.Load(arg)
	if err != nil {
		return nil, err
	}
	return ddbjson.ParseBinds(raw)
}
