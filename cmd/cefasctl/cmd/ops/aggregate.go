package ops

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
)

func registerAggregate(root *cobra.Command) {
	var (
		table, groupBy, metrics string
		minGroupSize            int
	)
	c := &cobra.Command{
		Use:   "aggregate",
		Short: "Server-side group-by aggregation with min-group-size privacy floor",
		Long: `Mirrors aws dynamodb aggregate. The server fails closed (returns
an error and no rows) when any group would fall below
--min-group-size.

Example:
  cefas aggregate \
    --table CampaignEvents \
    --group-by campaign_id,geohash5 \
    --metrics impressions,clicks,redemptions \
    --min-group-size 100`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" || groupBy == "" || metrics == "" {
				return fmt.Errorf("--table, --group-by, --metrics are required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			rows, err := cli.Aggregate(ctx, table,
				splitCSV(groupBy), splitCSV(metrics), minGroupSize)
			if err != nil {
				return fmt.Errorf("aggregate: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			out := make([]map[string]any, 0, len(rows))
			for _, r := range rows {
				out = append(out, map[string]any{
					"GroupKey": r.GroupKey,
					"Counts":   r.Counts,
					"Members":  r.Members,
				})
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{"Rows": out})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Target table (required)")
	f.StringVar(&groupBy, "group-by", "", "Comma-separated group-by attributes (required)")
	f.StringVar(&metrics, "metrics", "", "Comma-separated numeric attributes to sum (required)")
	f.IntVar(&minGroupSize, "min-group-size", 0, "Privacy floor — reject groups below this size")
	root.AddCommand(c)
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
