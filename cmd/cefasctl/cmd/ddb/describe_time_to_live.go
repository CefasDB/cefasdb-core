package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
)

func registerDescribeTimeToLive(root *cobra.Command) {
	var table string
	c := &cobra.Command{
		Use:   "describe-time-to-live",
		Short: "Show the TTL configuration of a table",
		Long: `Mirrors aws dynamodb describe-time-to-live.

Example:
  cefas describe-time-to-live --table-name Sessions`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" {
				return fmt.Errorf("--table-name is required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			st, err := cli.DescribeTimeToLive(ctx, table)
			if err != nil {
				return fmt.Errorf("describe time to live: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			desc := map[string]any{
				"TimeToLiveDescription": map[string]any{
					"TimeToLiveStatus": ttlStatusString(st.Enabled),
					"AttributeName":    st.AttributeName,
				},
			}
			return output.New(cmd.OutOrStdout(), fm).Object(desc)
		},
	}
	c.Flags().StringVar(&table, "table-name", "", "Target table (required)")
	_ = c.MarkFlagRequired("table-name")
	root.AddCommand(c)
}

func ttlStatusString(enabled bool) string {
	if enabled {
		return "ENABLED"
	}
	return "DISABLED"
}
