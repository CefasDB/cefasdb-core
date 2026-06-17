package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/fileloader"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/pkg/ddbjson"
)

func registerScan(root *cobra.Command) {
	var (
		table          string
		filter         string
		bindsArg       string
		limit          int
		consistentRead bool
	)
	c := &cobra.Command{
		Use:   "scan",
		Short: "Stream every item in a table (optionally server-side filtered)",
		Long: `Mirrors aws dynamodb scan. cefas evaluates FilterExpression
server-side using the same DDB condition subset PutItem's
--condition-expression accepts. Parallel scan (Segment/TotalSegments)
and ExclusiveStartKey are not yet wired.

Example:
  cefas scan --table-name Users
  cefas scan \
    --table-name Users \
    --filter-expression "tier = :gold" \
    --expression-attribute-values '{":gold":{"S":"gold"}}'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" {
				return fmt.Errorf("--table-name is required")
			}
			opts := client.ScanOptions{
				FilterExpression: filter,
				Limit:            limit,
				Strong:           consistentRead,
			}
			if bindsArg != "" {
				raw, err := fileloader.Load(bindsArg)
				if err != nil {
					return err
				}
				binds, err := ddbjson.ParseBinds(raw)
				if err != nil {
					return err
				}
				opts.Binds = binds
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			items, err := cli.Scan(ctx, table, opts)
			if err != nil {
				return fmt.Errorf("scan: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Items(items)
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table-name", "", "Target table (required)")
	f.StringVar(&filter, "filter-expression", "", "Server-side filter (DDB ConditionExpression grammar)")
	f.StringVar(&bindsArg, "expression-attribute-values", "", "DynamoDB-JSON bind map (inline or file://path)")
	f.IntVar(&limit, "limit", 0, "Cap the number of items returned (0 = no cap)")
	f.BoolVar(&consistentRead, "consistent-read", false, "Strong consistency: routes through the leader + barrier")
	_ = c.MarkFlagRequired("table-name")
	root.AddCommand(c)
}
