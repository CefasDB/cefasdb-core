package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
)

func registerDescribeTable(root *cobra.Command) {
	var table string
	c := &cobra.Command{
		Use:   "describe-table",
		Short: "Return the table's schema, indexes, and TTL config",
		Long: `Mirrors aws dynamodb describe-table.

Example:
  cefas describe-table --table-name Users`,
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
			td, err := cli.DescribeTable(ctx, table)
			if err != nil {
				return fmt.Errorf("describe table: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			// AWS-style envelope around the cefas TableDescriptor.
			resp := map[string]any{
				"Table": map[string]any{
					"TableName":            td.Name,
					"TableStatus":          "ACTIVE",
					"KeySchema":            keySchemaWire(td.KeySchema.PK, td.KeySchema.SK),
					"GSIs":                 td.GSIs,
					"LSIs":                 td.LSIs,
					"SpatialIndexes":       td.SpatialIndexes,
					"TTLAttribute":         td.TTLAttribute,
					"AttributeDefinitions": td.AttributeDefinitions,
					"StorageClass":         td.StorageClass,
					"MemoryFootprintBytes": td.MemoryFootprintBytes,
				},
			}
			return output.New(cmd.OutOrStdout(), fm).Object(resp)
		},
	}
	c.Flags().StringVar(&table, "table-name", "", "Target table (required)")
	_ = c.MarkFlagRequired("table-name")
	root.AddCommand(c)
}

func keySchemaWire(pk, sk string) []map[string]string {
	out := []map[string]string{{"AttributeName": pk, "KeyType": "HASH"}}
	if sk != "" {
		out = append(out, map[string]string{"AttributeName": sk, "KeyType": "RANGE"})
	}
	return out
}
