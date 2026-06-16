package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/types"
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
			tableDesc := map[string]any{
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
			}
			addTableStreamFields(tableDesc, td)
			resp := map[string]any{
				"Table": tableDesc,
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

func addTableStreamFields(out map[string]any, td types.TableDescriptor) {
	if td.StreamSpecification != nil {
		out["StreamSpecification"] = map[string]any{
			"StreamEnabled":  td.StreamSpecification.StreamEnabled,
			"StreamViewType": td.StreamSpecification.StreamViewType,
		}
		out["StreamViewType"] = td.StreamSpecification.StreamViewType
	}
	if td.LatestStreamArn != "" {
		out["LatestStreamArn"] = td.LatestStreamArn
	}
	if td.LatestStreamLabel != "" {
		out["LatestStreamLabel"] = td.LatestStreamLabel
	}
	if td.StreamStatus != "" {
		out["StreamStatus"] = td.StreamStatus
	}
}
