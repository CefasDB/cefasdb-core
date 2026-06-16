package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
)

func registerDeleteTable(root *cobra.Command) {
	var table string
	c := &cobra.Command{
		Use:   "delete-table",
		Short: "Drop a table",
		Long: `Mirrors aws dynamodb delete-table.

Example:
  cefas delete-table --table-name Users`,
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
			if err := cli.DropTable(ctx, table); err != nil {
				return fmt.Errorf("delete table: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"TableDescription": map[string]any{
					"TableName":   table,
					"TableStatus": "DELETING",
				},
			})
		},
	}
	c.Flags().StringVar(&table, "table-name", "", "Table to drop (required)")
	_ = c.MarkFlagRequired("table-name")
	root.AddCommand(c)
}
