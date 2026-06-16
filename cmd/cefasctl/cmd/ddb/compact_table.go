package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
)

func registerCompactTable(root *cobra.Command) {
	var parallelize bool
	c := &cobra.Command{
		Use:   "compact-table <table>",
		Short: "Compact a table's Pebble key ranges",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			results, err := cli.CompactTable(ctx, args[0], parallelize)
			if err != nil {
				return fmt.Errorf("compact table: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"TableName":   args[0],
				"Compactions": results,
			})
		},
	}
	c.Flags().BoolVar(&parallelize, "parallelize", false, "Allow Pebble to parallelize manual compaction")
	root.AddCommand(c)
}
