package ops

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
)

func registerRebuildIndex(root *cobra.Command) {
	var table, name string
	c := &cobra.Command{
		Use:   "rebuild-index",
		Short: "Re-seed a plugin-backed index from the current table contents",
		Long: `Calls the plugin's Build on every current row in the table. Use
this after a config change, after bulk ingest, or to recover from a
plugin restart.

Example:
  cefas rebuild-index --table Merchants --name merchant_name_trigram`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" || name == "" {
				return fmt.Errorf("--table and --name are required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			n, err := cli.RebuildIndex(ctx, table, name)
			if err != nil {
				return fmt.Errorf("rebuild index: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"ItemsIndexed": n,
			})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Target table (required)")
	f.StringVar(&name, "name", "", "Index name (required)")
	root.AddCommand(c)
}
