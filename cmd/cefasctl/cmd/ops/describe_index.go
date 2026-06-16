package ops

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
)

func registerDescribeIndex(root *cobra.Command) {
	var table, name string
	c := &cobra.Command{
		Use:   "describe-index",
		Short: "Describe a plugin-backed secondary index",
		Long: `Returns the registered descriptor for a plugin-backed index
created via cefas create-index.

Example:
  cefas describe-index --table Merchants --name merchant_name_trigram`,
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
			d, err := cli.DescribeIndex(ctx, table, name)
			if err != nil {
				return fmt.Errorf("describe index: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"IndexDescription": map[string]any{
					"Table":        d.Table,
					"Name":         d.Name,
					"PluginName":   d.PluginName,
					"PluginConfig": string(d.PluginConfig),
					"KeySchema":    map[string]string{"PK": d.KeySchema.PK, "SK": d.KeySchema.SK},
				},
			})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Target table (required)")
	f.StringVar(&name, "name", "", "Index name (required)")
	root.AddCommand(c)
}
