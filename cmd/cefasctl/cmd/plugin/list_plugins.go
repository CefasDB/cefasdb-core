package plugin

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
)

func registerListPlugins(root *cobra.Command) {
	var kind string
	c := &cobra.Command{
		Use:   "list-plugins",
		Short: "List every plugin registered with the server",
		Long: `Lists each plugin's name, kind, and state. --kind narrows the
result to a single kind: index | distance | estimator | audience.

Example:
  cefas list-plugins
  cefas list-plugins --kind distance`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			plugs, err := cli.ListPlugins(ctx, kind)
			if err != nil {
				return fmt.Errorf("list plugins: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			rows := make([]map[string]any, 0, len(plugs))
			for _, p := range plugs {
				rows = append(rows, map[string]any{
					"Name":         p.Name,
					"Kind":         p.Kind,
					"Version":      p.Version,
					"State":        p.State,
					"ItemsIndexed": p.ItemsIndexed,
				})
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Plugins": rows,
			})
		},
	}
	c.Flags().StringVar(&kind, "kind", "", "Filter by plugin kind (index|distance|estimator|audience)")
	root.AddCommand(c)
}
