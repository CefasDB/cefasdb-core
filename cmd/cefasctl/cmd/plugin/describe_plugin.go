package plugin

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
)

func registerDescribePlugin(root *cobra.Command) {
	var name string
	c := &cobra.Command{
		Use:   "describe-plugin",
		Short: "Describe a single plugin by name",
		Long: `Returns name, kind, version, state, last error (if any), and
counters surfaced by the plugin's StatusProvider implementation
(when supplied).

Example:
  cefas describe-plugin --name trigram`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if name == "" {
				return fmt.Errorf("--name is required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			p, err := cli.DescribePlugin(ctx, name)
			if err != nil {
				return fmt.Errorf("describe plugin: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Plugin": map[string]any{
					"Name":          p.Name,
					"Kind":          p.Kind,
					"Version":       p.Version,
					"Description":   p.Description,
					"State":         p.State,
					"LastError":     p.LastError,
					"LastErrorUnix": p.LastErrorUnix,
					"ItemsIndexed":  p.ItemsIndexed,
					"StartedAtUnix": p.StartedAtUnix,
				},
			})
		},
	}
	c.Flags().StringVar(&name, "name", "", "Plugin name (required)")
	_ = c.MarkFlagRequired("name")
	root.AddCommand(c)
}
