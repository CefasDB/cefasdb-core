package ops

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
)

func registerExplain(root *cobra.Command) {
	var (
		table, where, fmtFlag string
	)
	c := &cobra.Command{
		Use:   "explain",
		Short: "Render the planner's plan tree for a query",
		Long: `Returns the planner's plan tree in text (default) or JSON form.
v1 emits a synthetic tree so the CLI surface works while the
SQL-planner integration deepens; the wire format is forward-
compatible (RenderExplain in pkg/core/query).

Example:
  cefas explain --table Users --where "levenshtein(name, 'ova') <= 1"
  cefas explain --table Docs --where "cosine(embedding, :q) <= 0.1" --format json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" {
				return fmt.Errorf("--table is required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			plan, err := cli.Explain(ctx, table, where, fmtFlag)
			if err != nil {
				return fmt.Errorf("explain: %w", err)
			}
			if fmtFlag == "json" || profile.Output == "json" || profile.Output == "" {
				return output.New(cmd.OutOrStdout(), output.JSON).Object(map[string]any{"Plan": plan})
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), plan)
			return err
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Target table (required)")
	f.StringVar(&where, "where", "", "Predicate text (e.g. \"levenshtein(name, 'ova') <= 1\")")
	f.StringVar(&fmtFlag, "format", "text", "Output format for the plan: text | json")
	root.AddCommand(c)
}
