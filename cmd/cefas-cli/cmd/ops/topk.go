package ops

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/fileloader"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/ddbjson"
)

// byExprRegex grabs (op, field, :bind) out of the aws-cli style
// `--by "cosine(embedding, :query)"`. Three captures: 1) operator
// name, 2) attribute, 3) bind name (with leading colon).
var byExprRegex = regexp.MustCompile(`^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*,\s*(:[a-zA-Z_][a-zA-Z0-9_]*)\s*\)\s*$`)

func registerTopK(root *cobra.Command) {
	var (
		table, by, queryArg string
		k                   int
	)
	c := &cobra.Command{
		Use:   "top-k",
		Short: "Stream the K items ranked by a distance plugin",
		Long: `Mirrors aws dynamodb query --limit + sort, with a distance plugin
doing the ranking. --by is an aws-cli-style operator expression
(distance_op(field, :placeholder)); --query supplies the
DynamoDB-JSON value for the placeholder.

Example:
  cefas top-k \
    --table Documents \
    --by "cosine(embedding, :query)" \
    --k 20 \
    --query '{"L":[{"N":"0.1"},{"N":"0.2"},{"N":"0.3"}]}'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" || by == "" || k <= 0 {
				return fmt.Errorf("--table, --by, and --k > 0 are required")
			}
			m := byExprRegex.FindStringSubmatch(by)
			if m == nil {
				return fmt.Errorf("--by must look like 'op(field, :bind)'")
			}
			op, field, bind := m[1], m[2], m[3]
			if queryArg == "" {
				return fmt.Errorf("--query is required (DDB-JSON value for the operator's right-hand side)")
			}
			raw, err := fileloader.Load(queryArg)
			if err != nil {
				return err
			}
			var attr ddbjson.Attribute
			if err := json.Unmarshal(raw, &attr); err != nil {
				return fmt.Errorf("--query: %w", err)
			}
			target, err := attr.ToAttr()
			if err != nil {
				return fmt.Errorf("--query: %w", err)
			}
			_ = bind // the server resolves the operator against `target`; the bind is decorative

			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			rows, err := cli.TopK(ctx, table, field, op, target, k)
			if err != nil {
				return fmt.Errorf("top-k: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			out := make([]map[string]any, 0, len(rows))
			for _, r := range rows {
				out = append(out, map[string]any{
					"Item":     ddbjson.EncodeItem(r.Item),
					"Distance": r.Distance,
				})
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"Rows":             out,
				"DistanceOperator": op,
				"Field":            field,
			})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Target table (required)")
	f.StringVar(&by, "by", "", "Distance expression: 'op(field, :bind)' (required)")
	f.IntVar(&k, "k", 0, "Number of rows to return (required, > 0)")
	f.StringVar(&queryArg, "query", "", "DynamoDB-JSON attribute value the distance op compares against")
	// Make the helpful error message above show up when --by is malformed.
	_ = strings.Contains
	root.AddCommand(c)
}
