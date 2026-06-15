package ops

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/client"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func registerCreateIndex(root *cobra.Command) {
	var (
		table, name, kind, field, pkAttr, skAttr, config, metric, algorithm string
		dim                                                                 int
	)
	c := &cobra.Command{
		Use:   "create-index",
		Short: "Create a plugin-backed secondary index",
		Long: `Creates a new secondary index served by one of the registered
plugins (trigram, bloom, geohash, ...). The server resolves --type
against the plugin registry and seeds the index with the current
table contents.

Example:
  cefas create-index \
    --table Merchants \
    --name merchant_name_trigram \
    --type trigram \
    --field name

  cefas create-index \
    --table Stores \
    --name loc_geo \
    --type geohash \
    --field loc \
    --config '{"field":"loc","precision":7}'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" || name == "" || kind == "" {
				return fmt.Errorf("--table, --name, --type are required")
			}
			cfg := []byte(config)
			if len(cfg) == 0 && field != "" {
				if kind == "ann" {
					m := map[string]any{"field": field}
					if dim > 0 {
						m["dim"] = dim
					}
					if metric != "" {
						m["metric"] = metric
					}
					if algorithm != "" {
						m["algorithm"] = algorithm
					}
					var err error
					cfg, err = json.Marshal(m)
					if err != nil {
						return err
					}
				} else {
					cfg = []byte(fmt.Sprintf(`{"field":%q}`, field))
				}
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			d, err := cli.CreateIndex(ctx, client.PluginIndex{
				Table:        table,
				Name:         name,
				PluginName:   kind,
				PluginConfig: cfg,
				KeySchema:    types.KeySchema{PK: pkAttr, SK: skAttr},
			})
			if err != nil {
				return fmt.Errorf("create index: %w", err)
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
				},
			})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table", "", "Target table (required)")
	f.StringVar(&name, "name", "", "Index name (required)")
	f.StringVar(&kind, "type", "", "Plugin type (e.g. trigram, bloom, geohash) (required)")
	f.StringVar(&field, "field", "", "Indexed attribute (convenience; populates --config when --config is empty)")
	f.StringVar(&config, "config", "", "Plugin config JSON (overrides --field)")
	f.StringVar(&metric, "metric", "", "ANN distance metric, for example cosine or euclidean")
	f.StringVar(&algorithm, "algorithm", "", "ANN algorithm, defaults to lsh")
	f.IntVar(&dim, "dim", 0, "ANN vector dimension")
	f.StringVar(&pkAttr, "pk", "", "Table partition key attribute (defaults to the table partition key)")
	f.StringVar(&skAttr, "sk", "", "Table sort key attribute (defaults to the table sort key)")
	root.AddCommand(c)
}
