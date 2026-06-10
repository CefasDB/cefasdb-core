package ddb

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/fileloader"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/ddbjson"
	cefassql "github.com/osvaldoandrade/cefas/pkg/sql"
)

func registerExecuteStatement(root *cobra.Command) {
	var (
		statement string
		paramsArg string
	)
	c := &cobra.Command{
		Use:   "execute-statement",
		Short: "Run a PartiQL statement against the server",
		Long: `Binds positional ? placeholders client-side using the same
cefassql.BindPartiQL helper the HTTP /v1/PartiQL endpoint uses, then
ships the resulting SQL to the server via the Sql RPC.

Example:
  cefas execute-statement \
    --statement "SELECT * FROM Users WHERE pk = ?" \
    --parameters '[{"S":"USER#1"}]'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if statement == "" {
				return fmt.Errorf("--statement is required")
			}
			var params []cefassql.PartiQLParameter
			if paramsArg != "" {
				raw, err := fileloader.Load(paramsArg)
				if err != nil {
					return err
				}
				if err := json.Unmarshal(raw, &params); err != nil {
					return fmt.Errorf("--parameters: expected a JSON array of DDB-JSON attribute values: %w", err)
				}
			}
			bound, err := cefassql.BindPartiQL(statement, params)
			if err != nil {
				return fmt.Errorf("bind PartiQL: %w", err)
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			res, err := cli.Sql(ctx, bound)
			if err != nil {
				return fmt.Errorf("execute statement: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			wire := make([]map[string]ddbjson.Attribute, 0, len(res.Rows))
			for _, r := range res.Rows {
				wire = append(wire, ddbjson.EncodeItem(r))
			}
			return output.New(cmd.OutOrStdout(), fm).Object(struct {
				Items        []map[string]ddbjson.Attribute `json:"Items,omitempty"`
				AffectedRows int                            `json:"AffectedRows"`
			}{Items: wire, AffectedRows: res.AffectedRows})
		},
	}
	f := c.Flags()
	f.StringVar(&statement, "statement", "", "PartiQL statement (required); use ? for positional binds")
	f.StringVar(&paramsArg, "parameters", "", "JSON array of DDB-JSON attribute values to substitute for ? (inline or file://path)")
	_ = c.MarkFlagRequired("statement")
	root.AddCommand(c)
}
