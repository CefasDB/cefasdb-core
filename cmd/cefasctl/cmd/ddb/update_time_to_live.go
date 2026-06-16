package ddb

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/fileloader"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
)

// ttlSpec mirrors the aws-cli --time-to-live-specification shorthand
// (`{"Enabled": bool, "AttributeName": "..."}`).
type ttlSpec struct {
	Enabled       bool   `json:"Enabled"`
	AttributeName string `json:"AttributeName"`
}

func registerUpdateTimeToLive(root *cobra.Command) {
	var (
		table   string
		specArg string
	)
	c := &cobra.Command{
		Use:   "update-time-to-live",
		Short: "Enable or disable TTL on a table",
		Long: `Mirrors aws dynamodb update-time-to-live.

Example:
  cefas update-time-to-live \
    --table-name Sessions \
    --time-to-live-specification '{"Enabled":true,"AttributeName":"expires_at"}'

To disable TTL:
  cefas update-time-to-live \
    --table-name Sessions \
    --time-to-live-specification '{"Enabled":false,"AttributeName":""}'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" {
				return fmt.Errorf("--table-name is required")
			}
			if specArg == "" {
				return fmt.Errorf("--time-to-live-specification is required")
			}
			raw, err := fileloader.Load(specArg)
			if err != nil {
				return err
			}
			var spec ttlSpec
			if err := json.Unmarshal(raw, &spec); err != nil {
				return fmt.Errorf("--time-to-live-specification: %w", err)
			}
			if spec.Enabled && spec.AttributeName == "" {
				return fmt.Errorf("--time-to-live-specification: AttributeName required when Enabled is true")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			st, err := cli.UpdateTimeToLive(ctx, table, spec.AttributeName, spec.Enabled)
			if err != nil {
				return fmt.Errorf("update time to live: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"TimeToLiveSpecification": map[string]any{
					"Enabled":       st.Enabled,
					"AttributeName": st.AttributeName,
				},
			})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table-name", "", "Target table (required)")
	f.StringVar(&specArg, "time-to-live-specification", "", "{\"Enabled\":bool,\"AttributeName\":string} (required; inline or file://path)")
	_ = c.MarkFlagRequired("table-name")
	_ = c.MarkFlagRequired("time-to-live-specification")
	root.AddCommand(c)
}
