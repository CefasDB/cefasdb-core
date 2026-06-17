package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/fileloader"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/pkg/ddbjson"
)

func registerPutItem(root *cobra.Command) {
	var (
		table     string
		itemArg   string
		condition string
		bindsArg  string
	)
	c := &cobra.Command{
		Use:   "put-item",
		Short: "Insert or overwrite a single item",
		Long: `Mirrors aws dynamodb put-item.

Example:
  cefas put-item \
    --table-name Users \
    --item '{"pk":{"S":"USER#1"},"sk":{"S":"PROFILE"},"name":{"S":"Ova"}}'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" {
				return fmt.Errorf("--table-name is required")
			}
			if itemArg == "" {
				return fmt.Errorf("--item is required")
			}
			itemBytes, err := fileloader.Load(itemArg)
			if err != nil {
				return err
			}
			item, err := ddbjson.ParseItem(itemBytes)
			if err != nil {
				return err
			}
			opts := client.PutOptions{Condition: condition}
			if bindsArg != "" {
				bindBytes, err := fileloader.Load(bindsArg)
				if err != nil {
					return err
				}
				binds, err := ddbjson.ParseBinds(bindBytes)
				if err != nil {
					return err
				}
				opts.Binds = binds
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			if err := cli.PutItem(ctx, table, item, opts); err != nil {
				return fmt.Errorf("put item: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(struct{}{})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table-name", "", "Target table (required)")
	f.StringVar(&itemArg, "item", "", "DynamoDB-JSON item (required; inline or file://path)")
	f.StringVar(&condition, "condition-expression", "", "Optional ConditionExpression")
	f.StringVar(&bindsArg, "expression-attribute-values", "", "DynamoDB-JSON bind map (inline or file://path)")
	_ = c.MarkFlagRequired("table-name")
	_ = c.MarkFlagRequired("item")
	root.AddCommand(c)
}
