package ddb

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/fileloader"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
	"github.com/CefasDb/cefasdb/pkg/client"
	"github.com/CefasDb/cefasdb/pkg/ddbjson"
)

func registerUpdateItem(root *cobra.Command) {
	var (
		table        string
		keyArg       string
		updateExpr   string
		condition    string
		namesArg     string
		valuesArg    string
		returnValues string
	)
	c := &cobra.Command{
		Use:   "update-item",
		Short: "Apply an UpdateExpression to a single item",
		Long: `Mirrors aws dynamodb update-item. cefas resolves #names and :values
inline and dispatches the resulting cefas SQL UPDATE through the
standard executor (which already does SET / ADD / REMOVE / DELETE with
GSI / LSI / TTL maintenance).

Example:
  cefas update-item \
    --table-name Users \
    --key '{"pk":{"S":"USER#1"},"sk":{"S":"PROFILE"}}' \
    --update-expression "SET #n = :name, ADD score :inc" \
    --expression-attribute-names '{"#n":"name"}' \
    --expression-attribute-values '{":name":{"S":"Ova"},":inc":{"N":"1"}}' \
    --return-values ALL_NEW`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" {
				return fmt.Errorf("--table-name is required")
			}
			if keyArg == "" {
				return fmt.Errorf("--key is required")
			}
			if updateExpr == "" {
				return fmt.Errorf("--update-expression is required")
			}
			keyBytes, err := fileloader.Load(keyArg)
			if err != nil {
				return err
			}
			key, err := ddbjson.ParseItem(keyBytes)
			if err != nil {
				return fmt.Errorf("--key: %w", err)
			}
			opts := client.UpdateOptions{
				UpdateExpression:    updateExpr,
				ConditionExpression: condition,
				ReturnValues:        returnValues,
			}
			if namesArg != "" {
				raw, err := fileloader.Load(namesArg)
				if err != nil {
					return err
				}
				if err := json.Unmarshal(raw, &opts.ExpressionAttributeNames); err != nil {
					return fmt.Errorf("--expression-attribute-names: %w", err)
				}
			}
			if valuesArg != "" {
				raw, err := fileloader.Load(valuesArg)
				if err != nil {
					return err
				}
				v, err := ddbjson.ParseBinds(raw)
				if err != nil {
					return fmt.Errorf("--expression-attribute-values: %w", err)
				}
				opts.ExpressionAttributeValues = v
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			img, err := cli.UpdateItem(ctx, table, key, opts)
			if err != nil {
				return fmt.Errorf("update item: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			if img == nil {
				return output.New(cmd.OutOrStdout(), fm).Object(struct{}{})
			}
			return output.New(cmd.OutOrStdout(), fm).Object(struct {
				Attributes map[string]ddbjson.Attribute `json:"Attributes"`
			}{Attributes: ddbjson.EncodeItem(img)})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table-name", "", "Target table (required)")
	f.StringVar(&keyArg, "key", "", "DynamoDB-JSON primary key (required; inline or file://path)")
	f.StringVar(&updateExpr, "update-expression", "", "UpdateExpression: SET / ADD / REMOVE / DELETE clauses (required)")
	f.StringVar(&condition, "condition-expression", "", "Optional ConditionExpression")
	f.StringVar(&namesArg, "expression-attribute-names", "", "JSON object of #name → attribute name (inline or file://path)")
	f.StringVar(&valuesArg, "expression-attribute-values", "", "DynamoDB-JSON bind map (inline or file://path)")
	f.StringVar(&returnValues, "return-values", "", "NONE | ALL_NEW | ALL_OLD | UPDATED_NEW | UPDATED_OLD")
	_ = c.MarkFlagRequired("table-name")
	_ = c.MarkFlagRequired("key")
	_ = c.MarkFlagRequired("update-expression")
	root.AddCommand(c)
}
