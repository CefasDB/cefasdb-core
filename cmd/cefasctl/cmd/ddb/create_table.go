package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func registerCreateTable(root *cobra.Command) {
	var (
		table        string
		attrDefs     []string
		keySchema    []string
		billingMode  string
		storageClass string
		streamSpec   string
	)
	c := &cobra.Command{
		Use:   "create-table",
		Short: "Create a new table with the supplied schema",
		Long: `Mirrors aws dynamodb create-table.

Example:
  cefas create-table \
    --table-name Users \
    --attribute-definitions AttributeName=pk,AttributeType=S \
    --attribute-definitions AttributeName=sk,AttributeType=S \
    --key-schema AttributeName=pk,KeyType=HASH \
    --key-schema AttributeName=sk,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST

cefas does not bill per-request (the engine is self-hosted), so the
--billing-mode flag is accepted for aws-cli compatibility and
ignored.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if table == "" {
				return fmt.Errorf("--table-name is required")
			}
			if len(keySchema) == 0 {
				return fmt.Errorf("--key-schema is required at least once")
			}
			ks, err := parseKeySchema(keySchema)
			if err != nil {
				return err
			}
			pk, sk, err := PartitionAndSort(ks)
			if err != nil {
				return err
			}
			defs, err := parseAttrDefs(attrDefs)
			if err != nil {
				return err
			}
			streamSpecification, err := parseStreamSpecification(streamSpec)
			if err != nil {
				return err
			}
			_ = billingMode // aws-cli compat; cefas has no billing tier

			td := types.TableDescriptor{
				Name:                table,
				KeySchema:           types.KeySchema{PK: pk, SK: sk},
				StorageClass:        storageClass,
				StreamSpecification: streamSpecification,
			}
			for _, def := range defs {
				td.AttributeDefinitions = append(td.AttributeDefinitions, types.AttributeDefinition{
					Name:             def.Name,
					Type:             def.Type,
					VectorDimensions: def.VectorDimensions,
				})
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			created, err := cli.CreateTableWithDescriptor(ctx, td)
			if err != nil {
				return fmt.Errorf("create table: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			desc := map[string]any{
				"TableName":    created.Name,
				"TableStatus":  "ACTIVE",
				"KeySchema":    keySchemaWire(created.KeySchema.PK, created.KeySchema.SK),
				"StorageClass": created.StorageClass,
			}
			addTableStreamFields(desc, created)
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"TableDescription": desc,
			})
		},
	}
	f := c.Flags()
	f.StringVar(&table, "table-name", "", "Table name (required)")
	f.StringArrayVar(&attrDefs, "attribute-definitions", nil, "AttributeName=<n>,AttributeType=<S|N|B|V<dim>> (repeatable)")
	f.StringArrayVar(&keySchema, "key-schema", nil, "AttributeName=<n>,KeyType=<HASH|RANGE> (repeatable)")
	f.StringVar(&billingMode, "billing-mode", "", "Accepted for aws-cli compat; cefas ignores it")
	f.StringVar(&storageClass, "storage-class", "", "Storage class: disk or memory")
	f.StringVar(&streamSpec, "stream-specification", "", "StreamEnabled=<true|false>,StreamViewType=<KEYS_ONLY|NEW_IMAGE|OLD_IMAGE|NEW_AND_OLD_IMAGES>")
	_ = c.MarkFlagRequired("table-name")
	root.AddCommand(c)
}
