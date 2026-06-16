package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
)

func registerListTables(root *cobra.Command) {
	c := &cobra.Command{
		Use:   "list-tables",
		Short: "List every table the server knows about",
		Long: `Mirrors aws dynamodb list-tables.

Example:
  cefas list-tables`,
		Args: cobra.NoArgs,
		RunE: runListTables,
	}
	root.AddCommand(c)
}

func runListTables(c *cobra.Command, _ []string) error {
	ctx := c.Context()
	cli, profile, err := runtime.Dial(ctx)
	if err != nil {
		return err
	}
	defer cli.Close()

	tds, err := cli.ListTables(ctx)
	if err != nil {
		return fmt.Errorf("list tables: %w", err)
	}
	names := make([]string, 0, len(tds))
	for _, td := range tds {
		names = append(names, td.Name)
	}

	fm, err := output.Validate(profile.Output)
	if err != nil {
		return err
	}
	resp := struct {
		TableNames []string `json:"TableNames"`
	}{TableNames: names}
	return output.New(c.OutOrStdout(), fm).Object(resp)
}
