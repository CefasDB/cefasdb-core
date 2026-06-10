package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
)

func registerRestoreTableFromBackup(root *cobra.Command) {
	var (
		backup   string
		source   string
		target   string
	)
	c := &cobra.Command{
		Use:   "restore-table-from-backup",
		Short: "Recreate a table from an admin-named backup",
		Long: `Mirrors aws dynamodb restore-table-from-backup. cefas reads the
source table's descriptor out of the named backup's pebble checkpoint,
recreates it under the target name, and streams every row into the new
table — re-keyed under the target name so the live engine maintains
indexes + TTL.

Example:
  cefas restore-table-from-backup \
    --backup-name nightly \
    --source-table-name Users \
    --target-table-name Users_restored`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if backup == "" {
				return fmt.Errorf("--backup-name is required")
			}
			if source == "" {
				return fmt.Errorf("--source-table-name is required")
			}
			if target == "" {
				return fmt.Errorf("--target-table-name is required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			rows, err := cli.RestoreTableFromBackup(ctx, backup, source, target)
			if err != nil {
				return fmt.Errorf("restore: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"TableDescription": map[string]any{
					"TableName":   target,
					"TableStatus": "ACTIVE",
					"RowsCopied":  rows,
				},
			})
		},
	}
	f := c.Flags()
	f.StringVar(&backup, "backup-name", "", "Backup to restore from (required)")
	f.StringVar(&source, "source-table-name", "", "Table inside the backup to copy (required)")
	f.StringVar(&target, "target-table-name", "", "New table name in the live catalog (required)")
	_ = c.MarkFlagRequired("backup-name")
	_ = c.MarkFlagRequired("source-table-name")
	_ = c.MarkFlagRequired("target-table-name")
	root.AddCommand(c)
}
