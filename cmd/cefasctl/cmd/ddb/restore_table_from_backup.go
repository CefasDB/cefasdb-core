package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/output"
	"github.com/CefasDb/cefasdb/cmd/cefasctl/internal/runtime"
	"github.com/CefasDb/cefasdb/pkg/client"
)

func registerRestoreTableFromBackup(root *cobra.Command) {
	var (
		backup            string
		source            string
		target            string
		dryRun            bool
		targetChangeIndex uint64
		targetUnixNano    int64
	)
	c := &cobra.Command{
		Use:   "restore-table-from-backup",
		Short: "Recreate a table from an admin-named backup",
		Long: `Mirrors aws dynamodb restore-table-from-backup. cefas reads the
source table's descriptor out of the named backup's pebble checkpoint,
recreates it under the target name, and streams every row into the new
table — re-keyed under the target name so the live engine maintains
indexes + TTL. Use --dry-run to validate the source manifest and target
table name without creating the target table or copying rows.

Example:
  cefas restore-table-from-backup \
    --backup-name nightly \
    --source-table-name Users \
    --target-table-name Users_restored

  cefas restore-table-from-backup \
    --backup-name nightly \
    --source-table-name Users \
    --target-table-name Users_restored \
    --dry-run`,
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
			result, err := cli.RestoreTableFromBackupWithOptions(ctx, backup, source, target, client.RestoreTableFromBackupOptions{
				DryRun:            dryRun,
				TargetChangeIndex: targetChangeIndex,
				TargetUnixNano:    targetUnixNano,
			})
			if err != nil {
				return fmt.Errorf("restore: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"TableDescription": map[string]any{
					"TableName":         result.TargetTableName,
					"TableStatus":       restoreStatus(result.DryRun),
					"RowsCopied":        result.RowsCopied,
					"DryRun":            result.DryRun,
					"SourceTableStats":  result.SourceTableStats,
					"ManifestVersion":   result.ManifestVersion,
					"ManifestStatus":    result.ManifestStatus,
					"TargetChangeIndex": targetChangeIndex,
					"TargetUnixNano":    targetUnixNano,
				},
			})
		},
	}
	f := c.Flags()
	f.StringVar(&backup, "backup-name", "", "Backup to restore from (required)")
	f.StringVar(&source, "source-table-name", "", "Table inside the backup to copy (required)")
	f.StringVar(&target, "target-table-name", "", "New table name in the live catalog (required)")
	f.BoolVar(&dryRun, "dry-run", false, "Validate restore inputs and manifest without creating the target table")
	f.Uint64Var(&targetChangeIndex, "target-change-index", 0, "Replay changes through this storage change index")
	f.Int64Var(&targetUnixNano, "target-unix-nano", 0, "Replay changes through this Unix nanosecond timestamp")
	_ = c.MarkFlagRequired("backup-name")
	_ = c.MarkFlagRequired("source-table-name")
	_ = c.MarkFlagRequired("target-table-name")
	root.AddCommand(c)
}

func restoreStatus(dryRun bool) string {
	if dryRun {
		return "DRY_RUN"
	}
	return "ACTIVE"
}
