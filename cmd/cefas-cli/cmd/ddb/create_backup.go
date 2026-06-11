package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
)

func registerCreateBackup(root *cobra.Command) {
	var (
		name   string
		tables []string
	)
	c := &cobra.Command{
		Use:   "create-backup",
		Short: "Create an admin-named pebble checkpoint of the live keyspace",
		Long: `Mirrors aws dynamodb create-backup. cefas creates a pebble
checkpoint under <dbPath>/backups/<name> and registers metadata at
cefas/admin/backups/<name>. The checkpoint is consistent at the
moment of the call.

Example:
  cefas create-backup --backup-name users-2026-06-10
  cefas create-backup --backup-name nightly --table-name Users --table-name Orders`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if name == "" {
				return fmt.Errorf("--backup-name is required")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			b, err := cli.CreateBackup(ctx, name, tables)
			if err != nil {
				return fmt.Errorf("create backup: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"BackupDetails": map[string]any{
					"BackupName":               b.Name,
					"BackupCreationUnix":       b.CreatedAt,
					"BackupCheckpointAt":       b.CheckpointAt,
					"BackupTables":             b.Tables,
					"BackupRequestedTables":    b.RequestedTables,
					"BackupManifestVersion":    b.ManifestVersion,
					"BackupManifestStatus":     b.ManifestStatus,
					"BackupManifestTableStats": b.TableStats,
				},
			})
		},
	}
	f := c.Flags()
	f.StringVar(&name, "backup-name", "", "Admin-chosen backup name (required)")
	f.StringArrayVar(&tables, "table-name", nil, "Limit the backup to these tables (repeatable; omit for every table)")
	_ = c.MarkFlagRequired("backup-name")
	root.AddCommand(c)
}
