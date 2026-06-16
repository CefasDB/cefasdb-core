package ddb

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefasctl/internal/runtime"
)

func registerDeleteBackup(root *cobra.Command) {
	var name string
	c := &cobra.Command{
		Use:   "delete-backup",
		Short: "Delete an admin-named backup and its checkpoint directory",
		Long: `Deletes the metadata for an admin-named backup and removes its
checkpoint directory from disk. If checkpoint cleanup cannot complete, the
response reports PartialCleanup and CleanupError explicitly.

Example:
  cefas delete-backup --backup-name nightly-2026-06-10`,
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
			result, err := cli.DeleteBackup(ctx, name)
			if err != nil {
				return fmt.Errorf("delete backup: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"BackupDeletion": map[string]any{
					"BackupName":        result.BackupName,
					"CheckpointPath":    result.CheckpointPath,
					"MetadataDeleted":   result.MetadataDeleted,
					"CheckpointDeleted": result.CheckpointDeleted,
					"CheckpointMissing": result.CheckpointMissing,
					"PartialCleanup":    result.PartialCleanup,
					"CleanupError":      result.CleanupError,
				},
			})
		},
	}
	f := c.Flags()
	f.StringVar(&name, "backup-name", "", "Backup to delete (required)")
	_ = c.MarkFlagRequired("backup-name")
	root.AddCommand(c)
}
