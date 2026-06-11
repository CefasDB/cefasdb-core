package ddb

import (
	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
)

func registerListBackups(root *cobra.Command) {
	c := &cobra.Command{
		Use:   "list-backups",
		Short: "List every admin-named backup the server knows about",
		Long: `Mirrors aws dynamodb list-backups. Returns the cefas admin-named
checkpoints created via create-backup (not the internal raft snapshots
served by list-snapshots).

Example:
  cefas list-backups`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			backups, err := cli.ListBackups(ctx)
			if err != nil {
				return err
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			summaries := make([]map[string]any, 0, len(backups))
			for _, b := range backups {
				summaries = append(summaries, map[string]any{
					"BackupName":               b.Name,
					"BackupCreationUnix":       b.CreatedAt,
					"BackupCheckpointAt":       b.CheckpointAt,
					"BackupTables":             b.Tables,
					"BackupRequestedTables":    b.RequestedTables,
					"BackupManifestVersion":    b.ManifestVersion,
					"BackupManifestStatus":     b.ManifestStatus,
					"BackupManifestTableStats": b.TableStats,
					"BackupShardCoverage":      b.ShardCoverage,
					"BackupChangeIndex":        b.ChangeIndex,
					"BackupChangeUnixNano":     b.ChangeUnixNano,
				})
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"BackupSummaries": summaries,
			})
		},
	}
	root.AddCommand(c)
}
