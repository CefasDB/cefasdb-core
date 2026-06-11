package ddb

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/output"
	"github.com/osvaldoandrade/cefas/cmd/cefas-cli/internal/runtime"
	"github.com/osvaldoandrade/cefas/pkg/client"
)

func registerApplyBackupRetention(root *cobra.Command) {
	var (
		keepLatest int
		maxAge     string
		dryRun     bool
	)
	c := &cobra.Command{
		Use:   "apply-backup-retention",
		Short: "Delete backups outside a retention policy",
		Long: `Applies a backup retention policy. Use --dry-run to report the exact
backups that would be deleted without removing metadata or checkpoint
directories. When both --keep-latest and --max-age are set, the latest N
backups are retained even when older than max-age.

Examples:
  cefas apply-backup-retention --keep-latest 7 --dry-run
  cefas apply-backup-retention --max-age 168h --dry-run
  cefas apply-backup-retention --keep-latest 7 --max-age 720h`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if keepLatest < -1 {
				return fmt.Errorf("--keep-latest must be >= 0")
			}
			opts := client.BackupRetentionOptions{
				KeepLatest:    keepLatest,
				KeepLatestSet: keepLatest >= 0,
				DryRun:        dryRun,
			}
			if maxAge != "" {
				d, err := time.ParseDuration(maxAge)
				if err != nil {
					return fmt.Errorf("parse --max-age: %w", err)
				}
				opts.MaxAge = d
				opts.MaxAgeSet = true
			}
			if !opts.KeepLatestSet && !opts.MaxAgeSet {
				return fmt.Errorf("set --keep-latest, --max-age, or both")
			}
			ctx := cmd.Context()
			cli, profile, err := runtime.Dial(ctx)
			if err != nil {
				return err
			}
			defer cli.Close()
			result, err := cli.ApplyBackupRetention(ctx, opts)
			if err != nil {
				return fmt.Errorf("apply backup retention: %w", err)
			}
			fm, err := output.Validate(profile.Output)
			if err != nil {
				return err
			}
			return output.New(cmd.OutOrStdout(), fm).Object(map[string]any{
				"BackupRetention": map[string]any{
					"DryRun":        result.DryRun,
					"KeepLatest":    result.KeepLatest,
					"KeepLatestSet": result.KeepLatestSet,
					"MaxAgeSeconds": result.MaxAgeSeconds,
					"MaxAgeSet":     result.MaxAgeSet,
					"CutoffUnix":    result.CutoffUnix,
					"WouldDelete":   result.WouldDelete,
					"Deleted":       result.Deleted,
				},
			})
		},
	}
	f := c.Flags()
	f.IntVar(&keepLatest, "keep-latest", -1, "Retain this many newest backups; 0 retains none by latest-count policy")
	f.StringVar(&maxAge, "max-age", "", "Delete backups older than this Go duration, for example 168h")
	f.BoolVar(&dryRun, "dry-run", false, "Report matching backups without deleting metadata or checkpoints")
	root.AddCommand(c)
}
