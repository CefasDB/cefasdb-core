package client

import (
	"context"
	"time"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
)

// ---------- backups ----------

// BackupDescriptor mirrors the wire shape — name, creation time,
// captured tables, local checkpoint path, and manifest diagnostics.
type BackupDescriptor struct {
	Name            string
	CreatedAt       int64
	Tables          []string
	CheckpointAt    string
	ManifestVersion int
	ManifestStatus  string
	RequestedTables []string
	TableStats      []BackupTableStats
	ShardCoverage   []BackupShardCoverage
	ChangeIndex     uint64
	ChangeUnixNano  int64
}

// BackupTableStats reports per-table row count and checksum captured
// inside a backup manifest.
type BackupTableStats struct {
	Table    string
	Rows     int64
	Checksum string
}

// BackupShardCoverage reports the per-shard table statistics observed
// at the placement epoch the backup was taken under.
type BackupShardCoverage struct {
	ShardID        string
	PlacementEpoch uint64
	TableStats     []BackupTableStats
}

// BackupDeletionResult describes the outcome of a DeleteBackup call,
// including whether the metadata and the checkpoint directory were
// removed cleanly.
type BackupDeletionResult struct {
	BackupName        string
	CheckpointPath    string
	MetadataDeleted   bool
	CheckpointDeleted bool
	CheckpointMissing bool
	PartialCleanup    bool
	CleanupError      string
}

// BackupRetentionOptions parameterises ApplyBackupRetention; either
// or both of KeepLatest / MaxAge may be set, gated by their *Set
// companions.
type BackupRetentionOptions struct {
	KeepLatest    int
	KeepLatestSet bool
	MaxAge        time.Duration
	MaxAgeSet     bool
	DryRun        bool
}

// BackupRetentionCandidate is one backup the retention pass would
// (or did) delete, paired with the human-readable reason.
type BackupRetentionCandidate struct {
	Backup BackupDescriptor
	Reason string
}

// BackupRetentionResult bundles the effective retention thresholds
// and the deletion outcomes from ApplyBackupRetention.
type BackupRetentionResult struct {
	DryRun        bool
	KeepLatest    int
	KeepLatestSet bool
	MaxAgeSeconds int64
	MaxAgeSet     bool
	CutoffUnix    int64
	WouldDelete   []BackupRetentionCandidate
	Deleted       []BackupDeletionResult
}

// CreateBackup snapshots the live keyspace into a pebble checkpoint
// and records metadata under cefas/admin/backups/<name>. Pass nil for
// tables to back up every table the catalog currently knows.
func (c *Client) CreateBackup(ctx context.Context, name string, tables []string) (BackupDescriptor, error) {
	resp, err := c.stub.CreateBackup(c.withAuth(ctx), &cefaspb.CreateBackupRequest{Name: name, Tables: tables})
	if err != nil {
		return BackupDescriptor{}, err
	}
	return backupFromPB(resp.GetBackup()), nil
}

// ListBackups returns every admin-named backup the server knows about.
func (c *Client) ListBackups(ctx context.Context) ([]BackupDescriptor, error) {
	resp, err := c.stub.ListBackups(c.withAuth(ctx), &cefaspb.ListBackupsRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]BackupDescriptor, 0, len(resp.GetBackups()))
	for _, b := range resp.GetBackups() {
		out = append(out, backupFromPB(b))
	}
	return out, nil
}

// DeleteBackup removes the named backup and its checkpoint files,
// returning a per-step deletion summary.
func (c *Client) DeleteBackup(ctx context.Context, name string) (BackupDeletionResult, error) {
	resp, err := c.stub.DeleteBackup(c.withAuth(ctx), &cefaspb.DeleteBackupRequest{Name: name})
	if err != nil {
		return BackupDeletionResult{}, err
	}
	return backupDeletionFromPB(resp.GetResult()), nil
}

// ApplyBackupRetention deletes backups that fall outside the retention
// window described by opts; when opts.DryRun is set only the candidate
// list is returned.
func (c *Client) ApplyBackupRetention(ctx context.Context, opts BackupRetentionOptions) (BackupRetentionResult, error) {
	resp, err := c.stub.ApplyBackupRetention(c.withAuth(ctx), &cefaspb.ApplyBackupRetentionRequest{
		KeepLatest:    int32(opts.KeepLatest),
		KeepLatestSet: opts.KeepLatestSet,
		MaxAgeSeconds: int64(opts.MaxAge / time.Second),
		MaxAgeSet:     opts.MaxAgeSet,
		DryRun:        opts.DryRun,
	})
	if err != nil {
		return BackupRetentionResult{}, err
	}
	return backupRetentionFromPB(resp), nil
}

func backupFromPB(b *cefaspb.BackupDescriptor) BackupDescriptor {
	if b == nil {
		return BackupDescriptor{}
	}
	stats := make([]BackupTableStats, 0, len(b.GetTableStats()))
	for _, stat := range b.GetTableStats() {
		stats = append(stats, backupTableStatsFromPB(stat))
	}
	coverage := make([]BackupShardCoverage, 0, len(b.GetShardCoverage()))
	for _, shard := range b.GetShardCoverage() {
		shardStats := make([]BackupTableStats, 0, len(shard.GetTableStats()))
		for _, stat := range shard.GetTableStats() {
			shardStats = append(shardStats, backupTableStatsFromPB(stat))
		}
		coverage = append(coverage, BackupShardCoverage{
			ShardID:        shard.GetShardId(),
			PlacementEpoch: shard.GetPlacementEpoch(),
			TableStats:     shardStats,
		})
	}
	return BackupDescriptor{
		Name:            b.GetName(),
		CreatedAt:       b.GetCreatedAtUnix(),
		Tables:          b.GetTables(),
		CheckpointAt:    b.GetCheckpointPath(),
		ManifestVersion: int(b.GetManifestVersion()),
		ManifestStatus:  b.GetManifestStatus(),
		RequestedTables: b.GetRequestedTables(),
		TableStats:      stats,
		ShardCoverage:   coverage,
		ChangeIndex:     b.GetChangeIndex(),
		ChangeUnixNano:  b.GetChangeUnixNano(),
	}
}

func backupTableStatsFromPB(stat *cefaspb.BackupTableStats) BackupTableStats {
	if stat == nil {
		return BackupTableStats{}
	}
	return BackupTableStats{
		Table:    stat.GetTable(),
		Rows:     stat.GetRows(),
		Checksum: stat.GetChecksum(),
	}
}

func backupDeletionFromPB(r *cefaspb.BackupDeletionResult) BackupDeletionResult {
	if r == nil {
		return BackupDeletionResult{}
	}
	return BackupDeletionResult{
		BackupName:        r.GetBackupName(),
		CheckpointPath:    r.GetCheckpointPath(),
		MetadataDeleted:   r.GetMetadataDeleted(),
		CheckpointDeleted: r.GetCheckpointDeleted(),
		CheckpointMissing: r.GetCheckpointMissing(),
		PartialCleanup:    r.GetPartialCleanup(),
		CleanupError:      r.GetCleanupError(),
	}
}

func backupRetentionFromPB(resp *cefaspb.ApplyBackupRetentionResponse) BackupRetentionResult {
	if resp == nil {
		return BackupRetentionResult{}
	}
	wouldDelete := make([]BackupRetentionCandidate, 0, len(resp.GetWouldDelete()))
	for _, candidate := range resp.GetWouldDelete() {
		wouldDelete = append(wouldDelete, BackupRetentionCandidate{
			Backup: backupFromPB(candidate.GetBackup()),
			Reason: candidate.GetReason(),
		})
	}
	deleted := make([]BackupDeletionResult, 0, len(resp.GetDeleted()))
	for _, item := range resp.GetDeleted() {
		deleted = append(deleted, backupDeletionFromPB(item))
	}
	return BackupRetentionResult{
		DryRun:        resp.GetDryRun(),
		KeepLatest:    int(resp.GetKeepLatest()),
		KeepLatestSet: resp.GetKeepLatestSet(),
		MaxAgeSeconds: resp.GetMaxAgeSeconds(),
		MaxAgeSet:     resp.GetMaxAgeSet(),
		CutoffUnix:    resp.GetCutoffUnix(),
		WouldDelete:   wouldDelete,
		Deleted:       deleted,
	}
}

// RestoreTableFromBackupOptions tunes RestoreTableFromBackupWithOptions
// with an optional dry-run and point-in-time targets.
type RestoreTableFromBackupOptions struct {
	DryRun            bool
	TargetChangeIndex uint64
	TargetUnixNano    int64
}

// RestoreTableFromBackupResult summarises a restore call, reporting the
// destination table, the row count copied, and the backup manifest
// status the server inspected.
type RestoreTableFromBackupResult struct {
	TargetTableName  string
	RowsCopied       int
	DryRun           bool
	SourceTableStats BackupTableStats
	ManifestVersion  int
	ManifestStatus   string
}

// RestoreTableFromBackup reads `sourceTable`'s descriptor from
// `backupName` and reproduces it under `targetTable` in the live
// catalog, then copies every row from the checkpoint into the new
// table. Returns the number of rows copied.
func (c *Client) RestoreTableFromBackup(ctx context.Context, backupName, sourceTable, targetTable string) (int, error) {
	result, err := c.RestoreTableFromBackupWithOptions(ctx, backupName, sourceTable, targetTable, RestoreTableFromBackupOptions{})
	if err != nil {
		return 0, err
	}
	return result.RowsCopied, nil
}

// RestoreTableFromBackupWithOptions is the configurable form of
// RestoreTableFromBackup; opts toggles dry-run and point-in-time
// targets.
func (c *Client) RestoreTableFromBackupWithOptions(ctx context.Context, backupName, sourceTable, targetTable string, opts RestoreTableFromBackupOptions) (RestoreTableFromBackupResult, error) {
	resp, err := c.stub.RestoreTableFromBackup(c.withAuth(ctx), &cefaspb.RestoreTableFromBackupRequest{
		BackupName:        backupName,
		SourceTableName:   sourceTable,
		TargetTableName:   targetTable,
		DryRun:            opts.DryRun,
		TargetChangeIndex: opts.TargetChangeIndex,
		TargetUnixNano:    opts.TargetUnixNano,
	})
	if err != nil {
		return RestoreTableFromBackupResult{}, err
	}
	stat := resp.GetSourceTableStats()
	return RestoreTableFromBackupResult{
		TargetTableName: resp.GetTargetTableName(),
		RowsCopied:      int(resp.GetRowsCopied()),
		DryRun:          resp.GetDryRun(),
		ManifestVersion: int(resp.GetManifestVersion()),
		ManifestStatus:  resp.GetManifestStatus(),
		SourceTableStats: BackupTableStats{
			Table:    stat.GetTable(),
			Rows:     stat.GetRows(),
			Checksum: stat.GetChecksum(),
		},
	}, nil
}

// CompactionResult describes one compaction job executed by
// CompactTable, with timing and before/after LSM metrics.
type CompactionResult struct {
	Table            string
	Lower            []byte
	Upper            []byte
	StartedAtUnixNS  int64
	FinishedAtUnixNS int64
	ElapsedSeconds   float64
	Parallelized     bool
	BeforeL0Files    int64
	AfterL0Files     int64
	BeforeDebtBytes  uint64
	AfterDebtBytes   uint64
}

// CompactTable triggers a manual LSM compaction over the named table
// and returns one CompactionResult per executed job; parallelize lets
// the server fan out across shards when supported.
func (c *Client) CompactTable(ctx context.Context, table string, parallelize bool) ([]CompactionResult, error) {
	resp, err := c.stub.Compact(c.withAuth(ctx), &cefaspb.CompactRequest{Table: table, Parallelize: parallelize})
	if err != nil {
		return nil, err
	}
	out := make([]CompactionResult, 0, len(resp.GetResults()))
	for _, r := range resp.GetResults() {
		out = append(out, CompactionResult{
			Table:            r.GetTable(),
			Lower:            append([]byte(nil), r.GetLower()...),
			Upper:            append([]byte(nil), r.GetUpper()...),
			StartedAtUnixNS:  r.GetStartedAtUnixNs(),
			FinishedAtUnixNS: r.GetFinishedAtUnixNs(),
			ElapsedSeconds:   r.GetElapsedSeconds(),
			Parallelized:     r.GetParallelized(),
			BeforeL0Files:    r.GetBeforeL0Files(),
			AfterL0Files:     r.GetAfterL0Files(),
			BeforeDebtBytes:  r.GetBeforeDebtBytes(),
			AfterDebtBytes:   r.GetAfterDebtBytes(),
		})
	}
	return out, nil
}
