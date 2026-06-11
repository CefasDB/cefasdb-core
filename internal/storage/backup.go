package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

const BackupManifestVersion = 1

var (
	ErrBackupNotFound         = errors.New("cefas/storage: backup not found")
	ErrBackupInUse            = errors.New("cefas/storage: backup in use")
	ErrInvalidBackupRetention = errors.New("cefas/storage: invalid backup retention policy")
)

// BackupMetadata describes an admin-named backup of the live keyspace.
// One metadata blob per backup, persisted under cefas/admin/backups/<name>
// and listed by ListBackups.
type BackupMetadata struct {
	Name            string                `json:"name"`
	CreatedAt       int64                 `json:"createdAtUnix"`
	Tables          []string              `json:"tables"`
	CheckpointAt    string                `json:"checkpointAt"` // on-disk path of the pebble checkpoint
	ManifestVersion int                   `json:"manifestVersion,omitempty"`
	ManifestStatus  string                `json:"manifestStatus,omitempty"`
	RequestedTables []string              `json:"requestedTables,omitempty"`
	TableStats      []BackupTableStats    `json:"tableStats,omitempty"`
	ShardCoverage   []BackupShardCoverage `json:"shardCoverage,omitempty"`
	ChangeIndex     uint64                `json:"changeIndex,omitempty"`
	ChangeUnixNano  int64                 `json:"changeUnixNano,omitempty"`
}

type BackupTableStats struct {
	Table    string `json:"table"`
	Rows     int64  `json:"rows"`
	Checksum string `json:"checksum"`
}

type BackupShardCoverage struct {
	ShardID        string             `json:"shardId"`
	PlacementEpoch uint64             `json:"placementEpoch,omitempty"`
	TableStats     []BackupTableStats `json:"tableStats,omitempty"`
}

type BackupDeletionResult struct {
	BackupName        string `json:"backupName"`
	CheckpointPath    string `json:"checkpointPath,omitempty"`
	MetadataDeleted   bool   `json:"metadataDeleted"`
	CheckpointDeleted bool   `json:"checkpointDeleted"`
	CheckpointMissing bool   `json:"checkpointMissing"`
	PartialCleanup    bool   `json:"partialCleanup"`
	CleanupError      string `json:"cleanupError,omitempty"`
}

type BackupRetentionOptions struct {
	KeepLatest    int
	KeepLatestSet bool
	MaxAge        time.Duration
	MaxAgeSet     bool
	DryRun        bool
	Now           time.Time
}

type BackupRetentionCandidate struct {
	Backup BackupMetadata `json:"backup"`
	Reason string         `json:"reason"`
}

type BackupRetentionResult struct {
	DryRun        bool                       `json:"dryRun"`
	KeepLatest    int                        `json:"keepLatest,omitempty"`
	KeepLatestSet bool                       `json:"keepLatestSet,omitempty"`
	MaxAgeSeconds int64                      `json:"maxAgeSeconds,omitempty"`
	MaxAgeSet     bool                       `json:"maxAgeSet,omitempty"`
	CutoffUnix    int64                      `json:"cutoffUnix,omitempty"`
	WouldDelete   []BackupRetentionCandidate `json:"wouldDelete,omitempty"`
	Deleted       []BackupDeletionResult     `json:"deleted,omitempty"`
}

const backupKeyPrefix = "cefas/admin/backups/"

func backupKey(name string) []byte { return []byte(backupKeyPrefix + name) }

// CreateBackup snapshots the live keyspace into a pebble checkpoint
// rooted under <dbPath>/backups/<name> and records a metadata blob at
// cefas/admin/backups/<name>. Returns an error when name is empty,
// contains a path separator, or is already taken. The checkpoint
// directory must not already exist — pebble refuses to overwrite.
func (d *DB) CreateBackup(name string, tables []string) (BackupMetadata, error) {
	return d.CreateBackupForShard(name, tables, "0", 0)
}

func (d *DB) CreateBackupForShard(name string, tables []string, shardID string, placementEpoch uint64) (BackupMetadata, error) {
	if err := validateBackupName(name); err != nil {
		return BackupMetadata{}, err
	}
	keyBytes := backupKey(name)
	if exists, err := d.Has(keyBytes); err != nil {
		return BackupMetadata{}, err
	} else if exists {
		return BackupMetadata{}, fmt.Errorf("cefas/storage: backup %q already exists", name)
	}

	checkpointDir := filepath.Join(d.path, "backups", name)
	if _, err := os.Stat(checkpointDir); err == nil {
		return BackupMetadata{}, fmt.Errorf("cefas/storage: checkpoint dir %q already exists", checkpointDir)
	}
	if err := os.MkdirAll(filepath.Dir(checkpointDir), 0o755); err != nil {
		return BackupMetadata{}, fmt.Errorf("mkdir backups: %w", err)
	}
	// Flush forces the memtable out to an SSTable before checkpointing,
	// so the snapshot reflects every committed write — pebble's read-only
	// re-open does not replay the live WAL.
	if err := d.db.Flush(); err != nil {
		return BackupMetadata{}, fmt.Errorf("pebble flush: %w", err)
	}
	if err := d.db.Checkpoint(checkpointDir); err != nil {
		return BackupMetadata{}, fmt.Errorf("pebble checkpoint: %w", err)
	}

	capturedTables, tableStats, err := buildBackupManifest(checkpointDir, tables)
	if err != nil {
		_ = os.RemoveAll(checkpointDir)
		return BackupMetadata{}, err
	}
	changeIndex, err := d.CurrentChangeIndex()
	if err != nil {
		_ = os.RemoveAll(checkpointDir)
		return BackupMetadata{}, fmt.Errorf("read change index: %w", err)
	}
	now := time.Now()

	meta := BackupMetadata{
		Name:            name,
		CreatedAt:       now.Unix(),
		Tables:          capturedTables,
		CheckpointAt:    checkpointDir,
		ManifestVersion: BackupManifestVersion,
		ManifestStatus:  "ok",
		RequestedTables: sortedUniqueStrings(tables),
		TableStats:      tableStats,
		ShardCoverage: []BackupShardCoverage{{
			ShardID:        shardID,
			PlacementEpoch: placementEpoch,
			TableStats:     tableStats,
		}},
		ChangeIndex:    changeIndex,
		ChangeUnixNano: now.UnixNano(),
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		return BackupMetadata{}, fmt.Errorf("marshal metadata: %w", err)
	}
	if err := d.Set(keyBytes, raw); err != nil {
		// Best-effort cleanup: the metadata write failed, so the
		// checkpoint dir is orphaned. Surface both.
		_ = os.RemoveAll(checkpointDir)
		return BackupMetadata{}, fmt.Errorf("persist metadata: %w", err)
	}
	return meta, nil
}

func (d *DB) StoreBackupMetadata(meta BackupMetadata) error {
	raw, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}
	if err := d.Set(backupKey(meta.Name), raw); err != nil {
		return fmt.Errorf("persist metadata: %w", err)
	}
	return nil
}

// ListBackups returns every recorded backup in ascending name order.
func (d *DB) ListBackups() ([]BackupMetadata, error) {
	lower := []byte(backupKeyPrefix)
	upper := []byte(backupKeyPrefix + "\xff")
	it, err := d.Iter(lower, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []BackupMetadata
	for valid := it.First(); valid; valid = it.Next() {
		v := it.Value()
		cp := make([]byte, len(v))
		copy(cp, v)
		var meta BackupMetadata
		if err := json.Unmarshal(cp, &meta); err != nil {
			return nil, fmt.Errorf("decode backup at %s: %w", it.Key(), err)
		}
		normalizeBackupMetadata(&meta)
		out = append(out, meta)
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// GetBackup returns the metadata for a single named backup. Returns
// nil with no error when the backup doesn't exist.
func (d *DB) GetBackup(name string) (*BackupMetadata, error) {
	raw, err := d.Get(backupKey(name))
	if err == ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var meta BackupMetadata
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, fmt.Errorf("decode metadata: %w", err)
	}
	normalizeBackupMetadata(&meta)
	return &meta, nil
}

func (d *DB) DeleteBackup(name string) (BackupDeletionResult, error) {
	if err := validateBackupName(name); err != nil {
		return BackupDeletionResult{}, err
	}

	d.backupMu.Lock()
	defer d.backupMu.Unlock()

	if d.activeBackupRestores[name] > 0 {
		return BackupDeletionResult{}, fmt.Errorf("%w: %q", ErrBackupInUse, name)
	}

	meta, err := d.GetBackup(name)
	if err != nil {
		return BackupDeletionResult{}, err
	}
	if meta == nil {
		return BackupDeletionResult{}, fmt.Errorf("%w: %q", ErrBackupNotFound, name)
	}

	result := BackupDeletionResult{
		BackupName:     meta.Name,
		CheckpointPath: meta.CheckpointAt,
	}
	if err := validateBackupCheckpointPath(d.path, meta.CheckpointAt); err != nil {
		return result, err
	}

	if err := d.Delete(backupKey(name)); err != nil {
		return result, fmt.Errorf("delete backup metadata: %w", err)
	}
	result.MetadataDeleted = true

	if _, err := os.Stat(meta.CheckpointAt); err != nil {
		if os.IsNotExist(err) {
			result.CheckpointMissing = true
			return result, nil
		}
		result.PartialCleanup = true
		result.CleanupError = err.Error()
		return result, nil
	}
	if err := os.RemoveAll(meta.CheckpointAt); err != nil {
		result.PartialCleanup = true
		result.CleanupError = err.Error()
		return result, nil
	}
	result.CheckpointDeleted = true
	return result, nil
}

func (d *DB) ApplyBackupRetention(opts BackupRetentionOptions) (BackupRetentionResult, error) {
	if err := validateBackupRetentionOptions(opts); err != nil {
		return BackupRetentionResult{}, err
	}
	backups, err := d.ListBackups()
	if err != nil {
		return BackupRetentionResult{}, err
	}
	candidates, cutoff := selectBackupRetentionCandidates(backups, opts)
	result := BackupRetentionResult{
		DryRun:        opts.DryRun,
		KeepLatest:    opts.KeepLatest,
		KeepLatestSet: opts.KeepLatestSet,
		MaxAgeSeconds: int64(opts.MaxAge / time.Second),
		MaxAgeSet:     opts.MaxAgeSet,
		CutoffUnix:    cutoff,
		WouldDelete:   candidates,
	}
	if opts.DryRun {
		return result, nil
	}
	for _, candidate := range candidates {
		deleted, err := d.DeleteBackup(candidate.Backup.Name)
		if err != nil {
			return result, err
		}
		result.Deleted = append(result.Deleted, deleted)
	}
	return result, nil
}

func (d *DB) beginBackupRestore(name string) func() {
	d.backupMu.Lock()
	if d.activeBackupRestores == nil {
		d.activeBackupRestores = make(map[string]int)
	}
	d.activeBackupRestores[name]++
	d.backupMu.Unlock()

	return func() {
		d.backupMu.Lock()
		defer d.backupMu.Unlock()
		if d.activeBackupRestores[name] <= 1 {
			delete(d.activeBackupRestores, name)
			return
		}
		d.activeBackupRestores[name]--
	}
}

func validateBackupRetentionOptions(opts BackupRetentionOptions) error {
	if !opts.KeepLatestSet && !opts.MaxAgeSet {
		return fmt.Errorf("%w: requires keep-latest or max-age", ErrInvalidBackupRetention)
	}
	if opts.KeepLatestSet && opts.KeepLatest < 0 {
		return fmt.Errorf("%w: keep-latest must be >= 0", ErrInvalidBackupRetention)
	}
	if opts.MaxAgeSet && opts.MaxAge < 0 {
		return fmt.Errorf("%w: max-age must be >= 0", ErrInvalidBackupRetention)
	}
	return nil
}

func selectBackupRetentionCandidates(backups []BackupMetadata, opts BackupRetentionOptions) ([]BackupRetentionCandidate, int64) {
	now := opts.Now
	if now.IsZero() {
		now = time.Now()
	}
	cutoff := int64(0)
	if opts.MaxAgeSet {
		cutoff = now.Add(-opts.MaxAge).Unix()
	}

	newest := append([]BackupMetadata(nil), backups...)
	sort.Slice(newest, func(i, j int) bool {
		if newest[i].CreatedAt == newest[j].CreatedAt {
			return newest[i].Name < newest[j].Name
		}
		return newest[i].CreatedAt > newest[j].CreatedAt
	})
	retainedByLatest := make(map[string]bool)
	if opts.KeepLatestSet {
		limit := opts.KeepLatest
		if limit > len(newest) {
			limit = len(newest)
		}
		for _, meta := range newest[:limit] {
			retainedByLatest[meta.Name] = true
		}
	}

	var out []BackupRetentionCandidate
	for _, meta := range backups {
		outsideLatest := opts.KeepLatestSet && !retainedByLatest[meta.Name]
		olderThanMaxAge := opts.MaxAgeSet && meta.CreatedAt < cutoff
		deleteByPolicy := false
		reason := ""
		switch {
		case opts.KeepLatestSet && opts.MaxAgeSet:
			deleteByPolicy = outsideLatest && olderThanMaxAge
			reason = "outside_latest_and_older_than_max_age"
		case opts.KeepLatestSet:
			deleteByPolicy = outsideLatest
			reason = "outside_latest"
		case opts.MaxAgeSet:
			deleteByPolicy = olderThanMaxAge
			reason = "older_than_max_age"
		}
		if deleteByPolicy {
			out = append(out, BackupRetentionCandidate{Backup: meta, Reason: reason})
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Backup.CreatedAt == out[j].Backup.CreatedAt {
			return out[i].Backup.Name < out[j].Backup.Name
		}
		return out[i].Backup.CreatedAt < out[j].Backup.CreatedAt
	})
	return out, cutoff
}

func validateBackupCheckpointPath(dbPath, checkpointPath string) error {
	if checkpointPath == "" {
		return fmt.Errorf("backup checkpoint path is empty")
	}
	root, err := filepath.Abs(filepath.Join(dbPath, "backups"))
	if err != nil {
		return err
	}
	checkpoint, err := filepath.Abs(checkpointPath)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(root, checkpoint)
	if err != nil {
		return err
	}
	if rel == "." || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return fmt.Errorf("backup checkpoint path %q is outside %q", checkpointPath, root)
	}
	return nil
}

func buildBackupManifest(checkpointDir string, requested []string) ([]string, []BackupTableStats, error) {
	checkpoint, err := pebbledb.Open(checkpointDir, &pebbledb.Options{ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("open checkpoint for manifest: %w", err)
	}
	defer checkpoint.Close()

	tables := sortedUniqueStrings(requested)
	if len(tables) == 0 {
		tables, err = listCheckpointCatalogTables(checkpoint)
		if err != nil {
			return nil, nil, err
		}
	} else {
		for _, table := range tables {
			if err := requireCheckpointTable(checkpoint, table); err != nil {
				return nil, nil, err
			}
		}
	}

	stats := make([]BackupTableStats, 0, len(tables))
	for _, table := range tables {
		stat, err := checkpointTableStats(checkpoint, table)
		if err != nil {
			return nil, nil, err
		}
		stats = append(stats, stat)
	}
	return tables, stats, nil
}

func listCheckpointCatalogTables(checkpoint *pebbledb.DB) ([]string, error) {
	lower, upper := PrefixCatalog()
	iter, err := checkpoint.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("checkpoint catalog iter: %w", err)
	}
	defer iter.Close()

	var tables []string
	for valid := iter.First(); valid; valid = iter.Next() {
		raw := iter.Value()
		cp := make([]byte, len(raw))
		copy(cp, raw)
		var td types.TableDescriptor
		if err := json.Unmarshal(cp, &td); err != nil {
			return nil, fmt.Errorf("decode catalog descriptor at %s: %w", iter.Key(), err)
		}
		if td.Name != "" {
			tables = append(tables, td.Name)
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	sort.Strings(tables)
	return tables, nil
}

func requireCheckpointTable(checkpoint *pebbledb.DB, table string) error {
	raw, closer, err := checkpoint.Get(KeyCatalog(table))
	if err != nil {
		return fmt.Errorf("backup manifest: table %q descriptor missing in checkpoint: %w", table, err)
	}
	cp := append([]byte(nil), raw...)
	_ = closer.Close()
	var td types.TableDescriptor
	if err := json.Unmarshal(cp, &td); err != nil {
		return fmt.Errorf("backup manifest: decode table %q descriptor: %w", table, err)
	}
	if td.Name != table {
		return fmt.Errorf("backup manifest: descriptor key %q contains table %q", table, td.Name)
	}
	return nil
}

func checkpointTableStats(checkpoint *pebbledb.DB, table string) (BackupTableStats, error) {
	lower, upper := PrefixPrimaryAll(table)
	iter, err := checkpoint.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return BackupTableStats{}, fmt.Errorf("checkpoint table iter %q: %w", table, err)
	}
	defer iter.Close()

	h := fnv.New64a()
	var rows int64
	for valid := iter.First(); valid; valid = iter.Next() {
		_, _ = h.Write(iter.Key())
		_, _ = h.Write([]byte{0})
		_, _ = h.Write(iter.Value())
		_, _ = h.Write([]byte{0xff})
		rows++
	}
	if err := iter.Error(); err != nil {
		return BackupTableStats{}, err
	}
	return BackupTableStats{
		Table:    table,
		Rows:     rows,
		Checksum: fmt.Sprintf("%016x", h.Sum64()),
	}, nil
}

func validateBackupManifestTable(meta BackupMetadata, checkpoint *pebbledb.DB, table string) (BackupTableStats, error) {
	if meta.ManifestVersion == 0 {
		return checkpointTableStats(checkpoint, table)
	}
	if meta.ManifestVersion != BackupManifestVersion {
		return BackupTableStats{}, fmt.Errorf("backup %q manifest version %d is not supported", meta.Name, meta.ManifestVersion)
	}
	if !containsString(meta.Tables, table) {
		return BackupTableStats{}, fmt.Errorf("backup %q did not capture table %q", meta.Name, table)
	}
	if len(meta.ShardCoverage) > 0 && !coverageContainsTable(meta.ShardCoverage, table) {
		return BackupTableStats{}, fmt.Errorf("backup %q shard coverage did not capture table %q", meta.Name, table)
	}
	expected, ok := backupTableStat(meta.TableStats, table)
	if !ok {
		return BackupTableStats{}, fmt.Errorf("backup %q manifest has no stats for table %q", meta.Name, table)
	}
	actual, err := checkpointTableStats(checkpoint, table)
	if err != nil {
		return BackupTableStats{}, err
	}
	if actual.Rows != expected.Rows || actual.Checksum != expected.Checksum {
		return BackupTableStats{}, fmt.Errorf(
			"backup %q manifest mismatch for table %q: rows %d/%d checksum %s/%s",
			meta.Name, table, actual.Rows, expected.Rows, actual.Checksum, expected.Checksum,
		)
	}
	return actual, nil
}

func backupTableStat(stats []BackupTableStats, table string) (BackupTableStats, bool) {
	for _, stat := range stats {
		if stat.Table == table {
			return stat, true
		}
	}
	return BackupTableStats{}, false
}

func normalizeBackupMetadata(meta *BackupMetadata) {
	meta.Tables = sortedUniqueStrings(meta.Tables)
	meta.RequestedTables = sortedUniqueStrings(meta.RequestedTables)
	sort.Slice(meta.TableStats, func(i, j int) bool { return meta.TableStats[i].Table < meta.TableStats[j].Table })
	sort.Slice(meta.ShardCoverage, func(i, j int) bool { return meta.ShardCoverage[i].ShardID < meta.ShardCoverage[j].ShardID })
	for i := range meta.ShardCoverage {
		sort.Slice(meta.ShardCoverage[i].TableStats, func(a, b int) bool {
			return meta.ShardCoverage[i].TableStats[a].Table < meta.ShardCoverage[i].TableStats[b].Table
		})
	}
	if meta.ManifestVersion == 0 {
		meta.ManifestStatus = "legacy"
		return
	}
	if meta.ManifestVersion == BackupManifestVersion {
		meta.ManifestStatus = "ok"
		return
	}
	meta.ManifestStatus = "unsupported"
}

func coverageContainsTable(coverage []BackupShardCoverage, table string) bool {
	for _, shard := range coverage {
		for _, stat := range shard.TableStats {
			if stat.Table == table {
				return true
			}
		}
	}
	return false
}

func sortedUniqueStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	cp := append([]string(nil), in...)
	sort.Strings(cp)
	out := cp[:0]
	for _, v := range cp {
		if v == "" {
			continue
		}
		if len(out) == 0 || out[len(out)-1] != v {
			out = append(out, v)
		}
	}
	return out
}

func containsString(in []string, want string) bool {
	for _, v := range in {
		if v == want {
			return true
		}
	}
	return false
}

func validateBackupName(name string) error {
	if name == "" {
		return fmt.Errorf("backup name required")
	}
	if strings.ContainsAny(name, "/\\") {
		return fmt.Errorf("backup name %q contains path separator", name)
	}
	if strings.Contains(name, "..") {
		return fmt.Errorf("backup name %q contains ..", name)
	}
	return nil
}
