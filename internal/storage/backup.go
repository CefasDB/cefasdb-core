package storage

import (
	"encoding/json"
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

// BackupMetadata describes an admin-named backup of the live keyspace.
// One metadata blob per backup, persisted under cefas/admin/backups/<name>
// and listed by ListBackups.
type BackupMetadata struct {
	Name            string             `json:"name"`
	CreatedAt       int64              `json:"createdAtUnix"`
	Tables          []string           `json:"tables"`
	CheckpointAt    string             `json:"checkpointAt"` // on-disk path of the pebble checkpoint
	ManifestVersion int                `json:"manifestVersion,omitempty"`
	ManifestStatus  string             `json:"manifestStatus,omitempty"`
	RequestedTables []string           `json:"requestedTables,omitempty"`
	TableStats      []BackupTableStats `json:"tableStats,omitempty"`
}

type BackupTableStats struct {
	Table    string `json:"table"`
	Rows     int64  `json:"rows"`
	Checksum string `json:"checksum"`
}

const backupKeyPrefix = "cefas/admin/backups/"

func backupKey(name string) []byte { return []byte(backupKeyPrefix + name) }

// CreateBackup snapshots the live keyspace into a pebble checkpoint
// rooted under <dbPath>/backups/<name> and records a metadata blob at
// cefas/admin/backups/<name>. Returns an error when name is empty,
// contains a path separator, or is already taken. The checkpoint
// directory must not already exist — pebble refuses to overwrite.
func (d *DB) CreateBackup(name string, tables []string) (BackupMetadata, error) {
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

	meta := BackupMetadata{
		Name:            name,
		CreatedAt:       time.Now().Unix(),
		Tables:          capturedTables,
		CheckpointAt:    checkpointDir,
		ManifestVersion: BackupManifestVersion,
		ManifestStatus:  "ok",
		RequestedTables: sortedUniqueStrings(tables),
		TableStats:      tableStats,
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
