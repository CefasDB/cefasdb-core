package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// BackupMetadata describes an admin-named backup of the live keyspace.
// One metadata blob per backup, persisted under cefas/admin/backups/<name>
// and listed by ListBackups.
type BackupMetadata struct {
	Name         string   `json:"name"`
	CreatedAt    int64    `json:"createdAtUnix"`
	Tables       []string `json:"tables"`
	CheckpointAt string   `json:"checkpointAt"` // on-disk path of the pebble checkpoint
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

	meta := BackupMetadata{
		Name:         name,
		CreatedAt:    time.Now().Unix(),
		Tables:       append([]string(nil), tables...),
		CheckpointAt: checkpointDir,
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
	return &meta, nil
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
