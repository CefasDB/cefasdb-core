package storage

import (
	"encoding/json"
	"errors"
	"fmt"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

// RestoreResult describes the outcome of RestoreTableFromBackup.
type RestoreResult struct {
	TargetTable     types.TableDescriptor
	RowsCopied      int
	SourceStats     BackupTableStats
	DryRun          bool
	ManifestVersion int
	ManifestStatus  string
}

type RestoreOptions struct {
	DryRun bool
}

type RestorePreflightResult struct {
	BackupName      string
	SourceTableName string
	TargetTableName string
	TargetTable     types.TableDescriptor
	SourceStats     BackupTableStats
	ManifestVersion int
	ManifestStatus  string
}

// RestoreTableFromBackup reads the descriptor for `sourceTable` out of
// `backupName`'s pebble checkpoint, copies it (renamed to
// `targetTable`) into the live catalog via `register`, and streams the
// source table's keyspace into the new table — re-keyed under the
// target name so the live engine's index + TTL machinery owns it
// going forward.
//
// `register` is the catalog-level hook that persists the new
// descriptor. Callers pass catalog.Catalog.Create (or a shard-fanout
// wrapper) so the writer doesn't depend on the catalog package.
func (d *DB) RestoreTableFromBackup(
	backupName, sourceTable, targetTable string,
	register func(types.TableDescriptor) error,
) (RestoreResult, error) {
	return d.RestoreTableFromBackupWithOptions(backupName, sourceTable, targetTable, RestoreOptions{}, register)
}

func (d *DB) RestoreTableFromBackupWithOptions(
	backupName, sourceTable, targetTable string,
	opts RestoreOptions,
	register func(types.TableDescriptor) error,
) (RestoreResult, error) {
	preflight, checkpoint, err := d.restoreTablePreflight(backupName, sourceTable, targetTable)
	if err != nil {
		return RestoreResult{}, err
	}
	defer checkpoint.Close()

	if opts.DryRun {
		return RestoreResult{
			TargetTable:     preflight.TargetTable,
			RowsCopied:      int(preflight.SourceStats.Rows),
			SourceStats:     preflight.SourceStats,
			DryRun:          true,
			ManifestVersion: preflight.ManifestVersion,
			ManifestStatus:  preflight.ManifestStatus,
		}, nil
	}

	if err := register(preflight.TargetTable); err != nil {
		return RestoreResult{}, fmt.Errorf("register target descriptor: %w", err)
	}

	lower, upper := PrefixPrimaryAll(sourceTable)
	iter, err := checkpoint.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return RestoreResult{}, fmt.Errorf("checkpoint iter: %w", err)
	}
	defer iter.Close()

	n := 0
	for valid := iter.First(); valid; valid = iter.Next() {
		v := iter.Value()
		cp := make([]byte, len(v))
		copy(cp, v)
		item, err := DecodeItem(cp)
		if err != nil {
			return RestoreResult{}, fmt.Errorf("decode item at %s: %w", iter.Key(), err)
		}
		if err := d.PutItemWith(preflight.TargetTable, item, PutOptions{}); err != nil {
			return RestoreResult{}, fmt.Errorf("write item %d into target: %w", n, err)
		}
		n++
	}
	if err := iter.Error(); err != nil {
		return RestoreResult{}, err
	}
	return RestoreResult{
		TargetTable:     preflight.TargetTable,
		RowsCopied:      n,
		SourceStats:     preflight.SourceStats,
		ManifestVersion: preflight.ManifestVersion,
		ManifestStatus:  preflight.ManifestStatus,
	}, nil
}

func (d *DB) PreflightRestoreTableFromBackup(backupName, sourceTable, targetTable string) (RestorePreflightResult, error) {
	preflight, checkpoint, err := d.restoreTablePreflight(backupName, sourceTable, targetTable)
	if err != nil {
		return RestorePreflightResult{}, err
	}
	_ = checkpoint.Close()
	return preflight, nil
}

func (d *DB) restoreTablePreflight(
	backupName, sourceTable, targetTable string,
) (RestorePreflightResult, *pebbledb.DB, error) {
	if sourceTable == "" || targetTable == "" {
		return RestorePreflightResult{}, nil, fmt.Errorf("source and target table names required")
	}
	if backupName == "" {
		return RestorePreflightResult{}, nil, fmt.Errorf("backup name required")
	}
	if exists, err := d.Has(KeyCatalog(targetTable)); err != nil {
		return RestorePreflightResult{}, nil, err
	} else if exists {
		return RestorePreflightResult{}, nil, fmt.Errorf("%w: target table %q", types.ErrTableAlreadyExists, targetTable)
	}
	meta, err := d.GetBackup(backupName)
	if err != nil {
		return RestorePreflightResult{}, nil, err
	}
	if meta == nil {
		return RestorePreflightResult{}, nil, fmt.Errorf("cefas/storage: backup %q not found", backupName)
	}

	checkpoint, err := pebbledb.Open(meta.CheckpointAt, &pebbledb.Options{ReadOnly: true})
	if err != nil {
		return RestorePreflightResult{}, nil, fmt.Errorf("open checkpoint: %w", err)
	}

	srcDescBytes, closer, err := checkpoint.Get(KeyCatalog(sourceTable))
	if err != nil {
		_ = checkpoint.Close()
		if errors.Is(err, ErrNotFound) {
			return RestorePreflightResult{}, nil, fmt.Errorf("%w: source table %q", types.ErrTableNotFound, sourceTable)
		}
		return RestorePreflightResult{}, nil, fmt.Errorf("read source descriptor: %w", err)
	}
	srcDescCopy := append([]byte(nil), srcDescBytes...)
	_ = closer.Close()
	var srcTD types.TableDescriptor
	if err := json.Unmarshal(srcDescCopy, &srcTD); err != nil {
		_ = checkpoint.Close()
		return RestorePreflightResult{}, nil, fmt.Errorf("decode source descriptor: %w", err)
	}

	sourceStats, err := validateBackupManifestTable(*meta, checkpoint, sourceTable)
	if err != nil {
		_ = checkpoint.Close()
		return RestorePreflightResult{}, nil, fmt.Errorf("validate backup manifest: %w", err)
	}

	tgtTD := srcTD
	tgtTD.Name = targetTable
	return RestorePreflightResult{
		BackupName:      backupName,
		SourceTableName: sourceTable,
		TargetTableName: targetTable,
		TargetTable:     tgtTD,
		SourceStats:     sourceStats,
		ManifestVersion: meta.ManifestVersion,
		ManifestStatus:  meta.ManifestStatus,
	}, checkpoint, nil
}
