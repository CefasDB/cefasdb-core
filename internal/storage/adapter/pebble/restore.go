package pebble

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/CefasDb/cefasdb/pkg/types"
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
	DryRun            bool
	TargetChangeIndex uint64
	TargetUnixNano    int64
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

// ErrRestoreTargetPartial marks a restore that failed after its target
// table was registered. The target stays registered and holds only part of
// the source rows; it is not dropped (a failed write or decode mid-copy has
// always left it in place, and the descriptor may be mirrored on every
// shard). Delete the target table before retrying the restore.
var ErrRestoreTargetPartial = errors.New("cefas/storage: restore target is registered but partially filled")

// RestorePartialError is returned for every failure after the target table
// was registered. It matches ErrRestoreTargetPartial with errors.Is and
// unwraps to the failure that stopped the copy (a write error, a cancelled
// request context, an expired watermark wait).
type RestorePartialError struct {
	Target     string
	RowsCopied int
	Cause      error
}

func (e *RestorePartialError) Error() string {
	return fmt.Sprintf("%s: target table %q holds %d copied rows and must be deleted before retrying: %v",
		ErrRestoreTargetPartial.Error(), e.Target, e.RowsCopied, e.Cause)
}

func (e *RestorePartialError) Is(target error) bool { return target == ErrRestoreTargetPartial }
func (e *RestorePartialError) Unwrap() error        { return e.Cause }

func (d *DB) RestoreTableFromBackupWithOptions(
	backupName, sourceTable, targetTable string,
	opts RestoreOptions,
	register func(types.TableDescriptor) error,
) (RestoreResult, error) {
	return d.RestoreTableFromBackupWithOptionsCtx(context.Background(), backupName, sourceTable, targetTable, opts, register)
}

// RestoreTableFromBackupWithOptionsCtx is RestoreTableFromBackupWithOptions
// bound to ctx (the request context): cancellation or shutdown stops the
// copy and the point-in-time watermark wait, which is still capped at
// restoreWatermarkWait. A context that ends before the target is registered
// creates nothing; any failure after registration is a *RestorePartialError.
func (d *DB) RestoreTableFromBackupWithOptionsCtx(
	ctx context.Context,
	backupName, sourceTable, targetTable string,
	opts RestoreOptions,
	register func(types.TableDescriptor) error,
) (RestoreResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if backupName == "" {
		return RestoreResult{}, fmt.Errorf("backup name required")
	}
	releaseBackup := d.beginBackupRestore(backupName)
	defer releaseBackup()

	preflight, checkpoint, err := d.restoreTablePreflight(backupName, sourceTable, targetTable)
	if err != nil {
		return RestoreResult{}, err
	}
	defer checkpoint.Close()

	if opts.DryRun {
		if err := d.validatePITRTarget(preflight, opts); err != nil {
			return RestoreResult{}, err
		}
		return RestoreResult{
			TargetTable:     preflight.TargetTable,
			RowsCopied:      int(preflight.SourceStats.Rows),
			SourceStats:     preflight.SourceStats,
			DryRun:          true,
			ManifestVersion: preflight.ManifestVersion,
			ManifestStatus:  preflight.ManifestStatus,
		}, nil
	}

	if err := ctx.Err(); err != nil {
		return RestoreResult{}, fmt.Errorf("restore not started: %w", err)
	}
	if err := register(preflight.TargetTable); err != nil {
		return RestoreResult{}, fmt.Errorf("register target descriptor: %w", err)
	}

	n := 0
	partial := func(err error) (RestoreResult, error) {
		return RestoreResult{}, &RestorePartialError{Target: targetTable, RowsCopied: n, Cause: err}
	}
	lower, upper := storage.PrefixPrimaryAll(sourceTable)
	iter, err := checkpoint.NewIter(&pebbledb.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return partial(fmt.Errorf("checkpoint iter: %w", err))
	}
	defer iter.Close()

	for valid := iter.First(); valid; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return partial(fmt.Errorf("copy interrupted after %d items: %w", n, err))
		}
		v := iter.Value()
		cp := make([]byte, len(v))
		copy(cp, v)
		item, err := storage.DecodeItem(cp)
		if err != nil {
			return partial(fmt.Errorf("decode item at %s: %w", iter.Key(), err))
		}
		if err := d.PutItemWith(preflight.TargetTable, item, PutOptions{}); err != nil {
			return partial(fmt.Errorf("write item %d into target: %w", n, err))
		}
		n++
	}
	if err := iter.Error(); err != nil {
		return partial(err)
	}
	if opts.TargetChangeIndex != 0 || opts.TargetUnixNano != 0 {
		replayed, err := d.replayChangesToTarget(ctx, preflight, opts)
		n += replayed
		if err != nil {
			return partial(err)
		}
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
	if backupName == "" {
		return RestorePreflightResult{}, fmt.Errorf("backup name required")
	}
	releaseBackup := d.beginBackupRestore(backupName)
	defer releaseBackup()

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
	if exists, err := d.Has(storage.KeyCatalog(targetTable)); err != nil {
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

	srcDescBytes, closer, err := checkpoint.Get(storage.KeyCatalog(sourceTable))
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

func (d *DB) validatePITRTarget(preflight RestorePreflightResult, opts RestoreOptions) error {
	if opts.TargetChangeIndex == 0 && opts.TargetUnixNano == 0 {
		return nil
	}
	meta, err := d.GetBackup(preflight.BackupName)
	if err != nil {
		return err
	}
	if meta == nil {
		return fmt.Errorf("cefas/storage: backup %q not found", preflight.BackupName)
	}
	current, err := d.CurrentChangeIndex()
	if err != nil {
		return err
	}
	if opts.TargetChangeIndex != 0 {
		if opts.TargetChangeIndex < meta.ChangeIndex {
			return fmt.Errorf("target change index %d is before backup high-water index %d", opts.TargetChangeIndex, meta.ChangeIndex)
		}
		if opts.TargetChangeIndex > current {
			return fmt.Errorf("target change index %d is outside retained history; current high-water index is %d", opts.TargetChangeIndex, current)
		}
	}
	if opts.TargetUnixNano != 0 {
		if meta.ChangeUnixNano != 0 && opts.TargetUnixNano < meta.ChangeUnixNano {
			return fmt.Errorf("target timestamp %d is before backup timestamp %d", opts.TargetUnixNano, meta.ChangeUnixNano)
		}
		if opts.TargetUnixNano > time.Now().UnixNano() {
			return fmt.Errorf("target timestamp %d is outside retained history", opts.TargetUnixNano)
		}
	}
	return nil
}

// restoreWatermarkWait bounds how long point-in-time replay waits for
// in-flight native-stream writes below its target to commit or abandon.
const restoreWatermarkWait = 30 * time.Second

func (d *DB) replayChangesToTarget(ctx context.Context, preflight RestorePreflightResult, opts RestoreOptions) (int, error) {
	if err := d.validatePITRTarget(preflight, opts); err != nil {
		return 0, err
	}
	meta, err := d.GetBackup(preflight.BackupName)
	if err != nil {
		return 0, err
	}
	if meta == nil {
		return 0, fmt.Errorf("cefas/storage: backup %q not found", preflight.BackupName)
	}
	toIndex := opts.TargetChangeIndex
	if toIndex == 0 {
		toIndex, err = d.CurrentChangeIndex()
		if err != nil {
			return 0, err
		}
	}
	// Every record through toIndex must be replayed: wait for in-flight
	// native writes below it to settle instead of stopping at the
	// native-stream watermark. The wait ends at ctx (request cancellation,
	// server shutdown) or restoreWatermarkWait, whichever comes first.
	records, err := d.changeRecordsThroughCtx(ctx, preflight.SourceTableName, meta.ChangeIndex, toIndex, opts.TargetUnixNano, restoreWatermarkWait)
	if err != nil {
		return 0, fmt.Errorf("restore replay through change index %d: %w", toIndex, err)
	}
	applied := 0
	for _, rec := range records {
		if err := ctx.Err(); err != nil {
			return applied, fmt.Errorf("replay interrupted before index %d: %w", rec.Index, err)
		}
		switch rec.Op {
		case ChangePut:
			item := cloneStorageItem(rec.Item)
			if err := d.PutItemWith(preflight.TargetTable, item, PutOptions{}); err != nil {
				return applied, fmt.Errorf("replay put index %d: %w", rec.Index, err)
			}
			applied++
		case ChangeDelete:
			key := cloneStorageItem(rec.Key)
			if err := d.DeleteItemWith(preflight.TargetTable, key, DeleteOptions{}); err != nil {
				if errors.Is(err, types.ErrItemNotFound) {
					continue
				}
				return applied, fmt.Errorf("replay delete index %d: %w", rec.Index, err)
			}
			applied++
		default:
			return applied, fmt.Errorf("unknown change op %q at index %d", rec.Op, rec.Index)
		}
	}
	return applied, nil
}

func cloneStorageItem(in types.Item) types.Item {
	if in == nil {
		return nil
	}
	out := make(types.Item, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
