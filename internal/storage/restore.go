package storage

import (
	"encoding/json"
	"fmt"

	pebbledb "github.com/cockroachdb/pebble"

	"github.com/osvaldoandrade/cefas/pkg/types"
)

// RestoreResult describes the outcome of RestoreTableFromBackup.
type RestoreResult struct {
	TargetTable types.TableDescriptor
	RowsCopied  int
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
	if sourceTable == "" || targetTable == "" {
		return RestoreResult{}, fmt.Errorf("source and target table names required")
	}
	meta, err := d.GetBackup(backupName)
	if err != nil {
		return RestoreResult{}, err
	}
	if meta == nil {
		return RestoreResult{}, fmt.Errorf("cefas/storage: backup %q not found", backupName)
	}

	checkpoint, err := pebbledb.Open(meta.CheckpointAt, &pebbledb.Options{ReadOnly: true})
	if err != nil {
		return RestoreResult{}, fmt.Errorf("open checkpoint: %w", err)
	}
	defer checkpoint.Close()

	srcDescBytes, closer, err := checkpoint.Get(KeyCatalog(sourceTable))
	if err != nil {
		return RestoreResult{}, fmt.Errorf("read source descriptor: %w", err)
	}
	srcDescCopy := append([]byte(nil), srcDescBytes...)
	_ = closer.Close()
	var srcTD types.TableDescriptor
	if err := json.Unmarshal(srcDescCopy, &srcTD); err != nil {
		return RestoreResult{}, fmt.Errorf("decode source descriptor: %w", err)
	}

	tgtTD := srcTD
	tgtTD.Name = targetTable
	if err := register(tgtTD); err != nil {
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
		if err := d.PutItemWith(tgtTD, item, PutOptions{}); err != nil {
			return RestoreResult{}, fmt.Errorf("write item %d into target: %w", n, err)
		}
		n++
	}
	if err := iter.Error(); err != nil {
		return RestoreResult{}, err
	}
	return RestoreResult{TargetTable: tgtTD, RowsCopied: n}, nil
}
