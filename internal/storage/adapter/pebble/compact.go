package pebble

import (
	"fmt"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"
)

type CompactionResult struct {
	Table           string
	Lower           []byte
	Upper           []byte
	StartedAt       time.Time
	FinishedAt      time.Time
	Elapsed         time.Duration
	Parallelized    bool
	BeforeL0Files   int64
	AfterL0Files    int64
	BeforeDebtBytes uint64
	AfterDebtBytes  uint64
}

func (d *DB) CompactTable(table string, parallelize bool) (CompactionResult, error) {
	if table == "" {
		return CompactionResult{}, fmt.Errorf("table is required")
	}
	lower, upper := storage.PrefixTable(table)
	res, err := d.CompactRange(lower, upper, parallelize)
	res.Table = table
	return res, err
}

func (d *DB) CompactRange(lower, upper []byte, parallelize bool) (CompactionResult, error) {
	if d == nil || d.db == nil {
		return CompactionResult{}, fmt.Errorf("db closed")
	}
	if len(lower) == 0 || len(upper) == 0 {
		return CompactionResult{}, fmt.Errorf("lower and upper are required")
	}
	before := d.Metrics()
	res := CompactionResult{
		Lower:           append([]byte(nil), lower...),
		Upper:           append([]byte(nil), upper...),
		StartedAt:       time.Now(),
		Parallelized:    parallelize,
		BeforeL0Files:   before.Levels[0].NumFiles,
		BeforeDebtBytes: before.Compact.EstimatedDebt,
	}
	if err := d.db.Flush(); err != nil {
		return res, fmt.Errorf("flush before compact: %w", err)
	}
	if err := d.db.Compact(lower, upper, parallelize); err != nil {
		return res, fmt.Errorf("compact: %w", err)
	}
	after := d.Metrics()
	res.FinishedAt = time.Now()
	res.Elapsed = res.FinishedAt.Sub(res.StartedAt)
	res.AfterL0Files = after.Levels[0].NumFiles
	res.AfterDebtBytes = after.Compact.EstimatedDebt
	return res, nil
}
