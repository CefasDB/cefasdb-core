package pebble

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CefasDb/cefasdb/internal/storage"
)

const DefaultScheduledBackupNameTemplate = "scheduled-{{timestamp}}"

type ScheduledBackupMetrics interface {
	ObserveScheduledBackup(backupName, outcome, reason string, duration time.Duration, rows, bytes int64)
}

type ScheduledBackupConfig struct {
	Enabled      bool
	DryRun       bool
	Interval     time.Duration
	NameTemplate string
	Tables       []string
	Retention    BackupRetentionOptions
	Now          func() time.Time
	Logger       func(string, ...any)
	Metrics      ScheduledBackupMetrics
}

type ScheduledBackupStatus struct {
	Enabled         bool     `json:"enabled"`
	DryRun          bool     `json:"dryRun"`
	IntervalSeconds int64    `json:"intervalSeconds"`
	NameTemplate    string   `json:"nameTemplate"`
	Tables          []string `json:"tables,omitempty"`

	RetentionKeepLatest    int   `json:"retentionKeepLatest,omitempty"`
	RetentionKeepLatestSet bool  `json:"retentionKeepLatestSet,omitempty"`
	RetentionMaxAgeSeconds int64 `json:"retentionMaxAgeSeconds,omitempty"`
	RetentionMaxAgeSet     bool  `json:"retentionMaxAgeSet,omitempty"`
	RetentionDryRun        bool  `json:"retentionDryRun,omitempty"`

	Running             bool    `json:"running"`
	NextRunUnix         int64   `json:"nextRunUnix,omitempty"`
	LastStartedUnix     int64   `json:"lastStartedUnix,omitempty"`
	LastFinishedUnix    int64   `json:"lastFinishedUnix,omitempty"`
	LastDurationSeconds float64 `json:"lastDurationSeconds,omitempty"`
	LastStatus          string  `json:"lastStatus,omitempty"`
	LastBackupName      string  `json:"lastBackupName,omitempty"`
	LastError           string  `json:"lastError,omitempty"`
	LastRows            int64   `json:"lastRows,omitempty"`
	LastBytes           int64   `json:"lastBytes,omitempty"`
	LastSuccessUnix     int64   `json:"lastSuccessUnix,omitempty"`
	LastFailureUnix     int64   `json:"lastFailureUnix,omitempty"`

	LastRetention *BackupRetentionResult `json:"lastRetention,omitempty"`
}

type ScheduledBackupRunner struct {
	db  *DB
	cfg ScheduledBackupConfig

	mu     sync.Mutex
	status ScheduledBackupStatus
}

func NewScheduledBackupRunner(db *DB, cfg ScheduledBackupConfig) *ScheduledBackupRunner {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Hour
	}
	if cfg.NameTemplate == "" {
		cfg.NameTemplate = DefaultScheduledBackupNameTemplate
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	r := &ScheduledBackupRunner{db: db, cfg: cfg}
	r.status = r.baseStatus()
	if cfg.Enabled {
		r.status.NextRunUnix = cfg.Now().Add(cfg.Interval).Unix()
	}
	return r
}

func (r *ScheduledBackupRunner) Status() ScheduledBackupStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return cloneScheduledBackupStatus(r.status)
}

func (r *ScheduledBackupRunner) Run(ctx context.Context) {
	if r == nil || !r.cfg.Enabled {
		return
	}
	timer := time.NewTimer(r.cfg.Interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			_, _ = r.RunOnce(ctx)
			r.setNextRun(r.cfg.Now().Add(r.cfg.Interval))
			timer.Reset(r.cfg.Interval)
		}
	}
}

func (r *ScheduledBackupRunner) RunOnce(ctx context.Context) (ScheduledBackupStatus, error) {
	if r == nil {
		return ScheduledBackupStatus{}, fmt.Errorf("scheduled backup runner not configured")
	}
	if !r.cfg.Enabled {
		return r.Status(), nil
	}
	now := r.cfg.Now()
	name := renderBackupNameTemplate(r.cfg.NameTemplate, now)
	started := now

	r.mu.Lock()
	if r.status.Running {
		r.mu.Unlock()
		return r.Status(), fmt.Errorf("scheduled backup already running")
	}
	previous := cloneScheduledBackupStatus(r.status)
	r.status.Running = true
	r.status.LastStartedUnix = started.Unix()
	r.status.LastFinishedUnix = 0
	r.status.LastDurationSeconds = 0
	r.status.LastStatus = "running"
	r.status.LastBackupName = name
	r.status.LastError = ""
	r.status.LastRows = 0
	r.status.LastBytes = 0
	r.status.LastRetention = nil
	r.mu.Unlock()

	status, err := r.runOnce(ctx, name, started, previous)
	r.mu.Lock()
	r.status = status
	r.mu.Unlock()
	return status, err
}

func (r *ScheduledBackupRunner) runOnce(ctx context.Context, name string, started time.Time, previous ScheduledBackupStatus) (ScheduledBackupStatus, error) {
	status := r.baseStatus()
	status.LastStartedUnix = started.Unix()
	status.LastBackupName = name
	status.Running = false
	status.LastSuccessUnix = previous.LastSuccessUnix
	status.LastFailureUnix = previous.LastFailureUnix
	status.NextRunUnix = started.Add(r.cfg.Interval).Unix()

	if err := ctx.Err(); err != nil {
		r.finishRun(&status, started, "failed", err.Error(), 0, 0, nil)
		return status, err
	}
	if err := validateBackupName(name); err != nil {
		r.finishRun(&status, started, "failed", err.Error(), 0, 0, nil)
		return status, err
	}
	if r.cfg.DryRun {
		if err := r.validateDryRunTables(); err != nil {
			r.finishRun(&status, started, "failed", err.Error(), 0, 0, nil)
			return status, err
		}
		retention, err := r.applyScheduledRetention(true)
		if err != nil {
			r.finishRun(&status, started, "failed", err.Error(), 0, 0, nil)
			return status, err
		}
		r.finishRun(&status, started, "dry_run", "", 0, 0, retention)
		return status, nil
	}

	meta, err := r.db.CreateBackup(name, r.cfg.Tables)
	if err != nil {
		r.finishRun(&status, started, "failed", err.Error(), 0, 0, nil)
		return status, err
	}
	rows := backupRows(meta)
	bytes := checkpointDirSize(meta.CheckpointAt)
	retention, err := r.applyScheduledRetention(false)
	if err != nil {
		r.finishRun(&status, started, "failed", err.Error(), rows, bytes, nil)
		return status, err
	}
	r.finishRun(&status, started, "success", "", rows, bytes, retention)
	return status, nil
}

func (r *ScheduledBackupRunner) finishRun(status *ScheduledBackupStatus, started time.Time, outcome, reason string, rows, bytes int64, retention *BackupRetentionResult) {
	finished := r.cfg.Now()
	duration := finished.Sub(started)
	if duration < 0 {
		duration = 0
	}
	status.Running = false
	status.LastFinishedUnix = finished.Unix()
	status.LastDurationSeconds = duration.Seconds()
	status.LastStatus = outcome
	status.LastError = reason
	status.LastRows = rows
	status.LastBytes = bytes
	status.LastRetention = retention
	if outcome == "failed" {
		status.LastFailureUnix = finished.Unix()
	} else {
		status.LastSuccessUnix = finished.Unix()
	}
	if r.cfg.Metrics != nil {
		r.cfg.Metrics.ObserveScheduledBackup(status.LastBackupName, outcome, reason, duration, rows, bytes)
	}
	if r.cfg.Logger != nil {
		if outcome == "failed" {
			r.cfg.Logger("scheduled backup failed: backup=%s reason=%s", status.LastBackupName, reason)
		} else {
			r.cfg.Logger("scheduled backup %s: backup=%s rows=%d bytes=%d", outcome, status.LastBackupName, rows, bytes)
		}
	}
}

func (r *ScheduledBackupRunner) baseStatus() ScheduledBackupStatus {
	return ScheduledBackupStatus{
		Enabled:                r.cfg.Enabled,
		DryRun:                 r.cfg.DryRun,
		IntervalSeconds:        int64(r.cfg.Interval / time.Second),
		NameTemplate:           r.cfg.NameTemplate,
		Tables:                 append([]string(nil), r.cfg.Tables...),
		RetentionKeepLatest:    r.cfg.Retention.KeepLatest,
		RetentionKeepLatestSet: r.cfg.Retention.KeepLatestSet,
		RetentionMaxAgeSeconds: int64(r.cfg.Retention.MaxAge / time.Second),
		RetentionMaxAgeSet:     r.cfg.Retention.MaxAgeSet,
		RetentionDryRun:        r.cfg.Retention.DryRun,
	}
}

func (r *ScheduledBackupRunner) setNextRun(next time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status.NextRunUnix = next.Unix()
}

func (r *ScheduledBackupRunner) validateDryRunTables() error {
	for _, table := range sortedUniqueStrings(r.cfg.Tables) {
		ok, err := r.db.Has(storage.KeyCatalog(table))
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("scheduled backup dry-run: table %q descriptor missing", table)
		}
	}
	return nil
}

func (r *ScheduledBackupRunner) applyScheduledRetention(forceDryRun bool) (*BackupRetentionResult, error) {
	if !r.cfg.Retention.KeepLatestSet && !r.cfg.Retention.MaxAgeSet {
		return nil, nil
	}
	opts := r.cfg.Retention
	if forceDryRun {
		opts.DryRun = true
	}
	result, err := r.db.ApplyBackupRetention(opts)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func renderBackupNameTemplate(template string, t time.Time) string {
	if template == "" {
		template = DefaultScheduledBackupNameTemplate
	}
	utc := t.UTC()
	repl := strings.NewReplacer(
		"{{timestamp}}", utc.Format("20060102T150405000000000Z"),
		"{{unix}}", strconv.FormatInt(utc.Unix(), 10),
		"{{unix_nano}}", strconv.FormatInt(utc.UnixNano(), 10),
		"{{date}}", utc.Format("20060102"),
		"{{time}}", utc.Format("150405"),
	)
	return repl.Replace(template)
}

func backupRows(meta BackupMetadata) int64 {
	var rows int64
	for _, stat := range meta.TableStats {
		rows += stat.Rows
	}
	return rows
}

func checkpointDirSize(path string) int64 {
	var total int64
	_ = filepath.WalkDir(path, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

func cloneScheduledBackupStatus(in ScheduledBackupStatus) ScheduledBackupStatus {
	out := in
	out.Tables = append([]string(nil), in.Tables...)
	if in.LastRetention != nil {
		cp := *in.LastRetention
		cp.WouldDelete = append([]BackupRetentionCandidate(nil), in.LastRetention.WouldDelete...)
		cp.Deleted = append([]BackupDeletionResult(nil), in.LastRetention.Deleted...)
		out.LastRetention = &cp
	}
	return out
}
