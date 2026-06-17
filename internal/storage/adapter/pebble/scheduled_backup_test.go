package pebble_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
)

func TestScheduledBackupRunnerDisabledNoops(t *testing.T) {
	db := openDB(t)
	runner := pebble.NewScheduledBackupRunner(db, pebble.ScheduledBackupConfig{})

	status, err := runner.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("run disabled: %v", err)
	}
	if status.Enabled || status.LastStatus != "" {
		t.Fatalf("status = %+v", status)
	}
	if backups, err := db.ListBackups(); err != nil || len(backups) != 0 {
		t.Fatalf("backups = %+v err=%v", backups, err)
	}
}

func TestScheduledBackupDryRunDoesNotCreateBackup(t *testing.T) {
	db := openDB(t)
	seedBackupTable(t, db, "Users", "u1")
	now := time.Unix(100, 0).UTC()
	runner := pebble.NewScheduledBackupRunner(db, pebble.ScheduledBackupConfig{
		Enabled:      true,
		DryRun:       true,
		Interval:     time.Minute,
		NameTemplate: "sched-{{unix}}",
		Tables:       []string{"Users"},
		Now:          func() time.Time { return now },
	})

	status, err := runner.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("dry-run: %v", err)
	}
	if status.LastStatus != "dry_run" || status.LastBackupName != "sched-100" || status.LastSuccessUnix != 100 {
		t.Fatalf("status = %+v", status)
	}
	got, err := db.GetBackup("sched-100")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != nil {
		t.Fatalf("dry-run created backup: %+v", got)
	}
}

func TestScheduledBackupCreatesBackupAndReportsStatus(t *testing.T) {
	db := openDB(t)
	seedBackupTable(t, db, "Users", "u1", "u2")
	now := time.Unix(200, 0).UTC()
	metrics := &scheduledBackupMetricsRecorder{}
	var logs []string
	runner := pebble.NewScheduledBackupRunner(db, pebble.ScheduledBackupConfig{
		Enabled:      true,
		Interval:     time.Minute,
		NameTemplate: "sched-{{unix}}",
		Tables:       []string{"Users"},
		Retention: pebble.BackupRetentionOptions{
			KeepLatestSet: true,
			KeepLatest:    1,
			DryRun:        true,
		},
		Now:     func() time.Time { return now },
		Metrics: metrics,
		Logger: func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		},
	})

	status, err := runner.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if status.LastStatus != "success" || status.LastBackupName != "sched-200" || status.LastRows != 2 || status.LastBytes == 0 || status.LastRetention == nil {
		t.Fatalf("status = %+v", status)
	}
	if metrics.name != "sched-200" || metrics.outcome != "success" || metrics.rows != 2 || metrics.bytes == 0 {
		t.Fatalf("metrics = %+v", metrics)
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "backup=sched-200") {
		t.Fatalf("logs = %v", logs)
	}
	meta, err := db.GetBackup("sched-200")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if meta == nil || len(meta.TableStats) != 1 || meta.TableStats[0].Rows != 2 {
		t.Fatalf("backup metadata = %+v", meta)
	}
}

func TestScheduledBackupFailureReportsNameAndReason(t *testing.T) {
	db := openDB(t)
	now := time.Unix(300, 0).UTC()
	metrics := &scheduledBackupMetricsRecorder{}
	var logs []string
	runner := pebble.NewScheduledBackupRunner(db, pebble.ScheduledBackupConfig{
		Enabled:      true,
		DryRun:       true,
		Interval:     time.Minute,
		NameTemplate: "sched-{{unix}}",
		Tables:       []string{"Missing"},
		Now:          func() time.Time { return now },
		Metrics:      metrics,
		Logger: func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		},
	})

	status, err := runner.RunOnce(context.Background())
	if err == nil {
		t.Fatal("expected dry-run validation failure")
	}
	if status.LastStatus != "failed" || status.LastBackupName != "sched-300" || !strings.Contains(status.LastError, "Missing") {
		t.Fatalf("status = %+v err=%v", status, err)
	}
	if metrics.name != "sched-300" || metrics.outcome != "failed" || !strings.Contains(metrics.reason, "Missing") {
		t.Fatalf("metrics = %+v", metrics)
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "backup=sched-300") || !strings.Contains(logs[0], "Missing") {
		t.Fatalf("logs = %v", logs)
	}
}

type scheduledBackupMetricsRecorder struct {
	name     string
	outcome  string
	reason   string
	duration time.Duration
	rows     int64
	bytes    int64
}

func (r *scheduledBackupMetricsRecorder) ObserveScheduledBackup(name, outcome, reason string, duration time.Duration, rows, bytes int64) {
	r.name = name
	r.outcome = outcome
	r.reason = reason
	r.duration = duration
	r.rows = rows
	r.bytes = bytes
}
