package pebble_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

func openDB(t *testing.T) *pebble.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(pebble.Options{
		Path: dir,
		// Backup tests assert ChangeIndex / ChangeUnixNano captured on
		// non-stream tables. Default mode is streams-only, so opt into
		// always to keep that contract here.
		ChangeLogMode: pebble.ChangeLogModeAlways,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestCreateBackupWritesCheckpointAndMetadata(t *testing.T) {
	db := openDB(t)
	if err := db.Set([]byte("cefas/data/x"), []byte("v")); err != nil {
		t.Fatalf("set: %v", err)
	}
	seedBackupTable(t, db, "T1", "a", "b")
	seedBackupTable(t, db, "T2", "c")
	meta, err := db.CreateBackup("first", []string{"T1", "T2"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if meta.Name != "first" || len(meta.Tables) != 2 {
		t.Fatalf("meta = %+v", meta)
	}
	if meta.ManifestVersion != pebble.BackupManifestVersion || meta.ManifestStatus != "ok" {
		t.Fatalf("manifest = version %d status %q", meta.ManifestVersion, meta.ManifestStatus)
	}
	if len(meta.RequestedTables) != 2 || meta.RequestedTables[0] != "T1" || meta.RequestedTables[1] != "T2" {
		t.Fatalf("requested tables = %v", meta.RequestedTables)
	}
	if len(meta.TableStats) != 2 || meta.TableStats[0].Table != "T1" || meta.TableStats[0].Rows != 2 || meta.TableStats[0].Checksum == "" {
		t.Fatalf("table stats = %+v", meta.TableStats)
	}
	if len(meta.ShardCoverage) != 1 || meta.ShardCoverage[0].ShardID != "0" || len(meta.ShardCoverage[0].TableStats) != 2 {
		t.Fatalf("shard coverage = %+v", meta.ShardCoverage)
	}
	if meta.ChangeIndex == 0 || meta.ChangeUnixNano == 0 {
		t.Fatalf("change high-water not recorded: %+v", meta)
	}
	if _, err := os.Stat(meta.CheckpointAt); err != nil {
		t.Fatalf("checkpoint dir missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(meta.CheckpointAt, "OPTIONS-000003")); err != nil {
		// pebble emits OPTIONS-*; presence proves it materialised something.
		// (skip the exact filename check if pebble varies it; just make
		// sure the directory has files)
		entries, _ := os.ReadDir(meta.CheckpointAt)
		if len(entries) == 0 {
			t.Fatalf("checkpoint dir is empty")
		}
	}
}

func TestRestoreTablePointInTimeReplaysChanges(t *testing.T) {
	db := openDB(t)
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	td := types.TableDescriptor{Name: "Users", KeySchema: types.KeySchema{PK: "id"}}
	if err := cat.Create(td); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := db.PutItemWith(td, types.Item{"id": sAttr("u1"), "v": sAttr("old")}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put old: %v", err)
	}
	_, err = db.CreateBackup("snap", []string{"Users"})
	if err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := db.PutItemWith(td, types.Item{"id": sAttr("u1"), "v": sAttr("new")}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put new: %v", err)
	}
	if err := db.PutItemWith(td, types.Item{"id": sAttr("u2"), "v": sAttr("second")}, pebble.PutOptions{}); err != nil {
		t.Fatalf("put second: %v", err)
	}
	targetIndex, err := db.CurrentChangeIndex()
	if err != nil {
		t.Fatalf("change index: %v", err)
	}
	res, err := db.RestoreTableFromBackupWithOptions("snap", "Users", "Users_pitr", pebble.RestoreOptions{
		TargetChangeIndex: targetIndex,
	}, cat.Create)
	if err != nil {
		t.Fatalf("restore pitr: %v", err)
	}
	if res.RowsCopied < 2 {
		t.Fatalf("rows copied = %d, want base + replay", res.RowsCopied)
	}
	got, err := db.GetItem("Users_pitr", td.KeySchema, types.Item{"id": sAttr("u1")})
	if err != nil {
		t.Fatalf("get restored u1: %v", err)
	}
	if got["v"].S != "new" {
		t.Fatalf("restored u1 = %+v, want new", got)
	}
	got, err = db.GetItem("Users_pitr", td.KeySchema, types.Item{"id": sAttr("u2")})
	if err != nil {
		t.Fatalf("get restored u2: %v", err)
	}
	if got["v"].S != "second" {
		t.Fatalf("restored u2 = %+v, want second", got)
	}
	_, err = db.RestoreTableFromBackupWithOptions("snap", "Users", "before_backup", pebble.RestoreOptions{
		DryRun:            true,
		TargetChangeIndex: ^uint64(0),
	}, cat.Create)
	if err == nil || !strings.Contains(err.Error(), "outside retained history") {
		t.Fatalf("expected outside-history target rejection, got %v", err)
	}
}

func TestCreateBackupWithNoTablesCapturesCatalogTables(t *testing.T) {
	db := openDB(t)
	seedBackupTable(t, db, "T2", "b")
	seedBackupTable(t, db, "T1", "a")

	meta, err := db.CreateBackup("all", nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(meta.RequestedTables) != 0 {
		t.Fatalf("requested tables = %v, want empty all-tables marker", meta.RequestedTables)
	}
	if len(meta.Tables) != 2 || meta.Tables[0] != "T1" || meta.Tables[1] != "T2" {
		t.Fatalf("captured tables = %v", meta.Tables)
	}
}

func TestCreateBackupRejectsMissingRequestedTable(t *testing.T) {
	db := openDB(t)
	if _, err := db.CreateBackup("missing", []string{"Missing"}); err == nil || !strings.Contains(err.Error(), "descriptor missing") {
		t.Fatalf("expected missing descriptor error, got %v", err)
	}
}

func TestCreateBackupRejectsDuplicateName(t *testing.T) {
	db := openDB(t)
	if _, err := db.CreateBackup("dup", nil); err != nil {
		t.Fatalf("first: %v", err)
	}
	_, err := db.CreateBackup("dup", nil)
	if err == nil {
		t.Fatal("expected duplicate-name error")
	}
}

func TestCreateBackupRejectsBadName(t *testing.T) {
	db := openDB(t)
	for _, bad := range []string{"", "a/b", "..", "x/../y"} {
		if _, err := db.CreateBackup(bad, nil); err == nil {
			t.Errorf("expected error for %q", bad)
		}
	}
}

func TestListBackupsSorted(t *testing.T) {
	db := openDB(t)
	for _, n := range []string{"c", "a", "b"} {
		if _, err := db.CreateBackup(n, nil); err != nil {
			t.Fatalf("create %s: %v", n, err)
		}
	}
	got, err := db.ListBackups()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 3 || got[0].Name != "a" || got[1].Name != "b" || got[2].Name != "c" {
		t.Fatalf("order wrong: %+v", got)
	}
}

func TestLegacyBackupMetadataReportsLegacyManifestStatus(t *testing.T) {
	db := openDB(t)
	seedBackupTable(t, db, "T", "a")
	meta, err := db.CreateBackup("legacy", []string{"T"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	legacy := pebble.BackupMetadata{
		Name:         meta.Name,
		CreatedAt:    meta.CreatedAt,
		Tables:       meta.Tables,
		CheckpointAt: meta.CheckpointAt,
	}
	raw, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal legacy: %v", err)
	}
	if err := db.Set([]byte("cefas/admin/backups/legacy"), raw); err != nil {
		t.Fatalf("overwrite metadata: %v", err)
	}

	got, err := db.GetBackup("legacy")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ManifestStatus != "legacy" || got.ManifestVersion != 0 {
		t.Fatalf("legacy manifest = version %d status %q", got.ManifestVersion, got.ManifestStatus)
	}
}

func TestRestoreValidatesManifestBeforeRegister(t *testing.T) {
	db := openDB(t)
	seedBackupTable(t, db, "Users", "u1", "u2")
	meta, err := db.CreateBackup("snap", []string{"Users"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	meta.TableStats[0].Checksum = "bad"
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal corrupt metadata: %v", err)
	}
	if err := db.Set([]byte("cefas/admin/backups/snap"), raw); err != nil {
		t.Fatalf("overwrite metadata: %v", err)
	}

	called := false
	_, err = db.RestoreTableFromBackup("snap", "Users", "Users_restored", func(types.TableDescriptor) error {
		called = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "manifest mismatch") {
		t.Fatalf("expected manifest mismatch, got %v", err)
	}
	if called {
		t.Fatal("register was called before manifest validation failed")
	}
}

func TestRestoreDryRunValidatesWithoutWritingTarget(t *testing.T) {
	db := openDB(t)
	seedBackupTable(t, db, "Users", "u1", "u2")
	if _, err := db.CreateBackup("snap", []string{"Users"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	called := false
	res, err := db.RestoreTableFromBackupWithOptions("snap", "Users", "Users_restored", pebble.RestoreOptions{DryRun: true}, func(types.TableDescriptor) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("dry-run restore: %v", err)
	}
	if called {
		t.Fatal("register called during dry-run")
	}
	if !res.DryRun || res.RowsCopied != 2 || res.SourceStats.Rows != 2 || res.SourceStats.Checksum == "" {
		t.Fatalf("dry-run result = %+v", res)
	}
	if ok, err := db.Has(storage.KeyCatalog("Users_restored")); err != nil || ok {
		t.Fatalf("target descriptor exists=%v err=%v, want absent", ok, err)
	}
}

func TestRestorePreflightRejectsExistingTargetBeforeRegister(t *testing.T) {
	db := openDB(t)
	seedBackupTable(t, db, "Users", "u1")
	seedBackupTable(t, db, "Users_restored")
	if _, err := db.CreateBackup("snap", []string{"Users"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	called := false
	_, err := db.RestoreTableFromBackupWithOptions("snap", "Users", "Users_restored", pebble.RestoreOptions{}, func(types.TableDescriptor) error {
		called = true
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected target collision, got %v", err)
	}
	if called {
		t.Fatal("register called after target collision")
	}
}

func TestGetBackupAbsent(t *testing.T) {
	db := openDB(t)
	got, err := db.GetBackup("ghost")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != nil {
		t.Fatalf("got %+v, want nil", got)
	}
}

func TestDeleteBackupRemovesMetadataAndCheckpoint(t *testing.T) {
	db := openDB(t)
	seedBackupTable(t, db, "Users", "u1")
	meta, err := db.CreateBackup("snap", []string{"Users"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := os.Stat(meta.CheckpointAt); err != nil {
		t.Fatalf("checkpoint before delete: %v", err)
	}

	result, err := db.DeleteBackup("snap")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !result.MetadataDeleted || !result.CheckpointDeleted || result.PartialCleanup {
		t.Fatalf("delete result = %+v", result)
	}
	got, err := db.GetBackup("snap")
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if got != nil {
		t.Fatalf("metadata still exists: %+v", got)
	}
	if _, err := os.Stat(meta.CheckpointAt); !os.IsNotExist(err) {
		t.Fatalf("checkpoint stat after delete = %v, want not exist", err)
	}
}

func TestDeleteBackupMissingBackup(t *testing.T) {
	db := openDB(t)
	_, err := db.DeleteBackup("ghost")
	if !errors.Is(err, pebble.ErrBackupNotFound) {
		t.Fatalf("delete missing err = %v, want pebble.ErrBackupNotFound", err)
	}
}

func TestDeleteBackupRefusesActiveRestore(t *testing.T) {
	db := openDB(t)
	seedBackupTable(t, db, "Users", "u1")
	if _, err := db.CreateBackup("snap", []string{"Users"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := db.RestoreTableFromBackupWithOptions("snap", "Users", "Users_restored", pebble.RestoreOptions{}, func(types.TableDescriptor) error {
			close(started)
			<-release
			return nil
		})
		done <- err
	}()
	<-started

	_, err := db.DeleteBackup("snap")
	if !errors.Is(err, pebble.ErrBackupInUse) {
		t.Fatalf("delete active restore err = %v, want pebble.ErrBackupInUse", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("restore: %v", err)
	}
}

func TestApplyBackupRetentionDryRunOrdersOldestFirst(t *testing.T) {
	db := openDB(t)
	for _, name := range []string{"old", "middle", "new"} {
		if _, err := db.CreateBackup(name, nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	setBackupCreatedAt(t, db, "old", 100)
	setBackupCreatedAt(t, db, "middle", 200)
	setBackupCreatedAt(t, db, "new", 300)

	result, err := db.ApplyBackupRetention(pebble.BackupRetentionOptions{
		KeepLatestSet: true,
		KeepLatest:    1,
		DryRun:        true,
	})
	if err != nil {
		t.Fatalf("retention dry-run: %v", err)
	}
	if !result.DryRun || len(result.WouldDelete) != 2 {
		t.Fatalf("retention result = %+v", result)
	}
	if result.WouldDelete[0].Backup.Name != "old" || result.WouldDelete[1].Backup.Name != "middle" {
		t.Fatalf("would delete order = %+v", result.WouldDelete)
	}
	if backups, err := db.ListBackups(); err != nil || len(backups) != 3 {
		t.Fatalf("dry-run deleted backups: len=%d err=%v", len(backups), err)
	}
}

func TestApplyBackupRetentionMaxAgeDeletesOldCheckpoints(t *testing.T) {
	db := openDB(t)
	for _, name := range []string{"old", "fresh"} {
		if _, err := db.CreateBackup(name, nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	oldMeta := setBackupCreatedAt(t, db, "old", 100)
	freshMeta := setBackupCreatedAt(t, db, "fresh", 250)

	result, err := db.ApplyBackupRetention(pebble.BackupRetentionOptions{
		MaxAgeSet: true,
		MaxAge:    100 * time.Second,
		Now:       time.Unix(250, 0),
	})
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	if len(result.Deleted) != 1 || result.Deleted[0].BackupName != "old" {
		t.Fatalf("deleted = %+v", result.Deleted)
	}
	if _, err := os.Stat(oldMeta.CheckpointAt); !os.IsNotExist(err) {
		t.Fatalf("old checkpoint stat = %v, want not exist", err)
	}
	if _, err := os.Stat(freshMeta.CheckpointAt); err != nil {
		t.Fatalf("fresh checkpoint stat = %v", err)
	}
}

func TestApplyBackupRetentionKeepsLatestEvenWhenOlderThanMaxAge(t *testing.T) {
	db := openDB(t)
	for _, name := range []string{"old", "latest"} {
		if _, err := db.CreateBackup(name, nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	setBackupCreatedAt(t, db, "old", 100)
	setBackupCreatedAt(t, db, "latest", 200)

	result, err := db.ApplyBackupRetention(pebble.BackupRetentionOptions{
		KeepLatestSet: true,
		KeepLatest:    1,
		MaxAgeSet:     true,
		MaxAge:        time.Second,
		Now:           time.Unix(300, 0),
		DryRun:        true,
	})
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	if len(result.WouldDelete) != 1 || result.WouldDelete[0].Backup.Name != "old" {
		t.Fatalf("would delete = %+v", result.WouldDelete)
	}
}

func TestCreateBackupRefusesExistingCheckpointDir(t *testing.T) {
	db := openDB(t)
	// Force the checkpoint dir into existence before CreateBackup.
	// Note: this also exercises the "name validates but the dir is
	// already there" branch — operator removed metadata but left the
	// pebble dir, for instance.
	dir := filepath.Join(t.TempDir(), "manual")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// Re-open the DB inside that custom dir so the checkpoint target
	// collides with an existing file.
	root := filepath.Join(dir, "pebble")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("mkdir pebble: %v", err)
	}
	d2, err := pebble.Open(pebble.Options{Path: root})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer d2.Close()
	colliding := filepath.Join(root, "backups", "preexisting")
	if err := os.MkdirAll(colliding, 0o755); err != nil {
		t.Fatalf("mkdir collide: %v", err)
	}
	_, err = d2.CreateBackup("preexisting", nil)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected collision error, got %v", err)
	}
	_ = db
}

func setBackupCreatedAt(t *testing.T, db *pebble.DB, name string, createdAt int64) pebble.BackupMetadata {
	t.Helper()
	meta, err := db.GetBackup(name)
	if err != nil {
		t.Fatalf("get backup %s: %v", name, err)
	}
	if meta == nil {
		t.Fatalf("backup %s not found", name)
	}
	meta.CreatedAt = createdAt
	raw, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal backup %s: %v", name, err)
	}
	if err := db.Set([]byte("cefas/admin/backups/"+name), raw); err != nil {
		t.Fatalf("set backup %s: %v", name, err)
	}
	return *meta
}

func seedBackupTable(t *testing.T, db *pebble.DB, table string, ids ...string) {
	t.Helper()
	td := types.TableDescriptor{
		Name:      table,
		KeySchema: types.KeySchema{PK: "id"},
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if err := cat.Create(td); err != nil {
		t.Fatalf("create table %s: %v", table, err)
	}
	for _, id := range ids {
		item := types.Item{
			"id": types.AttributeValue{T: types.AttrS, S: id},
			"v":  types.AttributeValue{T: types.AttrS, S: id + "-value"},
		}
		if err := db.PutItemWith(td, item, pebble.PutOptions{}); err != nil {
			t.Fatalf("put %s/%s: %v", table, id, err)
		}
	}
}
