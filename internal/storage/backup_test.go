package storage_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func openDB(t *testing.T) *storage.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Path: dir})
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
	if meta.ManifestVersion != storage.BackupManifestVersion || meta.ManifestStatus != "ok" {
		t.Fatalf("manifest = version %d status %q", meta.ManifestVersion, meta.ManifestStatus)
	}
	if len(meta.RequestedTables) != 2 || meta.RequestedTables[0] != "T1" || meta.RequestedTables[1] != "T2" {
		t.Fatalf("requested tables = %v", meta.RequestedTables)
	}
	if len(meta.TableStats) != 2 || meta.TableStats[0].Table != "T1" || meta.TableStats[0].Rows != 2 || meta.TableStats[0].Checksum == "" {
		t.Fatalf("table stats = %+v", meta.TableStats)
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
	legacy := storage.BackupMetadata{
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
	res, err := db.RestoreTableFromBackupWithOptions("snap", "Users", "Users_restored", storage.RestoreOptions{DryRun: true}, func(types.TableDescriptor) error {
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
	_, err := db.RestoreTableFromBackupWithOptions("snap", "Users", "Users_restored", storage.RestoreOptions{}, func(types.TableDescriptor) error {
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
	d2, err := storage.Open(storage.Options{Path: root})
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

func seedBackupTable(t *testing.T, db *storage.DB, table string, ids ...string) {
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
		if err := db.PutItemWith(td, item, storage.PutOptions{}); err != nil {
			t.Fatalf("put %s/%s: %v", table, id, err)
		}
	}
}
