package storage_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/storage"
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
	meta, err := db.CreateBackup("first", []string{"T1", "T2"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if meta.Name != "first" || len(meta.Tables) != 2 {
		t.Fatalf("meta = %+v", meta)
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
