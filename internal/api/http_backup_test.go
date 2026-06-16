package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/internal/api"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func TestHTTPRestoreTableFromBackupDryRun(t *testing.T) {
	db, err := storage.Open(storage.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	catStore, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	td := types.TableDescriptor{Name: "Users", KeySchema: types.KeySchema{PK: "id"}}
	if err := catStore.Create(td); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for _, id := range []string{"u1", "u2"} {
		item := types.Item{
			"id": types.AttributeValue{T: types.AttrS, S: id},
			"v":  types.AttributeValue{T: types.AttrS, S: id + "-v"},
		}
		if err := db.PutItemWith(td, item, storage.PutOptions{}); err != nil {
			t.Fatalf("put %s: %v", id, err)
		}
	}
	if _, err := db.CreateBackup("snap", []string{"Users"}); err != nil {
		t.Fatalf("backup: %v", err)
	}

	srv := api.New(db, catStore)
	mux := http.NewServeMux()
	srv.Routes(mux)
	body := bytes.NewBufferString(`{"backupName":"snap","sourceTableName":"Users","targetTableName":"Users_restored","dryRun":true}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/RestoreTableFromBackup", body)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		TargetTableName  string `json:"targetTableName"`
		RowsCopied       int    `json:"rowsCopied"`
		DryRun           bool   `json:"dryRun"`
		ManifestVersion  int    `json:"manifestVersion"`
		ManifestStatus   string `json:"manifestStatus"`
		SourceTableStats struct {
			Table    string `json:"table"`
			Rows     int64  `json:"rows"`
			Checksum string `json:"checksum"`
		} `json:"sourceTableStats"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.DryRun || resp.RowsCopied != 2 || resp.TargetTableName != "Users_restored" {
		t.Fatalf("response = %+v", resp)
	}
	if resp.ManifestVersion != 1 || resp.ManifestStatus != "ok" || resp.SourceTableStats.Rows != 2 || resp.SourceTableStats.Checksum == "" {
		t.Fatalf("manifest diagnostics = %+v", resp)
	}
	if _, err := catStore.Describe("Users_restored"); err == nil {
		t.Fatal("dry-run created target table")
	}
}

func TestHTTPDeleteBackup(t *testing.T) {
	db, err := storage.Open(storage.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	catStore, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if _, err := db.CreateBackup("snap", nil); err != nil {
		t.Fatalf("backup: %v", err)
	}

	srv := api.New(db, catStore)
	mux := http.NewServeMux()
	srv.Routes(mux)
	req := httptest.NewRequest(http.MethodPost, "/v1/DeleteBackup", bytes.NewBufferString(`{"backupName":"snap"}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		BackupDeletion struct {
			BackupName        string `json:"backupName"`
			MetadataDeleted   bool   `json:"metadataDeleted"`
			CheckpointDeleted bool   `json:"checkpointDeleted"`
			PartialCleanup    bool   `json:"partialCleanup"`
		} `json:"BackupDeletion"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.BackupDeletion.BackupName != "snap" || !resp.BackupDeletion.MetadataDeleted || !resp.BackupDeletion.CheckpointDeleted || resp.BackupDeletion.PartialCleanup {
		t.Fatalf("delete response = %+v", resp)
	}
	got, err := db.GetBackup("snap")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != nil {
		t.Fatalf("backup still exists: %+v", got)
	}
}

func TestHTTPApplyBackupRetentionDryRun(t *testing.T) {
	db, err := storage.Open(storage.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	catStore, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	for _, name := range []string{"a", "b"} {
		if _, err := db.CreateBackup(name, nil); err != nil {
			t.Fatalf("backup %s: %v", name, err)
		}
	}

	srv := api.New(db, catStore)
	mux := http.NewServeMux()
	srv.Routes(mux)
	req := httptest.NewRequest(http.MethodPost, "/v1/ApplyBackupRetention", bytes.NewBufferString(`{"keepLatest":0,"keepLatestSet":true,"dryRun":true}`))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		BackupRetention struct {
			DryRun      bool `json:"dryRun"`
			WouldDelete []struct {
				Backup struct {
					Name string `json:"name"`
				} `json:"backup"`
			} `json:"wouldDelete"`
			Deleted []any `json:"deleted"`
		} `json:"BackupRetention"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.BackupRetention.DryRun || len(resp.BackupRetention.WouldDelete) != 2 || len(resp.BackupRetention.Deleted) != 0 {
		t.Fatalf("retention response = %+v", resp)
	}
	backups, err := db.ListBackups()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(backups) != 2 {
		t.Fatalf("dry-run deleted backups: %+v", backups)
	}
}
