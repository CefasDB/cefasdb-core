package backup_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	backuphttp "github.com/osvaldoandrade/cefas/internal/server/http/backup"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func newHandlers(t *testing.T, stream backuphttp.ChangeStream, compact backuphttp.CompactFunc) (*backuphttp.Handlers, *pebble.DB, *catalog.Catalog, func()) {
	t.Helper()
	db, err := pebble.Open(pebble.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	shards := func() []*pebble.DB { return []*pebble.DB{db} }
	return backuphttp.New(db, cat, stream, shards, compact), db, cat, func() { _ = db.Close() }
}

func seedBackup(t *testing.T, db *pebble.DB, cat *catalog.Catalog, name string) {
	t.Helper()
	td := types.TableDescriptor{Name: "Users", KeySchema: types.KeySchema{PK: "id"}}
	if err := cat.Create(td); err != nil {
		t.Fatalf("create table: %v", err)
	}
	for _, id := range []string{"u1", "u2"} {
		item := types.Item{
			"id": types.AttributeValue{T: types.AttrS, S: id},
			"v":  types.AttributeValue{T: types.AttrS, S: id + "-v"},
		}
		if err := db.PutItemWith(td, item, pebble.PutOptions{}); err != nil {
			t.Fatalf("put %s: %v", id, err)
		}
	}
	if _, err := db.CreateBackup(name, []string{"Users"}); err != nil {
		t.Fatalf("backup: %v", err)
	}
}

func TestHandleDeleteBackupHappyPath(t *testing.T) {
	t.Parallel()
	h, db, cat, cleanup := newHandlers(t, nil, nil)
	defer cleanup()
	seedBackup(t, db, cat, "snap")

	body := bytes.NewBufferString(`{"backupName":"snap"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/DeleteBackup", body)
	rec := httptest.NewRecorder()
	h.HandleDeleteBackup(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		BackupDeletion struct {
			BackupName string `json:"backupName"`
		} `json:"BackupDeletion"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.BackupDeletion.BackupName != "snap" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestHandleDeleteBackupNotFound(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t, nil, nil)
	defer cleanup()

	body := bytes.NewBufferString(`{"backupName":"ghost"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/DeleteBackup", body)
	rec := httptest.NewRecorder()
	h.HandleDeleteBackup(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleApplyBackupRetentionRoundTrip(t *testing.T) {
	t.Parallel()
	h, db, cat, cleanup := newHandlers(t, nil, nil)
	defer cleanup()
	seedBackup(t, db, cat, "snap")

	body := bytes.NewBufferString(`{"keepLatest":1,"keepLatestSet":true,"dryRun":true}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/ApplyBackupRetention", body)
	rec := httptest.NewRecorder()
	h.HandleApplyBackupRetention(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		BackupRetention pebble.BackupRetentionResult `json:"BackupRetention"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.BackupRetention.DryRun {
		t.Fatalf("expected DryRun=true; got %+v", resp.BackupRetention)
	}
}

type fakeChangeStream struct {
	metas []backuphttp.SnapshotMetadata
	err   error
}

func (f *fakeChangeStream) ListSnapshots() ([]backuphttp.SnapshotMetadata, error) {
	return f.metas, f.err
}

func TestHandleListSnapshotsAttached(t *testing.T) {
	t.Parallel()
	fake := &fakeChangeStream{
		metas: []backuphttp.SnapshotMetadata{
			{ID: "s1", Index: 7, Term: 2, UnixSeconds: time.Now().Unix(), SizeBytes: 1024},
		},
	}
	h, _, _, cleanup := newHandlers(t, fake, nil)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/snapshots", nil)
	rec := httptest.NewRecorder()
	h.HandleListSnapshots(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Snapshots []backuphttp.SnapshotMetadata `json:"snapshots"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Snapshots) != 1 || resp.Snapshots[0].ID != "s1" {
		t.Fatalf("snapshots = %+v", resp.Snapshots)
	}
}

func TestHandleListSnapshotsNoStream(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t, nil, nil)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/snapshots", nil)
	rec := httptest.NewRecorder()
	h.HandleListSnapshots(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Snapshots []backuphttp.SnapshotMetadata `json:"snapshots"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Snapshots) != 0 {
		t.Fatalf("snapshots = %+v, want empty", resp.Snapshots)
	}
}

func TestHandleListSnapshotsStreamError(t *testing.T) {
	t.Parallel()
	fake := &fakeChangeStream{err: errors.New("boom")}
	h, _, _, cleanup := newHandlers(t, fake, nil)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/snapshots", nil)
	rec := httptest.NewRecorder()
	h.HandleListSnapshots(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleCompactHappyPath(t *testing.T) {
	t.Parallel()
	canned := []pebble.CompactionResult{
		{
			Table:           "Users",
			StartedAt:       time.Unix(1700000000, 0).UTC(),
			FinishedAt:      time.Unix(1700000005, 0).UTC(),
			Elapsed:         5 * time.Second,
			Parallelized:    true,
			BeforeL0Files:   4,
			AfterL0Files:    1,
			BeforeDebtBytes: 9001,
			AfterDebtBytes:  900,
		},
	}
	var seen struct {
		table       string
		lowerB64    string
		upperB64    string
		parallelize bool
	}
	compact := func(table, lowerB64, upperB64 string, parallelize bool) ([]pebble.CompactionResult, error) {
		seen.table = table
		seen.lowerB64 = lowerB64
		seen.upperB64 = upperB64
		seen.parallelize = parallelize
		return canned, nil
	}
	h, _, _, cleanup := newHandlers(t, nil, compact)
	defer cleanup()

	body := bytes.NewBufferString(`{"table":"Users","parallelize":true}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/admin/compact", body)
	rec := httptest.NewRecorder()
	h.HandleCompact(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if seen.table != "Users" || !seen.parallelize {
		t.Fatalf("compact callback received unexpected args: %+v", seen)
	}
	var resp struct {
		Compactions []map[string]any `json:"compactions"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Compactions) != 1 {
		t.Fatalf("compactions = %+v", resp.Compactions)
	}
	if got, _ := resp.Compactions[0]["table"].(string); got != "Users" {
		t.Fatalf("table field = %v; want Users", resp.Compactions[0]["table"])
	}
	if got, _ := resp.Compactions[0]["parallelized"].(bool); !got {
		t.Fatalf("parallelized field = %v; want true", resp.Compactions[0]["parallelized"])
	}
}

func TestHandleCompactMethodNotAllowed(t *testing.T) {
	t.Parallel()
	h, _, _, cleanup := newHandlers(t, nil, func(string, string, string, bool) ([]pebble.CompactionResult, error) {
		return nil, nil
	})
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/v1/admin/compact", nil)
	rec := httptest.NewRecorder()
	h.HandleCompact(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
