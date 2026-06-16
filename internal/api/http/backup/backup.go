// Package backup owns the HTTP handlers for the backup / admin
// endpoints:
//
//   - /v1/RestoreTableFromBackup
//   - /v1/DeleteBackup
//   - /v1/ApplyBackupRetention
//   - /v1/admin/snapshots
//   - /v1/admin/compact
//
// Handlers are exposed as methods on *Handlers so the composition
// root (internal/api.Server) can wrap each handler with its standard
// auth + metrics middleware via the same register helper it uses for
// every other route. The package depends only on:
//
//   - catalog.Catalog              — descriptor write during restore
//   - storage.DB                   — backup/restore primitives
//   - internal/auth                — scope checks
//   - internal/api/http/httpx      — JSON write helpers
//   - pkg/types                    — wire types
//
// It deliberately has no back-channel into internal/api so the import
// graph stays one-way (internal/api → internal/api/http/backup, never
// the reverse). Cluster fan-out during restore and compaction are
// passed in as callbacks so the package never imports
// internal/cluster.
package backup

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/osvaldoandrade/cefas/internal/api/http/httpx"
	"github.com/osvaldoandrade/cefas/internal/auth"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// SnapshotMetadata mirrors raft.SnapshotMetadata for HTTP consumers.
// The composition root adapts whatever its underlying ChangeStream
// returns into this shape.
type SnapshotMetadata struct {
	ID          string
	Index       uint64
	Term        uint64
	UnixSeconds int64
	SizeBytes   int64
}

// ChangeStream is the CDC subset the backup handlers depend on.
// Currently only ListSnapshots is required. Pass nil at construction
// time when no CDC source is attached — handleListSnapshots then
// returns an empty list (preserves the original behaviour).
type ChangeStream interface {
	ListSnapshots() ([]SnapshotMetadata, error)
}

// ShardsFunc returns every storage.DB this server manages. Shard 0
// already has the descriptor by virtue of Catalog.Create; the restore
// path uses this to mirror it onto every other shard.
type ShardsFunc func() []*storage.DB

// CompactFunc runs a compaction across every shard, scoped either to
// a table or a base64-encoded key range. The Server retains this
// helper because it is shared with paths outside the backup package;
// here we only need the callback shape.
type CompactFunc func(table, lowerB64, upperB64 string, parallelize bool) ([]storage.CompactionResult, error)

// Handlers carries the dependencies every backup/admin handler needs.
type Handlers struct {
	db      *storage.DB
	cat     *catalog.Catalog
	stream  ChangeStream
	shards  ShardsFunc
	compact CompactFunc
}

// New constructs the handler set. stream may be nil (no CDC source
// attached); shards may be nil (single-shard mode); compact must be
// non-nil — handleCompact has no useful behaviour without it.
func New(db *storage.DB, cat *catalog.Catalog, stream ChangeStream, shards ShardsFunc, compact CompactFunc) *Handlers {
	return &Handlers{db: db, cat: cat, stream: stream, shards: shards, compact: compact}
}

type restoreTableFromBackupRequest struct {
	BackupName      string `json:"backupName"`
	SourceTableName string `json:"sourceTableName"`
	TargetTableName string `json:"targetTableName"`
	DryRun          bool   `json:"dryRun,omitempty"`
}

type restoreTableFromBackupResponse struct {
	TargetTableName  string                   `json:"targetTableName"`
	RowsCopied       int                      `json:"rowsCopied"`
	DryRun           bool                     `json:"dryRun"`
	SourceTableStats storage.BackupTableStats `json:"sourceTableStats"`
	ManifestVersion  int                      `json:"manifestVersion"`
	ManifestStatus   string                   `json:"manifestStatus"`
	TableStatus      string                   `json:"tableStatus"`
}

type deleteBackupRequest struct {
	BackupName string `json:"backupName"`
}

type applyBackupRetentionRequest struct {
	KeepLatest    int    `json:"keepLatest,omitempty"`
	KeepLatestSet bool   `json:"keepLatestSet,omitempty"`
	MaxAgeSeconds int64  `json:"maxAgeSeconds,omitempty"`
	MaxAgeSet     bool   `json:"maxAgeSet,omitempty"`
	MaxAge        string `json:"maxAge,omitempty"`
	DryRun        bool   `json:"dryRun,omitempty"`
}

type compactRequest struct {
	Table       string `json:"table,omitempty"`
	LowerBase64 string `json:"lower,omitempty"`
	UpperBase64 string `json:"upper,omitempty"`
	Parallelize bool   `json:"parallelize,omitempty"`
}

// HandleRestoreTableFromBackup serves POST /v1/RestoreTableFromBackup.
func (h *Handlers) HandleRestoreTableFromBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	var req restoreTableFromBackupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	register := func(td types.TableDescriptor) error {
		if err := h.cat.Create(td); err != nil {
			return err
		}
		if h.shards != nil {
			shards := h.shards()
			for i, sh := range shards {
				if i == 0 {
					continue
				}
				if cat, cerr := catalog.New(sh); cerr == nil {
					_ = cat.Create(td)
				}
			}
		}
		return nil
	}
	res, err := h.db.RestoreTableFromBackupWithOptions(
		req.BackupName,
		req.SourceTableName,
		req.TargetTableName,
		storage.RestoreOptions{DryRun: req.DryRun},
		register,
	)
	if err != nil {
		httpx.WriteErr(w, mapBackupErr(err), err)
		return
	}
	status := "ACTIVE"
	if res.DryRun {
		status = "DRY_RUN"
	}
	httpx.WriteJSON(w, http.StatusOK, restoreTableFromBackupResponse{
		TargetTableName:  res.TargetTable.Name,
		RowsCopied:       res.RowsCopied,
		DryRun:           res.DryRun,
		SourceTableStats: res.SourceStats,
		ManifestVersion:  res.ManifestVersion,
		ManifestStatus:   res.ManifestStatus,
		TableStatus:      status,
	})
}

// HandleDeleteBackup serves POST /v1/DeleteBackup.
func (h *Handlers) HandleDeleteBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	var req deleteBackupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	result, err := h.db.DeleteBackup(req.BackupName)
	if err != nil {
		httpx.WriteErr(w, mapBackupErr(err), err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"BackupDeletion": result})
}

// HandleApplyBackupRetention serves POST /v1/ApplyBackupRetention.
func (h *Handlers) HandleApplyBackupRetention(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	var req applyBackupRetentionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	maxAge := time.Duration(req.MaxAgeSeconds) * time.Second
	maxAgeSet := req.MaxAgeSet
	if req.MaxAge != "" {
		parsed, err := time.ParseDuration(req.MaxAge)
		if err != nil {
			httpx.WriteErr(w, http.StatusBadRequest, err)
			return
		}
		maxAge = parsed
		maxAgeSet = true
	}
	result, err := h.db.ApplyBackupRetention(storage.BackupRetentionOptions{
		KeepLatest:    req.KeepLatest,
		KeepLatestSet: req.KeepLatestSet,
		MaxAge:        maxAge,
		MaxAgeSet:     maxAgeSet,
		DryRun:        req.DryRun,
	})
	if err != nil {
		httpx.WriteErr(w, mapBackupErr(err), err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"BackupRetention": result})
}

// HandleListSnapshots serves /v1/admin/snapshots. It is registered
// without auth middleware (plain mux.HandleFunc) for parity with the
// original Routes() wiring; the handler enforces ScopeClusterAdmin
// itself when a stream is attached.
func (h *Handlers) HandleListSnapshots(w http.ResponseWriter, r *http.Request) {
	if h.stream == nil {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"snapshots": nil})
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	metas, err := h.stream.ListSnapshots()
	if err != nil {
		httpx.WriteErr(w, http.StatusInternalServerError, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"snapshots": metas})
}

// HandleCompact serves /v1/admin/compact. Like HandleListSnapshots it
// is registered without auth middleware in Routes(); the handler
// enforces ScopeClusterAdmin itself.
func (h *Handlers) HandleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	var req compactRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	if h.compact == nil {
		httpx.WriteErr(w, http.StatusInternalServerError, errors.New("compact callback not configured"))
		return
	}
	results, err := h.compact(req.Table, req.LowerBase64, req.UpperBase64, req.Parallelize)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"compactions": compactionResultsJSON(results)})
}

// compactionResultsJSON shapes storage.CompactionResult slices into
// the wire form the admin endpoint emits.
func compactionResultsJSON(results []storage.CompactionResult) []map[string]any {
	out := make([]map[string]any, 0, len(results))
	for _, r := range results {
		out = append(out, map[string]any{
			"table":           r.Table,
			"lower":           base64.StdEncoding.EncodeToString(r.Lower),
			"upper":           base64.StdEncoding.EncodeToString(r.Upper),
			"startedAt":       r.StartedAt.Format(time.RFC3339Nano),
			"finishedAt":      r.FinishedAt.Format(time.RFC3339Nano),
			"elapsedSeconds":  r.Elapsed.Seconds(),
			"parallelized":    r.Parallelized,
			"beforeL0Files":   r.BeforeL0Files,
			"afterL0Files":    r.AfterL0Files,
			"beforeDebtBytes": r.BeforeDebtBytes,
			"afterDebtBytes":  r.AfterDebtBytes,
		})
	}
	return out
}

// mapBackupErr translates the subset of storage / catalog errors the
// backup handlers can produce into HTTP status codes. The original
// server-level mapWriteErr handles a wider surface; only the codes
// reachable from these handlers are reproduced here.
func mapBackupErr(err error) int {
	switch {
	case errors.Is(err, storage.ErrBackupNotFound):
		return http.StatusNotFound
	case errors.Is(err, storage.ErrBackupInUse):
		return http.StatusConflict
	case errors.Is(err, storage.ErrInvalidBackupRetention):
		return http.StatusBadRequest
	case errors.Is(err, types.ErrTableAlreadyExists):
		return http.StatusConflict
	case errors.Is(err, types.ErrTableNotFound):
		return http.StatusNotFound
	case errors.Is(err, types.ErrMissingKey), errors.Is(err, types.ErrInvalidKeyType):
		return http.StatusBadRequest
	case errors.Is(err, storage.ErrConditionFailed):
		return http.StatusPreconditionFailed
	case errors.Is(err, storage.ErrThrottled):
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}
