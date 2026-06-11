// Package api exposes cefas operations over HTTP/JSON.
//
// Phase 1 ships HTTP/JSON only. A gRPC server (generated from
// proto/cefas.proto) will plug in at Phase 2 — it will share the same
// underlying Server type and just translate from generated stubs.
//
// JSON shape: AttributeValue mirrors DynamoDB's single-letter tagged
// union, e.g. `{"S": "alice"}` or `{"N": "42"}`. Item is a flat object
// of attribute name → AttributeValue.
package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/osvaldoandrade/cefas/internal/auth"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/metrics"
	"github.com/osvaldoandrade/cefas/internal/spatial"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/ddbjson"
	cefassql "github.com/osvaldoandrade/cefas/pkg/sql"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// Cluster is the optional cluster-management surface the API exposes
// when the server runs in Raft mode. nil means single-node.
type Cluster interface {
	IsLeader() bool
	LeaderHTTPAddr() string
	AddVoter(id, addr string, timeout time.Duration) error
	RemoveServer(id string, timeout time.Duration) error
	Barrier(timeout time.Duration) error
	SelfID() string
	BindAddr() string
}

// ChangeStream is the CDC subset of the Cluster surface. Implemented
// by *raft.DB when CDC is wired; nil when the server runs without
// raft or without an attached publisher.
type ChangeStream interface {
	SubscribeChanges(ctx context.Context) (<-chan ChangeEvent, func())
	ListSnapshots() ([]SnapshotMetadata, error)
}

// ChangeEvent is the wire-agnostic shape of a CDC entry. raft.DB's
// ChangeEvent is convertible to this.
type ChangeEvent struct {
	RaftIndex uint64
	Op        string // "PUT" | "DELETE"
	Key       []byte
	Value     []byte
}

// SnapshotMetadata mirrors raft.SnapshotMetadata for API consumers.
type SnapshotMetadata struct {
	ID          string
	Index       uint64
	Term        uint64
	UnixSeconds int64
	SizeBytes   int64
}

type Server struct {
	db        *storage.DB
	cat       *catalog.Catalog
	cluster   Cluster          // nil when not running in raft mode
	stream    ChangeStream     // nil when no CDC source attached
	manager   *cluster.Manager // nil when single-shard
	validator *auth.Validator  // nil when auth disabled (dev mode)
	metrics   *metrics.Metrics // nil when metrics disabled
}

// AttachChangeStream wires the CDC source. Passing nil keeps the
// stream endpoints off.
func (s *Server) AttachChangeStream(c ChangeStream) { s.stream = c }

// AttachMetrics wires the Prometheus surface. When attached, every
// handler records latency + outcome; /metrics serves the registry.
func (s *Server) AttachMetrics(m *metrics.Metrics) { s.metrics = m }

func New(db *storage.DB, cat *catalog.Catalog) *Server {
	return &Server{db: db, cat: cat}
}

// AttachManager wires the multi-shard manager. Pass nil to keep the
// server single-shard. When attached, every PK-bearing handler
// resolves the table descriptor on shard 0 (the metadata shard) and
// routes the actual write/read to the shard that owns the PK.
func (s *Server) AttachManager(m *cluster.Manager) { s.manager = m }

// storageFor returns the storage.DB that owns the supplied partition
// key bytes. Single-shard mode always returns s.db.
func (s *Server) storageFor(pkBytes []byte) *storage.DB {
	if s.manager == nil {
		return s.db
	}
	if shard := s.manager.ShardForPK(pkBytes); shard != nil {
		return shard.Storage
	}
	return s.db
}

// allShards iterates every storage.DB this server manages. Catalog
// fan-out (CreateTable, DropTable) uses this so descriptors land on
// every shard.
func (s *Server) allShards() []*storage.DB {
	if s.manager == nil {
		return []*storage.DB{s.db}
	}
	out := make([]*storage.DB, 0, len(s.manager.Shards()))
	for _, sh := range s.manager.Shards() {
		out = append(out, sh.Storage)
	}
	return out
}

func (s *Server) compact(table, lowerB64, upperB64 string, parallelize bool) ([]storage.CompactionResult, error) {
	dbs := s.allShards()
	results := make([]storage.CompactionResult, 0, len(dbs))
	if table != "" {
		if _, err := s.cat.Describe(table); err != nil {
			return nil, err
		}
		for _, db := range dbs {
			res, err := db.CompactTable(table, parallelize)
			if err != nil {
				return nil, err
			}
			results = append(results, res)
		}
		return results, nil
	}
	if lowerB64 == "" || upperB64 == "" {
		return nil, fmt.Errorf("table or lower/upper range is required")
	}
	lower, err := base64.StdEncoding.DecodeString(lowerB64)
	if err != nil {
		return nil, fmt.Errorf("lower: %w", err)
	}
	upper, err := base64.StdEncoding.DecodeString(upperB64)
	if err != nil {
		return nil, fmt.Errorf("upper: %w", err)
	}
	for _, db := range dbs {
		res, err := db.CompactRange(lower, upper, parallelize)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}
	return results, nil
}

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

// AttachCluster wires the optional cluster-management surface. Pass a
// *raft.DB; nil keeps the server in single-node mode.
func (s *Server) AttachCluster(c Cluster) { s.cluster = c }

// AttachAuth wires bearer-token validation onto every non-public
// route. Pass nil to keep the server open (dev mode).
func (s *Server) AttachAuth(v *auth.Validator) { s.validator = v }

// publicPaths bypass the auth middleware so probes and cluster status
// stay reachable on an unjoined or pre-credentialed node.
var publicPaths = map[string]bool{
	"/v1/Health":         true,
	"/v1/cluster/status": true,
}

// Routes attaches cefas HTTP endpoints onto mux. Path layout follows
// DynamoDB-ish verbs as resources under /v1/. If AttachAuth has been
// called, every non-public route is wrapped in the bearer-token
// middleware; handlers then enforce per-operation, per-table scopes
// internally.
func (s *Server) Routes(mux *http.ServeMux) {
	register := func(path string, handler http.HandlerFunc) {
		var h http.Handler = handler
		if s.metrics != nil {
			h = s.instrument(path, h)
		}
		if s.validator != nil && !publicPaths[path] {
			h = s.validator.Middleware(publicPaths)(h)
		}
		mux.Handle(path, h)
	}
	register("/v1/tables", s.handleTables) // POST=create, GET=list
	register("/v1/tables/", s.handleTable) // GET=describe
	register("/v1/PutItem", s.handlePutItem)
	register("/v1/GetItem", s.handleGetItem)
	register("/v1/DeleteItem", s.handleDeleteItem)
	register("/v1/Query", s.handleQuery)
	register("/v1/BatchWriteItem", s.handleBatchWriteItem)
	register("/v1/BatchGetItem", s.handleBatchGetItem)
	register("/v1/SpatialQuery", s.handleSpatialQuery)
	register("/v1/Sql", s.handleSql)
	register("/v1/PartiQL", s.handlePartiQL)
	register("/v1/Health", s.handleHealth)
	register("/v1/cluster/status", s.handleClusterStatus)
	register("/v1/cluster/AddVoter", s.handleClusterAddVoter)
	register("/v1/cluster/RemoveServer", s.handleClusterRemoveServer)
	if s.metrics != nil {
		mux.Handle("/metrics", s.metrics.Handler())
	}
	mux.HandleFunc("/v1/Stream", s.handleStream)
	mux.HandleFunc("/v1/admin/snapshots", s.handleListSnapshots)
	mux.HandleFunc("/v1/admin/compact", s.handleCompact)
}

// handleStream is the HTTP/SSE variant of the CDC stream. Clients
// receive `data:` lines with one JSON ChangeEvent each.
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	if s.stream == nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("change stream not configured"))
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("server does not support streaming"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	events, cancel := s.stream.SubscribeChanges(r.Context())
	defer cancel()

	enc := json.NewEncoder(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			fmt.Fprint(w, "data: ")
			_ = enc.Encode(ev) // writes JSON + newline
			flusher.Flush()
		}
	}
}

// handleListSnapshots is the admin endpoint that lists every retained
// raft snapshot.
func (s *Server) handleListSnapshots(w http.ResponseWriter, r *http.Request) {
	if s.stream == nil {
		writeJSON(w, http.StatusOK, map[string]any{"snapshots": nil})
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	metas, err := s.stream.ListSnapshots()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": metas})
}

type compactRequest struct {
	Table       string `json:"table,omitempty"`
	LowerBase64 string `json:"lower,omitempty"`
	UpperBase64 string `json:"upper,omitempty"`
	Parallelize bool   `json:"parallelize,omitempty"`
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	var req compactRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	results, err := s.compact(req.Table, req.LowerBase64, req.UpperBase64, req.Parallelize)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"compactions": compactionResultsJSON(results)})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Health(r.Context()); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---------- table management ----------

type createTableRequest struct {
	Name           string                         `json:"name"`
	KeySchema      jsonKeySchema                  `json:"keySchema"`
	GSIs           []types.GSIDescriptor          `json:"gsis,omitempty"`
	SpatialIndexes []types.SpatialIndexDescriptor `json:"spatialIndexes,omitempty"`
}

type jsonKeySchema struct {
	PK string `json:"pk"`
	SK string `json:"sk,omitempty"`
}

func (s *Server) handleTables(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		if !auth.RequireAnyScope(w, r, auth.ScopeTableCreate) {
			return
		}
		var req createTableRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		td := types.TableDescriptor{
			Name:           req.Name,
			KeySchema:      types.KeySchema{PK: req.KeySchema.PK, SK: req.KeySchema.SK},
			GSIs:           req.GSIs,
			SpatialIndexes: req.SpatialIndexes,
		}
		if err := s.cat.Create(td); err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, types.ErrTableAlreadyExists) {
				status = http.StatusConflict
			}
			writeErr(w, status, err)
			return
		}
		// Fan the descriptor out to every shard so writes routed to
		// any of them can resolve the schema locally. Shard 0 already
		// has it via s.cat.Create above.
		s.fanOutCatalog(td)
		writeJSON(w, http.StatusCreated, td)
	case http.MethodGet:
		if !auth.RequireAnyScope(w, r, auth.ScopeTableDescribe) {
			return
		}
		writeJSON(w, http.StatusOK, s.cat.List())
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTable(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path[len("/v1/tables/"):]
	if name == "" {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("table name required"))
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeTableDescribe) {
		return
	}
	td, err := s.cat.Describe(name)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, types.ErrTableNotFound) {
			status = http.StatusNotFound
		}
		writeErr(w, status, err)
		return
	}
	writeJSON(w, http.StatusOK, td)
}

// ---------- item ops ----------

type putItemRequest struct {
	Table     string                       `json:"table"`
	Item      map[string]ddbjson.Attribute `json:"item"`
	Condition string                       `json:"condition,omitempty"`
	Binds     map[string]ddbjson.Attribute `json:"binds,omitempty"`
}

type getItemRequest struct {
	Table       string                       `json:"table"`
	Key         map[string]ddbjson.Attribute `json:"key"`
	Consistency string                       `json:"consistency,omitempty"` // "" or "eventual" → local; "strong" → leader + barrier
}

type deleteItemRequest struct {
	Table     string                       `json:"table"`
	Key       map[string]ddbjson.Attribute `json:"key"`
	Condition string                       `json:"condition,omitempty"`
	Binds     map[string]ddbjson.Attribute `json:"binds,omitempty"`
}

type queryRequest struct {
	Table       string             `json:"table"`
	IndexName   string             `json:"indexName,omitempty"`
	PKValue     ddbjson.Attribute  `json:"pkValue"`
	SKLow       *ddbjson.Attribute `json:"skLow,omitempty"`
	SKHigh      *ddbjson.Attribute `json:"skHigh,omitempty"`
	Limit       int                `json:"limit,omitempty"`
	Consistency string             `json:"consistency,omitempty"`
}

type batchWriteOp struct {
	Op   string                       `json:"op"` // "put" | "delete"
	Item map[string]ddbjson.Attribute `json:"item,omitempty"`
	Key  map[string]ddbjson.Attribute `json:"key,omitempty"`
}

type batchWriteRequest struct {
	Table string         `json:"table"`
	Ops   []batchWriteOp `json:"ops"`
}

type batchGetRequest struct {
	Table string                         `json:"table"`
	Keys  []map[string]ddbjson.Attribute `json:"keys"`
}

func (s *Server) handlePutItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req putItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeItemWrite, req.Table),
		auth.WildcardScope(auth.ScopeItemWrite)) {
		return
	}
	td, err := s.cat.Describe(req.Table)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	item, err := ddbjson.DecodeItem(req.Item)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	binds, err := ddbjson.DecodeBinds(req.Binds)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("binds: %w", err))
		return
	}
	pkBytes, err := pkBytesFromItem(item, td.KeySchema)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	err = s.storageFor(pkBytes).PutItemWith(td, item, storage.PutOptions{Condition: req.Condition, Binds: binds})
	if err != nil {
		writeWriteErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func (s *Server) handleGetItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req getItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeItemRead, req.Table),
		auth.WildcardScope(auth.ScopeItemRead)) {
		return
	}
	if req.Consistency == "strong" && !s.ensureStrongRead(w, r) {
		return
	}
	td, err := s.cat.Describe(req.Table)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	keyAttrs, err := ddbjson.DecodeItem(req.Key)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	pkBytes, err := pkBytesFromItem(keyAttrs, td.KeySchema)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	item, err := s.storageFor(pkBytes).GetItem(req.Table, td.KeySchema, keyAttrs)
	if err != nil {
		if errors.Is(err, types.ErrItemNotFound) {
			writeJSON(w, http.StatusOK, map[string]any{"found": false})
			return
		}
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"found": true,
		"item":  ddbjson.EncodeItem(item),
	})
}

func (s *Server) handleDeleteItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req deleteItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeItemDelete, req.Table),
		auth.WildcardScope(auth.ScopeItemDelete)) {
		return
	}
	td, err := s.cat.Describe(req.Table)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	keyAttrs, err := ddbjson.DecodeItem(req.Key)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	binds, err := ddbjson.DecodeBinds(req.Binds)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("binds: %w", err))
		return
	}
	pkBytes, err := pkBytesFromItem(keyAttrs, td.KeySchema)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	err = s.storageFor(pkBytes).DeleteItemWith(td, keyAttrs, storage.DeleteOptions{Condition: req.Condition, Binds: binds})
	if err != nil {
		writeWriteErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeQuery, req.Table),
		auth.WildcardScope(auth.ScopeQuery)) {
		return
	}
	if req.Consistency == "strong" && !s.ensureStrongRead(w, r) {
		return
	}
	td, err := s.cat.Describe(req.Table)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	pkVal, err := req.PKValue.ToAttr()
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("pkValue: %w", err))
		return
	}
	var lo, hi types.AttributeValue
	if req.SKLow != nil {
		lo, err = req.SKLow.ToAttr()
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("skLow: %w", err))
			return
		}
	}
	if req.SKHigh != nil {
		hi, err = req.SKHigh.ToAttr()
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("skHigh: %w", err))
			return
		}
	}

	pkBytes, err := storage.AttrCanonicalBytes(pkVal)
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("pkValue: %w", err))
		return
	}
	queryDB := s.storageFor(pkBytes)
	var items []types.Item
	if req.IndexName != "" {
		items, err = queryDB.QueryByGSI(td, req.IndexName, pkVal, storage.QueryOptions{
			SKLow:  lo,
			SKHigh: hi,
			Limit:  req.Limit,
		})
	} else if req.SKLow == nil && req.SKHigh == nil {
		items, err = queryDB.QueryByPK(req.Table, td.KeySchema, pkVal, req.Limit)
	} else {
		items, err = queryDB.QueryByPKRange(req.Table, td.KeySchema, pkVal, lo, hi, req.Limit)
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}

	out := make([]map[string]ddbjson.Attribute, 0, len(items))
	for _, it := range items {
		out = append(out, ddbjson.EncodeItem(it))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (s *Server) handleBatchWriteItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req batchWriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeItemWrite, req.Table),
		auth.WildcardScope(auth.ScopeItemWrite)) {
		return
	}
	td, err := s.cat.Describe(req.Table)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}

	ops := make([]storage.BatchOp, 0, len(req.Ops))
	for i, raw := range req.Ops {
		switch raw.Op {
		case "put":
			item, err := ddbjson.DecodeItem(raw.Item)
			if err != nil {
				writeErr(w, http.StatusBadRequest, fmt.Errorf("op %d: %w", i, err))
				return
			}
			ops = append(ops, storage.BatchOp{Op: storage.BatchOpPut, Item: item})
		case "delete":
			keyAttrs, err := ddbjson.DecodeItem(raw.Key)
			if err != nil {
				writeErr(w, http.StatusBadRequest, fmt.Errorf("op %d: %w", i, err))
				return
			}
			ops = append(ops, storage.BatchOp{Op: storage.BatchOpDelete, Key: keyAttrs})
		default:
			writeErr(w, http.StatusBadRequest, fmt.Errorf("op %d: unknown op %q", i, raw.Op))
			return
		}
	}
	if err := s.batchWriteByShard(td, ops); err != nil {
		writeWriteErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

func (s *Server) handleBatchGetItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req batchGetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeItemRead, req.Table),
		auth.WildcardScope(auth.ScopeItemRead)) {
		return
	}
	td, err := s.cat.Describe(req.Table)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	keys := make([]types.Item, 0, len(req.Keys))
	for i, raw := range req.Keys {
		k, err := ddbjson.DecodeItem(raw)
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("key %d: %w", i, err))
			return
		}
		keys = append(keys, k)
	}
	items, err := s.batchGetByShard(req.Table, td.KeySchema, keys)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]map[string]ddbjson.Attribute, len(items))
	for i, it := range items {
		if it == nil {
			out[i] = nil
			continue
		}
		out[i] = ddbjson.EncodeItem(it)
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

type sqlRequest struct {
	Query string `json:"query"`
}

func (s *Server) handleSql(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req sqlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	// Parse first so we can enforce the right scope per statement
	// type before the executor touches storage.
	stmt, err := cefassql.Parse(req.Query)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if !sqlEnforceScope(w, r, stmt) {
		return
	}
	plan, err := cefassql.PlanStmt(stmt, s.cat)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	ex := &cefassql.Executor{Storage: s.db, Catalog: s.cat}
	res, err := ex.Execute(plan)
	if err != nil {
		writeWriteErr(w, r, err)
		return
	}
	out := struct {
		AffectedRows int                            `json:"affectedRows"`
		Rows         []map[string]ddbjson.Attribute `json:"rows,omitempty"`
	}{AffectedRows: res.AffectedRows}
	for _, row := range res.Rows {
		out.Rows = append(out.Rows, ddbjson.EncodeItem(row))
	}
	writeJSON(w, http.StatusOK, out)
}

// sqlEnforceScope routes per-statement scope checks. SELECT requires
// the read or query scope on the affected table; DML requires write.
// CREATE/DROP follow the table-level scopes. Returns false when the
// response is already written.
func sqlEnforceScope(w http.ResponseWriter, r *http.Request, stmt cefassql.Stmt) bool {
	switch s := stmt.(type) {
	case *cefassql.SelectStmt:
		return auth.RequireAnyScope(w, r,
			auth.TableScope(auth.ScopeQuery, s.Table),
			auth.WildcardScope(auth.ScopeQuery),
			auth.TableScope(auth.ScopeItemRead, s.Table),
			auth.WildcardScope(auth.ScopeItemRead),
		)
	case *cefassql.InsertStmt:
		return auth.RequireAnyScope(w, r,
			auth.TableScope(auth.ScopeItemWrite, s.Table),
			auth.WildcardScope(auth.ScopeItemWrite),
		)
	case *cefassql.UpdateStmt:
		return auth.RequireAnyScope(w, r,
			auth.TableScope(auth.ScopeItemWrite, s.Table),
			auth.WildcardScope(auth.ScopeItemWrite),
		)
	case *cefassql.DeleteStmt:
		return auth.RequireAnyScope(w, r,
			auth.TableScope(auth.ScopeItemDelete, s.Table),
			auth.WildcardScope(auth.ScopeItemDelete),
		)
	case *cefassql.CreateTableStmt:
		return auth.RequireAnyScope(w, r, auth.ScopeTableCreate)
	case *cefassql.DropTableStmt:
		return auth.RequireAnyScope(w, r, auth.ScopeTableDrop)
	}
	return true
}

// partiqlRequest mirrors the AWS DynamoDB ExecuteStatement shape:
// `Statement` is the SQL text, `Parameters` substitute `?` markers in
// order. The handler binds the parameters into the SQL text and then
// runs the regular cefas SQL pipeline so the result shape matches
// /v1/Sql.
type partiqlRequest struct {
	Statement  string                      `json:"Statement"`
	Parameters []cefassql.PartiQLParameter `json:"Parameters,omitempty"`
}

func (s *Server) handlePartiQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req partiqlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	bound, err := cefassql.BindPartiQL(req.Statement, req.Parameters)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	stmt, err := cefassql.Parse(bound)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if !sqlEnforceScope(w, r, stmt) {
		return
	}
	plan, err := cefassql.PlanStmt(stmt, s.cat)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	ex := &cefassql.Executor{Storage: s.db, Catalog: s.cat}
	res, err := ex.Execute(plan)
	if err != nil {
		writeWriteErr(w, r, err)
		return
	}
	out := struct {
		AffectedRows int                            `json:"affectedRows"`
		Rows         []map[string]ddbjson.Attribute `json:"rows,omitempty"`
	}{AffectedRows: res.AffectedRows}
	for _, row := range res.Rows {
		out.Rows = append(out.Rows, ddbjson.EncodeItem(row))
	}
	writeJSON(w, http.StatusOK, out)
}

type spatialQueryRequest struct {
	Table     string      `json:"table"`
	IndexName string      `json:"indexName"`
	BBox      *bboxJSON   `json:"bbox,omitempty"`
	Radius    *radiusJSON `json:"radius,omitempty"`
	Z         *zboxJSON   `json:"z,omitempty"`
	Limit     int         `json:"limit,omitempty"`
}

type bboxJSON struct {
	MinLat float64 `json:"minLat"`
	MinLon float64 `json:"minLon"`
	MaxLat float64 `json:"maxLat"`
	MaxLon float64 `json:"maxLon"`
}

type radiusJSON struct {
	Lat    float64 `json:"lat"`
	Lon    float64 `json:"lon"`
	Meters float64 `json:"meters"`
}

type zboxJSON struct {
	Lo []uint32 `json:"lo"`
	Hi []uint32 `json:"hi"`
}

func (s *Server) handleSpatialQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req spatialQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeSpatial, req.Table),
		auth.WildcardScope(auth.ScopeSpatial)) {
		return
	}
	td, err := s.cat.Describe(req.Table)
	if err != nil {
		writeErr(w, http.StatusNotFound, err)
		return
	}
	q := storage.SpatialQuery{Limit: req.Limit}
	switch {
	case req.BBox != nil:
		b := spatial.BBox{
			MinLat: req.BBox.MinLat,
			MinLon: req.BBox.MinLon,
			MaxLat: req.BBox.MaxLat,
			MaxLon: req.BBox.MaxLon,
		}
		q.BBox = &b
	case req.Radius != nil:
		q.Radius = &storage.RadiusQuery{
			Lat:    req.Radius.Lat,
			Lon:    req.Radius.Lon,
			Meters: req.Radius.Meters,
		}
	case req.Z != nil:
		q.Z = &spatial.ZBBox{Lo: req.Z.Lo, Hi: req.Z.Hi}
	default:
		writeErr(w, http.StatusBadRequest, fmt.Errorf("one of bbox/radius/z required"))
		return
	}
	items, err := s.spatialAllShards(td, req.IndexName, q)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, types.ErrSpatialNotFound) {
			status = http.StatusNotFound
		} else if errors.Is(err, types.ErrInvalidSpatial) {
			status = http.StatusBadRequest
		}
		writeErr(w, status, err)
		return
	}
	out := make([]map[string]ddbjson.Attribute, 0, len(items))
	for _, it := range items {
		out = append(out, ddbjson.EncodeItem(it))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

// ---------- cluster endpoints ----------

type clusterStatusResponse struct {
	Mode              string                   `json:"mode"` // "single-node" or "raft"
	IsLeader          bool                     `json:"isLeader"`
	SelfID            string                   `json:"selfId,omitempty"`
	BindAddr          string                   `json:"bindAddr,omitempty"`
	LeaderHTTP        string                   `json:"leaderHttp,omitempty"`
	RoutingEpoch      uint64                   `json:"routingEpoch,omitempty"`
	PlacementVersion  uint64                   `json:"placementVersion,omitempty"`
	ShardCount        int                      `json:"shardCount,omitempty"`
	PlacementStrategy string                   `json:"placementStrategy,omitempty"`
	Shards            []cluster.ShardPlacement `json:"shards,omitempty"`
	Nodes             []cluster.NodeDescriptor `json:"nodes,omitempty"`
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	resp := clusterStatusResponse{Mode: "single-node"}
	if s.cluster != nil {
		resp.Mode = "raft"
		resp.IsLeader = s.cluster.IsLeader()
		resp.SelfID = s.cluster.SelfID()
		resp.BindAddr = s.cluster.BindAddr()
		resp.LeaderHTTP = s.cluster.LeaderHTTPAddr()
	}
	if s.manager != nil {
		_ = s.manager.RefreshPlacement()
		placement := s.manager.Placement()
		resp.RoutingEpoch = placement.Epoch
		resp.PlacementVersion = placement.Version
		resp.ShardCount = len(placement.Shards)
		resp.PlacementStrategy = placement.Strategy
		resp.Shards = append([]cluster.ShardPlacement(nil), placement.Shards...)
		resp.Nodes = sortedPlacementNodes(placement)
	}
	writeJSON(w, http.StatusOK, resp)
}

func sortedPlacementNodes(placement cluster.PlacementCatalog) []cluster.NodeDescriptor {
	out := make([]cluster.NodeDescriptor, 0, len(placement.Nodes))
	for _, node := range placement.Nodes {
		node.Capacity.Tags = append([]string(nil), node.Capacity.Tags...)
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

type addVoterRequest struct {
	ID        string  `json:"id"`
	Addr      string  `json:"addr"`
	TimeoutMS int     `json:"timeoutMs,omitempty"`
	ShardID   *uint32 `json:"shardId,omitempty"`
	AllShards bool    `json:"allShards,omitempty"`
}

func (s *Server) handleClusterAddVoter(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	var req addVoterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if s.manager != nil && req.AllShards {
		if err := s.manager.AddVoterAllShards(req.ID, req.Addr, timeout); err != nil {
			writeWriteErr(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, struct{}{})
		return
	}
	if s.manager != nil && req.ShardID != nil {
		if err := s.manager.AddShardVoter(*req.ShardID, req.ID, req.Addr, timeout); err != nil {
			writeWriteErr(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, struct{}{})
		return
	}
	if s.cluster == nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("cluster not configured"))
		return
	}
	if err := s.cluster.AddVoter(req.ID, req.Addr, timeout); err != nil {
		writeWriteErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

type removeServerRequest struct {
	ID        string  `json:"id"`
	TimeoutMS int     `json:"timeoutMs,omitempty"`
	ShardID   *uint32 `json:"shardId,omitempty"`
	AllShards bool    `json:"allShards,omitempty"`
}

func (s *Server) handleClusterRemoveServer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	var req removeServerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if s.manager != nil && req.AllShards {
		if err := s.manager.RemoveServerAllShards(req.ID, timeout); err != nil {
			writeWriteErr(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, struct{}{})
		return
	}
	if s.manager != nil && req.ShardID != nil {
		if err := s.manager.RemoveShardServer(*req.ShardID, req.ID, timeout); err != nil {
			writeWriteErr(w, r, err)
			return
		}
		writeJSON(w, http.StatusOK, struct{}{})
		return
	}
	if s.cluster == nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("cluster not configured"))
		return
	}
	if err := s.cluster.RemoveServer(req.ID, timeout); err != nil {
		writeWriteErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

// barrierTimeout is the per-request wait the strong-consistency read
// path applies when forcing this node to catch up on the log before
// serving a read.
const barrierTimeout = 5 * time.Second

// ensureStrongRead implements the strong-consistency contract for
// reads: redirect non-leader reads with 307, then Barrier the leader
// so the local Pebble has applied every entry committed before this
// call returned.
//
// Returns true when the caller can proceed with a local read; false
// when the response has already been written and the handler should
// return.
func (s *Server) ensureStrongRead(w http.ResponseWriter, r *http.Request) bool {
	if s.cluster == nil {
		return true
	}
	if !s.cluster.IsLeader() {
		leader := s.cluster.LeaderHTTPAddr()
		if leader != "" {
			w.Header().Set("Location", leader+r.URL.RequestURI())
			http.Error(w, "strong read must hit the leader; redirected", http.StatusTemporaryRedirect)
		} else {
			writeErr(w, http.StatusServiceUnavailable, fmt.Errorf("no leader currently elected"))
		}
		return false
	}
	if err := s.cluster.Barrier(barrierTimeout); err != nil {
		writeErr(w, http.StatusInternalServerError, fmt.Errorf("barrier: %w", err))
		return false
	}
	return true
}

// batchWriteByShard groups ops by the shard that owns each op's PK
// and issues a per-shard BatchWriteItem. Multi-shard mode loses the
// global atomicity guarantee — the trade-off for horizontal scale.
// Single-shard mode collapses to one batch.
func (s *Server) batchWriteByShard(td types.TableDescriptor, ops []storage.BatchOp) error {
	if s.manager == nil {
		return s.db.BatchWriteItem(td, ops)
	}
	buckets := make(map[*storage.DB][]storage.BatchOp)
	for _, op := range ops {
		probe := op.Item
		if op.Op == storage.BatchOpDelete {
			probe = op.Key
		}
		pkBytes, err := pkBytesFromItem(probe, td.KeySchema)
		if err != nil {
			return err
		}
		db := s.storageFor(pkBytes)
		buckets[db] = append(buckets[db], op)
	}
	for db, group := range buckets {
		if err := db.BatchWriteItem(td, group); err != nil {
			return err
		}
	}
	return nil
}

// batchGetByShard routes each key to the shard that owns it and
// preserves input ordering in the response. Misses stay nil.
func (s *Server) batchGetByShard(table string, ks types.KeySchema, keys []types.Item) ([]types.Item, error) {
	if s.manager == nil {
		return s.db.BatchGetItem(table, ks, keys)
	}
	out := make([]types.Item, len(keys))
	for i, k := range keys {
		pkBytes, err := pkBytesFromItem(k, ks)
		if err != nil {
			return nil, err
		}
		db := s.storageFor(pkBytes)
		single, err := db.BatchGetItem(table, ks, []types.Item{k})
		if err != nil {
			return nil, err
		}
		if len(single) == 1 {
			out[i] = single[0]
		}
	}
	return out, nil
}

// spatialAllShards scatter-gathers a spatial query across every
// shard. Spatial indexes are partitioned by the item's PK (same as
// every other write), so the matching rows can live on any shard.
// We merge results client-side; honouring the limit globally.
func (s *Server) spatialAllShards(td types.TableDescriptor, idxName string, q storage.SpatialQuery) ([]types.Item, error) {
	if s.manager == nil {
		return s.db.SpatialQueryItems(td, idxName, q)
	}
	var out []types.Item
	for _, sh := range s.manager.Shards() {
		got, err := sh.Storage.SpatialQueryItems(td, idxName, q)
		if err != nil {
			return nil, err
		}
		out = append(out, got...)
		if q.Limit > 0 && len(out) >= q.Limit {
			out = out[:q.Limit]
			break
		}
	}
	return out, nil
}

// fanOutCatalog mirrors the descriptor to every shard except shard 0
// (which catalog.Create already touched). Single-shard mode is a
// no-op.
func (s *Server) fanOutCatalog(td types.TableDescriptor) {
	if s.manager == nil {
		return
	}
	for i, sh := range s.manager.Shards() {
		if i == 0 {
			continue
		}
		cat, err := catalog.New(sh.Storage)
		if err != nil {
			continue
		}
		_ = cat.Create(td)
	}
}

// instrument wraps an http.Handler with Prometheus latency +
// outcome reporting. Op label is the trailing segment of the URL
// path (e.g. "PutItem"); table label is left empty because we'd
// need to parse every request body to fill it — per-table metrics
// can be added in a follow-up by handlers calling s.metrics.Observe
// directly with the resolved table name.
func (s *Server) instrument(path string, h http.Handler) http.Handler {
	op := path
	if idx := strings.LastIndex(path, "/"); idx >= 0 && idx+1 < len(path) {
		op = path[idx+1:]
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		h.ServeHTTP(rw, r)
		outcome := classify(rw.status)
		s.metrics.Observe(op, "", outcome, time.Since(start).Seconds())
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func classify(status int) string {
	switch {
	case status >= 500:
		return "err"
	case status == 401:
		return "unauth"
	case status == 403:
		return "forbidden"
	case status == 412:
		return "precondition_failed"
	case status == 404:
		return "notfound"
	case status == 307:
		return "notleader"
	case status >= 400:
		return "client_err"
	}
	return "ok"
}

// pkBytesFromItem extracts the canonical PK byte form from `item`
// under the supplied key schema. Shared by every router decision.
func pkBytesFromItem(item types.Item, ks types.KeySchema) ([]byte, error) {
	pkAttr, ok := item[ks.PK]
	if !ok {
		return nil, fmt.Errorf("%w: %q", types.ErrMissingKey, ks.PK)
	}
	return storage.AttrCanonicalBytes(pkAttr)
}

// ---------- helpers ----------

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func mapWriteErr(err error) int {
	if errors.Is(err, types.ErrMissingKey) || errors.Is(err, types.ErrInvalidKeyType) {
		return http.StatusBadRequest
	}
	if errors.Is(err, storage.ErrConditionFailed) {
		// 412 Precondition Failed is the canonical status for an
		// optimistic-concurrency check that did not hold.
		return http.StatusPreconditionFailed
	}
	if errors.Is(err, storage.ErrNotLeader) {
		// Handled separately in writeWriteErr — we want 307 with a
		// Location header, not a JSON body.
		return http.StatusTemporaryRedirect
	}
	if errors.Is(err, storage.ErrThrottled) {
		return http.StatusTooManyRequests
	}
	return http.StatusInternalServerError
}

// writeWriteErr is the shared error-emit path for write handlers. It
// turns a NotLeaderError into a real 307 redirect when the leader's
// HTTP URL is known; falls back to JSON otherwise.
func writeWriteErr(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, storage.ErrNotLeader) {
		var nle *storage.NotLeaderError
		leader := ""
		if errors.As(err, &nle) {
			leader = nle.LeaderURL
		}
		if leader != "" {
			// Redirect preserves method + body on 307. The client
			// resubmits the same POST to the leader's HTTP listener.
			w.Header().Set("Location", leader+r.URL.RequestURI())
			http.Error(w, "not leader; redirected", http.StatusTemporaryRedirect)
			return
		}
		// Leader unknown (election in progress). 503 lets clients
		// back off and retry — there is no useful URL to redirect to.
		writeErr(w, http.StatusServiceUnavailable, err)
		return
	}
	writeErr(w, mapWriteErr(err), err)
}
