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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/osvaldoandrade/cefas/internal/auth"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/metrics"
	"github.com/osvaldoandrade/cefas/internal/spatial"
	"github.com/osvaldoandrade/cefas/internal/storage"
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

type Server struct {
	db        *storage.DB
	cat       *catalog.Catalog
	cluster   Cluster          // nil when not running in raft mode
	manager   *cluster.Manager // nil when single-shard
	validator *auth.Validator  // nil when auth disabled (dev mode)
	metrics   *metrics.Metrics // nil when metrics disabled
}

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

// AttachCluster wires the optional cluster-management surface. Pass a
// *raft.DB; nil keeps the server in single-node mode.
func (s *Server) AttachCluster(c Cluster) { s.cluster = c }

// AttachAuth wires bearer-token validation onto every non-public
// route. Pass nil to keep the server open (dev mode).
func (s *Server) AttachAuth(v *auth.Validator) { s.validator = v }

// publicPaths bypass the auth middleware so probes and cluster status
// stay reachable on an unjoined or pre-credentialed node.
var publicPaths = map[string]bool{
	"/v1/Health":          true,
	"/v1/cluster/status":  true,
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
	register("/v1/tables", s.handleTables)              // POST=create, GET=list
	register("/v1/tables/", s.handleTable)              // GET=describe
	register("/v1/PutItem", s.handlePutItem)
	register("/v1/GetItem", s.handleGetItem)
	register("/v1/DeleteItem", s.handleDeleteItem)
	register("/v1/Query", s.handleQuery)
	register("/v1/BatchWriteItem", s.handleBatchWriteItem)
	register("/v1/BatchGetItem", s.handleBatchGetItem)
	register("/v1/SpatialQuery", s.handleSpatialQuery)
	register("/v1/Sql", s.handleSql)
	register("/v1/Health", s.handleHealth)
	register("/v1/cluster/status", s.handleClusterStatus)
	register("/v1/cluster/AddVoter", s.handleClusterAddVoter)
	register("/v1/cluster/RemoveServer", s.handleClusterRemoveServer)
	if s.metrics != nil {
		mux.Handle("/metrics", s.metrics.Handler())
	}
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
	Name           string                          `json:"name"`
	KeySchema      jsonKeySchema                   `json:"keySchema"`
	GSIs           []types.GSIDescriptor           `json:"gsis,omitempty"`
	SpatialIndexes []types.SpatialIndexDescriptor  `json:"spatialIndexes,omitempty"`
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
	Table     string                    `json:"table"`
	Item      map[string]jsonAttribute  `json:"item"`
	Condition string                    `json:"condition,omitempty"`
	Binds     map[string]jsonAttribute  `json:"binds,omitempty"`
}

type getItemRequest struct {
	Table       string                   `json:"table"`
	Key         map[string]jsonAttribute `json:"key"`
	Consistency string                   `json:"consistency,omitempty"` // "" or "eventual" → local; "strong" → leader + barrier
}

type deleteItemRequest struct {
	Table     string                   `json:"table"`
	Key       map[string]jsonAttribute `json:"key"`
	Condition string                   `json:"condition,omitempty"`
	Binds     map[string]jsonAttribute `json:"binds,omitempty"`
}

type queryRequest struct {
	Table       string         `json:"table"`
	IndexName   string         `json:"indexName,omitempty"`
	PKValue     jsonAttribute  `json:"pkValue"`
	SKLow       *jsonAttribute `json:"skLow,omitempty"`
	SKHigh      *jsonAttribute `json:"skHigh,omitempty"`
	Limit       int            `json:"limit,omitempty"`
	Consistency string         `json:"consistency,omitempty"`
}

type batchWriteOp struct {
	Op   string                   `json:"op"` // "put" | "delete"
	Item map[string]jsonAttribute `json:"item,omitempty"`
	Key  map[string]jsonAttribute `json:"key,omitempty"`
}

type batchWriteRequest struct {
	Table string         `json:"table"`
	Ops   []batchWriteOp `json:"ops"`
}

type batchGetRequest struct {
	Table string                     `json:"table"`
	Keys  []map[string]jsonAttribute `json:"keys"`
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
	item, err := decodeItem(req.Item)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	binds, err := decodeBinds(req.Binds)
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
	keyAttrs, err := decodeItem(req.Key)
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
		"item":  encodeItem(item),
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
	keyAttrs, err := decodeItem(req.Key)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	binds, err := decodeBinds(req.Binds)
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
	pkVal, err := req.PKValue.toAttr()
	if err != nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("pkValue: %w", err))
		return
	}
	var lo, hi types.AttributeValue
	if req.SKLow != nil {
		lo, err = req.SKLow.toAttr()
		if err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("skLow: %w", err))
			return
		}
	}
	if req.SKHigh != nil {
		hi, err = req.SKHigh.toAttr()
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

	out := make([]map[string]jsonAttribute, 0, len(items))
	for _, it := range items {
		out = append(out, encodeItem(it))
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
			item, err := decodeItem(raw.Item)
			if err != nil {
				writeErr(w, http.StatusBadRequest, fmt.Errorf("op %d: %w", i, err))
				return
			}
			ops = append(ops, storage.BatchOp{Op: storage.BatchOpPut, Item: item})
		case "delete":
			keyAttrs, err := decodeItem(raw.Key)
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
		k, err := decodeItem(raw)
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
	out := make([]map[string]jsonAttribute, len(items))
	for i, it := range items {
		if it == nil {
			out[i] = nil
			continue
		}
		out[i] = encodeItem(it)
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
		AffectedRows int                              `json:"affectedRows"`
		Rows         []map[string]jsonAttribute       `json:"rows,omitempty"`
	}{AffectedRows: res.AffectedRows}
	for _, row := range res.Rows {
		out.Rows = append(out.Rows, encodeItem(row))
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

type spatialQueryRequest struct {
	Table     string         `json:"table"`
	IndexName string         `json:"indexName"`
	BBox      *bboxJSON      `json:"bbox,omitempty"`
	Radius    *radiusJSON    `json:"radius,omitempty"`
	Z         *zboxJSON      `json:"z,omitempty"`
	Limit     int            `json:"limit,omitempty"`
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
	out := make([]map[string]jsonAttribute, 0, len(items))
	for _, it := range items {
		out = append(out, encodeItem(it))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out})
}

// ---------- cluster endpoints ----------

type clusterStatusResponse struct {
	Mode       string `json:"mode"`              // "single-node" or "raft"
	IsLeader   bool   `json:"isLeader"`
	SelfID     string `json:"selfId,omitempty"`
	BindAddr   string `json:"bindAddr,omitempty"`
	LeaderHTTP string `json:"leaderHttp,omitempty"`
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
	writeJSON(w, http.StatusOK, resp)
}

type addVoterRequest struct {
	ID        string `json:"id"`
	Addr      string `json:"addr"`
	TimeoutMS int    `json:"timeoutMs,omitempty"`
}

func (s *Server) handleClusterAddVoter(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("cluster not configured"))
		return
	}
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
	if err := s.cluster.AddVoter(req.ID, req.Addr, timeout); err != nil {
		writeWriteErr(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, struct{}{})
}

type removeServerRequest struct {
	ID        string `json:"id"`
	TimeoutMS int    `json:"timeoutMs,omitempty"`
}

func (s *Server) handleClusterRemoveServer(w http.ResponseWriter, r *http.Request) {
	if s.cluster == nil {
		writeErr(w, http.StatusBadRequest, fmt.Errorf("cluster not configured"))
		return
	}
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

func decodeBinds(in map[string]jsonAttribute) (map[string]types.AttributeValue, error) {
	if in == nil {
		return nil, nil
	}
	out := make(map[string]types.AttributeValue, len(in))
	for k, a := range in {
		v, err := a.toAttr()
		if err != nil {
			return nil, fmt.Errorf("bind :%s: %w", k, err)
		}
		out[k] = v
	}
	return out, nil
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

// jsonAttribute is the wire form of an AttributeValue. Exactly one of
// the tagged fields is set on the wire — mirrors DynamoDB JSON.
type jsonAttribute struct {
	S    *string                   `json:"S,omitempty"`
	N    *string                   `json:"N,omitempty"`
	B    *string                   `json:"B,omitempty"` // base64
	BOOL *bool                     `json:"BOOL,omitempty"`
	NULL *bool                     `json:"NULL,omitempty"`
	SS   []string                  `json:"SS,omitempty"`
	NS   []string                  `json:"NS,omitempty"`
	BS   []string                  `json:"BS,omitempty"` // base64 each
	L    []jsonAttribute           `json:"L,omitempty"`
	M    map[string]jsonAttribute  `json:"M,omitempty"`
}

func (a jsonAttribute) toAttr() (types.AttributeValue, error) {
	switch {
	case a.S != nil:
		return types.AttributeValue{T: types.AttrS, S: *a.S}, nil
	case a.N != nil:
		return types.AttributeValue{T: types.AttrN, N: *a.N}, nil
	case a.B != nil:
		raw, err := base64.StdEncoding.DecodeString(*a.B)
		if err != nil {
			return types.AttributeValue{}, fmt.Errorf("invalid base64 in B: %w", err)
		}
		return types.AttributeValue{T: types.AttrB, B: raw}, nil
	case a.BOOL != nil:
		return types.AttributeValue{T: types.AttrBOOL, BOOL: *a.BOOL}, nil
	case a.NULL != nil && *a.NULL:
		return types.AttributeValue{T: types.AttrNull}, nil
	case a.SS != nil:
		return types.AttributeValue{T: types.AttrSS, SS: a.SS}, nil
	case a.NS != nil:
		return types.AttributeValue{T: types.AttrNS, NS: a.NS}, nil
	case a.BS != nil:
		bs := make([][]byte, len(a.BS))
		for i, s := range a.BS {
			raw, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return types.AttributeValue{}, fmt.Errorf("invalid base64 in BS[%d]: %w", i, err)
			}
			bs[i] = raw
		}
		return types.AttributeValue{T: types.AttrBS, BS: bs}, nil
	case a.L != nil:
		list := make([]types.AttributeValue, len(a.L))
		for i, av := range a.L {
			v, err := av.toAttr()
			if err != nil {
				return types.AttributeValue{}, fmt.Errorf("L[%d]: %w", i, err)
			}
			list[i] = v
		}
		return types.AttributeValue{T: types.AttrL, L: list}, nil
	case a.M != nil:
		m := make(map[string]types.AttributeValue, len(a.M))
		for k, av := range a.M {
			v, err := av.toAttr()
			if err != nil {
				return types.AttributeValue{}, fmt.Errorf("M[%q]: %w", k, err)
			}
			m[k] = v
		}
		return types.AttributeValue{T: types.AttrM, M: m}, nil
	}
	return types.AttributeValue{}, fmt.Errorf("attribute value has no field set")
}

func fromAttr(av types.AttributeValue) jsonAttribute {
	switch av.T {
	case types.AttrNull:
		t := true
		return jsonAttribute{NULL: &t}
	case types.AttrS:
		s := av.S
		return jsonAttribute{S: &s}
	case types.AttrN:
		s := av.N
		return jsonAttribute{N: &s}
	case types.AttrB:
		s := base64.StdEncoding.EncodeToString(av.B)
		return jsonAttribute{B: &s}
	case types.AttrBOOL:
		b := av.BOOL
		return jsonAttribute{BOOL: &b}
	case types.AttrSS:
		return jsonAttribute{SS: av.SS}
	case types.AttrNS:
		return jsonAttribute{NS: av.NS}
	case types.AttrBS:
		bs := make([]string, len(av.BS))
		for i, b := range av.BS {
			bs[i] = base64.StdEncoding.EncodeToString(b)
		}
		return jsonAttribute{BS: bs}
	case types.AttrL:
		list := make([]jsonAttribute, len(av.L))
		for i, v := range av.L {
			list[i] = fromAttr(v)
		}
		return jsonAttribute{L: list}
	case types.AttrM:
		m := make(map[string]jsonAttribute, len(av.M))
		for k, v := range av.M {
			m[k] = fromAttr(v)
		}
		return jsonAttribute{M: m}
	}
	return jsonAttribute{}
}

func decodeItem(in map[string]jsonAttribute) (types.Item, error) {
	out := make(types.Item, len(in))
	for k, a := range in {
		v, err := a.toAttr()
		if err != nil {
			return nil, fmt.Errorf("attribute %q: %w", k, err)
		}
		out[k] = v
	}
	return out, nil
}

func encodeItem(in types.Item) map[string]jsonAttribute {
	out := make(map[string]jsonAttribute, len(in))
	for k, v := range in {
		out[k] = fromAttr(v)
	}
	return out
}
