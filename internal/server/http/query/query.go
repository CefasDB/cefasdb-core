package query

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/CefasDb/cefasdb/internal/server/http/httpx"
	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/spatial"
	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/internal/tracing"
	"github.com/CefasDb/cefasdb/internal/compat/ddbjson"
	cefassql "github.com/CefasDb/cefasdb/internal/sql"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// StorageForFunc returns the pebble.DB that owns the supplied
// partition-key bytes. Single-shard mode always returns the same DB.
type StorageForFunc func(pkBytes []byte) *pebble.DB

// AllShardsFunc enumerates every pebble.DB this server manages so a
// cross-shard query can scatter-gather.
type AllShardsFunc func() []*pebble.DB

// SpatialAllShardsFunc fans a spatial query across every shard and
// merges the results, honouring the global limit.
type SpatialAllShardsFunc func(td types.TableDescriptor, idxName string, q pebble.SpatialQuery) ([]types.Item, error)

// EnsureStrongReadFunc gates a "strong" read on the raft leader and a
// barrier. It writes the redirect/error response and returns false when
// the caller should stop processing.
type EnsureStrongReadFunc func(w http.ResponseWriter, r *http.Request) bool

// ObserveRangeMetricFunc records a per-range latency/byte observation
// against the hotspot dashboards. nil means metrics are disabled.
type ObserveRangeMetricFunc func(op string, pkBytes []byte, approxBytes uint64, started time.Time)

// Range-metric op labels mirror the values internal/api uses.
const (
	// RangeMetricRead labels a read-side range-metric observation.
	RangeMetricRead = "read"
	// RangeMetricWrite labels a write-side range-metric observation.
	RangeMetricWrite = "write"
)

// Handlers carries the dependencies every query handler needs. Build
// it once during internal/api.Server.Routes and let the server register
// the methods with its existing middleware stack.
type Handlers struct {
	cat              *catalog.Catalog
	db               *pebble.DB
	storageFor       StorageForFunc
	allShards        AllShardsFunc
	spatialAllShards SpatialAllShardsFunc
	ensureStrongRead EnsureStrongReadFunc
	observeRange     ObserveRangeMetricFunc
}

// New constructs the handler set. observeRange may be nil — in that
// case the handlers skip the metric callback (metrics disabled).
// ensureStrongRead must not be nil; pass a function that always
// returns true to keep the route open in single-node setups.
func New(
	cat *catalog.Catalog,
	db *pebble.DB,
	storageFor StorageForFunc,
	allShards AllShardsFunc,
	spatialAllShards SpatialAllShardsFunc,
	ensureStrongRead EnsureStrongReadFunc,
	observeRange ObserveRangeMetricFunc,
) *Handlers {
	return &Handlers{
		cat:              cat,
		db:               db,
		storageFor:       storageFor,
		allShards:        allShards,
		spatialAllShards: spatialAllShards,
		ensureStrongRead: ensureStrongRead,
		observeRange:     observeRange,
	}
}

// ---------- request types ----------

type queryRequest struct {
	Table       string             `json:"table"`
	IndexName   string             `json:"indexName,omitempty"`
	PKValue     ddbjson.Attribute  `json:"pkValue"`
	SKLow       *ddbjson.Attribute `json:"skLow,omitempty"`
	SKHigh      *ddbjson.Attribute `json:"skHigh,omitempty"`
	Limit       int                `json:"limit,omitempty"`
	Consistency string             `json:"consistency,omitempty"`
}

type sqlRequest struct {
	Query string `json:"query"`
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

// ---------- handlers ----------

// HandleQuery serves /v1/Query — primary-key, range and indexed
// queries.
func (h *Handlers) HandleQuery(w http.ResponseWriter, r *http.Request) {
	_, span := tracing.Tracer().Start(r.Context(), "Query")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	started := time.Now()
	var req queryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeQuery, req.Table),
		auth.WildcardScope(auth.ScopeQuery)) {
		return
	}
	if req.Consistency == "strong" && !h.ensureStrongRead(w, r) {
		return
	}
	td, err := h.cat.Describe(req.Table)
	if err != nil {
		httpx.WriteErr(w, http.StatusNotFound, err)
		return
	}
	pkVal, err := req.PKValue.ToAttr()
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("pkValue: %w", err))
		return
	}
	var lo, hi types.AttributeValue
	if req.SKLow != nil {
		lo, err = req.SKLow.ToAttr()
		if err != nil {
			httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("skLow: %w", err))
			return
		}
	}
	if req.SKHigh != nil {
		hi, err = req.SKHigh.ToAttr()
		if err != nil {
			httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("skHigh: %w", err))
			return
		}
	}

	pkBytes, err := storage.AttrCanonicalBytes(pkVal)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("pkValue: %w", err))
		return
	}
	queryDB := h.storageFor(pkBytes)
	var items []types.Item
	if req.IndexName != "" {
		items, err = h.queryByIndex(td, req.IndexName, pkVal, pebble.QueryOptions{
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
		httpx.WriteErr(w, http.StatusInternalServerError, err)
		return
	}
	if h.observeRange != nil {
		h.observeRange(RangeMetricRead, pkBytes, estimatedItemsBytes(items), started)
	}

	out := make([]map[string]ddbjson.Attribute, 0, len(items))
	for _, it := range items {
		out = append(out, ddbjson.EncodeItem(it))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": out})
}

// HandleSpatialQuery serves /v1/SpatialQuery — bbox / radius / Z-box
// lookups against a table's spatial index.
func (h *Handlers) HandleSpatialQuery(w http.ResponseWriter, r *http.Request) {
	_, span := tracing.Tracer().Start(r.Context(), "SpatialQuery")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req spatialQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeSpatial, req.Table),
		auth.WildcardScope(auth.ScopeSpatial)) {
		return
	}
	td, err := h.cat.Describe(req.Table)
	if err != nil {
		httpx.WriteErr(w, http.StatusNotFound, err)
		return
	}
	q := pebble.SpatialQuery{Limit: req.Limit}
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
		q.Radius = &pebble.RadiusQuery{
			Lat:    req.Radius.Lat,
			Lon:    req.Radius.Lon,
			Meters: req.Radius.Meters,
		}
	case req.Z != nil:
		q.Z = &spatial.ZBBox{Lo: req.Z.Lo, Hi: req.Z.Hi}
	default:
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("one of bbox/radius/z required"))
		return
	}
	items, err := h.spatialAllShards(td, req.IndexName, q)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, types.ErrSpatialNotFound) {
			status = http.StatusNotFound
		} else if errors.Is(err, types.ErrInvalidSpatial) {
			status = http.StatusBadRequest
		}
		httpx.WriteErr(w, status, err)
		return
	}
	out := make([]map[string]ddbjson.Attribute, 0, len(items))
	for _, it := range items {
		out = append(out, ddbjson.EncodeItem(it))
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": out})
}

// HandleSql serves /v1/Sql — parses, plans and executes a single SQL
// statement.
func (h *Handlers) HandleSql(w http.ResponseWriter, r *http.Request) {
	_, span := tracing.Tracer().Start(r.Context(), "Sql")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req sqlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	// Parse first so we can enforce the right scope per statement
	// type before the executor touches storage.
	stmt, err := cefassql.Parse(req.Query)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	if !sqlEnforceScope(w, r, stmt) {
		return
	}
	h.executeSQL(w, r, stmt)
}

// HandlePartiQL serves /v1/PartiQL — AWS DynamoDB ExecuteStatement
// shape (statement + positional parameters).
func (h *Handlers) HandlePartiQL(w http.ResponseWriter, r *http.Request) {
	_, span := tracing.Tracer().Start(r.Context(), "PartiQL")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req partiqlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	bound, err := cefassql.BindPartiQL(req.Statement, req.Parameters)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	stmt, err := cefassql.Parse(bound)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	if !sqlEnforceScope(w, r, stmt) {
		return
	}
	h.executeSQL(w, r, stmt)
}

// executeSQL runs the planner + executor over a parsed statement and
// emits the standard {affectedRows, rows} response shape shared by
// /v1/Sql and /v1/PartiQL.
func (h *Handlers) executeSQL(w http.ResponseWriter, r *http.Request, stmt cefassql.Stmt) {
	plan, err := cefassql.PlanStmt(stmt, h.cat)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	ex := &cefassql.Executor{Storage: h.db, Catalog: h.cat}
	res, err := ex.Execute(plan)
	if err != nil {
		httpx.WriteErr(w, http.StatusInternalServerError, err)
		return
	}
	out := struct {
		AffectedRows int                            `json:"affectedRows"`
		Rows         []map[string]ddbjson.Attribute `json:"rows,omitempty"`
	}{AffectedRows: res.AffectedRows}
	for _, row := range res.Rows {
		out.Rows = append(out.Rows, ddbjson.EncodeItem(row))
	}
	httpx.WriteJSON(w, http.StatusOK, out)
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

// ---------- index routing ----------

// queryByIndex picks the right shard pattern for an indexed query. GSI
// hits scatter-gather across every shard; LSI hits target the single
// shard that owns the primary PK.
func (h *Handlers) queryByIndex(td types.TableDescriptor, indexName string, pkVal types.AttributeValue, opts pebble.QueryOptions) ([]types.Item, error) {
	if hasGSI(td, indexName) {
		return queryGSIAcrossShards(h.allShards(), td, indexName, pkVal, opts)
	}
	if hasLSI(td, indexName) {
		pkBytes, err := storage.AttrCanonicalBytes(pkVal)
		if err != nil {
			return nil, fmt.Errorf("primary PK: %w", err)
		}
		return h.storageFor(pkBytes).QueryByLSI(td, indexName, pkVal, opts)
	}
	return nil, fmt.Errorf("table %q has no index named %q", td.Name, indexName)
}

func queryGSIAcrossShards(dbs []*pebble.DB, td types.TableDescriptor, indexName string, pkVal types.AttributeValue, opts pebble.QueryOptions) ([]types.Item, error) {
	var out []types.Item
	seen := make(map[string]struct{})
	shardOpts := opts
	shardOpts.Limit = 0
	for _, db := range dbs {
		got, err := db.QueryByGSI(td, indexName, pkVal, shardOpts)
		if err != nil {
			return nil, err
		}
		for _, item := range got {
			id, err := primaryIdentity(item, td.KeySchema)
			if err != nil {
				return nil, err
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, item)
			if opts.Limit > 0 && len(out) >= opts.Limit {
				return out, nil
			}
		}
	}
	return out, nil
}

func hasGSI(td types.TableDescriptor, name string) bool {
	for _, g := range td.GSIs {
		if g.Name == name {
			return true
		}
	}
	return false
}

func hasLSI(td types.TableDescriptor, name string) bool {
	for _, l := range td.LSIs {
		if l.Name == name {
			return true
		}
	}
	return false
}

func primaryIdentity(item types.Item, ks types.KeySchema) (string, error) {
	pk, ok := item[ks.PK]
	if !ok {
		return "", fmt.Errorf("primary PK %q missing on item", ks.PK)
	}
	pkBytes, err := storage.AttrCanonicalBytes(pk)
	if err != nil {
		return "", err
	}
	var skBytes []byte
	if ks.SK != "" {
		sk, ok := item[ks.SK]
		if !ok {
			return "", fmt.Errorf("primary SK %q missing on item", ks.SK)
		}
		skBytes, err = storage.AttrCanonicalBytes(sk)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%d:%s/%d:%s", len(pkBytes), string(pkBytes), len(skBytes), string(skBytes)), nil
}

// ---------- size estimation ----------

func estimatedItemsBytes(items []types.Item) uint64 {
	var n uint64
	for _, item := range items {
		n += estimatedItemBytes(item)
	}
	return n
}

func estimatedItemBytes(item types.Item) uint64 {
	var n uint64
	for name, value := range item {
		n += uint64(len(name)) + estimatedAttrBytes(value)
	}
	return n
}

func estimatedAttrBytes(v types.AttributeValue) uint64 {
	switch v.T {
	case types.AttrS, types.AttrN:
		return uint64(len(v.S) + len(v.N))
	case types.AttrB:
		return uint64(len(v.B))
	case types.AttrBOOL, types.AttrNull:
		return 1
	case types.AttrSS:
		var n uint64
		for _, s := range v.SS {
			n += uint64(len(s))
		}
		return n
	case types.AttrNS:
		var n uint64
		for _, s := range v.NS {
			n += uint64(len(s))
		}
		return n
	case types.AttrBS:
		var n uint64
		for _, b := range v.BS {
			n += uint64(len(b))
		}
		return n
	case types.AttrL:
		var n uint64
		for _, item := range v.L {
			n += estimatedAttrBytes(item)
		}
		return n
	case types.AttrM:
		var n uint64
		for name, item := range v.M {
			n += uint64(len(name)) + estimatedAttrBytes(item)
		}
		return n
	default:
		return 0
	}
}
