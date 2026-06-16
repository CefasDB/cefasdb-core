// Package table owns the HTTP handlers for the table-resource
// endpoints (/v1/tables, /v1/tables/<name>).
//
// Handlers are exposed as methods on *Handlers so the composition
// root (pkg/api.Server) can wrap each handler with its standard
// auth + metrics middleware via the same register helper it uses
// for every other route. The package depends only on:
//
//   - catalog.Catalog        — table metadata read/write
//   - internal/auth          — scope checks
//   - pkg/api/http/httpx     — JSON write helpers
//   - pkg/types              — wire types
//
// It deliberately has no back-channel into pkg/api so the import
// graph stays one-way (pkg/api → pkg/api/http/table, never the
// reverse).
package table

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/osvaldoandrade/cefas/internal/auth"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/api/http/httpx"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// FanOutFunc replicates a freshly-created table descriptor to every
// shard the server manages so writes routed to any shard can resolve
// the schema locally. Shard 0 already has it via Catalog.Create. The
// table package never knows how many shards exist — that detail
// stays in pkg/api where the cluster.Manager lives.
type FanOutFunc func(td types.TableDescriptor)

// Handlers carries the dependencies every table handler needs. Build
// it once during pkg/api.Server.Routes and let the server register
// the methods with its existing middleware stack.
type Handlers struct {
	cat     *catalog.Catalog
	fanOut  FanOutFunc
}

// New constructs the handler set. fanOut may be nil — in that case
// Create skips the replication step (single-shard / dev mode).
func New(cat *catalog.Catalog, fanOut FanOutFunc) *Handlers {
	return &Handlers{cat: cat, fanOut: fanOut}
}

type createTableRequest struct {
	Name                string                         `json:"name"`
	KeySchema           jsonKeySchema                  `json:"keySchema"`
	GSIs                []types.GSIDescriptor          `json:"gsis,omitempty"`
	SpatialIndexes      []types.SpatialIndexDescriptor `json:"spatialIndexes,omitempty"`
	StreamSpecification *types.StreamSpecification     `json:"streamSpecification,omitempty"`
}

type jsonKeySchema struct {
	PK string `json:"pk"`
	SK string `json:"sk,omitempty"`
}

// HandleTables serves /v1/tables: POST creates a table, GET lists
// every table descriptor.
func (h *Handlers) HandleTables(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		h.create(w, r)
	case http.MethodGet:
		h.list(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// HandleTable serves /v1/tables/<name>: GET describes a single table.
func (h *Handlers) HandleTable(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Path[len("/v1/tables/"):]
	if name == "" {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("table name required"))
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeTableDescribe) {
		return
	}
	td, err := h.cat.Describe(name)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, types.ErrTableNotFound) {
			status = http.StatusNotFound
		}
		httpx.WriteErr(w, status, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, td)
}

func (h *Handlers) create(w http.ResponseWriter, r *http.Request) {
	if !auth.RequireAnyScope(w, r, auth.ScopeTableCreate) {
		return
	}
	var req createTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	td := types.TableDescriptor{
		Name:                req.Name,
		KeySchema:           types.KeySchema{PK: req.KeySchema.PK, SK: req.KeySchema.SK},
		GSIs:                req.GSIs,
		SpatialIndexes:      req.SpatialIndexes,
		StreamSpecification: req.StreamSpecification,
	}
	if err := h.cat.Create(td); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, types.ErrTableAlreadyExists) {
			status = http.StatusConflict
		}
		httpx.WriteErr(w, status, err)
		return
	}
	created, err := h.cat.Describe(td.Name)
	if err != nil {
		httpx.WriteErr(w, http.StatusInternalServerError, err)
		return
	}
	if h.fanOut != nil {
		h.fanOut(created)
	}
	httpx.WriteJSON(w, http.StatusCreated, created)
}

func (h *Handlers) list(w http.ResponseWriter, r *http.Request) {
	if !auth.RequireAnyScope(w, r, auth.ScopeTableDescribe) {
		return
	}
	httpx.WriteJSON(w, http.StatusOK, h.cat.List())
}
