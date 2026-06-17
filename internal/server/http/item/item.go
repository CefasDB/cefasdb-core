package item

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/CefasDb/cefasdb/internal/server/http/httpx"
	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/internal/tracing"
	"github.com/CefasDb/cefasdb/pkg/ddbjson"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// ObserveFunc records a per-shard range-operation metric. item is nil
// when the handler has no body to weigh (e.g. a Delete request, or a
// GetItem that returned ErrItemNotFound) — the callback should fall
// back to len(pkBytes) in that case.
type ObserveFunc func(pkBytes []byte, item types.Item, started time.Time)

// WriteTargets is the minimum surface a PK-routed write needs. The
// production implementation lives in internal/api (routedWriteTargets)
// and replicates writes to mirrors when the cluster manager is wired;
// the single-shard / test implementation collapses to one pebble.DB.
type WriteTargets interface {
	PutItemWith(td types.TableDescriptor, item types.Item, opts pebble.PutOptions) error
	DeleteItemWith(td types.TableDescriptor, key types.Item, opts pebble.DeleteOptions) error
	Release()
}

// Deps bundles every collaborator the item handlers need. internal/api
// builds it once during Server.Routes and passes it to New.
type Deps struct {
	// Cat resolves table descriptors. Required.
	Cat *catalog.Catalog
	// StorageFor returns the pebble.DB that owns pkBytes. Required.
	StorageFor func(pkBytes []byte) *pebble.DB
	// WriteTargetsForPK returns the routed write fan-out for pkBytes.
	// Required.
	WriteTargetsForPK func(pkBytes []byte) (WriteTargets, error)
	// BatchWriteByShard groups ops by owning shard and issues per-shard
	// batches. Required.
	BatchWriteByShard func(td types.TableDescriptor, ops []pebble.BatchOp) error
	// BatchGetByShard routes keys to owning shards and preserves input
	// order. Required.
	BatchGetByShard func(table string, ks types.KeySchema, keys []types.Item) ([]types.Item, error)
	// EnsureStrongRead enforces leader + barrier for strong reads.
	// Required when GetItem accepts "consistency":"strong" requests;
	// a nil callback treats every read as eventual.
	EnsureStrongRead func(w http.ResponseWriter, r *http.Request) bool
	// WriteWriteErr maps storage errors (NotLeader → 307, condition
	// failed → 412, throttled → 429, …) to HTTP responses. Required.
	WriteWriteErr func(w http.ResponseWriter, r *http.Request, err error)
	// ObserveWrite records a write-side range metric. Optional; nil
	// skips observation.
	ObserveWrite ObserveFunc
	// ObserveRead records a read-side range metric. Optional; nil
	// skips observation.
	ObserveRead ObserveFunc
}

// Handlers carries the dependencies every item handler needs.
type Handlers struct {
	deps Deps
}

// New constructs the handler set. It does not validate Deps — the
// composition root is responsible for wiring every required field.
func New(deps Deps) *Handlers {
	return &Handlers{deps: deps}
}

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

// HandlePutItem serves /v1/PutItem.
func (h *Handlers) HandlePutItem(w http.ResponseWriter, r *http.Request) {
	_, span := tracing.Tracer().Start(r.Context(), "PutItem")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	started := time.Now()
	var req putItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeItemWrite, req.Table),
		auth.WildcardScope(auth.ScopeItemWrite)) {
		return
	}
	td, err := h.deps.Cat.Describe(req.Table)
	if err != nil {
		httpx.WriteErr(w, http.StatusNotFound, err)
		return
	}
	item, err := ddbjson.DecodeItem(req.Item)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	binds, err := ddbjson.DecodeBinds(req.Binds)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("binds: %w", err))
		return
	}
	pkBytes, err := pkBytesFromItem(item, td.KeySchema)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	targets, err := h.deps.WriteTargetsForPK(pkBytes)
	if err != nil {
		h.deps.WriteWriteErr(w, r, err)
		return
	}
	defer targets.Release()
	if err := targets.PutItemWith(td, item, pebble.PutOptions{Condition: req.Condition, Binds: binds}); err != nil {
		h.deps.WriteWriteErr(w, r, err)
		return
	}
	h.observeWrite(pkBytes, item, started)
	httpx.WriteJSON(w, http.StatusOK, struct{}{})
}

// HandleGetItem serves /v1/GetItem.
func (h *Handlers) HandleGetItem(w http.ResponseWriter, r *http.Request) {
	_, span := tracing.Tracer().Start(r.Context(), "GetItem")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	started := time.Now()
	var req getItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeItemRead, req.Table),
		auth.WildcardScope(auth.ScopeItemRead)) {
		return
	}
	if req.Consistency == "strong" && h.deps.EnsureStrongRead != nil && !h.deps.EnsureStrongRead(w, r) {
		return
	}
	td, err := h.deps.Cat.Describe(req.Table)
	if err != nil {
		httpx.WriteErr(w, http.StatusNotFound, err)
		return
	}
	keyAttrs, err := ddbjson.DecodeItem(req.Key)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	pkBytes, err := pkBytesFromItem(keyAttrs, td.KeySchema)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	item, err := h.deps.StorageFor(pkBytes).GetItem(req.Table, td.KeySchema, keyAttrs)
	if err != nil {
		if errors.Is(err, types.ErrItemNotFound) {
			h.observeRead(pkBytes, nil, started)
			httpx.WriteJSON(w, http.StatusOK, map[string]any{"found": false})
			return
		}
		httpx.WriteErr(w, http.StatusInternalServerError, err)
		return
	}
	h.observeRead(pkBytes, item, started)
	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"found": true,
		"item":  ddbjson.EncodeItem(item),
	})
}

// HandleDeleteItem serves /v1/DeleteItem.
func (h *Handlers) HandleDeleteItem(w http.ResponseWriter, r *http.Request) {
	_, span := tracing.Tracer().Start(r.Context(), "DeleteItem")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	started := time.Now()
	var req deleteItemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeItemDelete, req.Table),
		auth.WildcardScope(auth.ScopeItemDelete)) {
		return
	}
	td, err := h.deps.Cat.Describe(req.Table)
	if err != nil {
		httpx.WriteErr(w, http.StatusNotFound, err)
		return
	}
	keyAttrs, err := ddbjson.DecodeItem(req.Key)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	binds, err := ddbjson.DecodeBinds(req.Binds)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("binds: %w", err))
		return
	}
	pkBytes, err := pkBytesFromItem(keyAttrs, td.KeySchema)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	targets, err := h.deps.WriteTargetsForPK(pkBytes)
	if err != nil {
		h.deps.WriteWriteErr(w, r, err)
		return
	}
	defer targets.Release()
	if err := targets.DeleteItemWith(td, keyAttrs, pebble.DeleteOptions{Condition: req.Condition, Binds: binds}); err != nil {
		h.deps.WriteWriteErr(w, r, err)
		return
	}
	h.observeWrite(pkBytes, nil, started)
	httpx.WriteJSON(w, http.StatusOK, struct{}{})
}

// HandleBatchWriteItem serves /v1/BatchWriteItem.
func (h *Handlers) HandleBatchWriteItem(w http.ResponseWriter, r *http.Request) {
	_, span := tracing.Tracer().Start(r.Context(), "BatchWriteItem")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req batchWriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeItemWrite, req.Table),
		auth.WildcardScope(auth.ScopeItemWrite)) {
		return
	}
	td, err := h.deps.Cat.Describe(req.Table)
	if err != nil {
		httpx.WriteErr(w, http.StatusNotFound, err)
		return
	}

	ops := make([]pebble.BatchOp, 0, len(req.Ops))
	for i, raw := range req.Ops {
		switch raw.Op {
		case "put":
			item, err := ddbjson.DecodeItem(raw.Item)
			if err != nil {
				httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("op %d: %w", i, err))
				return
			}
			ops = append(ops, pebble.BatchOp{Op: pebble.BatchOpPut, Item: item})
		case "delete":
			keyAttrs, err := ddbjson.DecodeItem(raw.Key)
			if err != nil {
				httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("op %d: %w", i, err))
				return
			}
			ops = append(ops, pebble.BatchOp{Op: pebble.BatchOpDelete, Key: keyAttrs})
		default:
			httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("op %d: unknown op %q", i, raw.Op))
			return
		}
	}
	if err := h.deps.BatchWriteByShard(td, ops); err != nil {
		h.deps.WriteWriteErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, struct{}{})
}

// HandleBatchGetItem serves /v1/BatchGetItem.
func (h *Handlers) HandleBatchGetItem(w http.ResponseWriter, r *http.Request) {
	_, span := tracing.Tracer().Start(r.Context(), "BatchGetItem")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req batchGetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	if !auth.RequireAnyScope(w, r,
		auth.TableScope(auth.ScopeItemRead, req.Table),
		auth.WildcardScope(auth.ScopeItemRead)) {
		return
	}
	td, err := h.deps.Cat.Describe(req.Table)
	if err != nil {
		httpx.WriteErr(w, http.StatusNotFound, err)
		return
	}
	keys := make([]types.Item, 0, len(req.Keys))
	for i, raw := range req.Keys {
		k, err := ddbjson.DecodeItem(raw)
		if err != nil {
			httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("key %d: %w", i, err))
			return
		}
		keys = append(keys, k)
	}
	items, err := h.deps.BatchGetByShard(req.Table, td.KeySchema, keys)
	if err != nil {
		httpx.WriteErr(w, http.StatusInternalServerError, err)
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
	httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": out})
}

func (h *Handlers) observeWrite(pkBytes []byte, item types.Item, started time.Time) {
	if h.deps.ObserveWrite == nil {
		return
	}
	h.deps.ObserveWrite(pkBytes, item, started)
}

func (h *Handlers) observeRead(pkBytes []byte, item types.Item, started time.Time) {
	if h.deps.ObserveRead == nil {
		return
	}
	h.deps.ObserveRead(pkBytes, item, started)
}

func pkBytesFromItem(item types.Item, ks types.KeySchema) ([]byte, error) {
	pkAttr, ok := item[ks.PK]
	if !ok {
		return nil, fmt.Errorf("%w: %q", types.ErrMissingKey, ks.PK)
	}
	return storage.AttrCanonicalBytes(pkAttr)
}
