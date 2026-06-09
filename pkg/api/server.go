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

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

type Server struct {
	db  *storage.DB
	cat *catalog.Catalog
}

func New(db *storage.DB, cat *catalog.Catalog) *Server {
	return &Server{db: db, cat: cat}
}

// Routes attaches cefas HTTP endpoints onto mux. Path layout follows
// DynamoDB-ish verbs as resources under /v1/.
func (s *Server) Routes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/tables", s.handleTables)             // POST=create, GET=list
	mux.HandleFunc("/v1/tables/", s.handleTable)             // GET=describe (path: /v1/tables/{name})
	mux.HandleFunc("/v1/PutItem", s.handlePutItem)
	mux.HandleFunc("/v1/GetItem", s.handleGetItem)
	mux.HandleFunc("/v1/DeleteItem", s.handleDeleteItem)
	mux.HandleFunc("/v1/Query", s.handleQuery)
	mux.HandleFunc("/v1/Health", s.handleHealth)
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
	Name      string               `json:"name"`
	KeySchema jsonKeySchema        `json:"keySchema"`
	GSIs      []types.GSIDescriptor `json:"gsis,omitempty"`
}

type jsonKeySchema struct {
	PK string `json:"pk"`
	SK string `json:"sk,omitempty"`
}

func (s *Server) handleTables(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req createTableRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, err)
			return
		}
		td := types.TableDescriptor{
			Name:      req.Name,
			KeySchema: types.KeySchema{PK: req.KeySchema.PK, SK: req.KeySchema.SK},
			GSIs:      req.GSIs,
		}
		if err := s.cat.Create(td); err != nil {
			status := http.StatusBadRequest
			if errors.Is(err, types.ErrTableAlreadyExists) {
				status = http.StatusConflict
			}
			writeErr(w, status, err)
			return
		}
		writeJSON(w, http.StatusCreated, td)
	case http.MethodGet:
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
	Table string                    `json:"table"`
	Item  map[string]jsonAttribute  `json:"item"`
}

type getItemRequest struct {
	Table string                   `json:"table"`
	Key   map[string]jsonAttribute `json:"key"`
}

type deleteItemRequest = getItemRequest

type queryRequest struct {
	Table    string                    `json:"table"`
	PKValue  jsonAttribute             `json:"pkValue"`
	SKLow    *jsonAttribute            `json:"skLow,omitempty"`
	SKHigh   *jsonAttribute            `json:"skHigh,omitempty"`
	Limit    int                       `json:"limit,omitempty"`
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
	if err := s.db.PutItem(req.Table, td.KeySchema, item); err != nil {
		writeErr(w, mapWriteErr(err), err)
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
	item, err := s.db.GetItem(req.Table, td.KeySchema, keyAttrs)
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
	if err := s.db.DeleteItem(req.Table, td.KeySchema, keyAttrs); err != nil {
		writeErr(w, mapWriteErr(err), err)
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

	var items []types.Item
	if req.SKLow == nil && req.SKHigh == nil {
		items, err = s.db.QueryByPK(req.Table, td.KeySchema, pkVal, req.Limit)
	} else {
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
		items, err = s.db.QueryByPKRange(req.Table, td.KeySchema, pkVal, lo, hi, req.Limit)
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
	if errors.Is(err, storage.ErrNotLeader) {
		// In Raft mode (Phase 4) the controller should redirect to the
		// leader's HTTP address — keep 421 (Misdirected Request) for now;
		// Phase 4 will rewrite this to a 307 with Location.
		return http.StatusMisdirectedRequest
	}
	return http.StatusInternalServerError
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
