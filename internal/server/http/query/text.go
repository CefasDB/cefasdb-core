package query

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/compat/ddbjson"
	"github.com/CefasDb/cefasdb/internal/server/http/httpx"
	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type textRequest struct {
	Table           string                       `json:"table"`
	IndexName       string                       `json:"indexName"`
	PKValue         ddbjson.Attribute            `json:"pkValue"`
	SKLow           *ddbjson.Attribute           `json:"skLow,omitempty"`
	SKHigh          *ddbjson.Attribute           `json:"skHigh,omitempty"`
	Search          string                       `json:"search,omitempty"`
	Phrase          string                       `json:"phrase,omitempty"`
	Fields          map[string]ddbjson.Attribute `json:"fields,omitempty"`
	MinValues       map[string]ddbjson.Attribute `json:"minValues,omitempty"`
	MaxValues       map[string]ddbjson.Attribute `json:"maxValues,omitempty"`
	Descending      bool                         `json:"descending,omitempty"`
	Limit           int                          `json:"limit,omitempty"`
	Cursor          string                       `json:"cursor,omitempty"`
	Consistency     string                       `json:"consistency,omitempty"`
	TimeField       string                       `json:"timeField,omitempty"`
	IntervalSeconds int                          `json:"intervalSeconds,omitempty"`
	GroupBy         string                       `json:"groupBy,omitempty"`
	ErrorField      string                       `json:"errorField,omitempty"`
	ErrorValues     []string                     `json:"errorValues,omitempty"`
}

func (r textRequest) query() (types.TextQuery, error) {
	q := types.TextQuery{Table: r.Table, IndexName: r.IndexName, Search: r.Search, Phrase: r.Phrase, Descending: r.Descending, Limit: r.Limit, Cursor: r.Cursor, Consistency: r.Consistency}
	var err error
	q.PKValue, err = r.PKValue.ToAttr()
	if err != nil {
		return q, err
	}
	if r.SKLow != nil {
		q.SKLow, err = r.SKLow.ToAttr()
		if err != nil {
			return q, err
		}
	}
	if r.SKHigh != nil {
		q.SKHigh, err = r.SKHigh.ToAttr()
		if err != nil {
			return q, err
		}
	}
	q.Fields, err = ddbjson.DecodeItem(r.Fields)
	if err != nil {
		return q, err
	}
	q.MinValues, err = ddbjson.DecodeItem(r.MinValues)
	if err != nil {
		return q, err
	}
	q.MaxValues, err = ddbjson.DecodeItem(r.MaxValues)
	return q, err
}

func (h *Handlers) HandleTextQuery(w http.ResponseWriter, r *http.Request) { h.handleText(w, r, false) }

func (h *Handlers) HandleTextAggregate(w http.ResponseWriter, r *http.Request) {
	h.handleText(w, r, true)
}

func (h *Handlers) handleText(w http.ResponseWriter, r *http.Request, aggregate bool) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	var req textRequest
	if err := decoder.Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		httpx.WriteErr(w, http.StatusBadRequest, errors.New("exactly one JSON request required"))
		return
	}
	if !auth.RequireAnyScope(w, r, auth.TableScope(auth.ScopeQuery, req.Table), auth.WildcardScope(auth.ScopeQuery)) {
		return
	}
	q, err := req.query()
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	if q.Consistency != "" && q.Consistency != "eventual" && q.Consistency != "strong" {
		httpx.WriteErr(w, http.StatusBadRequest, errors.New("unsupported consistency"))
		return
	}
	td, err := h.cat.Describe(q.Table)
	if err != nil {
		httpx.WriteErr(w, http.StatusNotFound, err)
		return
	}
	pk, err := storage.AttrCanonicalBytes(q.PKValue)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	db, err := h.storageFor(pk)
	if err != nil {
		httpx.WriteErr(w, http.StatusServiceUnavailable, err)
		return
	}
	if q.Consistency == "strong" {
		if h.textStrongRead != nil {
			if !h.textStrongRead(w, r, db) {
				return
			}
		} else {
			if err := db.ReadBarrier(5 * time.Second); err != nil {
				var redirect *pebble.NotLeaderError
				if errors.As(err, &redirect) && redirect.LeaderURL != "" {
					w.Header().Set("Location", redirect.LeaderURL+r.URL.RequestURI())
					httpx.WriteErr(w, http.StatusTemporaryRedirect, errors.New("strong read must use partition leader"))
					return
				}
				httpx.WriteErr(w, http.StatusServiceUnavailable, errors.New("partition strong-read barrier unavailable"))
				return
			}
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	status := func(err error) {
		code := http.StatusServiceUnavailable
		if errors.Is(err, pebble.ErrTextQueryInvalid) {
			code = http.StatusBadRequest
		}
		if errors.Is(err, pebble.ErrTextQueryLimit) {
			code = http.StatusUnprocessableEntity
		}
		httpx.WriteErr(w, code, err)
	}
	if aggregate {
		result, err := db.TextAggregate(ctx, td, types.TextAggregate{TextQuery: q, TimeField: req.TimeField, IntervalSeconds: req.IntervalSeconds, GroupBy: req.GroupBy, ErrorField: req.ErrorField, ErrorValues: req.ErrorValues})
		if err != nil {
			status(err)
			return
		}
		httpx.WriteJSON(w, http.StatusOK, result)
		return
	}
	if req.IntervalSeconds != 0 || req.TimeField != "" || req.GroupBy != "" || req.ErrorField != "" || len(req.ErrorValues) > 0 {
		httpx.WriteErr(w, http.StatusBadRequest, errors.New("aggregate fields are not accepted by TextQuery"))
		return
	}
	result, err := db.TextQuery(ctx, td, q)
	if err != nil {
		status(err)
		return
	}
	items := make([]map[string]ddbjson.Attribute, 0, len(result.Items))
	for _, item := range result.Items {
		items = append(items, ddbjson.EncodeItem(item))
	}
	httpx.WriteJSON(w, http.StatusOK, struct {
		Items      []map[string]ddbjson.Attribute `json:"items"`
		NextCursor string                         `json:"nextCursor,omitempty"`
	}{items, result.NextCursor})
}
