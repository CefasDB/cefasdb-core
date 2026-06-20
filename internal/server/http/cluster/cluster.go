package cluster

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/cluster"
	"github.com/CefasDb/cefasdb/internal/core/model"
	"github.com/CefasDb/cefasdb/internal/placement"
	"github.com/CefasDb/cefasdb/internal/server/http/httpx"
	"github.com/CefasDb/cefasdb/internal/tracing"
)

// Cluster is the minimal cluster-management surface this package
// needs. internal/api.Server satisfies it; re-declaring keeps the
// import direction one-way.
type Cluster interface {
	IsLeader() bool
	LeaderHTTPAddr() string
	AddVoter(id, addr string, timeout time.Duration) error
	RemoveServer(id string, timeout time.Duration) error
	SelfID() string
	BindAddr() string
}

// WriteErrFunc emits a write-handler error response. The server injects
// its leader-aware redirect helper so a NotLeaderError becomes a 307
// to the leader's HTTP URL while this package stays free of the
// storage / raft imports those error types live in.
type WriteErrFunc func(w http.ResponseWriter, r *http.Request, err error)

// ExtraStatusFunc lets the server contribute optional sections of the
// /v1/cluster/status payload (hot ranges from the metrics package, the
// backup scheduler status from storage) without forcing this package
// to depend on those imports. Each returned value is marshalled
// verbatim by encoding/json under its json key; nil keys are omitted.
type ExtraStatusFunc func() map[string]any

// Handlers carries the dependencies every cluster-admin handler needs.
// Build it once during Server.Routes and let the server register the
// methods with its existing middleware stack.
type Handlers struct {
	cluster     Cluster
	manager     *cluster.Manager
	writeErr    WriteErrFunc
	extraStatus ExtraStatusFunc
}

// New constructs the handler set. cls may be nil (single-node mode);
// mgr may be nil (single-shard mode); extra may be nil (no optional
// status sections). writeErr must be non-nil — write handlers rely on
// it to map cluster.Manager errors to the right HTTP status, including
// the not-leader redirect path.
func New(cls Cluster, mgr *cluster.Manager, writeErr WriteErrFunc, extra ExtraStatusFunc) *Handlers {
	return &Handlers{cluster: cls, manager: mgr, writeErr: writeErr, extraStatus: extra}
}

type clusterStatusResponse struct {
	Mode              string                     `json:"mode"` // "single-node" or "raft"
	IsLeader          bool                       `json:"isLeader"`
	SelfID            string                     `json:"selfId,omitempty"`
	BindAddr          string                     `json:"bindAddr,omitempty"`
	LeaderHTTP        string                     `json:"leaderHttp,omitempty"`
	RoutingEpoch      uint64                     `json:"routingEpoch,omitempty"`
	PlacementVersion  uint64                     `json:"placementVersion,omitempty"`
	ShardCount        int                        `json:"shardCount,omitempty"`
	PlacementStrategy string                     `json:"placementStrategy,omitempty"`
	Shards            []placement.ShardPlacement `json:"shards,omitempty"`
	Nodes             []placement.NodeDescriptor `json:"nodes,omitempty"`
	HotRanges         any                        `json:"hotRanges,omitempty"`
	BackupScheduler   any                        `json:"backupScheduler,omitempty"`
}

// HandleStatus serves /v1/cluster/status: a snapshot of mode, leader,
// placement, optional hot ranges, and optional backup scheduler.
func (h *Handlers) HandleStatus(w http.ResponseWriter, r *http.Request) {
	_, span := tracing.Tracer().Start(r.Context(), "ClusterStatus")
	defer span.End()
	resp := clusterStatusResponse{Mode: "single-node"}
	if h.cluster != nil {
		resp.Mode = "raft"
		resp.IsLeader = h.cluster.IsLeader()
		resp.SelfID = h.cluster.SelfID()
		resp.BindAddr = h.cluster.BindAddr()
		resp.LeaderHTTP = h.cluster.LeaderHTTPAddr()
	}
	if h.manager != nil {
		_ = h.manager.RefreshPlacement()
		cat := h.manager.Placement()
		resp.RoutingEpoch = cat.Epoch
		resp.PlacementVersion = cat.Version
		resp.ShardCount = len(cat.Shards)
		resp.PlacementStrategy = cat.Strategy
		resp.Shards = h.manager.ShardPlacementsWithLeadership()
		resp.Nodes = sortedPlacementNodes(cat)
	}
	if h.extraStatus != nil {
		extra := h.extraStatus()
		if v, ok := extra["hotRanges"]; ok {
			resp.HotRanges = v
		}
		if v, ok := extra["backupScheduler"]; ok {
			resp.BackupScheduler = v
		}
	}
	httpx.WriteJSON(w, http.StatusOK, resp)
}

func sortedPlacementNodes(cat placement.PlacementCatalog) []placement.NodeDescriptor {
	out := make([]placement.NodeDescriptor, 0, len(cat.Nodes))
	for _, node := range cat.Nodes {
		node.Capacity.Tags = append([]string(nil), node.Capacity.Tags...)
		out = append(out, node)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

type addVoterRequest struct {
	ID        model.NodeID `json:"id"`
	Addr      string       `json:"addr"`
	TimeoutMS int          `json:"timeoutMs,omitempty"`
	ShardID   *uint32      `json:"shardId,omitempty"`
	AllShards bool         `json:"allShards,omitempty"`
}

// HandleAddVoter serves /v1/cluster/AddVoter. POST only.
func (h *Handlers) HandleAddVoter(w http.ResponseWriter, r *http.Request) {
	_, span := tracing.Tracer().Start(r.Context(), "AddVoter")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	var req addVoterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	id := req.ID.String()
	if h.manager != nil && req.AllShards {
		if err := h.manager.AddVoterAllShards(id, req.Addr, timeout); err != nil {
			h.writeErr(w, r, err)
			return
		}
		httpx.WriteJSON(w, http.StatusOK, struct{}{})
		return
	}
	if h.manager != nil && req.ShardID != nil {
		if err := h.manager.AddShardVoter(*req.ShardID, id, req.Addr, timeout); err != nil {
			h.writeErr(w, r, err)
			return
		}
		httpx.WriteJSON(w, http.StatusOK, struct{}{})
		return
	}
	if h.cluster == nil {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("cluster not configured"))
		return
	}
	if err := h.cluster.AddVoter(id, req.Addr, timeout); err != nil {
		h.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, struct{}{})
}

type removeServerRequest struct {
	ID        model.NodeID `json:"id"`
	TimeoutMS int          `json:"timeoutMs,omitempty"`
	ShardID   *uint32      `json:"shardId,omitempty"`
	AllShards bool         `json:"allShards,omitempty"`
}

// HandleRemoveServer serves /v1/cluster/RemoveServer. POST only.
func (h *Handlers) HandleRemoveServer(w http.ResponseWriter, r *http.Request) {
	_, span := tracing.Tracer().Start(r.Context(), "RemoveServer")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	var req removeServerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	timeout := time.Duration(req.TimeoutMS) * time.Millisecond
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	id := req.ID.String()
	if h.manager != nil && req.AllShards {
		if err := h.manager.RemoveServerAllShards(id, timeout); err != nil {
			h.writeErr(w, r, err)
			return
		}
		httpx.WriteJSON(w, http.StatusOK, struct{}{})
		return
	}
	if h.manager != nil && req.ShardID != nil {
		if err := h.manager.RemoveShardServer(*req.ShardID, id, timeout); err != nil {
			h.writeErr(w, r, err)
			return
		}
		httpx.WriteJSON(w, http.StatusOK, struct{}{})
		return
	}
	if h.cluster == nil {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("cluster not configured"))
		return
	}
	if err := h.cluster.RemoveServer(id, timeout); err != nil {
		h.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, struct{}{})
}

// HandlePlacementPlan serves /v1/cluster/placement/plan. POST only.
func (h *Handlers) HandlePlacementPlan(w http.ResponseWriter, r *http.Request) {
	_, span := tracing.Tracer().Start(r.Context(), "PlanPlacement")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	if h.manager == nil {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("cluster manager not configured"))
		return
	}
	var req placement.PlacementPlanRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	plan, err := h.manager.PlanPlacement(req)
	if err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, plan)
}

// HandlePlacementApply serves /v1/cluster/placement/apply. POST only.
func (h *Handlers) HandlePlacementApply(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracing.Tracer().Start(r.Context(), "ApplyPlacement")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	if h.manager == nil {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("cluster manager not configured"))
		return
	}
	var req placement.PlacementApplyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	result, err := h.manager.ApplyPlacement(ctx, req)
	if err != nil {
		h.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, result)
}

// HandlePlacementAudit serves /v1/cluster/placement/audit. GET + POST.
func (h *Handlers) HandlePlacementAudit(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracing.Tracer().Start(r.Context(), "AuditPlacement")
	defer span.End()
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	if h.manager == nil {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("cluster manager not configured"))
		return
	}
	req := placement.PlacementAuditRequest{IncludeRepairPlan: true}
	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			httpx.WriteErr(w, http.StatusBadRequest, err)
			return
		}
	} else {
		var err error
		q := r.URL.Query()
		if req.MaxPrimaryKeysPerShard, err = queryInt(q.Get("maxPrimaryKeysPerShard")); err != nil {
			httpx.WriteErr(w, http.StatusBadRequest, err)
			return
		}
		if req.MaxIssues, err = queryInt(q.Get("maxIssues")); err != nil {
			httpx.WriteErr(w, http.StatusBadRequest, err)
			return
		}
		if q.Has("repairPlan") {
			req.IncludeRepairPlan, err = strconv.ParseBool(q.Get("repairPlan"))
			if err != nil {
				httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("repairPlan: %w", err))
				return
			}
		}
	}
	report, err := h.manager.AuditPlacement(ctx, req)
	if err != nil {
		httpx.WriteErr(w, http.StatusInternalServerError, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, report)
}

func queryInt(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// HandleSplitFinalize serves /v1/cluster/placement/split/finalize.
func (h *Handlers) HandleSplitFinalize(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracing.Tracer().Start(r.Context(), "FinalizeSplit")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	if h.manager == nil {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("cluster manager not configured"))
		return
	}
	var req cluster.SplitFinalizeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	result, err := h.manager.FinalizeSplit(ctx, req)
	if err != nil {
		h.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, result)
}

// HandleSplitRollback serves /v1/cluster/placement/split/rollback.
func (h *Handlers) HandleSplitRollback(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracing.Tracer().Start(r.Context(), "RollbackSplit")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	if h.manager == nil {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("cluster manager not configured"))
		return
	}
	var req cluster.SplitRollbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	result, err := h.manager.RollbackSplit(ctx, req)
	if err != nil {
		h.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, result)
}

// HandleRangeMoveFinalize serves
// /v1/cluster/placement/range-move/finalize.
func (h *Handlers) HandleRangeMoveFinalize(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracing.Tracer().Start(r.Context(), "FinalizeRangeMove")
	defer span.End()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !auth.RequireAnyScope(w, r, auth.ScopeClusterAdmin) {
		return
	}
	if h.manager == nil {
		httpx.WriteErr(w, http.StatusBadRequest, fmt.Errorf("cluster manager not configured"))
		return
	}
	var req cluster.RangeMoveFinalizeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.WriteErr(w, http.StatusBadRequest, err)
		return
	}
	result, err := h.manager.FinalizeRangeMove(ctx, req)
	if err != nil {
		h.writeErr(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, result)
}
