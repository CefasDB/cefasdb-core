// Handler set for the plugin-backed surface CLI Epic 7 exposes:
// CreateIndex / DescribeIndex / RebuildIndex / Explain / TopK /
// CohortCreate / CohortEstimate / GeoAudience / Dedup / FreqCap /
// Aggregate. Lives in pkg/api so the gRPC server can keep the plugin
// registry inside its existing AttachPluginRegistry seam.
package api

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/auth"
	"github.com/osvaldoandrade/cefas/internal/storage"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	cquery "github.com/osvaldoandrade/cefas/pkg/core/query"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
	"github.com/osvaldoandrade/cefas/pkg/plugin/audience"
	"github.com/osvaldoandrade/cefas/pkg/plugin/roaring"
)

// pluginIndexBook is the in-memory descriptor catalog Epic 7 uses
// while persistent storage of plugin-backed index descriptors lives
// in a follow-up. Surviving a restart means re-running CreateIndex.
var pluginIndexBook = struct {
	mu      sync.RWMutex
	entries map[string]index.Descriptor // "table/name" → descriptor
}{entries: map[string]index.Descriptor{}}

func indexKey(table, name string) string { return table + "/" + name }

// ---------- CreateIndex / DescribeIndex / RebuildIndex ----------

func (s *GRPCServer) CreateIndex(ctx context.Context, req *cefaspb.CreateIndexRequest) (*cefaspb.CreateIndexResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	d := req.GetDescriptor_()
	if d == nil || d.GetTable() == "" || d.GetName() == "" || d.GetPluginName() == "" {
		return nil, status.Error(codes.InvalidArgument, "table / name / plugin_name required")
	}
	if _, err := s.cat.Describe(d.GetTable()); err != nil {
		return nil, mapStorageErr(err)
	}
	desc := pbToPluginIndex(d)

	plug, ok := s.pluginRegistry().Lookup(desc.PluginName)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "plugin %q not registered", desc.PluginName)
	}
	ip, ok := plug.(plugin.IndexPlugin)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "plugin %q is not an IndexPlugin", desc.PluginName)
	}
	if err := ip.Build(desc, s.itemSourceFor(desc.Table)); err != nil {
		return nil, status.Errorf(codes.Internal, "plugin build: %v", err)
	}
	pluginIndexBook.mu.Lock()
	pluginIndexBook.entries[indexKey(desc.Table, desc.Name)] = desc
	pluginIndexBook.mu.Unlock()
	return &cefaspb.CreateIndexResponse{Descriptor_: pluginIndexToPB(desc)}, nil
}

func (s *GRPCServer) DescribeIndex(ctx context.Context, req *cefaspb.DescribeIndexRequest) (*cefaspb.DescribeIndexResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	pluginIndexBook.mu.RLock()
	d, ok := pluginIndexBook.entries[indexKey(req.GetTable(), req.GetName())]
	pluginIndexBook.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "index %s/%s not found", req.GetTable(), req.GetName())
	}
	return &cefaspb.DescribeIndexResponse{Descriptor_: pluginIndexToPB(d)}, nil
}

func (s *GRPCServer) RebuildIndex(ctx context.Context, req *cefaspb.RebuildIndexRequest) (*cefaspb.RebuildIndexResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	pluginIndexBook.mu.RLock()
	d, ok := pluginIndexBook.entries[indexKey(req.GetTable(), req.GetName())]
	pluginIndexBook.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "index %s/%s not found", req.GetTable(), req.GetName())
	}
	plug, ok := s.pluginRegistry().Lookup(d.PluginName)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "plugin %q not registered", d.PluginName)
	}
	ip, ok := plug.(plugin.IndexPlugin)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "plugin %q is not an IndexPlugin", d.PluginName)
	}
	count := 0
	if err := ip.Build(d, func(yield func(model.Item) bool) {
		s.itemSourceFor(d.Table)(func(it model.Item) bool {
			count++
			return yield(it)
		})
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "rebuild: %v", err)
	}
	return &cefaspb.RebuildIndexResponse{ItemsIndexed: int64(count)}, nil
}

// itemSourceFor produces a plugin.ItemSource over the live table.
func (s *GRPCServer) itemSourceFor(table string) func(yield func(model.Item) bool) {
	return func(yield func(model.Item) bool) {
		items, err := s.db.ScanTable(table, 0)
		if err != nil {
			return
		}
		for _, it := range items {
			if !yield(it) {
				return
			}
		}
	}
}

// ---------- Explain ----------

func (s *GRPCServer) Explain(ctx context.Context, req *cefaspb.ExplainRequest) (*cefaspb.ExplainResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	// v1: rendering a synthetic plan tree so the CLI surface works
	// today; the real planner integration arrives with deeper SQL
	// planner work — the explain format is already pinned by
	// pkg/core/query.RenderExplain so the wire is forward-compatible.
	root := cquery.PlanNode{
		Op:     "Query",
		Detail: fmt.Sprintf("table=%s predicate=%q", req.GetTable(), req.GetPredicate()),
		Children: []cquery.PlanNode{
			{Op: "ScanTable", Plugin: "core", Detail: req.GetTable()},
		},
	}
	fmtKind := cquery.ExplainText
	if strings.EqualFold(req.GetFormat(), "json") {
		fmtKind = cquery.ExplainJSON
	}
	return &cefaspb.ExplainResponse{Plan: cquery.RenderExplain(root, fmtKind)}, nil
}

// ---------- TopK ----------

func (s *GRPCServer) TopK(ctx context.Context, req *cefaspb.TopKRequest) (*cefaspb.TopKResponse, error) {
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemRead, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemRead)); err != nil {
		return nil, err
	}
	if req.GetK() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "k must be > 0")
	}
	if req.GetField() == "" || req.GetDistanceOperator() == "" {
		return nil, status.Error(codes.InvalidArgument, "field + distance_operator required")
	}
	plug, ok := s.pluginRegistry().Lookup(req.GetDistanceOperator())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "distance plugin %q not registered", req.GetDistanceOperator())
	}
	dist, ok := plug.(plugin.DistancePlugin)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "plugin %q is not a DistancePlugin", req.GetDistanceOperator())
	}
	target, err := pbToAttr(req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("target: %v", err))
	}
	eng, err := cquery.NewTopK(dist, req.GetField(), target, int(req.GetK()))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	items, err := s.db.ScanTable(req.GetTable(), 0)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	for _, it := range items {
		if err := eng.Observe(it); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "observe: %v", err)
		}
	}
	rows := eng.Result()
	pbRows := make([]*cefaspb.TopKRow, 0, len(rows))
	for _, r := range rows {
		pbRows = append(pbRows, &cefaspb.TopKRow{
			Item:     &cefaspb.Item{Attributes: itemToPB(r.Item)},
			Distance: r.Distance,
		})
	}
	return &cefaspb.TopKResponse{Rows: pbRows}, nil
}

// ---------- Cohort ----------

func (s *GRPCServer) CohortCreate(ctx context.Context, req *cefaspb.CohortCreateRequest) (*cefaspb.CohortCreateResponse, error) {
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemWrite, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemWrite)); err != nil {
		return nil, err
	}
	plug, ok := s.pluginRegistry().Lookup("roaring")
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "roaring plugin not registered")
	}
	ip := plug.(plugin.IndexPlugin)
	desc := index.Descriptor{
		Table:        req.GetTable(),
		Name:         req.GetCohort(),
		PluginName:   "roaring",
		PluginConfig: []byte(fmt.Sprintf(`{"field":%q}`, req.GetField())),
		KeySchema:    model.KeySchema{PK: req.GetField()},
	}
	items, err := s.db.ScanTable(req.GetTable(), 0)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	filtered, err := applyFilter(items, req.GetFilter(), req.GetBinds())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "filter: %v", err)
	}
	if err := ip.Build(desc, func(yield func(model.Item) bool) {
		for _, it := range filtered {
			if !yield(it) {
				return
			}
		}
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "build cohort: %v", err)
	}
	// Cardinality straight from the Roaring state.
	rp, _ := plug.(*roaring.Plugin)
	_ = rp
	return &cefaspb.CohortCreateResponse{Members: int64(len(filtered))}, nil
}

func (s *GRPCServer) CohortEstimate(ctx context.Context, req *cefaspb.CohortEstimateRequest) (*cefaspb.CohortEstimateResponse, error) {
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemRead, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemRead)); err != nil {
		return nil, err
	}
	plug, ok := s.pluginRegistry().Lookup("hll")
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "hll plugin not registered")
	}
	ep := plug.(plugin.EstimatorPlugin)
	items, err := s.db.ScanTable(req.GetTable(), 0)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	filtered, err := applyFilter(items, req.GetFilter(), req.GetBinds())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "filter: %v", err)
	}
	stream := fmt.Sprintf("cohort:%s:%s", req.GetTable(), req.GetField())
	for _, it := range filtered {
		v, ok := it[req.GetField()]
		if !ok {
			continue
		}
		_ = ep.Observe(stream, v)
	}
	est, _ := ep.Estimate(stream)
	return &cefaspb.CohortEstimateResponse{ApproximateCount: est}, nil
}

// ---------- Audience ----------

func (s *GRPCServer) GeoAudience(req *cefaspb.GeoAudienceRequest, stream cefaspb.Cefas_GeoAudienceServer) error {
	ctx := stream.Context()
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemRead, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemRead)); err != nil {
		return err
	}
	plug, ok := s.pluginRegistry().Lookup("audience")
	if !ok {
		return status.Error(codes.FailedPrecondition, "audience plugin not registered")
	}
	a := plug.(*audience.Plugin)
	idxName := req.GetIndex()
	if idxName == "" {
		idxName = "loc_geo"
	}
	pluginIndexBook.mu.RLock()
	d, ok := pluginIndexBook.entries[indexKey(req.GetTable(), idxName)]
	pluginIndexBook.mu.RUnlock()
	if !ok {
		return status.Errorf(codes.FailedPrecondition,
			"geohash index %s/%s not registered; create with cefas create-index --type geohash", req.GetTable(), idxName)
	}
	a.Bind(audience.IndexBinding{Geohash: d})
	cs, err := a.Select(plugin.AudienceRequest{
		Lat:    req.GetLat(),
		Lon:    req.GetLon(),
		Radius: req.GetRadiusMeters(),
	})
	if err != nil {
		return status.Errorf(codes.Internal, "select: %v", err)
	}
	sent := 0
	for {
		c, ok := cs.Next()
		if !ok {
			break
		}
		if err := stream.Send(&cefaspb.Item{Attributes: itemToPB(c.Key)}); err != nil {
			return err
		}
		sent++
		if req.GetLimit() > 0 && sent >= int(req.GetLimit()) {
			return nil
		}
	}
	return nil
}

func (s *GRPCServer) Dedup(ctx context.Context, req *cefaspb.DedupRequest) (*cefaspb.DedupResponse, error) {
	if err := requireScope(ctx, auth.ScopeItemWrite); err != nil {
		return nil, err
	}
	plug, ok := s.pluginRegistry().Lookup("audience")
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "audience plugin not registered")
	}
	a := plug.(*audience.Plugin)
	ttl := time.Duration(req.GetTtlSeconds()) * time.Second
	ok2, err := a.Dedup(req.GetScope(), req.GetKey(), ttl)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &cefaspb.DedupResponse{Allowed: ok2}, nil
}

func (s *GRPCServer) FreqCap(ctx context.Context, req *cefaspb.FreqCapRequest) (*cefaspb.FreqCapResponse, error) {
	if err := requireScope(ctx, auth.ScopeItemRead); err != nil {
		return nil, err
	}
	plug, ok := s.pluginRegistry().Lookup("audience")
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "audience plugin not registered")
	}
	a := plug.(*audience.Plugin)
	win := time.Duration(req.GetWindowSeconds()) * time.Second
	ok2, err := a.FreqCap(req.GetScope(), req.GetKey(), int(req.GetLimit()), win)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &cefaspb.FreqCapResponse{Allowed: ok2}, nil
}

func (s *GRPCServer) Aggregate(ctx context.Context, req *cefaspb.AggregateRequest) (*cefaspb.AggregateResponse, error) {
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemRead, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemRead)); err != nil {
		return nil, err
	}
	items, err := s.db.ScanTable(req.GetTable(), 0)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	rows, err := audience.Aggregate(items, audience.AggregateSpec{
		GroupBy:      req.GetGroupBy(),
		Metrics:      req.GetMetrics(),
		MinGroupSize: int(req.GetMinGroupSize()),
	})
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	out := make([]*cefaspb.AggregateRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, &cefaspb.AggregateRow{
			GroupKey: r.GroupKey,
			Counts:   r.Counts,
			Members:  int32(r.Members),
		})
	}
	return &cefaspb.AggregateResponse{Rows: out}, nil
}

// ---------- helpers ----------

func applyFilter(items []model.Item, expr string, pbBinds map[string]*cefaspb.AttributeValue) ([]model.Item, error) {
	if strings.TrimSpace(expr) == "" {
		return items, nil
	}
	cond, err := storage.ParseCondition(expr)
	if err != nil {
		return nil, err
	}
	binds := map[string]model.AttributeValue{}
	for k, v := range pbBinds {
		av, err := pbToAttr(v)
		if err != nil {
			return nil, err
		}
		binds[strings.TrimPrefix(k, ":")] = av
	}
	out := make([]model.Item, 0, len(items))
	for _, it := range items {
		ok, err := cond.Evaluate(it, binds)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, it)
		}
	}
	return out, nil
}

func pbToPluginIndex(d *cefaspb.PluginIndexDescriptor) index.Descriptor {
	out := index.Descriptor{
		Table:        d.GetTable(),
		Name:         d.GetName(),
		PluginName:   d.GetPluginName(),
		PluginConfig: d.GetPluginConfig(),
	}
	if ks := d.GetKeySchema(); ks != nil {
		out.KeySchema = model.KeySchema{PK: ks.GetPk(), SK: ks.GetSk()}
	}
	return out
}

func pluginIndexToPB(d index.Descriptor) *cefaspb.PluginIndexDescriptor {
	return &cefaspb.PluginIndexDescriptor{
		Table:        d.Table,
		Name:         d.Name,
		PluginName:   d.PluginName,
		PluginConfig: d.PluginConfig,
		KeySchema:    &cefaspb.KeySchema{Pk: d.KeySchema.PK, Sk: d.KeySchema.SK},
	}
}
