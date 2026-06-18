// Handler set for the plugin-backed surface CLI Epic 7 exposes:
// CreateIndex / DescribeIndex / RebuildIndex / Explain / TopK /
// CohortCreate / CohortEstimate / GeoAudience / Dedup / FreqCap /
// Aggregate. Lives in pkg/api so the gRPC server can keep the plugin
// registry inside its existing AttachPluginRegistry seam.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/core/index"
	"github.com/CefasDb/cefasdb/internal/core/model"
	cquery "github.com/CefasDb/cefasdb/internal/core/query"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/audience"
	"github.com/CefasDb/cefasdb/internal/plugin/builtin/roaring"
	"github.com/CefasDb/cefasdb/internal/storage"
	"github.com/CefasDb/cefasdb/internal/tracing"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

// pluginIndexBook is the in-memory descriptor cache for plugin-backed
// indexes. Durable descriptor records live in storage under the
// cefas/internal/plugin-index/ keyspace; reads hydrate this cache.
var pluginIndexBook = struct {
	createMu sync.Mutex
	mu       sync.RWMutex
	entries  map[string]index.Descriptor // "table/name" → descriptor
}{entries: map[string]index.Descriptor{}}

func indexKey(table, name string) string { return table + "/" + name }

type annConfig struct {
	Field         string `json:"field"`
	Dim           int    `json:"dim"`
	Algorithm     string `json:"algorithm,omitempty"`
	Metric        string `json:"metric,omitempty"`
	Sketches      int    `json:"sketches,omitempty"`
	BitsPerSketch int    `json:"bits_per_sketch,omitempty"`
}

// ---------- CreateIndex / DescribeIndex / RebuildIndex ----------

func (s *GRPCServer) CreateIndex(ctx context.Context, req *cefaspb.CreateIndexRequest) (*cefaspb.CreateIndexResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "CreateIndex")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	d := req.GetDescriptor_()
	if d == nil || d.GetTable() == "" || d.GetName() == "" || d.GetPluginName() == "" {
		return nil, status.Error(codes.InvalidArgument, "table / name / plugin_name required")
	}
	td, err := s.cat.Describe(d.GetTable())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	desc := pbToPluginIndex(d)
	if desc.KeySchema.PK == "" {
		desc.KeySchema = td.KeySchema
	}
	storedDesc, buildDesc, err := normalizePluginIndexDescriptor(desc, td)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pluginIndexBook.createMu.Lock()
	defer pluginIndexBook.createMu.Unlock()
	if _, ok, err := s.lookupPluginIndexDescriptor(storedDesc.Table, storedDesc.Name); err != nil {
		return nil, mapStorageErr(err)
	} else if ok {
		return nil, status.Errorf(codes.AlreadyExists, "index %s/%s already exists", storedDesc.Table, storedDesc.Name)
	}

	plug, ok := s.pluginRegistry().Lookup(buildDesc.PluginName)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "plugin %q not registered", buildDesc.PluginName)
	}
	ip, ok := plug.(plugin.IndexPlugin)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "plugin %q is not an IndexPlugin", buildDesc.PluginName)
	}
	source, _, err := s.indexItemSourceFor(buildDesc.Table)
	if err != nil {
		return nil, err
	}
	if err := ip.Build(buildDesc, source); err != nil {
		return nil, status.Errorf(codes.Internal, "plugin build: %v", err)
	}
	if err := s.db.PutPluginIndexDescriptor(storedDesc); err != nil {
		return nil, mapStorageErr(err)
	}
	cachePluginIndexDescriptor(storedDesc)
	return &cefaspb.CreateIndexResponse{Descriptor_: pluginIndexToPB(storedDesc)}, nil
}

func (s *GRPCServer) DescribeIndex(ctx context.Context, req *cefaspb.DescribeIndexRequest) (*cefaspb.DescribeIndexResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "DescribeIndex")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	d, ok, err := s.lookupPluginIndexDescriptor(req.GetTable(), req.GetName())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound, "index %s/%s not found", req.GetTable(), req.GetName())
	}
	return &cefaspb.DescribeIndexResponse{Descriptor_: pluginIndexToPB(d)}, nil
}

func (s *GRPCServer) RebuildIndex(ctx context.Context, req *cefaspb.RebuildIndexRequest) (*cefaspb.RebuildIndexResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "RebuildIndex")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	d, ok, err := s.lookupPluginIndexDescriptor(req.GetTable(), req.GetName())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound, "index %s/%s not found", req.GetTable(), req.GetName())
	}
	td, err := s.cat.Describe(d.Table)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	_, buildDesc, err := normalizePluginIndexDescriptor(d, td)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	plug, ok := s.pluginRegistry().Lookup(buildDesc.PluginName)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "plugin %q not registered", buildDesc.PluginName)
	}
	ip, ok := plug.(plugin.IndexPlugin)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "plugin %q is not an IndexPlugin", buildDesc.PluginName)
	}
	source, count, err := s.indexItemSourceFor(buildDesc.Table)
	if err != nil {
		return nil, err
	}
	if err := ip.Build(buildDesc, source); err != nil {
		return nil, status.Errorf(codes.Internal, "rebuild: %v", err)
	}
	return &cefaspb.RebuildIndexResponse{ItemsIndexed: int64(count)}, nil
}

func normalizePluginIndexDescriptor(desc index.Descriptor, td model.TableDescriptor) (index.Descriptor, index.Descriptor, error) {
	if !strings.EqualFold(desc.PluginName, "ann") {
		return desc, desc, nil
	}
	cfg, err := parseANNConfig(desc.PluginConfig)
	if err != nil {
		return index.Descriptor{}, index.Descriptor{}, err
	}
	if cfg.Field == "" {
		return index.Descriptor{}, index.Descriptor{}, fmt.Errorf("ann: config.field required")
	}
	if cfg.Algorithm == "" {
		cfg.Algorithm = "lsh"
	}
	if cfg.Metric == "" {
		cfg.Metric = "cosine"
	}
	if cfg.Algorithm != "lsh" && cfg.Algorithm != "vectorlsh" {
		return index.Descriptor{}, index.Descriptor{}, fmt.Errorf("ann: unsupported algorithm %q", cfg.Algorithm)
	}
	if cfg.Dim <= 0 {
		if dim, ok := tableVectorDim(td, cfg.Field); ok {
			cfg.Dim = dim
		}
	}
	if cfg.Dim <= 0 {
		return index.Descriptor{}, index.Descriptor{}, fmt.Errorf("ann: config.dim required")
	}
	storedCfg, err := json.Marshal(cfg)
	if err != nil {
		return index.Descriptor{}, index.Descriptor{}, err
	}
	buildCfg := map[string]any{
		"field": cfg.Field,
		"dim":   cfg.Dim,
	}
	if cfg.Sketches > 0 {
		buildCfg["sketches"] = cfg.Sketches
	}
	if cfg.BitsPerSketch > 0 {
		buildCfg["bits_per_sketch"] = cfg.BitsPerSketch
	}
	buildRaw, err := json.Marshal(buildCfg)
	if err != nil {
		return index.Descriptor{}, index.Descriptor{}, err
	}
	stored := desc
	stored.PluginName = "ann"
	stored.PluginConfig = storedCfg
	build := desc
	build.PluginName = "vectorlsh"
	build.PluginConfig = buildRaw
	return stored, build, nil
}

func parseANNConfig(raw []byte) (annConfig, error) {
	var cfg annConfig
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return cfg, fmt.Errorf("ann: parse config: %w", err)
		}
	}
	cfg.Algorithm = strings.ToLower(strings.TrimSpace(cfg.Algorithm))
	cfg.Metric = strings.ToLower(strings.TrimSpace(cfg.Metric))
	return cfg, nil
}

func tableVectorDim(td model.TableDescriptor, field string) (int, bool) {
	for _, def := range td.AttributeDefinitions {
		if def.Name == field && strings.EqualFold(def.Type, "V") && def.VectorDimensions > 0 {
			return def.VectorDimensions, true
		}
	}
	return 0, false
}

// itemSourceFor produces a plugin.ItemSource over the live table.
func (s *GRPCServer) itemSourceFor(table string) func(yield func(model.Item) bool) {
	source, _, err := s.indexItemSourceFor(table)
	if err != nil {
		return func(yield func(model.Item) bool) {}
	}
	return source
}

func (s *GRPCServer) indexItemSourceFor(table string) (func(yield func(model.Item) bool), int, error) {
	td, err := s.cat.Describe(table)
	if err != nil {
		return nil, 0, mapStorageErr(err)
	}
	stores, err := s.scatterReadStores()
	if err != nil {
		return nil, 0, mapStorageErr(err)
	}
	if len(stores) == 0 {
		return nil, 0, status.Error(codes.Unavailable, "no readable shards available")
	}
	seen := make(map[string]struct{})
	items := make([]model.Item, 0)
	for _, db := range stores {
		scanned, err := db.ScanTable(table, 0)
		if err != nil {
			return nil, 0, mapStorageErr(err)
		}
		for _, it := range scanned {
			id, err := primaryIdentity(it, td.KeySchema)
			if err != nil {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			items = append(items, it)
		}
	}
	return func(yield func(model.Item) bool) {
		for _, it := range items {
			if !yield(it) {
				return
			}
		}
	}, len(items), nil
}

// ---------- Explain ----------

func (s *GRPCServer) Explain(ctx context.Context, req *cefaspb.ExplainRequest) (*cefaspb.ExplainResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "Explain")
	defer span.End()
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
	if strings.Contains(strings.ToUpper(req.GetPredicate()), " ANN ") {
		root = cquery.PlanNode{
			Op:     "TopK",
			Plugin: "ann",
			Detail: fmt.Sprintf("table=%s predicate=%q", req.GetTable(), req.GetPredicate()),
			Children: []cquery.PlanNode{
				{Op: "ScanTable", Plugin: "core", Detail: req.GetTable()},
			},
		}
	}
	fmtKind := cquery.ExplainText
	if strings.EqualFold(req.GetFormat(), "json") {
		fmtKind = cquery.ExplainJSON
	}
	return &cefaspb.ExplainResponse{Plan: cquery.RenderExplain(root, fmtKind)}, nil
}

// ---------- TopK ----------

func (s *GRPCServer) TopK(ctx context.Context, req *cefaspb.TopKRequest) (*cefaspb.TopKResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "TopK")
	defer span.End()
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemRead, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemRead)); err != nil {
		return nil, err
	}
	if req.GetK() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "k must be > 0")
	}
	if req.GetField() == "" {
		return nil, status.Error(codes.InvalidArgument, "field required")
	}
	target, err := pbToAttr(req.GetTarget())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("target: %v", err))
	}
	dist, err := s.resolveTopKDistance(req.GetTable(), req.GetField(), req.GetDistanceOperator(), target)
	if err != nil {
		return nil, err
	}
	if ann, ok, err := s.indexedANNTopK(req.GetTable(), req.GetField(), target, int(req.GetK()), req.GetDistanceOperator()); err != nil {
		return nil, err
	} else if ok {
		return &cefaspb.TopKResponse{Rows: topKRowsToPB(ann.rows)}, nil
	}
	if strings.TrimSpace(req.GetDistanceOperator()) == "" {
		return nil, status.Errorf(codes.FailedPrecondition, "no ann index for %s.%s", req.GetTable(), req.GetField())
	}
	rows, _, err := s.exactScanTopK(req.GetTable(), req.GetField(), target, int(req.GetK()), dist, req.GetDistanceOperator())
	if err != nil {
		return nil, err
	}
	return &cefaspb.TopKResponse{Rows: topKRowsToPB(rows)}, nil
}

func (s *GRPCServer) resolveTopKDistance(table, field, explicit string, target model.AttributeValue) (cquery.DistanceOp, error) {
	name := strings.TrimSpace(explicit)
	if name == "" {
		cfg, ok, err := s.findANNConfig(table, field, target)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if !ok {
			return nil, status.Errorf(codes.FailedPrecondition, "no ann index for %s.%s", table, field)
		}
		name = cfg.Metric
	}
	plug, ok := s.pluginRegistry().Lookup(name)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "distance plugin %q not registered", name)
	}
	dist, ok := plug.(plugin.DistancePlugin)
	if !ok {
		return nil, status.Errorf(codes.InvalidArgument, "plugin %q is not a DistancePlugin", name)
	}
	return dist, nil
}

func (s *GRPCServer) findANNConfig(table, field string, target model.AttributeValue) (annConfig, bool, error) {
	_, cfg, ok, err := s.findANNDescriptor(table, field, target)
	return cfg, ok, err
}

func attrVectorDim(av model.AttributeValue) int {
	switch av.T {
	case model.AttrVec:
		return len(av.Vec)
	case model.AttrL:
		return len(av.L)
	case model.AttrNS:
		return len(av.NS)
	default:
		return 0
	}
}

// ---------- Cohort ----------

func (s *GRPCServer) CohortCreate(ctx context.Context, req *cefaspb.CohortCreateRequest) (*cefaspb.CohortCreateResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "CohortCreate")
	defer span.End()
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
	ctx, span := tracing.Tracer().Start(ctx, "CohortEstimate")
	defer span.End()
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
	ctx, span := tracing.Tracer().Start(stream.Context(), "GeoAudience")
	defer span.End()
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
	d, ok, err := s.lookupPluginIndexDescriptor(req.GetTable(), idxName)
	if err != nil {
		return status.Errorf(codes.Internal, "index lookup: %v", err)
	}
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
	ctx, span := tracing.Tracer().Start(ctx, "Dedup")
	defer span.End()
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
	ctx, span := tracing.Tracer().Start(ctx, "FreqCap")
	defer span.End()
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
	ctx, span := tracing.Tracer().Start(ctx, "Aggregate")
	defer span.End()
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
