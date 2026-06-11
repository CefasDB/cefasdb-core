package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/auth"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/metrics"
	craft "github.com/osvaldoandrade/cefas/internal/raft"
	"github.com/osvaldoandrade/cefas/internal/spatial"
	"github.com/osvaldoandrade/cefas/internal/storage"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
	cefassql "github.com/osvaldoandrade/cefas/pkg/sql"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// GRPCServer implements cefaspb.CefasServer over the cefas storage,
// catalog, and (optional) cluster surface. It reuses the auth package
// for per-operation scope checks so the gRPC and HTTP entry points
// enforce the same matrix.
type GRPCServer struct {
	cefaspb.UnimplementedCefasServer

	db      *storage.DB
	cat     *catalog.Catalog
	cluster Cluster          // nil in single-node mode
	stream  ChangeStream     // nil when no CDC source attached
	manager *cluster.Manager // nil in single-shard mode
	plugins *plugin.Registry // nil → uses plugin.Default
	metrics *metrics.Metrics // nil when metrics disabled
}

// NewGRPCServer wires the gRPC handler over the same storage / catalog
// instances the HTTP server uses. Cluster may be nil.
func NewGRPCServer(db *storage.DB, cat *catalog.Catalog, cluster Cluster) *GRPCServer {
	return &GRPCServer{db: db, cat: cat, cluster: cluster}
}

// AttachManager wires the multi-shard manager onto the gRPC handler.
// Without it the handler routes every write to s.db (single-shard).
func (s *GRPCServer) AttachManager(m *cluster.Manager) { s.manager = m }

// AttachPluginRegistry overrides the default plugin registry. The
// server falls back to plugin.Default when no registry is attached —
// the same registry built-in plugins register against via init().
func (s *GRPCServer) AttachPluginRegistry(r *plugin.Registry) { s.plugins = r }

// AttachMetrics wires bounded range-hotspot summaries into gRPC
// cluster status and records PK-bearing gRPC operations.
func (s *GRPCServer) AttachMetrics(m *metrics.Metrics) { s.metrics = m }

func (s *GRPCServer) pluginRegistry() *plugin.Registry {
	if s.plugins != nil {
		return s.plugins
	}
	return plugin.Default
}

func (s *GRPCServer) storageFor(pkBytes []byte) *storage.DB {
	if s.manager == nil {
		return s.db
	}
	if shard := s.manager.ShardForPK(pkBytes); shard != nil {
		return shard.Storage
	}
	return s.db
}

func (s *GRPCServer) allShards() []*storage.DB {
	if s.manager == nil {
		return []*storage.DB{s.db}
	}
	out := make([]*storage.DB, 0, len(s.manager.Shards()))
	for _, sh := range s.manager.Shards() {
		out = append(out, sh.Storage)
	}
	return out
}

func (s *GRPCServer) compact(table string, lower, upper []byte, parallelize bool) ([]storage.CompactionResult, error) {
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
	if len(lower) == 0 || len(upper) == 0 {
		return nil, fmt.Errorf("table or lower/upper range is required")
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

// ---------- schema ----------

func (s *GRPCServer) CreateTable(ctx context.Context, req *cefaspb.CreateTableRequest) (*cefaspb.CreateTableResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	td := pbToTableDescriptor(req.GetDescriptor_())
	if err := s.cat.Create(td); err != nil {
		return nil, mapStorageErr(err)
	}
	// Fan out to every other shard so each can resolve the schema
	// locally when a write lands on it.
	if s.manager != nil {
		for i, sh := range s.manager.Shards() {
			if i == 0 {
				continue
			}
			if cat, err := catalog.New(sh.Storage); err == nil {
				_ = cat.Create(td)
			}
		}
	}
	return &cefaspb.CreateTableResponse{Descriptor_: tableDescriptorToPB(td)}, nil
}

func (s *GRPCServer) DescribeTable(ctx context.Context, req *cefaspb.DescribeTableRequest) (*cefaspb.DescribeTableResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	td, err := s.cat.Describe(req.GetName())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.DescribeTableResponse{Descriptor_: tableDescriptorToPB(td)}, nil
}

func (s *GRPCServer) ListTables(ctx context.Context, _ *cefaspb.ListTablesRequest) (*cefaspb.ListTablesResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	all := s.cat.List()
	out := make([]*cefaspb.TableDescriptor, 0, len(all))
	for _, td := range all {
		out = append(out, tableDescriptorToPB(td))
	}
	return &cefaspb.ListTablesResponse{Tables: out}, nil
}

func (s *GRPCServer) DropTable(ctx context.Context, req *cefaspb.DropTableRequest) (*cefaspb.DropTableResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableDrop); err != nil {
		return nil, err
	}
	if err := s.cat.Drop(req.GetName()); err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.DropTableResponse{}, nil
}

func (s *GRPCServer) UpdateTimeToLive(ctx context.Context, req *cefaspb.UpdateTimeToLiveRequest) (*cefaspb.UpdateTimeToLiveResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	spec := req.GetTimeToLiveSpecification()
	if spec == nil {
		return nil, status.Error(codes.InvalidArgument, "TimeToLiveSpecification required")
	}
	td, err := s.cat.Describe(req.GetTableName())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	if spec.GetEnabled() {
		if spec.GetAttributeName() == "" {
			return nil, status.Error(codes.InvalidArgument, "attribute_name required when enabling TTL")
		}
		td.TTLAttribute = spec.GetAttributeName()
	} else {
		td.TTLAttribute = ""
	}
	if err := s.cat.UpdateTable(td); err != nil {
		return nil, mapStorageErr(err)
	}
	if s.manager != nil {
		for i, sh := range s.manager.Shards() {
			if i == 0 {
				continue
			}
			if cat, cerr := catalog.New(sh.Storage); cerr == nil {
				_ = cat.UpdateTable(td)
			}
		}
	}
	return &cefaspb.UpdateTimeToLiveResponse{
		TimeToLiveSpecification: &cefaspb.TimeToLiveSpecification{
			Enabled:       td.TTLAttribute != "",
			AttributeName: td.TTLAttribute,
		},
	}, nil
}

func (s *GRPCServer) DescribeTimeToLive(ctx context.Context, req *cefaspb.DescribeTimeToLiveRequest) (*cefaspb.DescribeTimeToLiveResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	td, err := s.cat.Describe(req.GetTableName())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	resp := &cefaspb.DescribeTimeToLiveResponse{Status: "DISABLED"}
	if td.TTLAttribute != "" {
		resp.Status = "ENABLED"
		resp.AttributeName = td.TTLAttribute
	}
	return resp, nil
}

// ---------- item ----------

func (s *GRPCServer) PutItem(ctx context.Context, req *cefaspb.PutItemRequest) (*cefaspb.PutItemResponse, error) {
	started := time.Now()
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemWrite, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemWrite)); err != nil {
		return nil, err
	}
	td, err := s.cat.Describe(req.GetTable())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	item, err := pbToItem(req.GetItem())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	binds, err := pbToItem(req.GetBinds())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("binds: %v", err))
	}
	pkBytes, err := pkBytesFromItem(item, td.KeySchema)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	targets, err := s.writeTargetsForPK(pkBytes)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	defer targets.Release()
	if err := targets.PutItemWith(td, item, storage.PutOptions{Condition: req.GetCondition(), Binds: binds}); err != nil {
		return nil, mapStorageErr(err)
	}
	s.observeRangeMetric(rangeMetricWrite, pkBytes, estimatedItemBytes(item), started)
	return &cefaspb.PutItemResponse{}, nil
}

func (s *GRPCServer) GetItem(ctx context.Context, req *cefaspb.GetItemRequest) (*cefaspb.GetItemResponse, error) {
	started := time.Now()
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemRead, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemRead)); err != nil {
		return nil, err
	}
	if req.GetConsistency() == cefaspb.Consistency_CONSISTENCY_STRONG {
		if err := s.strongReadGate(); err != nil {
			return nil, err
		}
	}
	td, err := s.cat.Describe(req.GetTable())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	key, err := pbToItem(req.GetKey())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	pkBytes, err := pkBytesFromItem(key, td.KeySchema)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	item, err := s.storageFor(pkBytes).GetItem(req.GetTable(), td.KeySchema, key)
	if err != nil {
		if errors.Is(err, types.ErrItemNotFound) {
			s.observeRangeMetric(rangeMetricRead, pkBytes, uint64(len(pkBytes)), started)
			return &cefaspb.GetItemResponse{Found: false}, nil
		}
		return nil, mapStorageErr(err)
	}
	s.observeRangeMetric(rangeMetricRead, pkBytes, estimatedItemBytes(item), started)
	return &cefaspb.GetItemResponse{Found: true, Item: itemToPB(item)}, nil
}

func (s *GRPCServer) UpdateItem(ctx context.Context, req *cefaspb.UpdateItemRequest) (*cefaspb.UpdateItemResponse, error) {
	started := time.Now()
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemWrite, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemWrite)); err != nil {
		return nil, err
	}
	td, err := s.cat.Describe(req.GetTable())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	key, err := pbToItem(req.GetKey())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	values, err := pbToItem(req.GetExpressionAttributeValues())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("expression_attribute_values: %v", err))
	}
	sql, wantImage, err := translateUpdateItem(
		req.GetTable(),
		key,
		td.KeySchema,
		req.GetUpdateExpression(),
		req.GetConditionExpression(),
		req.GetExpressionAttributeNames(),
		values,
		returnValuesName(req.GetReturnValues()),
	)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	stmt, err := cefassql.Parse(sql)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("translate: %v", err))
	}
	pkBytes, err := pkBytesFromItem(key, td.KeySchema)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	targets, err := s.writeTargetsForPK(pkBytes)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	defer targets.Release()
	db := targets.primary
	plan, err := cefassql.PlanStmt(stmt, s.cat)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("plan: %v", err))
	}
	ex := &cefassql.Executor{Storage: db, Catalog: s.cat}
	res, err := ex.Execute(plan)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	if len(targets.mirrors) > 0 {
		finalItem, err := db.GetItem(req.GetTable(), td.KeySchema, key)
		if err != nil {
			return nil, mapStorageErr(err)
		}
		if err := targets.MirrorPutItem(td, finalItem); err != nil {
			return nil, mapStorageErr(err)
		}
	}
	resp := &cefaspb.UpdateItemResponse{}
	if wantImage != "" && len(res.Rows) > 0 {
		resp.Attributes = itemToPB(res.Rows[0])
	}
	approxBytes := uint64(len(pkBytes))
	if len(res.Rows) > 0 {
		approxBytes = estimatedItemBytes(res.Rows[0])
	}
	s.observeRangeMetric(rangeMetricWrite, pkBytes, approxBytes, started)
	return resp, nil
}

func returnValuesName(v cefaspb.ReturnValues) string {
	switch v {
	case cefaspb.ReturnValues_RETURN_VALUES_ALL_NEW:
		return "ALL_NEW"
	case cefaspb.ReturnValues_RETURN_VALUES_ALL_OLD:
		return "ALL_OLD"
	case cefaspb.ReturnValues_RETURN_VALUES_UPDATED_NEW:
		return "UPDATED_NEW"
	case cefaspb.ReturnValues_RETURN_VALUES_UPDATED_OLD:
		return "UPDATED_OLD"
	}
	return "NONE"
}

func (s *GRPCServer) DeleteItem(ctx context.Context, req *cefaspb.DeleteItemRequest) (*cefaspb.DeleteItemResponse, error) {
	started := time.Now()
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemDelete, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemDelete)); err != nil {
		return nil, err
	}
	td, err := s.cat.Describe(req.GetTable())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	key, err := pbToItem(req.GetKey())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	binds, err := pbToItem(req.GetBinds())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("binds: %v", err))
	}
	pkBytes, err := pkBytesFromItem(key, td.KeySchema)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	targets, err := s.writeTargetsForPK(pkBytes)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	defer targets.Release()
	if err := targets.DeleteItemWith(td, key, storage.DeleteOptions{Condition: req.GetCondition(), Binds: binds}); err != nil {
		return nil, mapStorageErr(err)
	}
	s.observeRangeMetric(rangeMetricWrite, pkBytes, uint64(len(pkBytes)), started)
	return &cefaspb.DeleteItemResponse{}, nil
}

func (s *GRPCServer) BatchWriteItem(ctx context.Context, req *cefaspb.BatchWriteItemRequest) (*cefaspb.BatchWriteItemResponse, error) {
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemWrite, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemWrite)); err != nil {
		return nil, err
	}
	td, err := s.cat.Describe(req.GetTable())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	ops := make([]storage.BatchOp, 0, len(req.GetOps()))
	for i, raw := range req.GetOps() {
		switch raw.GetKind() {
		case cefaspb.BatchWriteOp_KIND_PUT:
			item, err := pbToItem(raw.GetItem())
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "op %d: %v", i, err)
			}
			ops = append(ops, storage.BatchOp{Op: storage.BatchOpPut, Item: item})
		case cefaspb.BatchWriteOp_KIND_DELETE:
			key, err := pbToItem(raw.GetKey())
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "op %d: %v", i, err)
			}
			ops = append(ops, storage.BatchOp{Op: storage.BatchOpDelete, Key: key})
		default:
			return nil, status.Errorf(codes.InvalidArgument, "op %d: unknown kind", i)
		}
	}
	if err := s.batchWriteFanOut(td, ops); err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.BatchWriteItemResponse{}, nil
}

func (s *GRPCServer) batchWriteFanOut(td types.TableDescriptor, ops []storage.BatchOp) error {
	if s.manager == nil {
		started := time.Now()
		if err := s.db.BatchWriteItem(td, ops); err != nil {
			return err
		}
		for _, op := range ops {
			probe := op.Item
			approxBytes := estimatedItemBytes(op.Item)
			if op.Op == storage.BatchOpDelete {
				probe = op.Key
				approxBytes = estimatedItemBytes(op.Key)
			}
			pkBytes, err := pkBytesFromItem(probe, td.KeySchema)
			if err != nil {
				return err
			}
			s.observeRangeMetric(rangeMetricWrite, pkBytes, approxBytes, started)
		}
		return nil
	}
	primaryBuckets := make(map[*storage.DB][]storage.BatchOp)
	mirrorBuckets := make(map[*storage.DB][]storage.BatchOp)
	type observation struct {
		pkBytes     []byte
		approxBytes uint64
	}
	observations := make([]observation, 0, len(ops))
	var releases []func()
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}()
	for _, op := range ops {
		probe := op.Item
		approxBytes := estimatedItemBytes(op.Item)
		if op.Op == storage.BatchOpDelete {
			probe = op.Key
			approxBytes = estimatedItemBytes(op.Key)
		}
		pkBytes, err := pkBytesFromItem(probe, td.KeySchema)
		if err != nil {
			return err
		}
		observations = append(observations, observation{pkBytes: append([]byte(nil), pkBytes...), approxBytes: approxBytes})
		targets, err := s.writeTargetsForPK(pkBytes)
		if err != nil {
			return err
		}
		releases = append(releases, targets.Release)
		primaryBuckets[targets.primary] = append(primaryBuckets[targets.primary], op)
		for _, mirror := range targets.mirrors {
			mirrorBuckets[mirror] = append(mirrorBuckets[mirror], op)
		}
	}
	started := time.Now()
	for db, group := range primaryBuckets {
		if err := db.BatchWriteItem(td, group); err != nil {
			return err
		}
	}
	for db, group := range mirrorBuckets {
		if err := db.BatchWriteItem(td, group); err != nil {
			return err
		}
	}
	for _, obs := range observations {
		s.observeRangeMetric(rangeMetricWrite, obs.pkBytes, obs.approxBytes, started)
	}
	return nil
}

func (s *GRPCServer) BatchGetItem(ctx context.Context, req *cefaspb.BatchGetItemRequest) (*cefaspb.BatchGetItemResponse, error) {
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemRead, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemRead)); err != nil {
		return nil, err
	}
	td, err := s.cat.Describe(req.GetTable())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	keys := make([]types.Item, 0, len(req.GetKeys()))
	for i, k := range req.GetKeys() {
		ka, err := pbToItem(k.GetAttributes())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "key %d: %v", i, err)
		}
		keys = append(keys, ka)
	}
	items, err := s.batchGetFanOut(req.GetTable(), td.KeySchema, keys)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	out := make([]*cefaspb.Item, len(items))
	for i, it := range items {
		if it == nil {
			out[i] = &cefaspb.Item{} // empty attributes — caller distinguishes by missing values
			continue
		}
		out[i] = &cefaspb.Item{Attributes: itemToPB(it)}
	}
	return &cefaspb.BatchGetItemResponse{Items: out}, nil
}

// ---------- streaming ----------

func (s *GRPCServer) Query(req *cefaspb.QueryRequest, stream cefaspb.Cefas_QueryServer) error {
	started := time.Now()
	ctx := stream.Context()
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeQuery, req.GetTable()),
		auth.WildcardScope(auth.ScopeQuery)); err != nil {
		return err
	}
	if req.GetConsistency() == cefaspb.Consistency_CONSISTENCY_STRONG {
		if err := s.strongReadGate(); err != nil {
			return err
		}
	}
	td, err := s.cat.Describe(req.GetTable())
	if err != nil {
		return mapStorageErr(err)
	}
	pkVal, err := pbToAttr(req.GetPkValue())
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("pk_value: %v", err))
	}
	var lo, hi types.AttributeValue
	if v := req.GetSkLow(); v != nil {
		lo, err = pbToAttr(v)
		if err != nil {
			return status.Error(codes.InvalidArgument, fmt.Sprintf("sk_low: %v", err))
		}
	}
	if v := req.GetSkHigh(); v != nil {
		hi, err = pbToAttr(v)
		if err != nil {
			return status.Error(codes.InvalidArgument, fmt.Sprintf("sk_high: %v", err))
		}
	}

	pkBytes, err := storage.AttrCanonicalBytes(pkVal)
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("pk_value: %v", err))
	}
	queryDB := s.storageFor(pkBytes)
	var items []types.Item
	limit := int(req.GetLimit())
	switch {
	case req.GetIndexName() != "":
		items, err = s.queryByIndex(td, req.GetIndexName(), pkVal, storage.QueryOptions{SKLow: lo, SKHigh: hi, Limit: limit})
	case req.GetSkLow() == nil && req.GetSkHigh() == nil:
		items, err = queryDB.QueryByPK(req.GetTable(), td.KeySchema, pkVal, limit)
	default:
		items, err = queryDB.QueryByPKRange(req.GetTable(), td.KeySchema, pkVal, lo, hi, limit)
	}
	if err != nil {
		return mapStorageErr(err)
	}
	s.observeRangeMetric(rangeMetricRead, pkBytes, estimatedItemsBytes(items), started)
	for _, it := range items {
		if err := stream.Send(&cefaspb.Item{Attributes: itemToPB(it)}); err != nil {
			return err
		}
	}
	return nil
}

func (s *GRPCServer) Scan(req *cefaspb.ScanRequest, stream cefaspb.Cefas_ScanServer) error {
	ctx := stream.Context()
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeScan, req.GetTable()),
		auth.WildcardScope(auth.ScopeScan)); err != nil {
		return err
	}
	if req.GetConsistency() == cefaspb.Consistency_CONSISTENCY_STRONG {
		if err := s.strongReadGate(); err != nil {
			return err
		}
	}
	if _, err := s.cat.Describe(req.GetTable()); err != nil {
		return mapStorageErr(err)
	}
	cond, err := storage.ParseCondition(req.GetFilterExpression())
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("filter_expression: %v", err))
	}
	rawBinds, err := pbToItem(req.GetBinds())
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("binds: %v", err))
	}
	// Wire keeps the `:name` prefix; the condition evaluator expects
	// the bare name. Strip here so callers pass the AWS-shaped map.
	binds := make(map[string]types.AttributeValue, len(rawBinds))
	for k, v := range rawBinds {
		binds[strings.TrimPrefix(k, ":")] = v
	}
	limit := int(req.GetLimit())

	dbs := []*storage.DB{s.db}
	if s.manager != nil {
		dbs = dbs[:0]
		for _, sh := range s.manager.Shards() {
			dbs = append(dbs, sh.Storage)
		}
	}

	sent := 0
	for _, db := range dbs {
		// Pull every primary item; filter + cap on the way out so a
		// permissive filter doesn't materialise the full shard in
		// memory before stopping.
		items, err := db.ScanTable(req.GetTable(), 0)
		if err != nil {
			return mapStorageErr(err)
		}
		for _, it := range items {
			if !cond.IsZero() {
				ok, err := cond.Evaluate(it, binds)
				if err != nil {
					return status.Error(codes.InvalidArgument, fmt.Sprintf("evaluate filter: %v", err))
				}
				if !ok {
					continue
				}
			}
			if err := stream.Send(&cefaspb.Item{Attributes: itemToPB(it)}); err != nil {
				return err
			}
			sent++
			if limit > 0 && sent >= limit {
				return nil
			}
		}
	}
	return nil
}

func (s *GRPCServer) SpatialQuery(req *cefaspb.SpatialQueryRequest, stream cefaspb.Cefas_SpatialQueryServer) error {
	ctx := stream.Context()
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeSpatial, req.GetTable()),
		auth.WildcardScope(auth.ScopeSpatial)); err != nil {
		return err
	}
	td, err := s.cat.Describe(req.GetTable())
	if err != nil {
		return mapStorageErr(err)
	}
	q := storage.SpatialQuery{Limit: int(req.GetLimit())}
	switch shape := req.GetShape().(type) {
	case *cefaspb.SpatialQueryRequest_Bbox:
		b := shape.Bbox
		q.BBox = &spatial.BBox{MinLat: b.GetMinLat(), MinLon: b.GetMinLon(), MaxLat: b.GetMaxLat(), MaxLon: b.GetMaxLon()}
	case *cefaspb.SpatialQueryRequest_Radius:
		r := shape.Radius
		q.Radius = &storage.RadiusQuery{Lat: r.GetLat(), Lon: r.GetLon(), Meters: r.GetMeters()}
	case *cefaspb.SpatialQueryRequest_Z:
		q.Z = &spatial.ZBBox{Lo: append([]uint32(nil), shape.Z.GetLo()...), Hi: append([]uint32(nil), shape.Z.GetHi()...)}
	default:
		return status.Error(codes.InvalidArgument, "one of bbox/radius/z required")
	}
	items, err := s.scatterSpatial(td, req.GetIndexName(), q)
	if err != nil {
		return mapStorageErr(err)
	}
	for _, it := range items {
		if err := stream.Send(&cefaspb.Item{Attributes: itemToPB(it)}); err != nil {
			return err
		}
	}
	return nil
}

// scatterSpatial fans the spatial query across every shard. Spatial
// indexes are partitioned by the item's PK so the matching rows can
// live on any shard.
func (s *GRPCServer) scatterSpatial(td types.TableDescriptor, idxName string, q storage.SpatialQuery) ([]types.Item, error) {
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

// ---------- sql ----------

func (s *GRPCServer) Sql(ctx context.Context, req *cefaspb.SqlRequest) (*cefaspb.SqlResponse, error) {
	stmt, err := cefassql.Parse(req.GetQuery())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := sqlScopeCheck(ctx, stmt); err != nil {
		return nil, err
	}
	plan, err := cefassql.PlanStmt(stmt, s.cat)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ex := &cefassql.Executor{Storage: s.db, Catalog: s.cat}
	res, err := ex.Execute(plan)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	out := &cefaspb.SqlResponse{AffectedRows: int64(res.AffectedRows)}
	for _, row := range res.Rows {
		out.Rows = append(out.Rows, &cefaspb.Item{Attributes: itemToPB(row)})
	}
	return out, nil
}

func sqlScopeCheck(ctx context.Context, stmt cefassql.Stmt) error {
	switch s := stmt.(type) {
	case *cefassql.SelectStmt:
		return requireAnyScope(ctx,
			auth.TableScope(auth.ScopeQuery, s.Table),
			auth.WildcardScope(auth.ScopeQuery),
			auth.TableScope(auth.ScopeItemRead, s.Table),
			auth.WildcardScope(auth.ScopeItemRead))
	case *cefassql.InsertStmt:
		return requireAnyScope(ctx,
			auth.TableScope(auth.ScopeItemWrite, s.Table),
			auth.WildcardScope(auth.ScopeItemWrite))
	case *cefassql.UpdateStmt:
		return requireAnyScope(ctx,
			auth.TableScope(auth.ScopeItemWrite, s.Table),
			auth.WildcardScope(auth.ScopeItemWrite))
	case *cefassql.DeleteStmt:
		return requireAnyScope(ctx,
			auth.TableScope(auth.ScopeItemDelete, s.Table),
			auth.WildcardScope(auth.ScopeItemDelete))
	case *cefassql.CreateTableStmt:
		return requireScope(ctx, auth.ScopeTableCreate)
	case *cefassql.DropTableStmt:
		return requireScope(ctx, auth.ScopeTableDrop)
	}
	return nil
}

// ---------- CDC + snapshot admin ----------

// AttachChangeStream wires the CDC + snapshot listing source onto
// the gRPC handler.
func (s *GRPCServer) AttachChangeStream(c ChangeStream) { s.stream = c }

func (s *GRPCServer) StreamChanges(req *cefaspb.StreamChangesRequest, stream cefaspb.Cefas_StreamChangesServer) error {
	if s.stream == nil {
		return status.Error(codes.FailedPrecondition, "change stream not configured")
	}
	events, cancel := s.stream.SubscribeChanges(stream.Context())
	defer cancel()
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			if req.GetFromIndex() != 0 && ev.RaftIndex < req.GetFromIndex() {
				continue
			}
			op := cefaspb.ChangeEvent_OP_PUT
			if ev.Op == "DELETE" {
				op = cefaspb.ChangeEvent_OP_DELETE
			}
			if err := stream.Send(&cefaspb.ChangeEvent{
				RaftIndex: ev.RaftIndex,
				Op:        op,
				Key:       ev.Key,
				Value:     ev.Value,
			}); err != nil {
				return err
			}
		}
	}
}

func (s *GRPCServer) CreateBackup(ctx context.Context, req *cefaspb.CreateBackupRequest) (*cefaspb.CreateBackupResponse, error) {
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	tables := req.GetTables()
	if len(tables) == 0 {
		for _, td := range s.cat.List() {
			tables = append(tables, td.Name)
		}
	}
	meta, err := s.db.CreateBackup(req.GetName(), tables)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.CreateBackupResponse{Backup: backupMetaToPB(meta)}, nil
}

func (s *GRPCServer) ListBackups(ctx context.Context, _ *cefaspb.ListBackupsRequest) (*cefaspb.ListBackupsResponse, error) {
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	metas, err := s.db.ListBackups()
	if err != nil {
		return nil, mapStorageErr(err)
	}
	out := make([]*cefaspb.BackupDescriptor, 0, len(metas))
	for _, m := range metas {
		out = append(out, backupMetaToPB(m))
	}
	return &cefaspb.ListBackupsResponse{Backups: out}, nil
}

func (s *GRPCServer) DeleteBackup(ctx context.Context, req *cefaspb.DeleteBackupRequest) (*cefaspb.DeleteBackupResponse, error) {
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	result, err := s.db.DeleteBackup(req.GetName())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.DeleteBackupResponse{Result: backupDeletionToPB(result)}, nil
}

func (s *GRPCServer) ApplyBackupRetention(ctx context.Context, req *cefaspb.ApplyBackupRetentionRequest) (*cefaspb.ApplyBackupRetentionResponse, error) {
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	result, err := s.db.ApplyBackupRetention(storage.BackupRetentionOptions{
		KeepLatest:    int(req.GetKeepLatest()),
		KeepLatestSet: req.GetKeepLatestSet(),
		MaxAge:        time.Duration(req.GetMaxAgeSeconds()) * time.Second,
		MaxAgeSet:     req.GetMaxAgeSet(),
		DryRun:        req.GetDryRun(),
	})
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return backupRetentionToPB(result), nil
}

func (s *GRPCServer) RestoreTableFromBackup(ctx context.Context, req *cefaspb.RestoreTableFromBackupRequest) (*cefaspb.RestoreTableFromBackupResponse, error) {
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	register := func(td types.TableDescriptor) error {
		if err := s.cat.Create(td); err != nil {
			return err
		}
		if s.manager != nil {
			for i, sh := range s.manager.Shards() {
				if i == 0 {
					continue
				}
				if cat, cerr := catalog.New(sh.Storage); cerr == nil {
					_ = cat.Create(td)
				}
			}
		}
		return nil
	}
	var res storage.RestoreResult
	var err error
	if req.GetDryRun() {
		res, err = s.db.RestoreTableFromBackupWithOptions(
			req.GetBackupName(),
			req.GetSourceTableName(),
			req.GetTargetTableName(),
			storage.RestoreOptions{DryRun: true},
			register,
		)
	} else {
		res, err = s.db.RestoreTableFromBackup(
			req.GetBackupName(),
			req.GetSourceTableName(),
			req.GetTargetTableName(),
			register,
		)
	}
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.RestoreTableFromBackupResponse{
		TargetTableName:  res.TargetTable.Name,
		RowsCopied:       int64(res.RowsCopied),
		DryRun:           res.DryRun,
		SourceTableStats: backupTableStatsToPB(res.SourceStats),
		ManifestVersion:  int32(res.ManifestVersion),
		ManifestStatus:   res.ManifestStatus,
	}, nil
}

func backupMetaToPB(m storage.BackupMetadata) *cefaspb.BackupDescriptor {
	stats := make([]*cefaspb.BackupTableStats, 0, len(m.TableStats))
	for _, stat := range m.TableStats {
		stats = append(stats, backupTableStatsToPB(stat))
	}
	return &cefaspb.BackupDescriptor{
		Name:            m.Name,
		CreatedAtUnix:   m.CreatedAt,
		Tables:          m.Tables,
		CheckpointPath:  m.CheckpointAt,
		ManifestVersion: int32(m.ManifestVersion),
		ManifestStatus:  m.ManifestStatus,
		RequestedTables: m.RequestedTables,
		TableStats:      stats,
	}
}

func backupTableStatsToPB(stat storage.BackupTableStats) *cefaspb.BackupTableStats {
	if stat.Table == "" && stat.Rows == 0 && stat.Checksum == "" {
		return nil
	}
	return &cefaspb.BackupTableStats{
		Table:    stat.Table,
		Rows:     stat.Rows,
		Checksum: stat.Checksum,
	}
}

func backupDeletionToPB(result storage.BackupDeletionResult) *cefaspb.BackupDeletionResult {
	return &cefaspb.BackupDeletionResult{
		BackupName:        result.BackupName,
		CheckpointPath:    result.CheckpointPath,
		MetadataDeleted:   result.MetadataDeleted,
		CheckpointDeleted: result.CheckpointDeleted,
		CheckpointMissing: result.CheckpointMissing,
		PartialCleanup:    result.PartialCleanup,
		CleanupError:      result.CleanupError,
	}
}

func backupRetentionToPB(result storage.BackupRetentionResult) *cefaspb.ApplyBackupRetentionResponse {
	wouldDelete := make([]*cefaspb.BackupRetentionCandidate, 0, len(result.WouldDelete))
	for _, candidate := range result.WouldDelete {
		wouldDelete = append(wouldDelete, backupRetentionCandidateToPB(candidate))
	}
	deleted := make([]*cefaspb.BackupDeletionResult, 0, len(result.Deleted))
	for _, item := range result.Deleted {
		deleted = append(deleted, backupDeletionToPB(item))
	}
	return &cefaspb.ApplyBackupRetentionResponse{
		DryRun:        result.DryRun,
		KeepLatest:    int32(result.KeepLatest),
		KeepLatestSet: result.KeepLatestSet,
		MaxAgeSeconds: result.MaxAgeSeconds,
		MaxAgeSet:     result.MaxAgeSet,
		CutoffUnix:    result.CutoffUnix,
		WouldDelete:   wouldDelete,
		Deleted:       deleted,
	}
}

func backupRetentionCandidateToPB(candidate storage.BackupRetentionCandidate) *cefaspb.BackupRetentionCandidate {
	return &cefaspb.BackupRetentionCandidate{
		Backup: backupMetaToPB(candidate.Backup),
		Reason: candidate.Reason,
	}
}

func (s *GRPCServer) ListSnapshots(ctx context.Context, _ *cefaspb.ListSnapshotsRequest) (*cefaspb.ListSnapshotsResponse, error) {
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	if s.stream == nil {
		return &cefaspb.ListSnapshotsResponse{}, nil
	}
	metas, err := s.stream.ListSnapshots()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := make([]*cefaspb.SnapshotMetadata, 0, len(metas))
	for _, m := range metas {
		out = append(out, &cefaspb.SnapshotMetadata{
			Id:          m.ID,
			RaftIndex:   m.Index,
			RaftTerm:    m.Term,
			UnixSeconds: m.UnixSeconds,
			SizeBytes:   m.SizeBytes,
		})
	}
	return &cefaspb.ListSnapshotsResponse{Snapshots: out}, nil
}

func (s *GRPCServer) Compact(ctx context.Context, req *cefaspb.CompactRequest) (*cefaspb.CompactResponse, error) {
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	results, err := s.compact(req.GetTable(), req.GetLower(), req.GetUpper(), req.GetParallelize())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	out := make([]*cefaspb.CompactResult, 0, len(results))
	for _, r := range results {
		out = append(out, &cefaspb.CompactResult{
			Table:            r.Table,
			Lower:            append([]byte(nil), r.Lower...),
			Upper:            append([]byte(nil), r.Upper...),
			StartedAtUnixNs:  r.StartedAt.UnixNano(),
			FinishedAtUnixNs: r.FinishedAt.UnixNano(),
			ElapsedSeconds:   r.Elapsed.Seconds(),
			Parallelized:     r.Parallelized,
			BeforeL0Files:    r.BeforeL0Files,
			AfterL0Files:     r.AfterL0Files,
			BeforeDebtBytes:  r.BeforeDebtBytes,
			AfterDebtBytes:   r.AfterDebtBytes,
		})
	}
	return &cefaspb.CompactResponse{Results: out}, nil
}

// ---------- cluster ----------

func (s *GRPCServer) batchGetFanOut(table string, ks types.KeySchema, keys []types.Item) ([]types.Item, error) {
	if s.manager == nil {
		started := time.Now()
		out, err := s.db.BatchGetItem(table, ks, keys)
		if err != nil {
			return nil, err
		}
		for i, k := range keys {
			pkBytes, err := pkBytesFromItem(k, ks)
			if err != nil {
				return nil, err
			}
			approxBytes := uint64(len(pkBytes))
			if i < len(out) && out[i] != nil {
				approxBytes = estimatedItemBytes(out[i])
			}
			s.observeRangeMetric(rangeMetricRead, pkBytes, approxBytes, started)
		}
		return out, nil
	}
	out := make([]types.Item, len(keys))
	for i, k := range keys {
		started := time.Now()
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
		approxBytes := uint64(len(pkBytes))
		if out[i] != nil {
			approxBytes = estimatedItemBytes(out[i])
		}
		s.observeRangeMetric(rangeMetricRead, pkBytes, approxBytes, started)
	}
	return out, nil
}

func (s *GRPCServer) ClusterStatus(ctx context.Context, _ *cefaspb.ClusterStatusRequest) (*cefaspb.ClusterStatusResponse, error) {
	resp := &cefaspb.ClusterStatusResponse{Mode: "single-node"}
	if s.cluster != nil {
		resp.Mode = "raft"
		resp.IsLeader = s.cluster.IsLeader()
		resp.SelfId = s.cluster.SelfID()
		resp.BindAddr = s.cluster.BindAddr()
		resp.LeaderHttp = s.cluster.LeaderHTTPAddr()
	}
	if s.manager != nil {
		_ = s.manager.RefreshPlacement()
		placement := s.manager.Placement()
		resp.RoutingEpoch = placement.Epoch
		resp.PlacementVersion = placement.Version
		resp.ShardCount = int32(len(placement.Shards))
		resp.PlacementStrategy = placement.Strategy
		resp.Shards = pbShardPlacements(placement.Shards)
		resp.Nodes = pbNodeDescriptors(sortedPlacementNodes(placement))
	}
	if s.metrics != nil {
		resp.HotRanges = pbRangeHotspotSummaries(s.metrics.RangeHotspotSummaries(0))
	}
	return resp, nil
}

func pbShardPlacements(in []cluster.ShardPlacement) []*cefaspb.ShardPlacement {
	out := make([]*cefaspb.ShardPlacement, 0, len(in))
	for _, sh := range in {
		out = append(out, &cefaspb.ShardPlacement{
			Id:         sh.ID,
			Ranges:     pbTokenRanges(sh.Ranges),
			State:      string(sh.State),
			Epoch:      sh.Epoch,
			Voters:     append([]string(nil), sh.Voters...),
			NonVoters:  append([]string(nil), sh.NonVoters...),
			LeaderHint: sh.LeaderHint,
		})
	}
	return out
}

func pbTokenRanges(in []cluster.TokenRange) []*cefaspb.TokenRange {
	out := make([]*cefaspb.TokenRange, 0, len(in))
	for _, r := range in {
		out = append(out, pbTokenRange(r))
	}
	return out
}

func pbTokenRange(r cluster.TokenRange) *cefaspb.TokenRange {
	return &cefaspb.TokenRange{Start: r.Start, End: r.End}
}

func pbRangeHotspotSummaries(in []metrics.RangeHotspotSummary) []*cefaspb.RangeHotspotSummary {
	out := make([]*cefaspb.RangeHotspotSummary, 0, len(in))
	for _, hs := range in {
		out = append(out, &cefaspb.RangeHotspotSummary{
			ShardId:             hs.ShardID,
			Bucket:              int32(hs.Bucket),
			BucketCount:         int32(hs.BucketCount),
			TokenStart:          hs.TokenStart,
			TokenEnd:            hs.TokenEnd,
			Reads:               hs.Reads,
			Writes:              hs.Writes,
			Bytes:               hs.Bytes,
			AvgLatencySeconds:   hs.AvgLatencySeconds,
			MaxLatencySeconds:   hs.MaxLatencySeconds,
			CompactionDebtBytes: hs.CompactionDebtBytes,
			ThrottleState:       int32(hs.ThrottleState),
			Status:              hs.Status,
			Reasons:             append([]string(nil), hs.Reasons...),
			WindowStartedUnix:   hs.WindowStartedUnix,
			LastSeenUnix:        hs.LastSeenUnix,
			HotUntilUnix:        hs.HotUntilUnix,
		})
	}
	return out
}

func pbNodeDescriptors(in []cluster.NodeDescriptor) []*cefaspb.NodeDescriptor {
	out := make([]*cefaspb.NodeDescriptor, 0, len(in))
	for _, node := range in {
		out = append(out, &cefaspb.NodeDescriptor{
			Id:       node.ID,
			RaftAddr: node.RaftAddr,
			HttpAddr: node.HTTPAddr,
			State:    string(node.State),
			Capacity: &cefaspb.NodeCapacity{
				Weight:      int32(node.Capacity.Weight),
				Cpu:         int32(node.Capacity.CPU),
				MemoryBytes: node.Capacity.MemoryBytes,
				DiskBytes:   node.Capacity.DiskBytes,
				Zone:        node.Capacity.Zone,
				Tags:        append([]string(nil), node.Capacity.Tags...),
			},
			LastSeenUnix: node.LastSeenUnix,
		})
	}
	return out
}

func (s *GRPCServer) AddVoter(ctx context.Context, req *cefaspb.AddVoterRequest) (*cefaspb.AddVoterResponse, error) {
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	timeout := time.Duration(req.GetTimeoutMs()) * time.Millisecond
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if s.manager != nil && req.GetAllShards() {
		if err := s.manager.AddVoterAllShards(req.GetId(), req.GetAddr(), timeout); err != nil {
			return nil, mapStorageErr(err)
		}
		return &cefaspb.AddVoterResponse{}, nil
	}
	if s.manager != nil && req.ShardId != nil {
		if err := s.manager.AddShardVoter(req.GetShardId(), req.GetId(), req.GetAddr(), timeout); err != nil {
			return nil, mapStorageErr(err)
		}
		return &cefaspb.AddVoterResponse{}, nil
	}
	if s.cluster == nil {
		return nil, status.Error(codes.FailedPrecondition, "cluster not configured")
	}
	if err := s.cluster.AddVoter(req.GetId(), req.GetAddr(), timeout); err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.AddVoterResponse{}, nil
}

func (s *GRPCServer) RemoveServer(ctx context.Context, req *cefaspb.RemoveServerRequest) (*cefaspb.RemoveServerResponse, error) {
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	timeout := time.Duration(req.GetTimeoutMs()) * time.Millisecond
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if s.manager != nil && req.GetAllShards() {
		if err := s.manager.RemoveServerAllShards(req.GetId(), timeout); err != nil {
			return nil, mapStorageErr(err)
		}
		return &cefaspb.RemoveServerResponse{}, nil
	}
	if s.manager != nil && req.ShardId != nil {
		if err := s.manager.RemoveShardServer(req.GetShardId(), req.GetId(), timeout); err != nil {
			return nil, mapStorageErr(err)
		}
		return &cefaspb.RemoveServerResponse{}, nil
	}
	if s.cluster == nil {
		return nil, status.Error(codes.FailedPrecondition, "cluster not configured")
	}
	if err := s.cluster.RemoveServer(req.GetId(), timeout); err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.RemoveServerResponse{}, nil
}

func (s *GRPCServer) PlanPlacement(ctx context.Context, req *cefaspb.PlanPlacementRequest) (*cefaspb.PlanPlacementResponse, error) {
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	if s.manager == nil {
		return nil, status.Error(codes.FailedPrecondition, "cluster manager not configured")
	}
	plan, err := s.manager.PlanPlacement(placementPlanRequestFromPB(req))
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.PlanPlacementResponse{Plan: pbPlacementPlan(plan)}, nil
}

func (s *GRPCServer) ApplyPlacement(ctx context.Context, req *cefaspb.ApplyPlacementRequest) (*cefaspb.ApplyPlacementResponse, error) {
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	if s.manager == nil {
		return nil, status.Error(codes.FailedPrecondition, "cluster manager not configured")
	}
	result, err := s.manager.ApplyPlacement(ctx, cluster.PlacementApplyRequest{
		Plan:          placementPlanFromPB(req.GetPlan()),
		ExpectedEpoch: req.GetExpectedEpoch(),
		TimeoutMS:     int(req.GetTimeoutMs()),
	})
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.ApplyPlacementResponse{Result: pbPlacementApplyResult(result)}, nil
}

func (s *GRPCServer) FinalizeSplit(ctx context.Context, req *cefaspb.FinalizeSplitRequest) (*cefaspb.FinalizeSplitResponse, error) {
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	if s.manager == nil {
		return nil, status.Error(codes.FailedPrecondition, "cluster manager not configured")
	}
	result, err := s.manager.FinalizeSplit(ctx, cluster.SplitFinalizeRequest{
		ParentShardID:  req.GetParentShardId(),
		ChildShardID:   req.GetChildShardId(),
		ExpectedEpoch:  req.GetExpectedEpoch(),
		TimeoutMS:      int(req.GetTimeoutMs()),
		WritesQuiesced: req.GetWritesQuiesced(),
	})
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.FinalizeSplitResponse{Result: pbSplitFinalizeResult(result)}, nil
}

func (s *GRPCServer) FinalizeRangeMove(ctx context.Context, req *cefaspb.FinalizeRangeMoveRequest) (*cefaspb.FinalizeRangeMoveResponse, error) {
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	if s.manager == nil {
		return nil, status.Error(codes.FailedPrecondition, "cluster manager not configured")
	}
	result, err := s.manager.FinalizeRangeMove(ctx, cluster.RangeMoveFinalizeRequest{
		SourceShardID: req.GetSourceShardId(),
		TargetShardID: req.GetTargetShardId(),
		ExpectedEpoch: req.GetExpectedEpoch(),
		TimeoutMS:     int(req.GetTimeoutMs()),
	})
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.FinalizeRangeMoveResponse{Result: pbRangeMoveFinalizeResult(result)}, nil
}

func placementPlanRequestFromPB(req *cefaspb.PlanPlacementRequest) cluster.PlacementPlanRequest {
	out := cluster.PlacementPlanRequest{
		Operation:    cluster.PlacementOperation(req.GetOperation()),
		ShardID:      req.GetShardId(),
		SourceNode:   req.GetSourceNode(),
		TargetNode:   req.GetTargetNode(),
		TargetNodes:  append([]string(nil), req.GetTargetNodes()...),
		TargetVoters: append([]string(nil), req.GetTargetVoters()...),
		NodeID:       req.GetNodeId(),
		MinVoters:    int(req.GetMinVoters()),
	}
	if req.SplitToken != nil {
		v := req.GetSplitToken()
		out.SplitToken = &v
	}
	if req.NewShardId != nil {
		v := req.GetNewShardId()
		out.NewShardID = &v
	}
	if req.TargetShardId != nil {
		v := req.GetTargetShardId()
		out.TargetShardID = &v
	}
	if req.RangeStart != nil {
		v := req.GetRangeStart()
		out.RangeStart = &v
	}
	if req.RangeEnd != nil {
		v := req.GetRangeEnd()
		out.RangeEnd = &v
	}
	return out
}

func pbPlacementPlan(plan cluster.PlacementPlan) *cefaspb.PlacementPlan {
	return &cefaspb.PlacementPlan{
		Operation:        string(plan.Operation),
		BeforeEpoch:      plan.BeforeEpoch,
		AfterEpoch:       plan.AfterEpoch,
		Before:           pbPlacementCatalog(plan.Before),
		After:            pbPlacementCatalog(plan.After),
		Steps:            pbPlacementPlanSteps(plan.Steps),
		Warnings:         append([]string(nil), plan.Warnings...),
		RequiresDataCopy: plan.RequiresDataCopy,
		RequiresRestart:  plan.RequiresRestart,
		ApplySupported:   plan.ApplySupported,
	}
}

func pbPlacementCatalog(cat cluster.PlacementCatalog) *cefaspb.PlacementCatalog {
	return &cefaspb.PlacementCatalog{
		Version:       cat.Version,
		Epoch:         cat.Epoch,
		Strategy:      cat.Strategy,
		Shards:        pbShardPlacements(cat.Shards),
		Nodes:         pbNodeDescriptors(sortedPlacementNodes(cat)),
		UpdatedAtUnix: cat.UpdatedAtUnix,
	}
}

func pbPlacementPlanSteps(in []cluster.PlacementPlanStep) []*cefaspb.PlacementPlanStep {
	out := make([]*cefaspb.PlacementPlanStep, 0, len(in))
	for _, step := range in {
		out = append(out, &cefaspb.PlacementPlanStep{
			Action:  step.Action,
			ShardId: step.ShardID,
			NodeId:  step.NodeID,
			Addr:    step.Addr,
			Detail:  step.Detail,
		})
	}
	return out
}

func placementPlanFromPB(in *cefaspb.PlacementPlan) cluster.PlacementPlan {
	if in == nil {
		return cluster.PlacementPlan{}
	}
	return cluster.PlacementPlan{
		Operation:        cluster.PlacementOperation(in.GetOperation()),
		BeforeEpoch:      in.GetBeforeEpoch(),
		AfterEpoch:       in.GetAfterEpoch(),
		Before:           placementCatalogFromPB(in.GetBefore()),
		After:            placementCatalogFromPB(in.GetAfter()),
		Steps:            placementPlanStepsFromPB(in.GetSteps()),
		Warnings:         append([]string(nil), in.GetWarnings()...),
		RequiresDataCopy: in.GetRequiresDataCopy(),
		RequiresRestart:  in.GetRequiresRestart(),
		ApplySupported:   in.GetApplySupported(),
	}
}

func placementCatalogFromPB(in *cefaspb.PlacementCatalog) cluster.PlacementCatalog {
	if in == nil {
		return cluster.PlacementCatalog{}
	}
	nodes := make(map[string]cluster.NodeDescriptor, len(in.GetNodes()))
	for _, node := range in.GetNodes() {
		desc := cluster.NodeDescriptor{
			ID:           node.GetId(),
			RaftAddr:     node.GetRaftAddr(),
			HTTPAddr:     node.GetHttpAddr(),
			State:        cluster.NodeState(node.GetState()),
			LastSeenUnix: node.GetLastSeenUnix(),
		}
		if c := node.GetCapacity(); c != nil {
			desc.Capacity = cluster.NodeCapacity{
				Weight:      int(c.GetWeight()),
				CPU:         int(c.GetCpu()),
				MemoryBytes: c.GetMemoryBytes(),
				DiskBytes:   c.GetDiskBytes(),
				Zone:        c.GetZone(),
				Tags:        append([]string(nil), c.GetTags()...),
			}
		}
		nodes[desc.ID] = desc
	}
	return cluster.PlacementCatalog{
		Version:       in.GetVersion(),
		Epoch:         in.GetEpoch(),
		Strategy:      in.GetStrategy(),
		Shards:        placementShardsFromPB(in.GetShards()),
		Nodes:         nodes,
		UpdatedAtUnix: in.GetUpdatedAtUnix(),
	}
}

func placementShardsFromPB(in []*cefaspb.ShardPlacement) []cluster.ShardPlacement {
	out := make([]cluster.ShardPlacement, 0, len(in))
	for _, sh := range in {
		out = append(out, cluster.ShardPlacement{
			ID:         sh.GetId(),
			Ranges:     placementTokenRangesFromPB(sh.GetRanges()),
			State:      cluster.ShardState(sh.GetState()),
			Epoch:      sh.GetEpoch(),
			Voters:     append([]string(nil), sh.GetVoters()...),
			NonVoters:  append([]string(nil), sh.GetNonVoters()...),
			LeaderHint: sh.GetLeaderHint(),
		})
	}
	return out
}

func placementTokenRangesFromPB(in []*cefaspb.TokenRange) []cluster.TokenRange {
	out := make([]cluster.TokenRange, 0, len(in))
	for _, r := range in {
		out = append(out, cluster.TokenRange{Start: r.GetStart(), End: r.GetEnd()})
	}
	return out
}

func placementPlanStepsFromPB(in []*cefaspb.PlacementPlanStep) []cluster.PlacementPlanStep {
	out := make([]cluster.PlacementPlanStep, 0, len(in))
	for _, step := range in {
		var shardID *uint32
		if step.ShardId != nil {
			v := step.GetShardId()
			shardID = &v
		}
		out = append(out, cluster.PlacementPlanStep{
			Action:  step.GetAction(),
			ShardID: shardID,
			NodeID:  step.GetNodeId(),
			Addr:    step.GetAddr(),
			Detail:  step.GetDetail(),
		})
	}
	return out
}

func pbPlacementApplyResult(result cluster.PlacementApplyResult) *cefaspb.PlacementApplyResult {
	return &cefaspb.PlacementApplyResult{
		Operation:   string(result.Operation),
		BeforeEpoch: result.BeforeEpoch,
		AfterEpoch:  result.AfterEpoch,
		Steps:       pbPlacementApplySteps(result.Steps),
		Placement:   pbPlacementCatalog(result.Placement),
	}
}

func pbSplitFinalizeResult(result cluster.SplitFinalizeResult) *cefaspb.FinalizeSplitResult {
	return &cefaspb.FinalizeSplitResult{
		ParentShardId:     result.ParentShardID,
		ChildShardId:      result.ChildShardID,
		BeforeEpoch:       result.BeforeEpoch,
		AfterEpoch:        result.AfterEpoch,
		ParentRangeBefore: pbTokenRange(result.ParentRangeBefore),
		ParentRangeAfter:  pbTokenRange(result.ParentRangeAfter),
		ChildRange:        pbTokenRange(result.ChildRange),
		CopiedKeys:        result.CopiedKeys,
		CopiedCatalogKeys: result.CopiedCatalogKeys,
		DeletedKeys:       result.DeletedKeys,
		Placement:         pbPlacementCatalog(result.Placement),
	}
}

func pbRangeMoveFinalizeResult(result cluster.RangeMoveFinalizeResult) *cefaspb.FinalizeRangeMoveResult {
	return &cefaspb.FinalizeRangeMoveResult{
		SourceShardId:      result.SourceShardID,
		TargetShardId:      result.TargetShardID,
		BeforeEpoch:        result.BeforeEpoch,
		AfterEpoch:         result.AfterEpoch,
		SourceRangesBefore: pbTokenRanges(result.SourceRangesBefore),
		SourceRangesAfter:  pbTokenRanges(result.SourceRangesAfter),
		MovedRange:         pbTokenRange(result.MovedRange),
		CopiedKeys:         result.CopiedKeys,
		CopiedCatalogKeys:  result.CopiedCatalogKeys,
		DeletedKeys:        result.DeletedKeys,
		Placement:          pbPlacementCatalog(result.Placement),
		Phase:              result.Phase,
	}
}

func pbPlacementApplySteps(in []cluster.PlacementApplyStep) []*cefaspb.PlacementApplyStep {
	out := make([]*cefaspb.PlacementApplyStep, 0, len(in))
	for _, step := range in {
		out = append(out, &cefaspb.PlacementApplyStep{
			Action:  step.Action,
			ShardId: step.ShardID,
			NodeId:  step.NodeID,
			Status:  step.Status,
			Detail:  step.Detail,
		})
	}
	return out
}

// strongReadGate redirects the caller to the leader and waits for the
// raft barrier before serving a strong read. Single-node mode is a
// no-op.
func (s *GRPCServer) strongReadGate() error {
	if s.cluster == nil {
		return nil
	}
	if !s.cluster.IsLeader() {
		leader := s.cluster.LeaderHTTPAddr()
		if leader == "" {
			return status.Error(codes.Unavailable, "no leader currently elected")
		}
		// gRPC has no built-in 307; encode the leader URL in the
		// status message so the client SDK can retarget.
		return status.Errorf(codes.FailedPrecondition, "not leader; retry at %s", leader)
	}
	if err := s.cluster.Barrier(5 * time.Second); err != nil {
		return status.Errorf(codes.Internal, "barrier: %v", err)
	}
	return nil
}

// mapStorageErr translates internal sentinels to gRPC status codes
// every gRPC handler can return. Centralised so HTTP and gRPC keep
// the same error contract.
func mapStorageErr(err error) error {
	switch {
	case errors.Is(err, types.ErrTableNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, types.ErrTableAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, types.ErrItemNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, types.ErrMissingKey), errors.Is(err, types.ErrInvalidKeyType):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, storage.ErrBackupNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, storage.ErrBackupInUse):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, storage.ErrInvalidBackupRetention):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, cluster.ErrInvalidPlacementPlan):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, cluster.ErrStaleRoute):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, types.ErrSpatialNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, types.ErrInvalidSpatial):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, storage.ErrConditionFailed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, storage.ErrNotLeader):
		var nle *storage.NotLeaderError
		if errors.As(err, &nle) && nle.LeaderURL != "" {
			return status.Errorf(codes.FailedPrecondition, "not leader; retry at %s", nle.LeaderURL)
		}
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, craft.ErrNotLeader):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, storage.ErrThrottled):
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
