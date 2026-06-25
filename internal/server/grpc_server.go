package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/cluster"
	cquery "github.com/CefasDb/cefasdb/internal/core/query"
	"github.com/CefasDb/cefasdb/internal/metrics"
	"github.com/CefasDb/cefasdb/internal/placement"
	craft "github.com/CefasDb/cefasdb/internal/replication"
	"github.com/CefasDb/cefasdb/internal/spatial"
	cefassql "github.com/CefasDb/cefasdb/internal/sql"
	"github.com/CefasDb/cefasdb/internal/storage"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/internal/tracing"
	"github.com/CefasDb/cefasdb/pkg/plugin"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// GRPCServer implements cefaspb.CefasServer over the cefas storage,
// catalog, and (optional) cluster surface. It reuses the auth package
// for per-operation scope checks so the gRPC and HTTP entry points
// enforce the same matrix.
type GRPCServer struct {
	cefaspb.UnimplementedCefasServer
	cefaspb.UnimplementedReplicaServer

	db        *pebble.DB
	cat       *catalog.Catalog
	cluster   Cluster          // nil in single-node mode
	stream    ChangeStream     // nil when no CDC source attached
	manager   *cluster.Manager // nil in single-shard mode
	plugins   *plugin.Registry // nil → uses plugin.Default
	metrics   *metrics.Metrics // nil when metrics disabled
	backups   BackupSchedulerStatusProvider
	lifecycle *Lifecycle

	mvScheduler *mvScheduler
}

// NewGRPCServer wires the gRPC handler over the same storage / catalog
// instances the HTTP server uses. Cluster may be nil.
func NewGRPCServer(db *pebble.DB, cat *catalog.Catalog, cluster Cluster) *GRPCServer {
	s := &GRPCServer{db: db, cat: cat, cluster: cluster}
	if db != nil {
		s.attachServiceLevelLaneResolver(db)
		_ = s.hydratePluginIndexCatalog()
	}
	if cat != nil {
		cat.OnServiceLevelChanged(func(name string) {
			s.invalidateServiceLevelLaneShares(name)
		})
		s.mvScheduler = newMVScheduler(s, 30*time.Second)
		s.mvScheduler.Start(context.Background())
	}
	return s
}

// StopMVScheduler stops the background materialized view refresh
// scheduler. Exposed so callers (server shutdown, tests) can drain
// pending refreshes cleanly. Safe to call when the scheduler was
// never started.
func (s *GRPCServer) StopMVScheduler() {
	if s.mvScheduler != nil {
		s.mvScheduler.Stop()
	}
}

// AttachManager wires the multi-shard manager onto the gRPC handler.
// Without it the handler routes every write to s.db (single-shard).
func (s *GRPCServer) AttachManager(m *cluster.Manager) {
	s.manager = m
	s.attachServiceLevelLaneResolvers()
}

// AttachPluginRegistry overrides the default plugin registry. The
// server falls back to plugin.Default when no registry is attached —
// the same registry built-in plugins register against via init().
func (s *GRPCServer) AttachPluginRegistry(r *plugin.Registry) { s.plugins = r }

// AttachMetrics wires bounded range-hotspot summaries into gRPC
// cluster status and records PK-bearing gRPC operations.
func (s *GRPCServer) AttachMetrics(m *metrics.Metrics) { s.metrics = m }

func (s *GRPCServer) AttachBackupScheduler(p BackupSchedulerStatusProvider) { s.backups = p }

func (s *GRPCServer) attachServiceLevelLaneResolvers() {
	for _, db := range s.allShards() {
		s.attachServiceLevelLaneResolver(db)
	}
}

func (s *GRPCServer) attachServiceLevelLaneResolver(db *pebble.DB) {
	if db == nil {
		return
	}
	db.AttachServiceLevelSharesResolver(func(name string) (int, error) {
		if s.cat == nil {
			return 1, nil
		}
		sl, err := s.cat.GetServiceLevel(name)
		if err != nil {
			return 1, err
		}
		if sl.Shares <= 0 {
			return 1, nil
		}
		return sl.Shares, nil
	})
}

func (s *GRPCServer) invalidateServiceLevelLaneShares(name string) {
	for _, db := range s.allShards() {
		if db != nil {
			db.InvalidateServiceLevelShares(name)
		}
	}
}

func (s *GRPCServer) pluginRegistry() *plugin.Registry {
	if s.plugins != nil {
		return s.plugins
	}
	return plugin.Default
}

func (s *GRPCServer) readStorageFor(pkBytes []byte) (*pebble.DB, error) {
	if s.manager == nil {
		return s.db, nil
	}
	shard, err := s.manager.ReadShardForPK(pkBytes, 0)
	if err != nil {
		return nil, err
	}
	if shard.Storage == nil {
		return nil, fmt.Errorf("cluster: read shard %d has no storage", shard.ID)
	}
	return shard.Storage, nil
}

func (s *GRPCServer) allShards() []*pebble.DB {
	if s.manager == nil {
		return []*pebble.DB{s.db}
	}
	out := make([]*pebble.DB, 0, len(s.manager.Shards()))
	for _, sh := range s.manager.Shards() {
		out = append(out, sh.Storage)
	}
	return out
}

func (s *GRPCServer) readShardStores() ([]*pebble.DB, error) {
	if s.manager == nil {
		return []*pebble.DB{s.db}, nil
	}
	shards, err := s.manager.ReadShards(0)
	if err != nil {
		return nil, err
	}
	out := make([]*pebble.DB, 0, len(shards))
	for _, sh := range shards {
		if sh.Storage == nil {
			return nil, fmt.Errorf("cluster: read shard %d has no storage", sh.ID)
		}
		out = append(out, sh.Storage)
	}
	return out, nil
}

func (s *GRPCServer) compact(table string, lower, upper []byte, parallelize bool) ([]pebble.CompactionResult, error) {
	dbs := s.allShards()
	results := make([]pebble.CompactionResult, 0, len(dbs))
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
	ctx, span := tracing.Tracer().Start(ctx, "CreateTable")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	td := pbToTableDescriptor(req.GetDescriptor_())
	if err := s.cat.Create(td); err != nil {
		return nil, mapStorageErr(err)
	}
	created, err := s.cat.Describe(td.Name)
	if err != nil {
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
				_ = cat.Create(created)
			}
		}
	}
	return &cefaspb.CreateTableResponse{Descriptor_: tableDescriptorToPB(created)}, nil
}

func (s *GRPCServer) DescribeTable(ctx context.Context, req *cefaspb.DescribeTableRequest) (*cefaspb.DescribeTableResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "DescribeTable")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	td, err := s.cat.Describe(req.GetName())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	td = s.enrichTableDescriptor(td)
	return &cefaspb.DescribeTableResponse{Descriptor_: tableDescriptorToPB(td)}, nil
}

func (s *GRPCServer) ListTables(ctx context.Context, _ *cefaspb.ListTablesRequest) (*cefaspb.ListTablesResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "ListTables")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	all := s.cat.List()
	out := make([]*cefaspb.TableDescriptor, 0, len(all))
	for _, td := range all {
		td = s.enrichTableDescriptor(td)
		out = append(out, tableDescriptorToPB(td))
	}
	return &cefaspb.ListTablesResponse{Tables: out}, nil
}

func (s *GRPCServer) enrichTableDescriptor(td types.TableDescriptor) types.TableDescriptor {
	if td.StorageClass != types.StorageClassMemory {
		return td
	}
	var bytes int64
	for _, db := range s.allShards() {
		_ = db.LoadMemoryTable(td.Name)
		bytes += db.MemoryTableFootprint(td.Name)
	}
	td.MemoryFootprintBytes = bytes
	return td
}

func (s *GRPCServer) DropTable(ctx context.Context, req *cefaspb.DropTableRequest) (*cefaspb.DropTableResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "DropTable")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableDrop); err != nil {
		return nil, err
	}
	if err := s.cat.Drop(req.GetName()); err != nil {
		return nil, mapStorageErr(err)
	}
	if err := s.deletePluginIndexDescriptorsForTable(req.GetName()); err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.DropTableResponse{}, nil
}

func (s *GRPCServer) UpdateTimeToLive(ctx context.Context, req *cefaspb.UpdateTimeToLiveRequest) (*cefaspb.UpdateTimeToLiveResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "UpdateTimeToLive")
	defer span.End()
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
	ctx, span := tracing.Tracer().Start(ctx, "DescribeTimeToLive")
	defer span.End()
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
	ctx, span := tracing.Tracer().Start(ctx, "PutItem")
	defer span.End()
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
	pluginPlan, err := s.planPluginIndexPut(targets.primary, td, item)
	if err != nil {
		return nil, mapWriteMutationErr(err)
	}
	hasAggMV, err := s.tableHasAggregatingEagerMV(td)
	if err != nil {
		return nil, mapWriteMutationErr(err)
	}
	var mvMut mvBaseMutation
	if hasAggMV {
		mvMut, err = captureMVEagerMutation(targets.primary, td, item, nil)
		if err != nil {
			return nil, mapWriteMutationErr(err)
		}
	}
	if err := targets.PutItemWithCtx(ctx, td, item, pebble.PutOptions{Condition: req.GetCondition(), Binds: binds}); err != nil {
		return nil, mapStorageErr(err)
	}
	if err := s.applyPluginIndexPlan(pluginPlan); err != nil {
		return nil, mapWriteMutationErr(err)
	}
	if hasAggMV {
		if err := s.applyMVEagerMutation(ctx, td, mvMut); err != nil {
			return nil, mapWriteMutationErr(err)
		}
	} else {
		if err := s.applyMVEagerPut(ctx, td, item); err != nil {
			return nil, mapWriteMutationErr(err)
		}
	}
	if err := s.applyGlobalIndexPut(ctx, td, item); err != nil {
		return nil, mapWriteMutationErr(err)
	}
	s.observeRangeMetric(rangeMetricWrite, pkBytes, estimatedItemBytes(item), started)
	return &cefaspb.PutItemResponse{}, nil
}

func (s *GRPCServer) GetItem(ctx context.Context, req *cefaspb.GetItemRequest) (*cefaspb.GetItemResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "GetItem")
	defer span.End()
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
	ks, err := s.cat.KeySchema(req.GetTable())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	pkBytes, skBytes, err := pbKeyBytes(req.GetKey(), ks)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	db, err := s.readStorageFor(pkBytes)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	rawItem, err := db.GetEncodedItemByKeyBytesCtx(ctx, req.GetTable(), pkBytes, skBytes)
	if err != nil {
		if errors.Is(err, types.ErrItemNotFound) {
			s.observeRangeMetric(rangeMetricRead, pkBytes, uint64(len(pkBytes)), started)
			return &cefaspb.GetItemResponse{Found: false}, nil
		}
		return nil, mapStorageErr(err)
	}
	pbItem, err := encodedItemToPB(rawItem)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	s.observeRangeMetric(rangeMetricRead, pkBytes, uint64(len(rawItem)), started)
	return &cefaspb.GetItemResponse{Found: true, Item: pbItem}, nil
}

func (s *GRPCServer) UpdateItem(ctx context.Context, req *cefaspb.UpdateItemRequest) (*cefaspb.UpdateItemResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "UpdateItem")
	defer span.End()
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
	hasAggMV, err := s.tableHasAggregatingEagerMV(td)
	if err != nil {
		return nil, mapWriteMutationErr(err)
	}
	var oldForMV types.Item
	if hasAggMV {
		oldForMV, err = db.GetItemCtx(ctx, req.GetTable(), td.KeySchema, key)
		if errors.Is(err, types.ErrItemNotFound) {
			oldForMV = nil
		} else if err != nil {
			return nil, mapStorageErr(err)
		}
	}
	plan, err := cefassql.PlanStmt(stmt, s.cat)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("plan: %v", err))
	}
	ex := &cefassql.Executor{Storage: db, Catalog: s.cat}
	ex.MutationHook = s.pluginIndexMutationHookForPlan(plan)
	res, err := ex.Execute(plan)
	if err != nil {
		return nil, mapWriteMutationErr(err)
	}
	var finalItem types.Item
	if res.AffectedRows > 0 && (len(targets.mirrors) > 0 || len(td.MaterializedViews) > 0) {
		finalItem, err = db.GetItemCtx(ctx, req.GetTable(), td.KeySchema, key)
		if err != nil {
			return nil, mapStorageErr(err)
		}
	}
	if len(targets.mirrors) > 0 && res.AffectedRows > 0 {
		if err := targets.MirrorPutItemCtx(ctx, td, finalItem); err != nil {
			return nil, mapStorageErr(err)
		}
	}
	if len(td.MaterializedViews) > 0 && res.AffectedRows > 0 {
		if hasAggMV {
			if err := s.applyMVEagerMutation(ctx, td, mvBaseMutation{OldItem: oldForMV, NewItem: finalItem}); err != nil {
				return nil, mapWriteMutationErr(err)
			}
		} else {
			if err := s.applyMVEagerPut(ctx, td, finalItem); err != nil {
				return nil, mapWriteMutationErr(err)
			}
		}
	}
	if len(td.GlobalIndexes) > 0 {
		if err := s.applyGlobalIndexPut(ctx, td, finalItem); err != nil {
			return nil, mapWriteMutationErr(err)
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
	ctx, span := tracing.Tracer().Start(ctx, "DeleteItem")
	defer span.End()
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
	pluginPlan, err := s.planPluginIndexDelete(targets.primary, td, key)
	if err != nil {
		return nil, mapWriteMutationErr(err)
	}
	hasAggMV, err := s.tableHasAggregatingEagerMV(td)
	if err != nil {
		return nil, mapWriteMutationErr(err)
	}
	mvMut := mvBaseMutation{DeleteKey: key}
	if hasAggMV {
		mvMut, err = captureMVEagerMutation(targets.primary, td, nil, key)
		if err != nil {
			return nil, mapWriteMutationErr(err)
		}
	}
	if err := targets.DeleteItemWithCtx(ctx, td, key, pebble.DeleteOptions{Condition: req.GetCondition(), Binds: binds}); err != nil {
		return nil, mapStorageErr(err)
	}
	if err := s.applyPluginIndexPlan(pluginPlan); err != nil {
		return nil, mapWriteMutationErr(err)
	}
	if hasAggMV {
		if err := s.applyMVEagerMutation(ctx, td, mvMut); err != nil {
			return nil, mapWriteMutationErr(err)
		}
	} else {
		if err := s.applyMVEagerDelete(ctx, td, key); err != nil {
			return nil, mapWriteMutationErr(err)
		}
	}
	if err := s.applyGlobalIndexDelete(ctx, td, key); err != nil {
		return nil, mapWriteMutationErr(err)
	}
	s.observeRangeMetric(rangeMetricWrite, pkBytes, uint64(len(pkBytes)), started)
	return &cefaspb.DeleteItemResponse{}, nil
}

func (s *GRPCServer) BatchWriteItem(ctx context.Context, req *cefaspb.BatchWriteItemRequest) (*cefaspb.BatchWriteItemResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "BatchWriteItem")
	defer span.End()
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemWrite, req.GetTable()),
		auth.WildcardScope(auth.ScopeItemWrite)); err != nil {
		return nil, err
	}
	td, err := s.cat.Describe(req.GetTable())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	ops := make([]pebble.BatchOp, 0, len(req.GetOps()))
	for i, raw := range req.GetOps() {
		switch raw.GetKind() {
		case cefaspb.BatchWriteOp_KIND_PUT:
			item, err := pbToItem(raw.GetItem())
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "op %d: %v", i, err)
			}
			ops = append(ops, pebble.BatchOp{Op: pebble.BatchOpPut, Item: item})
		case cefaspb.BatchWriteOp_KIND_DELETE:
			key, err := pbToItem(raw.GetKey())
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "op %d: %v", i, err)
			}
			ops = append(ops, pebble.BatchOp{Op: pebble.BatchOpDelete, Key: key})
		default:
			return nil, status.Errorf(codes.InvalidArgument, "op %d: unknown kind", i)
		}
	}
	if err := s.batchWriteFanOut(ctx, td, ops); err != nil {
		return nil, mapWriteMutationErr(err)
	}
	return &cefaspb.BatchWriteItemResponse{}, nil
}

func (s *GRPCServer) batchWriteFanOut(ctx context.Context, td types.TableDescriptor, ops []pebble.BatchOp) error {
	if s.manager == nil {
		return s.batchWriteFanOutSingleShard(ctx, td, ops)
	}
	return s.batchWriteFanOutMultiShard(ctx, td, ops)
}

type batchWriteOpMeta struct {
	pkBytes     []byte
	approxBytes uint64
	routeIdx    int
}

func extractBatchOpMeta(op pebble.BatchOp, ks types.KeySchema) (pkBytes []byte, approxBytes uint64, err error) {
	probe := op.Item
	approxBytes = estimatedItemBytes(op.Item)
	if op.Op == pebble.BatchOpDelete {
		probe = op.Key
		approxBytes = estimatedItemBytes(op.Key)
	}
	pkBytes, err = pkBytesFromItem(probe, ks)
	return pkBytes, approxBytes, err
}

func (s *GRPCServer) batchWriteFanOutSingleShard(ctx context.Context, td types.TableDescriptor, ops []pebble.BatchOp) error {
	started := time.Now()
	pluginPlan, err := s.planPluginIndexBatch(s.db, td, ops)
	if err != nil {
		return err
	}
	type obs struct {
		pkBytes     []byte
		approxBytes uint64
	}
	observations := make([]obs, 0, len(ops))
	for _, op := range ops {
		pkBytes, approxBytes, err := extractBatchOpMeta(op, td.KeySchema)
		if err != nil {
			return err
		}
		observations = append(observations, obs{pkBytes: append([]byte(nil), pkBytes...), approxBytes: approxBytes})
	}
	hasAggMV, err := s.tableHasAggregatingEagerMV(td)
	if err != nil {
		return err
	}
	var mvMuts []mvBaseMutation
	if hasAggMV {
		mvMuts, err = captureMVEagerBatchMutations(func(int) *pebble.DB { return s.db }, td, ops)
		if err != nil {
			return err
		}
	}
	if err := s.db.BatchWriteItemCtx(ctx, td, ops); err != nil {
		return err
	}
	if err := s.applyPluginIndexPlan(pluginPlan); err != nil {
		return err
	}
	if err := s.applyMVEagerBatch(ctx, td, ops, mvMuts); err != nil {
		return err
	}
	if err := s.applyGlobalIndexBatch(ctx, td, ops); err != nil {
		return err
	}
	for _, o := range observations {
		s.observeRangeMetric(rangeMetricWrite, o.pkBytes, o.approxBytes, started)
	}
	return nil
}

// batchWriteFanOutMultiShard groups ops by their owning shard and the
// matching mirror set. Before issue #455 the implementation rebuilt
// the per-mirror bucket map by appending on every op; with K = 500
// batch ops that produced O(K * M) map appends plus K splitMu RLock
// cycles. The new shape splits the work in two passes: first a
// routing dedup that hits writeTargetsForPK once per unique PK, then
// a single bucket-fill loop on pre-sized slices.
func (s *GRPCServer) batchWriteFanOutMultiShard(ctx context.Context, td types.TableDescriptor, ops []pebble.BatchOp) error {
	metas := make([]batchWriteOpMeta, 0, len(ops))
	keyToRoute := make(map[string]int, len(ops))
	routes := make([]routedWriteTargets, 0, len(ops))
	defer func() {
		for i := len(routes) - 1; i >= 0; i-- {
			routes[i].Release()
		}
	}()

	for _, op := range ops {
		pkBytes, approxBytes, err := extractBatchOpMeta(op, td.KeySchema)
		if err != nil {
			return err
		}
		key := string(pkBytes)
		idx, ok := keyToRoute[key]
		if !ok {
			targets, err := s.writeTargetsForPK(pkBytes)
			if err != nil {
				return err
			}
			idx = len(routes)
			routes = append(routes, targets)
			keyToRoute[key] = idx
		}
		metas = append(metas, batchWriteOpMeta{
			pkBytes:     append([]byte(nil), pkBytes...),
			approxBytes: approxBytes,
			routeIdx:    idx,
		})
	}

	primaryBuckets := make(map[*pebble.DB][]pebble.BatchOp, len(routes))
	mirrorBuckets := make(map[*pebble.DB][]pebble.BatchOp, len(routes))
	for _, r := range routes {
		if _, ok := primaryBuckets[r.primary]; !ok {
			primaryBuckets[r.primary] = make([]pebble.BatchOp, 0, len(ops))
		}
		for _, mirror := range r.mirrors {
			if _, ok := mirrorBuckets[mirror]; !ok {
				mirrorBuckets[mirror] = make([]pebble.BatchOp, 0, len(ops))
			}
		}
	}
	for i, meta := range metas {
		r := routes[meta.routeIdx]
		op := ops[i]
		primaryBuckets[r.primary] = append(primaryBuckets[r.primary], op)
		for _, mirror := range r.mirrors {
			mirrorBuckets[mirror] = append(mirrorBuckets[mirror], op)
		}
	}

	started := time.Now()
	pluginPlans := make([]pluginIndexWritePlan, 0, len(primaryBuckets))
	for db, group := range primaryBuckets {
		pluginPlan, err := s.planPluginIndexBatch(db, td, group)
		if err != nil {
			return err
		}
		pluginPlans = append(pluginPlans, pluginPlan)
	}
	hasAggMV, err := s.tableHasAggregatingEagerMV(td)
	if err != nil {
		return err
	}
	var mvMuts []mvBaseMutation
	if hasAggMV {
		mvMuts, err = captureMVEagerBatchMutations(func(i int) *pebble.DB {
			return routes[metas[i].routeIdx].primary
		}, td, ops)
		if err != nil {
			return err
		}
	}
	if err := batchWriteBucketsCtx(ctx, td, primaryBuckets); err != nil {
		return err
	}
	if err := batchWriteBucketsCtx(ctx, td, mirrorBuckets); err != nil {
		return err
	}
	for _, pluginPlan := range pluginPlans {
		if err := s.applyPluginIndexPlan(pluginPlan); err != nil {
			return err
		}
	}
	if err := s.applyMVEagerBatch(ctx, td, ops, mvMuts); err != nil {
		return err
	}
	if err := s.applyGlobalIndexBatch(ctx, td, ops); err != nil {
		return err
	}
	for _, meta := range metas {
		s.observeRangeMetric(rangeMetricWrite, meta.pkBytes, meta.approxBytes, started)
	}
	return nil
}

func (s *GRPCServer) BatchGetItem(ctx context.Context, req *cefaspb.BatchGetItemRequest) (*cefaspb.BatchGetItemResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "BatchGetItem")
	defer span.End()
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
	items, err := s.batchGetFanOut(ctx, req.GetTable(), td.KeySchema, keys)
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
	ctx, span := tracing.Tracer().Start(stream.Context(), "Query")
	defer span.End()
	started := time.Now()
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
	var items []types.Item
	limit := int(req.GetLimit())
	switch {
	case req.GetIndexName() != "":
		items, err = s.queryByIndex(ctx, td, req.GetIndexName(), pkVal, pebble.QueryOptions{SKLow: lo, SKHigh: hi, Limit: limit})
	case req.GetSkLow() == nil && req.GetSkHigh() == nil:
		queryDB, qerr := s.readStorageFor(pkBytes)
		if qerr != nil {
			return mapStorageErr(qerr)
		}
		items, err = queryDB.QueryByPKCtx(ctx, req.GetTable(), td.KeySchema, pkVal, limit)
	default:
		queryDB, qerr := s.readStorageFor(pkBytes)
		if qerr != nil {
			return mapStorageErr(qerr)
		}
		items, err = queryDB.QueryByPKRangeCtx(ctx, req.GetTable(), td.KeySchema, pkVal, lo, hi, limit)
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
	ctx, span := tracing.Tracer().Start(stream.Context(), "Scan")
	defer span.End()
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
	// CDC alias (#523): redirect to the changelog scan handler.
	if base, isCDC := cdcAliasBase(req.GetTable()); isCDC {
		return s.scanCDCStream(req, base, stream)
	}
	s.maybeSetMVStalenessHeader(grpcStreamHeaderCtx{stream: stream}, req.GetTable())
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

	sent := 0
	emit := func(it types.Item) (bool, error) {
		if !cond.IsZero() {
			ok, err := cond.Evaluate(it, binds)
			if err != nil {
				return false, status.Error(codes.InvalidArgument, fmt.Sprintf("evaluate filter: %v", err))
			}
			if !ok {
				return true, nil
			}
		}
		if err := stream.Send(&cefaspb.Item{Attributes: itemToPB(it)}); err != nil {
			return false, err
		}
		sent++
		if limit > 0 && sent >= limit {
			return false, nil
		}
		return true, nil
	}

	// Single-shard / no-manager fallback: stream the local pebble.DB
	// directly via the predicate-pushdown variant so a permissive
	// filter never materialises the whole table.
	if s.manager == nil {
		var emitErr error
		err := s.db.ScanTableWithCtx(ctx, req.GetTable(), func(it types.Item) bool {
			cont, err := emit(it)
			if err != nil {
				emitErr = err
				return false
			}
			return cont
		})
		if emitErr != nil {
			return emitErr
		}
		if err != nil {
			return mapStorageErr(err)
		}
		return nil
	}

	// Multi-shard: scan every logical shard exactly once. Shards
	// this node hosts locally come from the in-process pebble.DB;
	// remote shards stream through Replica.ScanShard on a peer that
	// holds a replica. The seen map dedups across transitional
	// placements that briefly leave two shards holding the same key.
	seen := make(map[string]struct{})
	emitOnce := func(it types.Item) (bool, error) {
		id := scanItemKeyString(it)
		if id != "" {
			if _, dup := seen[id]; dup {
				return true, nil
			}
			seen[id] = struct{}{}
		}
		return emit(it)
	}

	for _, sh := range s.manager.Shards() {
		if sh == nil {
			continue
		}
		if sh.IsLocalVoter || sh.IsLocalNonVoter {
			if sh.Storage == nil {
				return status.Errorf(codes.Unavailable, "cluster: local shard %d has no storage", sh.ID)
			}
			var stopErr error
			stopped := false
			err := sh.Storage.ScanTableWithCtx(ctx, req.GetTable(), func(it types.Item) bool {
				cont, err := emitOnce(it)
				if err != nil {
					stopErr = err
					return false
				}
				if !cont {
					stopped = true
					return false
				}
				return true
			})
			if stopErr != nil {
				return stopErr
			}
			if err != nil {
				return mapStorageErr(err)
			}
			if stopped {
				return nil
			}
			continue
		}
		if err := s.scanRemoteShard(ctx, sh.ID, req.GetTable(), emitOnce); err != nil {
			return err
		}
	}
	return nil
}

func (s *GRPCServer) scanRemoteShard(ctx context.Context, shardID uint32, table string, sink func(types.Item) (bool, error)) error {
	itemCh, errCh := s.manager.PeerScanShard(ctx, shardID, table)
	stopRecv := false
	for it := range itemCh {
		if stopRecv {
			continue
		}
		decoded, err := pbToItem(it.GetAttributes())
		if err != nil {
			return status.Errorf(codes.Internal, "decode peer item: %v", err)
		}
		cont, err := sink(decoded)
		if err != nil {
			return err
		}
		if !cont {
			stopRecv = true
		}
	}
	if err := <-errCh; err != nil {
		return status.Errorf(codes.Unavailable, "peer scan shard %d: %v", shardID, err)
	}
	return nil
}

// scanItemKeyString builds a stable identity string for dedup across
// shards. Scan does not see the table descriptor's key schema at this
// layer, so it sorts and concatenates the primitive (S/N/B) attributes
// found in the item — enough to detect "same item served by two
// shards" without dragging the catalog into the hot path.
func scanItemKeyString(it types.Item) string {
	type kv struct{ k, v string }
	pairs := make([]kv, 0, len(it))
	for k, v := range it {
		switch v.T {
		case types.AttrS:
			pairs = append(pairs, kv{k, "S:" + v.S})
		case types.AttrN:
			pairs = append(pairs, kv{k, "N:" + v.N})
		}
	}
	if len(pairs) == 0 {
		return ""
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].k < pairs[j].k })
	var sb strings.Builder
	for i, p := range pairs {
		if i > 0 {
			sb.WriteByte('|')
		}
		sb.WriteString(p.k)
		sb.WriteByte('=')
		sb.WriteString(p.v)
	}
	return sb.String()
}

func (s *GRPCServer) SpatialQuery(req *cefaspb.SpatialQueryRequest, stream cefaspb.Cefas_SpatialQueryServer) error {
	ctx, span := tracing.Tracer().Start(stream.Context(), "SpatialQuery")
	defer span.End()
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeSpatial, req.GetTable()),
		auth.WildcardScope(auth.ScopeSpatial)); err != nil {
		return err
	}
	td, err := s.cat.Describe(req.GetTable())
	if err != nil {
		return mapStorageErr(err)
	}
	q := pebble.SpatialQuery{Limit: int(req.GetLimit())}
	switch shape := req.GetShape().(type) {
	case *cefaspb.SpatialQueryRequest_Bbox:
		b := shape.Bbox
		q.BBox = &spatial.BBox{MinLat: b.GetMinLat(), MinLon: b.GetMinLon(), MaxLat: b.GetMaxLat(), MaxLon: b.GetMaxLon()}
	case *cefaspb.SpatialQueryRequest_Radius:
		r := shape.Radius
		q.Radius = &pebble.RadiusQuery{Lat: r.GetLat(), Lon: r.GetLon(), Meters: r.GetMeters()}
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
func (s *GRPCServer) scatterSpatial(td types.TableDescriptor, idxName string, q pebble.SpatialQuery) ([]types.Item, error) {
	if s.manager == nil {
		return s.db.SpatialQueryItems(td, idxName, q)
	}
	dbs, err := s.readShardStores()
	if err != nil {
		return nil, err
	}
	var out []types.Item
	for _, db := range dbs {
		got, err := db.SpatialQueryItems(td, idxName, q)
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
	ctx, span := tracing.Tracer().Start(ctx, "Sql")
	defer span.End()
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
	ex := &cefassql.Executor{
		Storage:              s.db,
		Catalog:              s.cat,
		TableDropHook:        s.deletePluginIndexDescriptorsForTable,
		DistanceResolver:     s.sqlDistanceResolver,
		ANNCandidateResolver: s.sqlANNCandidateResolver,
	}
	ex.MutationHook = combineMutationHooks(
		s.pluginIndexMutationHookForPlan(plan),
		s.mvMutationHookForPlan(ctx, plan),
	)
	res, err := ex.Execute(plan)
	if err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, mapStorageErr(err)
	}
	out := &cefaspb.SqlResponse{AffectedRows: int64(res.AffectedRows)}
	for _, row := range res.Rows {
		out.Rows = append(out.Rows, &cefaspb.Item{Attributes: itemToPB(row)})
	}
	return out, nil
}

func (s *GRPCServer) sqlDistanceResolver(table, field string, target types.AttributeValue) (cquery.DistanceOp, error) {
	return s.resolveTopKDistance(table, field, "", target)
}

func (s *GRPCServer) sqlANNCandidateResolver(table, field string, target types.AttributeValue, limit int) ([]cquery.TopKResult, bool, error) {
	ann, ok, err := s.indexedANNTopK(table, field, target, limit, "")
	return ann.rows, ok, err
}

func combineMutationHooks(hooks ...cefassql.MutationHook) cefassql.MutationHook {
	active := make([]cefassql.MutationHook, 0, len(hooks))
	for _, hook := range hooks {
		if hook != nil {
			active = append(active, hook)
		}
	}
	if len(active) == 0 {
		return nil
	}
	return func(mut cefassql.ItemMutation) error {
		for _, hook := range active {
			if err := hook(mut); err != nil {
				return err
			}
		}
		return nil
	}
}

func (s *GRPCServer) mvMutationHookForPlan(ctx context.Context, plan cefassql.Plan) cefassql.MutationHook {
	var td types.TableDescriptor
	switch p := plan.(type) {
	case *cefassql.PlanPutItem:
		td = p.Descriptor
	case *cefassql.PlanUpdate:
		td = p.Descriptor
	case *cefassql.PlanDelete:
		td = p.Descriptor
	default:
		return nil
	}
	if len(td.MaterializedViews) == 0 {
		return nil
	}
	return func(mut cefassql.ItemMutation) error {
		return s.applyMVEagerMutation(ctx, td, mvBaseMutation{
			OldItem:   mut.OldItem,
			NewItem:   mut.NewItem,
			DeleteKey: mut.DeleteKey,
		})
	}
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
	ctx, span := tracing.Tracer().Start(stream.Context(), "StreamChanges")
	defer span.End()
	if s.stream == nil {
		return status.Error(codes.FailedPrecondition, "change stream not configured")
	}
	events, cancel := s.stream.SubscribeChanges(ctx)
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
	ctx, span := tracing.Tracer().Start(ctx, "CreateBackup")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	tables := req.GetTables()
	if len(tables) == 0 {
		for _, td := range s.cat.List() {
			tables = append(tables, td.Name)
		}
	}
	meta, err := s.createBackupAcrossShards(req.GetName(), tables)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.CreateBackupResponse{Backup: backupMetaToPB(meta)}, nil
}

func (s *GRPCServer) createBackupAcrossShards(name string, tables []string) (pebble.BackupMetadata, error) {
	if s.manager == nil {
		return s.db.CreateBackupForShard(name, tables, "0", 0)
	}
	var primary pebble.BackupMetadata
	coverage := make([]pebble.BackupShardCoverage, 0, len(s.manager.Shards()))
	for i, sh := range s.manager.Shards() {
		meta, err := sh.Storage.CreateBackupForShard(name, tables, fmt.Sprint(sh.ID), sh.Epoch)
		if err != nil {
			return pebble.BackupMetadata{}, err
		}
		if i == 0 {
			primary = meta
		}
		if len(meta.ShardCoverage) > 0 {
			coverage = append(coverage, meta.ShardCoverage[0])
		} else {
			coverage = append(coverage, pebble.BackupShardCoverage{
				ShardID:        fmt.Sprint(sh.ID),
				PlacementEpoch: sh.Epoch,
				TableStats:     meta.TableStats,
			})
		}
	}
	primary.ShardCoverage = coverage
	if err := s.db.StoreBackupMetadata(primary); err != nil {
		return pebble.BackupMetadata{}, err
	}
	return primary, nil
}

func (s *GRPCServer) ListBackups(ctx context.Context, _ *cefaspb.ListBackupsRequest) (*cefaspb.ListBackupsResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "ListBackups")
	defer span.End()
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
	ctx, span := tracing.Tracer().Start(ctx, "DeleteBackup")
	defer span.End()
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
	ctx, span := tracing.Tracer().Start(ctx, "ApplyBackupRetention")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	result, err := s.db.ApplyBackupRetention(pebble.BackupRetentionOptions{
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
	ctx, span := tracing.Tracer().Start(ctx, "RestoreTableFromBackup")
	defer span.End()
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
	var res pebble.RestoreResult
	var err error
	if req.GetDryRun() {
		res, err = s.db.RestoreTableFromBackupWithOptions(
			req.GetBackupName(),
			req.GetSourceTableName(),
			req.GetTargetTableName(),
			pebble.RestoreOptions{
				DryRun:            true,
				TargetChangeIndex: req.GetTargetChangeIndex(),
				TargetUnixNano:    req.GetTargetUnixNano(),
			},
			register,
		)
	} else {
		res, err = s.db.RestoreTableFromBackupWithOptions(
			req.GetBackupName(),
			req.GetSourceTableName(),
			req.GetTargetTableName(),
			pebble.RestoreOptions{
				TargetChangeIndex: req.GetTargetChangeIndex(),
				TargetUnixNano:    req.GetTargetUnixNano(),
			},
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

func backupMetaToPB(m pebble.BackupMetadata) *cefaspb.BackupDescriptor {
	stats := make([]*cefaspb.BackupTableStats, 0, len(m.TableStats))
	for _, stat := range m.TableStats {
		stats = append(stats, backupTableStatsToPB(stat))
	}
	coverage := make([]*cefaspb.BackupShardCoverage, 0, len(m.ShardCoverage))
	for _, shard := range m.ShardCoverage {
		shardStats := make([]*cefaspb.BackupTableStats, 0, len(shard.TableStats))
		for _, stat := range shard.TableStats {
			shardStats = append(shardStats, backupTableStatsToPB(stat))
		}
		coverage = append(coverage, &cefaspb.BackupShardCoverage{
			ShardId:        shard.ShardID,
			PlacementEpoch: shard.PlacementEpoch,
			TableStats:     shardStats,
		})
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
		ShardCoverage:   coverage,
		ChangeIndex:     m.ChangeIndex,
		ChangeUnixNano:  m.ChangeUnixNano,
	}
}

func backupTableStatsToPB(stat pebble.BackupTableStats) *cefaspb.BackupTableStats {
	if stat.Table == "" && stat.Rows == 0 && stat.Checksum == "" {
		return nil
	}
	return &cefaspb.BackupTableStats{
		Table:    stat.Table,
		Rows:     stat.Rows,
		Checksum: stat.Checksum,
	}
}

func backupDeletionToPB(result pebble.BackupDeletionResult) *cefaspb.BackupDeletionResult {
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

func backupRetentionToPB(result pebble.BackupRetentionResult) *cefaspb.ApplyBackupRetentionResponse {
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

func backupRetentionCandidateToPB(candidate pebble.BackupRetentionCandidate) *cefaspb.BackupRetentionCandidate {
	return &cefaspb.BackupRetentionCandidate{
		Backup: backupMetaToPB(candidate.Backup),
		Reason: candidate.Reason,
	}
}

func (s *GRPCServer) ListSnapshots(ctx context.Context, _ *cefaspb.ListSnapshotsRequest) (*cefaspb.ListSnapshotsResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "ListSnapshots")
	defer span.End()
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
	ctx, span := tracing.Tracer().Start(ctx, "Compact")
	defer span.End()
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

func (s *GRPCServer) batchGetFanOut(ctx context.Context, table string, ks types.KeySchema, keys []types.Item) ([]types.Item, error) {
	if s.manager == nil {
		started := time.Now()
		out, err := s.db.BatchGetItemCtx(ctx, table, ks, keys)
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
		db, err := s.readStorageFor(pkBytes)
		if err != nil {
			return nil, err
		}
		single, err := db.BatchGetItemCtx(ctx, table, ks, []types.Item{k})
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
	_, span := tracing.Tracer().Start(ctx, "ClusterStatus")
	defer span.End()
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
	if s.backups != nil {
		status := s.backups.Status()
		resp.BackupScheduler = scheduledBackupStatusToPB(status)
	}
	return resp, nil
}

func scheduledBackupStatusToPB(status pebble.ScheduledBackupStatus) *cefaspb.ScheduledBackupStatus {
	var retention *cefaspb.ApplyBackupRetentionResponse
	if status.LastRetention != nil {
		retention = backupRetentionToPB(*status.LastRetention)
	}
	return &cefaspb.ScheduledBackupStatus{
		Enabled:                status.Enabled,
		DryRun:                 status.DryRun,
		IntervalSeconds:        status.IntervalSeconds,
		NameTemplate:           status.NameTemplate,
		Tables:                 append([]string(nil), status.Tables...),
		RetentionKeepLatest:    int32(status.RetentionKeepLatest),
		RetentionKeepLatestSet: status.RetentionKeepLatestSet,
		RetentionMaxAgeSeconds: status.RetentionMaxAgeSeconds,
		RetentionMaxAgeSet:     status.RetentionMaxAgeSet,
		RetentionDryRun:        status.RetentionDryRun,
		Running:                status.Running,
		NextRunUnix:            status.NextRunUnix,
		LastStartedUnix:        status.LastStartedUnix,
		LastFinishedUnix:       status.LastFinishedUnix,
		LastDurationSeconds:    status.LastDurationSeconds,
		LastStatus:             status.LastStatus,
		LastBackupName:         status.LastBackupName,
		LastError:              status.LastError,
		LastRows:               status.LastRows,
		LastBytes:              status.LastBytes,
		LastSuccessUnix:        status.LastSuccessUnix,
		LastFailureUnix:        status.LastFailureUnix,
		LastRetention:          retention,
	}
}

func pbShardPlacements(in []placement.ShardPlacement) []*cefaspb.ShardPlacement {
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

func pbTokenRanges(in []placement.TokenRange) []*cefaspb.TokenRange {
	out := make([]*cefaspb.TokenRange, 0, len(in))
	for _, r := range in {
		out = append(out, pbTokenRange(r))
	}
	return out
}

func pbTokenRange(r placement.TokenRange) *cefaspb.TokenRange {
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

func pbNodeDescriptors(in []placement.NodeDescriptor) []*cefaspb.NodeDescriptor {
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
	ctx, span := tracing.Tracer().Start(ctx, "AddVoter")
	defer span.End()
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
	ctx, span := tracing.Tracer().Start(ctx, "RemoveServer")
	defer span.End()
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
	ctx, span := tracing.Tracer().Start(ctx, "PlanPlacement")
	defer span.End()
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
	ctx, span := tracing.Tracer().Start(ctx, "ApplyPlacement")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	if s.manager == nil {
		return nil, status.Error(codes.FailedPrecondition, "cluster manager not configured")
	}
	result, err := s.manager.ApplyPlacement(ctx, placement.PlacementApplyRequest{
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
	ctx, span := tracing.Tracer().Start(ctx, "FinalizeSplit")
	defer span.End()
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
	ctx, span := tracing.Tracer().Start(ctx, "FinalizeRangeMove")
	defer span.End()
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

func placementPlanRequestFromPB(req *cefaspb.PlanPlacementRequest) placement.PlacementPlanRequest {
	out := placement.PlacementPlanRequest{
		Operation:    placement.PlacementOperation(req.GetOperation()),
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

func pbPlacementPlan(plan placement.PlacementPlan) *cefaspb.PlacementPlan {
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

func pbPlacementCatalog(cat placement.PlacementCatalog) *cefaspb.PlacementCatalog {
	return &cefaspb.PlacementCatalog{
		Version:       cat.Version,
		Epoch:         cat.Epoch,
		Strategy:      cat.Strategy,
		Shards:        pbShardPlacements(cat.Shards),
		Nodes:         pbNodeDescriptors(sortedPlacementNodes(cat)),
		UpdatedAtUnix: cat.UpdatedAtUnix,
	}
}

func pbPlacementPlanSteps(in []placement.PlacementPlanStep) []*cefaspb.PlacementPlanStep {
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

func placementPlanFromPB(in *cefaspb.PlacementPlan) placement.PlacementPlan {
	if in == nil {
		return placement.PlacementPlan{}
	}
	return placement.PlacementPlan{
		Operation:        placement.PlacementOperation(in.GetOperation()),
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

func placementCatalogFromPB(in *cefaspb.PlacementCatalog) placement.PlacementCatalog {
	if in == nil {
		return placement.PlacementCatalog{}
	}
	nodes := make(map[string]placement.NodeDescriptor, len(in.GetNodes()))
	for _, node := range in.GetNodes() {
		desc := placement.NodeDescriptor{
			ID:           node.GetId(),
			RaftAddr:     node.GetRaftAddr(),
			HTTPAddr:     node.GetHttpAddr(),
			State:        placement.NodeState(node.GetState()),
			LastSeenUnix: node.GetLastSeenUnix(),
		}
		if c := node.GetCapacity(); c != nil {
			desc.Capacity = placement.NodeCapacity{
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
	return placement.PlacementCatalog{
		Version:       in.GetVersion(),
		Epoch:         in.GetEpoch(),
		Strategy:      in.GetStrategy(),
		Shards:        placementShardsFromPB(in.GetShards()),
		Nodes:         nodes,
		UpdatedAtUnix: in.GetUpdatedAtUnix(),
	}
}

func placementShardsFromPB(in []*cefaspb.ShardPlacement) []placement.ShardPlacement {
	out := make([]placement.ShardPlacement, 0, len(in))
	for _, sh := range in {
		out = append(out, placement.ShardPlacement{
			ID:         sh.GetId(),
			Ranges:     placementTokenRangesFromPB(sh.GetRanges()),
			State:      placement.ShardState(sh.GetState()),
			Epoch:      sh.GetEpoch(),
			Voters:     append([]string(nil), sh.GetVoters()...),
			NonVoters:  append([]string(nil), sh.GetNonVoters()...),
			LeaderHint: sh.GetLeaderHint(),
		})
	}
	return out
}

func placementTokenRangesFromPB(in []*cefaspb.TokenRange) []placement.TokenRange {
	out := make([]placement.TokenRange, 0, len(in))
	for _, r := range in {
		out = append(out, placement.TokenRange{Start: r.GetStart(), End: r.GetEnd()})
	}
	return out
}

func placementPlanStepsFromPB(in []*cefaspb.PlacementPlanStep) []placement.PlacementPlanStep {
	out := make([]placement.PlacementPlanStep, 0, len(in))
	for _, step := range in {
		var shardID *uint32
		if step.ShardId != nil {
			v := step.GetShardId()
			shardID = &v
		}
		out = append(out, placement.PlacementPlanStep{
			Action:  step.GetAction(),
			ShardID: shardID,
			NodeID:  step.GetNodeId(),
			Addr:    step.GetAddr(),
			Detail:  step.GetDetail(),
		})
	}
	return out
}

func pbPlacementApplyResult(result placement.PlacementApplyResult) *cefaspb.PlacementApplyResult {
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

func pbPlacementApplySteps(in []placement.PlacementApplyStep) []*cefaspb.PlacementApplyStep {
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
	case errors.Is(err, types.ErrMVNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, types.ErrGlobalIndexNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, types.ErrGlobalIndexExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, types.ErrStreamNotFound), errors.Is(err, types.ErrStreamShardNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, types.ErrTableAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, types.ErrItemNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, types.ErrMissingKey), errors.Is(err, types.ErrInvalidKeyType):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, types.ErrInvalidAttributeDefinition):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, pebble.ErrBackupNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, pebble.ErrBackupInUse):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, pebble.ErrInvalidBackupRetention):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, placement.ErrInvalidPlacementPlan):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, cluster.ErrStaleRoute):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, cluster.ErrNoLocalReplica):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, types.ErrSpatialNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, types.ErrInvalidSpatial):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, types.ErrStreamIteratorInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, types.ErrStreamIteratorExpired), errors.Is(err, types.ErrStreamTrimmed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, storage.ErrConditionFailed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, storage.ErrInvalidCounterMutation):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, pebble.ErrNotLeader):
		var nle *pebble.NotLeaderError
		if errors.As(err, &nle) && nle.LeaderURL != "" {
			return status.Errorf(codes.FailedPrecondition, "not leader; retry at %s", nle.LeaderURL)
		}
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, craft.ErrNotLeader):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, pebble.ErrDraining):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, pebble.ErrThrottled):
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
