package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/auth"
	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/spatial"
	"github.com/osvaldoandrade/cefas/internal/storage"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
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
	cluster Cluster // nil in single-node mode
}

// NewGRPCServer wires the gRPC handler over the same storage / catalog
// instances the HTTP server uses. Cluster may be nil.
func NewGRPCServer(db *storage.DB, cat *catalog.Catalog, cluster Cluster) *GRPCServer {
	return &GRPCServer{db: db, cat: cat, cluster: cluster}
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

// ---------- item ----------

func (s *GRPCServer) PutItem(ctx context.Context, req *cefaspb.PutItemRequest) (*cefaspb.PutItemResponse, error) {
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
	if err := s.db.PutItemWith(td, item, storage.PutOptions{Condition: req.GetCondition(), Binds: binds}); err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.PutItemResponse{}, nil
}

func (s *GRPCServer) GetItem(ctx context.Context, req *cefaspb.GetItemRequest) (*cefaspb.GetItemResponse, error) {
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
	item, err := s.db.GetItem(req.GetTable(), td.KeySchema, key)
	if err != nil {
		if errors.Is(err, types.ErrItemNotFound) {
			return &cefaspb.GetItemResponse{Found: false}, nil
		}
		return nil, mapStorageErr(err)
	}
	return &cefaspb.GetItemResponse{Found: true, Item: itemToPB(item)}, nil
}

func (s *GRPCServer) DeleteItem(ctx context.Context, req *cefaspb.DeleteItemRequest) (*cefaspb.DeleteItemResponse, error) {
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
	if err := s.db.DeleteItemWith(td, key, storage.DeleteOptions{Condition: req.GetCondition(), Binds: binds}); err != nil {
		return nil, mapStorageErr(err)
	}
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
	if err := s.db.BatchWriteItem(td, ops); err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.BatchWriteItemResponse{}, nil
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
	items, err := s.db.BatchGetItem(req.GetTable(), td.KeySchema, keys)
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

	var items []types.Item
	limit := int(req.GetLimit())
	switch {
	case req.GetIndexName() != "":
		items, err = s.db.QueryByGSI(td, req.GetIndexName(), pkVal, storage.QueryOptions{SKLow: lo, SKHigh: hi, Limit: limit})
	case req.GetSkLow() == nil && req.GetSkHigh() == nil:
		items, err = s.db.QueryByPK(req.GetTable(), td.KeySchema, pkVal, limit)
	default:
		items, err = s.db.QueryByPKRange(req.GetTable(), td.KeySchema, pkVal, lo, hi, limit)
	}
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
	items, err := s.db.SpatialQueryItems(td, req.GetIndexName(), q)
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

// ---------- cluster ----------

func (s *GRPCServer) ClusterStatus(ctx context.Context, _ *cefaspb.ClusterStatusRequest) (*cefaspb.ClusterStatusResponse, error) {
	resp := &cefaspb.ClusterStatusResponse{Mode: "single-node"}
	if s.cluster != nil {
		resp.Mode = "raft"
		resp.IsLeader = s.cluster.IsLeader()
		resp.SelfId = s.cluster.SelfID()
		resp.BindAddr = s.cluster.BindAddr()
		resp.LeaderHttp = s.cluster.LeaderHTTPAddr()
	}
	return resp, nil
}

func (s *GRPCServer) AddVoter(ctx context.Context, req *cefaspb.AddVoterRequest) (*cefaspb.AddVoterResponse, error) {
	if err := requireScope(ctx, auth.ScopeClusterAdmin); err != nil {
		return nil, err
	}
	if s.cluster == nil {
		return nil, status.Error(codes.FailedPrecondition, "cluster not configured")
	}
	timeout := time.Duration(req.GetTimeoutMs()) * time.Millisecond
	if timeout == 0 {
		timeout = 5 * time.Second
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
	if s.cluster == nil {
		return nil, status.Error(codes.FailedPrecondition, "cluster not configured")
	}
	timeout := time.Duration(req.GetTimeoutMs()) * time.Millisecond
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if err := s.cluster.RemoveServer(req.GetId(), timeout); err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.RemoveServerResponse{}, nil
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
	}
	return status.Error(codes.Internal, err.Error())
}
