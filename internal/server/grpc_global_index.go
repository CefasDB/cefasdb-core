package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/tracing"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// CreateGlobalIndex persists a new global secondary index descriptor.
// Phase 1 of epic #509 — catalog only. Phase 2 (#511) will fire the
// eager write hook, Phase 3 (#512) the read routing.
func (s *GRPCServer) CreateGlobalIndex(ctx context.Context, req *cefaspb.CreateGlobalIndexRequest) (*cefaspb.CreateGlobalIndexResponse, error) {
	_, span := tracing.Tracer().Start(ctx, "CreateGlobalIndex")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	desc := pbToGlobalIndexDescriptor(req.GetDescriptor_())
	if desc.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	created, err := s.cat.CreateGlobalIndex(desc)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.CreateGlobalIndexResponse{Descriptor_: globalIndexDescriptorToPB(created)}, nil
}

func (s *GRPCServer) DescribeGlobalIndex(ctx context.Context, req *cefaspb.DescribeGlobalIndexRequest) (*cefaspb.DescribeGlobalIndexResponse, error) {
	_, span := tracing.Tracer().Start(ctx, "DescribeGlobalIndex")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	gi, err := s.cat.DescribeGlobalIndex(req.GetName())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.DescribeGlobalIndexResponse{Descriptor_: globalIndexDescriptorToPB(gi)}, nil
}

func (s *GRPCServer) DropGlobalIndex(ctx context.Context, req *cefaspb.DropGlobalIndexRequest) (*cefaspb.DropGlobalIndexResponse, error) {
	_, span := tracing.Tracer().Start(ctx, "DropGlobalIndex")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	if err := s.cat.DropGlobalIndex(req.GetName()); err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.DropGlobalIndexResponse{}, nil
}

func (s *GRPCServer) ListGlobalIndexes(ctx context.Context, req *cefaspb.ListGlobalIndexesRequest) (*cefaspb.ListGlobalIndexesResponse, error) {
	_, span := tracing.Tracer().Start(ctx, "ListGlobalIndexes")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	all := s.cat.ListGlobalIndexes(req.GetBaseTable())
	out := &cefaspb.ListGlobalIndexesResponse{Indexes: make([]*cefaspb.GlobalIndexDescriptor, 0, len(all))}
	for _, gi := range all {
		out.Indexes = append(out.Indexes, globalIndexDescriptorToPB(gi))
	}
	return out, nil
}

func globalIndexDescriptorToPB(gi types.GlobalIndexDescriptor) *cefaspb.GlobalIndexDescriptor {
	return &cefaspb.GlobalIndexDescriptor{
		Name:              gi.Name,
		BaseTable:         gi.BaseTable,
		IndexedColumn:     gi.IndexedColumn,
		ProjectedColumns:  append([]string(nil), gi.ProjectedColumns...),
		Status:            gi.Status,
		Shards:            int32(gi.Shards),
		ReplicationFactor: int32(gi.ReplicationFactor),
		Paused:            gi.Paused,
	}
}

func pbToGlobalIndexDescriptor(pb *cefaspb.GlobalIndexDescriptor) types.GlobalIndexDescriptor {
	if pb == nil {
		return types.GlobalIndexDescriptor{}
	}
	return types.GlobalIndexDescriptor{
		Name:              pb.GetName(),
		BaseTable:         pb.GetBaseTable(),
		IndexedColumn:     pb.GetIndexedColumn(),
		ProjectedColumns:  append([]string(nil), pb.GetProjectedColumns()...),
		Status:            pb.GetStatus(),
		Shards:            int(pb.GetShards()),
		ReplicationFactor: int(pb.GetReplicationFactor()),
		Paused:            pb.GetPaused(),
	}
}
