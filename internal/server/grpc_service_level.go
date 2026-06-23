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

// CreateServiceLevel persists a new workload-prioritization service
// level. Phase 1 of epic #489: catalog only — scheduling enforcement
// lands in #498 (Phase 3).
func (s *GRPCServer) CreateServiceLevel(ctx context.Context, req *cefaspb.CreateServiceLevelRequest) (*cefaspb.CreateServiceLevelResponse, error) {
	_, span := tracing.Tracer().Start(ctx, "CreateServiceLevel")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	desc := pbToServiceLevelDescriptor(req.GetDescriptor_())
	if desc.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	created, err := s.cat.CreateServiceLevel(desc)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.CreateServiceLevelResponse{Descriptor_: serviceLevelDescriptorToPB(created)}, nil
}

func (s *GRPCServer) AlterServiceLevel(ctx context.Context, req *cefaspb.AlterServiceLevelRequest) (*cefaspb.AlterServiceLevelResponse, error) {
	_, span := tracing.Tracer().Start(ctx, "AlterServiceLevel")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	desc := pbToServiceLevelDescriptor(req.GetDescriptor_())
	if desc.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	updated, err := s.cat.UpdateServiceLevel(desc)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.AlterServiceLevelResponse{Descriptor_: serviceLevelDescriptorToPB(updated)}, nil
}

func (s *GRPCServer) DropServiceLevel(ctx context.Context, req *cefaspb.DropServiceLevelRequest) (*cefaspb.DropServiceLevelResponse, error) {
	_, span := tracing.Tracer().Start(ctx, "DropServiceLevel")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	if err := s.cat.DropServiceLevel(req.GetName()); err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.DropServiceLevelResponse{}, nil
}

func (s *GRPCServer) ListServiceLevels(ctx context.Context, req *cefaspb.ListServiceLevelsRequest) (*cefaspb.ListServiceLevelsResponse, error) {
	_, span := tracing.Tracer().Start(ctx, "ListServiceLevels")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	_ = req
	all := s.cat.ListServiceLevels()
	out := &cefaspb.ListServiceLevelsResponse{ServiceLevels: make([]*cefaspb.ServiceLevelDescriptor, 0, len(all))}
	for _, sl := range all {
		out.ServiceLevels = append(out.ServiceLevels, serviceLevelDescriptorToPB(sl))
	}
	return out, nil
}

func serviceLevelDescriptorToPB(sl types.ServiceLevelDescriptor) *cefaspb.ServiceLevelDescriptor {
	return &cefaspb.ServiceLevelDescriptor{
		Name:           sl.Name,
		Shares:         int32(sl.Shares),
		MaxInFlight:    int32(sl.MaxInFlight),
		MaxRowsPerSec:  sl.MaxRowsPerSec,
		MaxBytesPerSec: sl.MaxBytesPerSec,
	}
}

func pbToServiceLevelDescriptor(pb *cefaspb.ServiceLevelDescriptor) types.ServiceLevelDescriptor {
	if pb == nil {
		return types.ServiceLevelDescriptor{}
	}
	return types.ServiceLevelDescriptor{
		Name:           pb.GetName(),
		Shares:         int(pb.GetShares()),
		MaxInFlight:    int(pb.GetMaxInFlight()),
		MaxRowsPerSec:  pb.GetMaxRowsPerSec(),
		MaxBytesPerSec: pb.GetMaxBytesPerSec(),
	}
}
