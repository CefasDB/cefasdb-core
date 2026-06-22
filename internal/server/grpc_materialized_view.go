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

// CreateMaterializedView persists a new view descriptor.
//
// Epic #488 Phase 1: the catalog is the only thing this handler touches.
// Subsequent phases wire write maintenance (#491), reads (#492), the
// refresh engine (#493), and the scheduler (#502).
func (s *GRPCServer) CreateMaterializedView(ctx context.Context, req *cefaspb.CreateMaterializedViewRequest) (*cefaspb.CreateMaterializedViewResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "CreateMaterializedView")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	desc := pbToMVDescriptor(req.GetDescriptor_())
	if desc.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	created, err := s.cat.CreateView(desc)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.CreateMaterializedViewResponse{Descriptor_: mvDescriptorToPB(created)}, nil
}

func (s *GRPCServer) DescribeMaterializedView(ctx context.Context, req *cefaspb.DescribeMaterializedViewRequest) (*cefaspb.DescribeMaterializedViewResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "DescribeMaterializedView")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	mv, err := s.cat.DescribeView(req.GetName())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.DescribeMaterializedViewResponse{Descriptor_: mvDescriptorToPB(mv)}, nil
}

func (s *GRPCServer) DropMaterializedView(ctx context.Context, req *cefaspb.DropMaterializedViewRequest) (*cefaspb.DropMaterializedViewResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "DropMaterializedView")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	if err := s.cat.DropView(req.GetName()); err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.DropMaterializedViewResponse{}, nil
}

func (s *GRPCServer) ListMaterializedViews(ctx context.Context, req *cefaspb.ListMaterializedViewsRequest) (*cefaspb.ListMaterializedViewsResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "ListMaterializedViews")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	all := s.cat.ListViews(req.GetBaseTable())
	out := make([]*cefaspb.MaterializedViewDescriptor, 0, len(all))
	for _, mv := range all {
		out = append(out, mvDescriptorToPB(mv))
	}
	return &cefaspb.ListMaterializedViewsResponse{Views: out}, nil
}

// mvDescriptorToPB converts the catalog-side type to the wire shape.
func mvDescriptorToPB(mv types.MaterializedViewDescriptor) *cefaspb.MaterializedViewDescriptor {
	return &cefaspb.MaterializedViewDescriptor{
		Name:                mv.Name,
		BaseTable:           mv.BaseTable,
		KeySchema:           &cefaspb.KeySchema{Pk: mv.KeySchema.PK, Sk: mv.KeySchema.SK},
		ProjectedAttributes: append([]string(nil), mv.ProjectedAttributes...),
		RefreshPolicy:       refreshPolicyToPB(mv.RefreshPolicy),
		Status:              mv.Status,
		LastRefreshAtUnix:   mv.LastRefreshAtUnix,
	}
}

// pbToMVDescriptor reverses mvDescriptorToPB.
func pbToMVDescriptor(pb *cefaspb.MaterializedViewDescriptor) types.MaterializedViewDescriptor {
	if pb == nil {
		return types.MaterializedViewDescriptor{}
	}
	out := types.MaterializedViewDescriptor{
		Name:                pb.GetName(),
		BaseTable:           pb.GetBaseTable(),
		ProjectedAttributes: append([]string(nil), pb.GetProjectedAttributes()...),
		Status:              pb.GetStatus(),
		LastRefreshAtUnix:   pb.GetLastRefreshAtUnix(),
	}
	if ks := pb.GetKeySchema(); ks != nil {
		out.KeySchema = types.KeySchema{PK: ks.GetPk(), SK: ks.GetSk()}
	}
	if rp := pb.GetRefreshPolicy(); rp != nil {
		out.RefreshPolicy = types.RefreshPolicy{
			Mode:            pbRefreshModeToTypes(rp.GetMode()),
			IntervalSeconds: rp.GetIntervalSeconds(),
		}
	}
	return out
}

func refreshPolicyToPB(rp types.RefreshPolicy) *cefaspb.RefreshPolicy {
	out := &cefaspb.RefreshPolicy{
		IntervalSeconds: rp.IntervalSeconds,
	}
	switch rp.Mode {
	case types.RefreshModeEager:
		out.Mode = cefaspb.RefreshPolicy_EAGER
	case types.RefreshModeScheduled:
		out.Mode = cefaspb.RefreshPolicy_SCHEDULED
	case types.RefreshModeOnDemand:
		out.Mode = cefaspb.RefreshPolicy_ON_DEMAND
	default:
		out.Mode = cefaspb.RefreshPolicy_MODE_UNSPECIFIED
	}
	return out
}

func pbRefreshModeToTypes(m cefaspb.RefreshPolicy_Mode) types.RefreshMode {
	switch m {
	case cefaspb.RefreshPolicy_EAGER:
		return types.RefreshModeEager
	case cefaspb.RefreshPolicy_SCHEDULED:
		return types.RefreshModeScheduled
	case cefaspb.RefreshPolicy_ON_DEMAND:
		return types.RefreshModeOnDemand
	default:
		return types.RefreshModeUnspecified
	}
}
