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

// PauseMaterializedView marks an MV as paused: the eager hook
// short-circuits writes to it and the scheduler skips it. Reads
// continue to serve whatever rows the view currently holds — the
// pause does not flush, only stops maintenance.
func (s *GRPCServer) PauseMaterializedView(ctx context.Context, req *cefaspb.PauseMaterializedViewRequest) (*cefaspb.PauseMaterializedViewResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "PauseMaterializedView")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	mv, err := s.cat.DescribeView(req.GetName())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	mv.Status = types.MVStatusPaused
	if err := s.cat.UpdateView(mv); err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.PauseMaterializedViewResponse{}, nil
}

// ResumeMaterializedView clears the paused flag. Status drops back
// to active; the eager hook and scheduler resume. If the view fell
// behind during the pause, an explicit Refresh closes the gap.
func (s *GRPCServer) ResumeMaterializedView(ctx context.Context, req *cefaspb.ResumeMaterializedViewRequest) (*cefaspb.ResumeMaterializedViewResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "ResumeMaterializedView")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}
	mv, err := s.cat.DescribeView(req.GetName())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	if mv.Status != types.MVStatusPaused {
		return &cefaspb.ResumeMaterializedViewResponse{}, nil
	}
	mv.Status = types.MVStatusActive
	if err := s.cat.UpdateView(mv); err != nil {
		return nil, mapStorageErr(err)
	}
	return &cefaspb.ResumeMaterializedViewResponse{}, nil
}
