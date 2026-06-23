package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/tracing"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// UpdateStreamSpecification flips a table's CDC capture on or off
// without recreating the table (#525). Operators can:
//
//   - Enable streams on an existing table (creates the stream
//     descriptor, starts the changelog from the next mutation).
//   - Disable streams (stops capture; existing records age out via
//     the retention loop).
//   - Change view type without recreating the table.
//   - Change per-table retention (#521).
//
// v1 transitions are immediate — DISABLED → ENABLED on commit, no
// intermediate ENABLING state. Records already in the changelog
// keep their original view-type tagging; new records reflect the
// updated spec. The state-machine refinement (ENABLING /
// DISABLING) lands when a workload demands drain orchestration.
func (s *GRPCServer) UpdateStreamSpecification(ctx context.Context, req *cefaspb.UpdateStreamSpecificationRequest) (*cefaspb.UpdateStreamSpecificationResponse, error) {
	_, span := tracing.Tracer().Start(ctx, "UpdateStreamSpecification")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	if req.GetTableName() == "" {
		return nil, status.Error(codes.InvalidArgument, "table_name required")
	}
	td, err := s.cat.Describe(req.GetTableName())
	if err != nil {
		return nil, mapStorageErr(err)
	}
	spec := pbToStreamSpecification(req.GetStreamSpecification())
	// View-type changes are disallowed by the in-place UpdateTable
	// contract; the runtime-toggle RPC relaxes that by going through
	// an explicit disable → re-enable transition when the view
	// actually changes. ARN / label continuity is sacrificed here
	// in exchange for the lifecycle flexibility.
	priorEnabled := td.StreamSpecification != nil && td.StreamSpecification.StreamEnabled
	if priorEnabled && spec != nil && td.StreamSpecification.StreamViewType != spec.StreamViewType {
		disabled := td
		disabled.StreamSpecification = nil
		disabled.LatestStreamArn = ""
		disabled.LatestStreamLabel = ""
		disabled.StreamStatus = ""
		if err := s.cat.UpdateTable(disabled); err != nil {
			return nil, mapStorageErr(err)
		}
		// Reload the post-disable descriptor; the normaliser may have
		// cleared additional fields we should respect.
		td, err = s.cat.Describe(req.GetTableName())
		if err != nil {
			return nil, mapStorageErr(err)
		}
	}
	if spec == nil {
		td.StreamSpecification = nil
		td.LatestStreamArn = ""
		td.LatestStreamLabel = ""
		td.StreamStatus = ""
	} else {
		td.StreamSpecification = spec
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
	resp := &cefaspb.UpdateStreamSpecificationResponse{
		LatestStreamArn:   td.LatestStreamArn,
		LatestStreamLabel: td.LatestStreamLabel,
		StreamStatus:      td.StreamStatus,
	}
	if td.StreamSpecification != nil {
		resp.StreamSpecification = streamSpecificationToPB(td.StreamSpecification)
	}
	return resp, nil
}

func pbToStreamSpecification(pb *cefaspb.StreamSpecification) *types.StreamSpecification {
	if pb == nil || !pb.GetStreamEnabled() {
		return nil
	}
	return &types.StreamSpecification{
		StreamEnabled:    true,
		StreamViewType:   pb.GetStreamViewType(),
		RetentionSeconds: pb.GetRetentionSeconds(),
	}
}

func streamSpecificationToPB(s *types.StreamSpecification) *cefaspb.StreamSpecification {
	if s == nil {
		return nil
	}
	return &cefaspb.StreamSpecification{
		StreamEnabled:    s.StreamEnabled,
		StreamViewType:   s.StreamViewType,
		RetentionSeconds: s.RetentionSeconds,
	}
}
