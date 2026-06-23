package server

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/CefasDb/cefasdb/internal/auth"
	"github.com/CefasDb/cefasdb/internal/tracing"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// mvPropagationTimeout bounds how long CreateMaterializedView will
// wait for every peer to acknowledge it can resolve the new view.
// The propagation race only opens during shard-0 raft Apply lag —
// in practice every peer settles in well under a second.
const mvPropagationTimeout = 5 * time.Second

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
	// The catalog Create call returns once the local raft Apply landed
	// (#536 fallback handles cold-cache reads). On 8-node clusters the
	// peers' shard 0 may still be catching up — and the cross-shard MV
	// cascade (#535 / #537) hits the propagation race head-on. Wait
	// until every peer can resolve the view before declaring the DDL
	// done; the timeout is a soft bound, callers should still expect
	// a short tail of NotFound during heavy raft lag.
	s.waitForMVPropagation(ctx, created.Name)
	return &cefaspb.CreateMaterializedViewResponse{Descriptor_: mvDescriptorToPB(created)}, nil
}

// waitForMVPropagation polls every peer in parallel until each
// peer's catalog can resolve the new view via
// DescribeMaterializedView. Returns when all peers acknowledge or
// the soft timeout fires.
func (s *GRPCServer) waitForMVPropagation(ctx context.Context, name string) {
	if s.manager == nil {
		return
	}
	peers := s.manager.PeerIDs()
	if len(peers) == 0 {
		return
	}
	deadline := time.Now().Add(mvPropagationTimeout)
	var wg sync.WaitGroup
	for _, peerID := range peers {
		peerID := peerID
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				if ctx.Err() != nil {
					return
				}
				err := s.manager.PeerDescribeView(ctx, peerID, name)
				if err == nil {
					return
				}
				if status.Code(err) != codes.NotFound {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
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
		GroupBy:             append([]string(nil), mv.GroupBy...),
		Aggregations:        mvAggregationsToPB(mv.Aggregations),
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
		GroupBy:             append([]string(nil), pb.GetGroupBy()...),
		Aggregations:        pbAggregationsToTypes(pb.GetAggregations()),
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

func mvAggregationsToPB(in []types.MaterializedViewAggregation) []*cefaspb.MaterializedViewAggregation {
	if len(in) == 0 {
		return nil
	}
	out := make([]*cefaspb.MaterializedViewAggregation, 0, len(in))
	for _, agg := range in {
		out = append(out, &cefaspb.MaterializedViewAggregation{
			Function:        mvAggregationFunctionToPB(agg.Function),
			SourceAttribute: agg.SourceAttribute,
			TargetAttribute: agg.TargetAttribute,
		})
	}
	return out
}

func pbAggregationsToTypes(in []*cefaspb.MaterializedViewAggregation) []types.MaterializedViewAggregation {
	if len(in) == 0 {
		return nil
	}
	out := make([]types.MaterializedViewAggregation, 0, len(in))
	for _, agg := range in {
		out = append(out, types.MaterializedViewAggregation{
			Function:        pbAggregationFunctionToTypes(agg.GetFunction()),
			SourceAttribute: agg.GetSourceAttribute(),
			TargetAttribute: agg.GetTargetAttribute(),
		})
	}
	return out
}

func mvAggregationFunctionToPB(fn string) cefaspb.MaterializedViewAggregation_Function {
	switch fn {
	case types.MVAggregationCount:
		return cefaspb.MaterializedViewAggregation_COUNT
	case types.MVAggregationSum:
		return cefaspb.MaterializedViewAggregation_SUM
	default:
		return cefaspb.MaterializedViewAggregation_FUNCTION_UNSPECIFIED
	}
}

func pbAggregationFunctionToTypes(fn cefaspb.MaterializedViewAggregation_Function) string {
	switch fn {
	case cefaspb.MaterializedViewAggregation_COUNT:
		return types.MVAggregationCount
	case cefaspb.MaterializedViewAggregation_SUM:
		return types.MVAggregationSum
	default:
		return ""
	}
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
	case types.RefreshModeFast:
		out.Mode = cefaspb.RefreshPolicy_FAST
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
	case cefaspb.RefreshPolicy_FAST:
		return types.RefreshModeFast
	default:
		return types.RefreshModeUnspecified
	}
}
