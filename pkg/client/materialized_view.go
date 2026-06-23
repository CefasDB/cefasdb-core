package client

import (
	"context"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// RefreshMode mirrors types.RefreshMode at the client surface so
// callers do not have to import the server-side types package just
// to spell the policy.
type RefreshMode string

const (
	RefreshModeEager     RefreshMode = "eager"
	RefreshModeScheduled RefreshMode = "scheduled"
	RefreshModeOnDemand  RefreshMode = "on_demand"
	RefreshModeFast      RefreshMode = "fast"
)

// RefreshPolicy is the per-view refresh contract.
type RefreshPolicy struct {
	Mode            RefreshMode
	IntervalSeconds int64
}

// CreateMaterializedView persists a new materialized view definition.
// Projected may be nil (all base attributes).
func (c *Client) CreateMaterializedView(ctx context.Context, name, baseTable string, key types.KeySchema, projected []string, policy RefreshPolicy) (types.MaterializedViewDescriptor, error) {
	resp, err := c.stub.CreateMaterializedView(c.withAuth(ctx), &cefaspb.CreateMaterializedViewRequest{
		Descriptor_: &cefaspb.MaterializedViewDescriptor{
			Name:                name,
			BaseTable:           baseTable,
			KeySchema:           &cefaspb.KeySchema{Pk: key.PK, Sk: key.SK},
			ProjectedAttributes: append([]string(nil), projected...),
			RefreshPolicy:       refreshPolicyToPBClient(policy),
		},
	})
	if err != nil {
		return types.MaterializedViewDescriptor{}, err
	}
	return mvFromPB(resp.GetDescriptor_()), nil
}

// DescribeMaterializedView returns the descriptor for the named view.
func (c *Client) DescribeMaterializedView(ctx context.Context, name string) (types.MaterializedViewDescriptor, error) {
	resp, err := c.stub.DescribeMaterializedView(c.withAuth(ctx), &cefaspb.DescribeMaterializedViewRequest{Name: name})
	if err != nil {
		return types.MaterializedViewDescriptor{}, err
	}
	return mvFromPB(resp.GetDescriptor_()), nil
}

// DropMaterializedView removes a view.
func (c *Client) DropMaterializedView(ctx context.Context, name string) error {
	_, err := c.stub.DropMaterializedView(c.withAuth(ctx), &cefaspb.DropMaterializedViewRequest{Name: name})
	return err
}

// RefreshMaterializedView triggers a complete refresh. Returns the
// number of rows the engine wrote into the view.
func (c *Client) RefreshMaterializedView(ctx context.Context, name string) (int64, error) {
	resp, err := c.stub.RefreshMaterializedView(c.withAuth(ctx), &cefaspb.RefreshMaterializedViewRequest{Name: name})
	if err != nil {
		return 0, err
	}
	return resp.GetRowsIndexed(), nil
}

func refreshPolicyToPBClient(p RefreshPolicy) *cefaspb.RefreshPolicy {
	out := &cefaspb.RefreshPolicy{IntervalSeconds: p.IntervalSeconds}
	switch p.Mode {
	case RefreshModeEager:
		out.Mode = cefaspb.RefreshPolicy_EAGER
	case RefreshModeScheduled:
		out.Mode = cefaspb.RefreshPolicy_SCHEDULED
	case RefreshModeOnDemand:
		out.Mode = cefaspb.RefreshPolicy_ON_DEMAND
	case RefreshModeFast:
		out.Mode = cefaspb.RefreshPolicy_FAST
	default:
		out.Mode = cefaspb.RefreshPolicy_MODE_UNSPECIFIED
	}
	return out
}

func mvFromPB(pb *cefaspb.MaterializedViewDescriptor) types.MaterializedViewDescriptor {
	if pb == nil {
		return types.MaterializedViewDescriptor{}
	}
	out := types.MaterializedViewDescriptor{
		Name:                pb.GetName(),
		BaseTable:           pb.GetBaseTable(),
		ProjectedAttributes: append([]string(nil), pb.GetProjectedAttributes()...),
		GroupBy:             append([]string(nil), pb.GetGroupBy()...),
		Aggregations:        mvAggregationsFromPB(pb.GetAggregations()),
		Status:              pb.GetStatus(),
		LastRefreshAtUnix:   pb.GetLastRefreshAtUnix(),
	}
	if ks := pb.GetKeySchema(); ks != nil {
		out.KeySchema = types.KeySchema{PK: ks.GetPk(), SK: ks.GetSk()}
	}
	if rp := pb.GetRefreshPolicy(); rp != nil {
		switch rp.GetMode() {
		case cefaspb.RefreshPolicy_EAGER:
			out.RefreshPolicy.Mode = types.RefreshModeEager
		case cefaspb.RefreshPolicy_SCHEDULED:
			out.RefreshPolicy.Mode = types.RefreshModeScheduled
		case cefaspb.RefreshPolicy_ON_DEMAND:
			out.RefreshPolicy.Mode = types.RefreshModeOnDemand
		default:
			out.RefreshPolicy.Mode = types.RefreshModeUnspecified
		}
		out.RefreshPolicy.IntervalSeconds = rp.GetIntervalSeconds()
	}
	return out
}

func mvAggregationsFromPB(in []*cefaspb.MaterializedViewAggregation) []types.MaterializedViewAggregation {
	if len(in) == 0 {
		return nil
	}
	out := make([]types.MaterializedViewAggregation, 0, len(in))
	for _, agg := range in {
		out = append(out, types.MaterializedViewAggregation{
			Function:        mvAggregationFunctionFromPB(agg.GetFunction()),
			SourceAttribute: agg.GetSourceAttribute(),
			TargetAttribute: agg.GetTargetAttribute(),
		})
	}
	return out
}

func mvAggregationFunctionFromPB(fn cefaspb.MaterializedViewAggregation_Function) string {
	switch fn {
	case cefaspb.MaterializedViewAggregation_COUNT:
		return types.MVAggregationCount
	case cefaspb.MaterializedViewAggregation_SUM:
		return types.MVAggregationSum
	default:
		return ""
	}
}
