// Plugin, audience, dedup/freqcap and aggregate surfaces of the SDK.
//
// Split out of client.go as part of the per-resource sibling layout
// (issue #315 / epic #307). Verbatim relocation: no API change.
package client

import (
	"context"
	"errors"
	"io"
	"time"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// ---------- plugins ----------

// PluginInfo mirrors the wire-side PluginDescriptor in a Go-friendly
// shape. Plugins not implementing StatusProvider report empty
// counters.
type PluginInfo struct {
	Name          string
	Kind          string
	Version       string
	Description   string
	State         string
	LastError     string
	LastErrorUnix int64
	ItemsIndexed  int64
	StartedAtUnix int64
}

// ListPlugins enumerates every plugin registered with the server. The
// optional kind filter narrows the result ("index" / "distance" /
// "estimator" / "audience"); empty returns every kind.
func (c *Client) ListPlugins(ctx context.Context, kind string) ([]PluginInfo, error) {
	resp, err := c.stub.ListPlugins(c.withAuth(ctx), &cefaspb.ListPluginsRequest{Kind: kind})
	if err != nil {
		return nil, err
	}
	out := make([]PluginInfo, 0, len(resp.GetPlugins()))
	for _, p := range resp.GetPlugins() {
		out = append(out, pluginInfoFromPB(p))
	}
	return out, nil
}

// DescribePlugin returns one plugin's descriptor by name.
func (c *Client) DescribePlugin(ctx context.Context, name string) (PluginInfo, error) {
	resp, err := c.stub.DescribePlugin(c.withAuth(ctx), &cefaspb.DescribePluginRequest{Name: name})
	if err != nil {
		return PluginInfo{}, err
	}
	return pluginInfoFromPB(resp.GetPlugin()), nil
}

func pluginInfoFromPB(p *cefaspb.PluginDescriptor) PluginInfo {
	if p == nil {
		return PluginInfo{}
	}
	return PluginInfo{
		Name:          p.GetName(),
		Kind:          p.GetKind(),
		Version:       p.GetVersion(),
		Description:   p.GetDescription(),
		State:         p.GetState(),
		LastError:     p.GetLastError(),
		LastErrorUnix: p.GetLastErrorUnix(),
		ItemsIndexed:  p.GetItemsIndexed(),
		StartedAtUnix: p.GetStartedAtUnix(),
	}
}

// ---------- plugin-backed indexes + planner ops ----------

// PluginIndex packs a plugin-backed index descriptor for the SDK.
type PluginIndex struct {
	Table        string
	Name         string
	PluginName   string
	PluginConfig []byte
	KeySchema    types.KeySchema
}

// CreateIndex creates a plugin-backed secondary index over the
// supplied table. The server resolves PluginName against the
// registered plugins and seeds the index via plugin.Build over the
// current table contents.
func (c *Client) CreateIndex(ctx context.Context, d PluginIndex) (PluginIndex, error) {
	resp, err := c.stub.CreateIndex(c.withAuth(ctx), &cefaspb.CreateIndexRequest{
		Descriptor_: pluginIndexToWire(d),
	})
	if err != nil {
		return PluginIndex{}, err
	}
	return wireToPluginIndex(resp.GetDescriptor_()), nil
}

// DescribeIndex returns the descriptor of a previously-created
// plugin-backed index.
func (c *Client) DescribeIndex(ctx context.Context, table, name string) (PluginIndex, error) {
	resp, err := c.stub.DescribeIndex(c.withAuth(ctx), &cefaspb.DescribeIndexRequest{Table: table, Name: name})
	if err != nil {
		return PluginIndex{}, err
	}
	return wireToPluginIndex(resp.GetDescriptor_()), nil
}

// RebuildIndex re-seeds an existing plugin-backed index from the
// table's current contents. Returns the number of items processed.
func (c *Client) RebuildIndex(ctx context.Context, table, name string) (int, error) {
	resp, err := c.stub.RebuildIndex(c.withAuth(ctx), &cefaspb.RebuildIndexRequest{Table: table, Name: name})
	if err != nil {
		return 0, err
	}
	return int(resp.GetItemsIndexed()), nil
}

// Explain returns the planner's textual (or JSON) plan for a
// table+predicate. v1 returns a synthetic plan tree until the
// planner integration deepens; the wire format is forward-compatible.
func (c *Client) Explain(ctx context.Context, table, predicate, format string) (string, error) {
	resp, err := c.stub.Explain(c.withAuth(ctx), &cefaspb.ExplainRequest{
		Table: table, Predicate: predicate, Format: format,
	})
	if err != nil {
		return "", err
	}
	return resp.GetPlan(), nil
}

// TopKRow is one ranked row returned by TopK.
type TopKRow struct {
	Item     types.Item
	Distance float64
}

// TopK ranks every item in `table` by the named distance plugin
// applied to (item[field], target) and returns the K with smallest
// distance, ascending.
func (c *Client) TopK(ctx context.Context, table, field, distanceOp string, target types.AttributeValue, k int) ([]TopKRow, error) {
	resp, err := c.stub.TopK(c.withAuth(ctx), &cefaspb.TopKRequest{
		Table:            table,
		Field:            field,
		DistanceOperator: distanceOp,
		Target:           attrToPB(target),
		K:                int32(k),
	})
	if err != nil {
		return nil, err
	}
	out := make([]TopKRow, 0, len(resp.GetRows()))
	for _, r := range resp.GetRows() {
		out = append(out, TopKRow{
			Item:     itemFromPB(r.GetItem().GetAttributes()),
			Distance: r.GetDistance(),
		})
	}
	return out, nil
}

// CohortCreate builds a Roaring-bitmap cohort named `cohort` from the
// items of `table` that match `filter`. `field` is the numeric
// attribute used as the cohort identifier.
func (c *Client) CohortCreate(ctx context.Context, table, cohort, field, filter string, binds map[string]types.AttributeValue) (int, error) {
	resp, err := c.stub.CohortCreate(c.withAuth(ctx), &cefaspb.CohortCreateRequest{
		Table: table, Cohort: cohort, Field: field, Filter: filter,
		Binds: itemAttrMap(types.Item(binds)),
	})
	if err != nil {
		return 0, err
	}
	return int(resp.GetMembers()), nil
}

// CohortEstimate returns the approximate distinct count over
// `field` of items matching `filter` — backed by HLL.
func (c *Client) CohortEstimate(ctx context.Context, table, field, filter string, binds map[string]types.AttributeValue) (float64, error) {
	resp, err := c.stub.CohortEstimate(c.withAuth(ctx), &cefaspb.CohortEstimateRequest{
		Table: table, Field: field, Filter: filter,
		Binds: itemAttrMap(types.Item(binds)),
	})
	if err != nil {
		return 0, err
	}
	return resp.GetApproximateCount(), nil
}

// GeoAudienceRequest packs the inputs for GeoAudience.
type GeoAudienceRequest struct {
	Table        string
	Index        string // empty → "loc_geo"
	Lat, Lon     float64
	RadiusMeters float64
	Limit        int
}

// GeoAudience streams every item within radius of (lat, lon).
func (c *Client) GeoAudience(ctx context.Context, req GeoAudienceRequest) ([]types.Item, error) {
	stream, err := c.stub.GeoAudience(c.withAuth(ctx), &cefaspb.GeoAudienceRequest{
		Table: req.Table, Index: req.Index,
		Lat: req.Lat, Lon: req.Lon,
		RadiusMeters: req.RadiusMeters,
		Limit:        int32(req.Limit),
	})
	if err != nil {
		return nil, err
	}
	var out []types.Item
	for {
		it, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, itemFromPB(it.GetAttributes()))
	}
}

// Dedup records (scope, key) with the supplied TTL and returns
// whether delivery is allowed (true on first hit inside the window,
// false on a duplicate).
func (c *Client) Dedup(ctx context.Context, scope, key string, ttl time.Duration) (bool, error) {
	resp, err := c.stub.Dedup(c.withAuth(ctx), &cefaspb.DedupRequest{
		Scope: scope, Key: key, TtlSeconds: int64(ttl.Seconds()),
	})
	if err != nil {
		return false, err
	}
	return resp.GetAllowed(), nil
}

// FreqCap increments the cap counter for (scope, key) inside window
// and returns whether the call kept the count at or below limit.
func (c *Client) FreqCap(ctx context.Context, scope, key string, limit int, window time.Duration) (bool, error) {
	resp, err := c.stub.FreqCap(c.withAuth(ctx), &cefaspb.FreqCapRequest{
		Scope: scope, Key: key, Limit: int32(limit), WindowSeconds: int64(window.Seconds()),
	})
	if err != nil {
		return false, err
	}
	return resp.GetAllowed(), nil
}

// AggregateRow is one row in an Aggregate response.
type AggregateRow struct {
	GroupKey map[string]string
	Counts   map[string]float64
	Members  int
}

// Aggregate runs a server-side group-by aggregation with a
// MinGroupSize floor; the server fails closed (returns an error)
// when any group would fall below the floor.
func (c *Client) Aggregate(ctx context.Context, table string, groupBy, metrics []string, minGroupSize int) ([]AggregateRow, error) {
	resp, err := c.stub.Aggregate(c.withAuth(ctx), &cefaspb.AggregateRequest{
		Table:        table,
		GroupBy:      groupBy,
		Metrics:      metrics,
		MinGroupSize: int32(minGroupSize),
	})
	if err != nil {
		return nil, err
	}
	out := make([]AggregateRow, 0, len(resp.GetRows()))
	for _, r := range resp.GetRows() {
		out = append(out, AggregateRow{
			GroupKey: r.GetGroupKey(),
			Counts:   r.GetCounts(),
			Members:  int(r.GetMembers()),
		})
	}
	return out, nil
}

func pluginIndexToWire(d PluginIndex) *cefaspb.PluginIndexDescriptor {
	return &cefaspb.PluginIndexDescriptor{
		Table:        d.Table,
		Name:         d.Name,
		PluginName:   d.PluginName,
		PluginConfig: d.PluginConfig,
		KeySchema:    &cefaspb.KeySchema{Pk: d.KeySchema.PK, Sk: d.KeySchema.SK},
	}
}

func wireToPluginIndex(d *cefaspb.PluginIndexDescriptor) PluginIndex {
	if d == nil {
		return PluginIndex{}
	}
	out := PluginIndex{
		Table:        d.GetTable(),
		Name:         d.GetName(),
		PluginName:   d.GetPluginName(),
		PluginConfig: d.GetPluginConfig(),
	}
	if ks := d.GetKeySchema(); ks != nil {
		out.KeySchema = types.KeySchema{PK: ks.GetPk(), SK: ks.GetSk()}
	}
	return out
}
