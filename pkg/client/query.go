package client

import (
	"context"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// ---------- query ----------

// QueryBuilder collects Query parameters fluently.
type QueryBuilder struct {
	c          *Client
	table      string
	req        *cefaspb.QueryRequest
	routeAware *bool
}

// Query opens a builder for the given table. Call PK / SK / Limit /
// Index / Strong on the builder, then Run to execute.
func (c *Client) Query(ctx context.Context, table string) *QueryBuilder {
	return &QueryBuilder{c: c, table: table, req: &cefaspb.QueryRequest{Table: table}}
}

// PK pins the partition-key value the query must match.
func (b *QueryBuilder) PK(v types.AttributeValue) *QueryBuilder {
	b.req.PkValue = attrToPB(v)
	return b
}

// SKBetween restricts the sort-key range to [lo, hi]; either bound may
// be the zero (AttrNull) AttributeValue to leave that side open.
func (b *QueryBuilder) SKBetween(lo, hi types.AttributeValue) *QueryBuilder {
	if lo.T != types.AttrNull {
		b.req.SkLow = attrToPB(lo)
	}
	if hi.T != types.AttrNull {
		b.req.SkHigh = attrToPB(hi)
	}
	return b
}

// Index targets a named secondary index instead of the primary key.
func (b *QueryBuilder) Index(name string) *QueryBuilder {
	b.req.IndexName = name
	return b
}

// Limit caps the number of items returned by the query.
func (b *QueryBuilder) Limit(n int) *QueryBuilder {
	b.req.Limit = int32(n)
	return b
}

// Strong upgrades the query to strong (leader-served) consistency.
func (b *QueryBuilder) Strong() *QueryBuilder {
	b.req.Consistency = cefaspb.Consistency_CONSISTENCY_STRONG
	return b
}

// RouteAware overrides token-aware eventual-read routing for this query.
func (b *QueryBuilder) RouteAware(enabled bool) *QueryBuilder {
	b.routeAware = &enabled
	return b
}

// Run executes the query and collects all results. For large result
// sets prefer Stream.
func (b *QueryBuilder) Run(ctx context.Context) ([]types.Item, error) {
	if b.c.routeReads != nil && routeAwareOverride(b.routeAware) {
		pkBytes, ok, err := pkBytesForQuery(b.req)
		if err != nil {
			return nil, err
		}
		if ok {
			return b.c.routeAwareQueryRun(ctx, b.req, pkBytes)
		}
	}
	stream, err := b.c.stub.Query(b.c.withAuth(ctx), b.req)
	if err != nil {
		return nil, err
	}
	var out []types.Item
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, itemFromPB(item.GetAttributes()))
	}
}

// Stream returns the underlying server-streaming RPC for callers that
// want to iterate results lazily.
func (b *QueryBuilder) Stream(ctx context.Context) (grpc.ServerStreamingClient[cefaspb.Item], error) {
	if b.c.routeReads != nil && routeAwareOverride(b.routeAware) {
		pkBytes, ok, err := pkBytesForQuery(b.req)
		if err != nil {
			return nil, err
		}
		if ok {
			return b.c.routeAwareQueryStream(ctx, b.req, pkBytes)
		}
	}
	return b.c.stub.Query(b.c.withAuth(ctx), b.req)
}

func (c *Client) routeAwareQueryRun(ctx context.Context, req *cefaspb.QueryRequest, pkBytes []byte) ([]types.Item, error) {
	c.routeReads.attempts.Add(1)
	if err := c.ensureRoutePlacement(ctx, false); err != nil {
		return nil, err
	}

	var last error
	refreshed := false
	for {
		target, token, err := c.routeReads.routeForPK(pkBytes)
		if err != nil {
			return nil, err
		}
		candidates, err := c.routeReads.candidatesForTarget(target, token, time.Now())
		if err != nil {
			return nil, err
		}
		for _, node := range candidates {
			start := node.begin()
			stream, err := node.stub.Query(c.withAuth(ctx), req)
			if err != nil {
				node.finish(start, err)
				last = err
				if !c.routeReads.observeRetry(err) {
					return nil, err
				}
				continue
			}
			out, err := collectQueryStream(stream)
			node.finish(start, err)
			if err == nil {
				c.routeReads.successes.Add(1)
				c.routeReads.observeServedBy(target, node.id)
				return out, nil
			}
			last = err
			if len(out) > 0 || !c.routeReads.observeRetry(err) {
				return nil, err
			}
		}
		if refreshed {
			break
		}
		if err := c.ensureRoutePlacement(ctx, true); err != nil {
			return nil, err
		}
		refreshed = true
	}
	return nil, last
}

func (c *Client) routeAwareQueryStream(ctx context.Context, req *cefaspb.QueryRequest, pkBytes []byte) (grpc.ServerStreamingClient[cefaspb.Item], error) {
	c.routeReads.attempts.Add(1)
	if err := c.ensureRoutePlacement(ctx, false); err != nil {
		return nil, err
	}

	var last error
	refreshed := false
	for {
		target, token, err := c.routeReads.routeForPK(pkBytes)
		if err != nil {
			return nil, err
		}
		candidates, err := c.routeReads.candidatesForTarget(target, token, time.Now())
		if err != nil {
			return nil, err
		}
		for _, node := range candidates {
			start := node.begin()
			stream, err := node.stub.Query(c.withAuth(ctx), req)
			node.finish(start, err)
			if err == nil {
				c.routeReads.successes.Add(1)
				c.routeReads.observeServedBy(target, node.id)
				return stream, nil
			}
			last = err
			if !c.routeReads.observeRetry(err) {
				return nil, err
			}
		}
		if refreshed {
			break
		}
		if err := c.ensureRoutePlacement(ctx, true); err != nil {
			return nil, err
		}
		refreshed = true
	}
	return nil, last
}

func collectQueryStream(stream grpc.ServerStreamingClient[cefaspb.Item]) ([]types.Item, error) {
	var out []types.Item
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, itemFromPB(item.GetAttributes()))
	}
}

// ---------- scan ----------

// ScanOptions tweaks a Scan call. FilterExpression is the same DDB
// condition subset PutItem's Condition accepts; binds resolve `:name`
// placeholders inside it.
type ScanOptions struct {
	FilterExpression string
	Binds            map[string]types.AttributeValue
	Limit            int
	Strong           bool
}

// Scan streams every primary item in `table`, applying FilterExpression
// server-side. For large result sets prefer ScanStream — Scan
// materialises the whole stream in memory.
func (c *Client) Scan(ctx context.Context, table string, opts ...ScanOptions) ([]types.Item, error) {
	var o ScanOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	cons := cefaspb.Consistency_CONSISTENCY_EVENTUAL
	if o.Strong {
		cons = cefaspb.Consistency_CONSISTENCY_STRONG
	}
	stream, err := c.stub.Scan(c.withAuth(ctx), &cefaspb.ScanRequest{
		Table:            table,
		FilterExpression: o.FilterExpression,
		Binds:            itemAttrMap(types.Item(o.Binds)),
		Limit:            int32(o.Limit),
		Consistency:      cons,
	})
	if err != nil {
		return nil, err
	}
	var out []types.Item
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, itemFromPB(item.GetAttributes()))
	}
}

// ScanStream returns the underlying server-streaming RPC for callers
// that want to iterate large scans lazily.
func (c *Client) ScanStream(ctx context.Context, table string, opts ...ScanOptions) (grpc.ServerStreamingClient[cefaspb.Item], error) {
	var o ScanOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	cons := cefaspb.Consistency_CONSISTENCY_EVENTUAL
	if o.Strong {
		cons = cefaspb.Consistency_CONSISTENCY_STRONG
	}
	return c.stub.Scan(c.withAuth(ctx), &cefaspb.ScanRequest{
		Table:            table,
		FilterExpression: o.FilterExpression,
		Binds:            itemAttrMap(types.Item(o.Binds)),
		Limit:            int32(o.Limit),
		Consistency:      cons,
	})
}

// ---------- spatial ----------

// SpatialQueryByBBox runs a geohash bounding-box query.
func (c *Client) SpatialQueryByBBox(ctx context.Context, table, indexName string, minLat, minLon, maxLat, maxLon float64, limit int) ([]types.Item, error) {
	req := &cefaspb.SpatialQueryRequest{
		Table:     table,
		IndexName: indexName,
		Limit:     int32(limit),
		Shape: &cefaspb.SpatialQueryRequest_Bbox{Bbox: &cefaspb.BBox{
			MinLat: minLat, MinLon: minLon, MaxLat: maxLat, MaxLon: maxLon,
		}},
	}
	return c.runSpatial(ctx, req)
}

// SpatialQueryByRadius runs a geohash radius query (great-circle).
func (c *Client) SpatialQueryByRadius(ctx context.Context, table, indexName string, lat, lon, meters float64, limit int) ([]types.Item, error) {
	req := &cefaspb.SpatialQueryRequest{
		Table:     table,
		IndexName: indexName,
		Limit:     int32(limit),
		Shape: &cefaspb.SpatialQueryRequest_Radius{Radius: &cefaspb.Radius{
			Lat: lat, Lon: lon, Meters: meters,
		}},
	}
	return c.runSpatial(ctx, req)
}

// SpatialQueryByZ runs a Z-order box query.
func (c *Client) SpatialQueryByZ(ctx context.Context, table, indexName string, lo, hi []uint32, limit int) ([]types.Item, error) {
	req := &cefaspb.SpatialQueryRequest{
		Table:     table,
		IndexName: indexName,
		Limit:     int32(limit),
		Shape:     &cefaspb.SpatialQueryRequest_Z{Z: &cefaspb.ZBBox{Lo: lo, Hi: hi}},
	}
	return c.runSpatial(ctx, req)
}

func (c *Client) runSpatial(ctx context.Context, req *cefaspb.SpatialQueryRequest) ([]types.Item, error) {
	stream, err := c.stub.SpatialQuery(c.withAuth(ctx), req)
	if err != nil {
		return nil, err
	}
	var out []types.Item
	for {
		item, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, itemFromPB(item.GetAttributes()))
	}
}

// ---------- sql ----------

// SqlResult mirrors the executor's Result for SDK consumers.
type SqlResult struct {
	AffectedRows int
	Rows         []types.Item
}

// Sql runs a single SQL statement against the server. The exact subset
// supported is documented in pkg/sql/lexer.go.
func (c *Client) Sql(ctx context.Context, query string) (*SqlResult, error) {
	resp, err := c.stub.Sql(c.withAuth(ctx), &cefaspb.SqlRequest{Query: query})
	if err != nil {
		return nil, err
	}
	out := &SqlResult{AffectedRows: int(resp.GetAffectedRows())}
	for _, row := range resp.GetRows() {
		out.Rows = append(out.Rows, itemFromPB(row.GetAttributes()))
	}
	return out, nil
}
