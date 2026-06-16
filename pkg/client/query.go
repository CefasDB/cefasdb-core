package client

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// ---------- query ----------

// QueryBuilder collects Query parameters fluently.
type QueryBuilder struct {
	c     *Client
	table string
	req   *cefaspb.QueryRequest
}

// Query opens a builder for the given table. Call PK / SK / Limit /
// Index / Strong on the builder, then Run to execute.
func (c *Client) Query(ctx context.Context, table string) *QueryBuilder {
	return &QueryBuilder{c: c, table: table, req: &cefaspb.QueryRequest{Table: table}}
}

func (b *QueryBuilder) PK(v types.AttributeValue) *QueryBuilder {
	b.req.PkValue = attrToPB(v)
	return b
}

func (b *QueryBuilder) SKBetween(lo, hi types.AttributeValue) *QueryBuilder {
	if lo.T != types.AttrNull {
		b.req.SkLow = attrToPB(lo)
	}
	if hi.T != types.AttrNull {
		b.req.SkHigh = attrToPB(hi)
	}
	return b
}

func (b *QueryBuilder) Index(name string) *QueryBuilder {
	b.req.IndexName = name
	return b
}

func (b *QueryBuilder) Limit(n int) *QueryBuilder {
	b.req.Limit = int32(n)
	return b
}

func (b *QueryBuilder) Strong() *QueryBuilder {
	b.req.Consistency = cefaspb.Consistency_CONSISTENCY_STRONG
	return b
}

// Run executes the query and collects all results. For large result
// sets prefer Stream.
func (b *QueryBuilder) Run(ctx context.Context) ([]types.Item, error) {
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
	return b.c.stub.Query(b.c.withAuth(ctx), b.req)
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
