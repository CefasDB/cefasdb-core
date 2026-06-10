// Package client is the typed Go SDK for cefas. It wraps the
// generated gRPC stubs with helpers that consume the public
// pkg/types.Item / AttributeValue model so application code never
// touches generated protobuf structs directly.
//
// Usage:
//
//	c, err := client.Dial(ctx, "localhost:9090", client.WithBearer("..."))
//	defer c.Close()
//
//	err = c.PutItem(ctx, "events", types.Item{
//	    "user_id": types.AttributeValue{T: types.AttrS, S: "alice"},
//	    "ts":      types.AttributeValue{T: types.AttrN, N: "100"},
//	})
//
//	items, err := c.Query(ctx, "events").
//	    PK(types.AttributeValue{T: types.AttrS, S: "alice"}).
//	    Limit(50).
//	    Run(ctx)
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// Client is a typed cefas gRPC client. Safe for concurrent use.
type Client struct {
	conn   *grpc.ClientConn
	stub   cefaspb.CefasClient
	bearer string
}

// Option configures a Client at Dial time.
type Option func(*config)

type config struct {
	bearer    string
	tls       *tls.Config
	plaintext bool
	dialOpts  []grpc.DialOption
}

// WithBearer adds an "Authorization: Bearer <token>" metadata header
// to every RPC.
func WithBearer(token string) Option {
	return func(c *config) { c.bearer = token }
}

// WithTLS enables transport encryption using the supplied tls.Config.
// Pass &tls.Config{} for the system roots, or build a custom config
// for mTLS.
func WithTLS(cfg *tls.Config) Option { return func(c *config) { c.tls = cfg } }

// WithPlaintext disables transport security. Required for local dev
// against a -grpc-reflection enabled server with no TLS cert.
func WithPlaintext() Option { return func(c *config) { c.plaintext = true } }

// WithMTLSFiles wires mTLS from filesystem paths: client cert + key
// the server verifies, plus the CA bundle that signed the server's
// certificate.
func WithMTLSFiles(certPath, keyPath, serverCAPath string) Option {
	return func(c *config) {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			c.dialOpts = append(c.dialOpts, grpc.WithDisableHealthCheck()) // no-op marker
			c.tls = &tls.Config{InsecureSkipVerify: false}
			fmt.Fprintf(os.Stderr, "cefas/client: load client cert: %v\n", err)
			return
		}
		pool := x509.NewCertPool()
		if pem, err := os.ReadFile(serverCAPath); err == nil {
			pool.AppendCertsFromPEM(pem)
		}
		c.tls = &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
		}
	}
}

// WithDialOption appends a raw grpc.DialOption (escape hatch for
// keepalive, retry policies, etc.).
func WithDialOption(o grpc.DialOption) Option {
	return func(c *config) { c.dialOpts = append(c.dialOpts, o) }
}

// Dial opens a connection to a cefas server.
func Dial(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	cfg := &config{}
	for _, o := range opts {
		o(cfg)
	}
	dialOpts := append([]grpc.DialOption{}, cfg.dialOpts...)
	switch {
	case cfg.tls != nil:
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(cfg.tls)))
	case cfg.plaintext:
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	default:
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}

	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("cefas: dial %s: %w", addr, err)
	}
	return &Client{conn: conn, stub: cefaspb.NewCefasClient(conn), bearer: cfg.bearer}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error { return c.conn.Close() }

// withAuth augments outgoing metadata with the bearer token (when set).
func (c *Client) withAuth(ctx context.Context) context.Context {
	if c.bearer == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.bearer)
}

// ---------- schema ----------

// CreateTable persists a new table descriptor on the leader.
func (c *Client) CreateTable(ctx context.Context, td types.TableDescriptor) error {
	_, err := c.stub.CreateTable(c.withAuth(ctx), &cefaspb.CreateTableRequest{Descriptor_: tdToPB(td)})
	return err
}

// DescribeTable returns the descriptor for `name`.
func (c *Client) DescribeTable(ctx context.Context, name string) (types.TableDescriptor, error) {
	resp, err := c.stub.DescribeTable(c.withAuth(ctx), &cefaspb.DescribeTableRequest{Name: name})
	if err != nil {
		return types.TableDescriptor{}, err
	}
	return tdFromPB(resp.GetDescriptor_()), nil
}

// ListTables returns every table the server knows about.
func (c *Client) ListTables(ctx context.Context) ([]types.TableDescriptor, error) {
	resp, err := c.stub.ListTables(c.withAuth(ctx), &cefaspb.ListTablesRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]types.TableDescriptor, 0, len(resp.GetTables()))
	for _, t := range resp.GetTables() {
		out = append(out, tdFromPB(t))
	}
	return out, nil
}

// DropTable removes a table descriptor.
func (c *Client) DropTable(ctx context.Context, name string) error {
	_, err := c.stub.DropTable(c.withAuth(ctx), &cefaspb.DropTableRequest{Name: name})
	return err
}

// ---------- item ops ----------

// PutOptions exposes the same surface as storage.PutOptions.
type PutOptions struct {
	Condition string
	Binds     map[string]types.AttributeValue
}

// PutItem upserts `item` into `table`. Returns wrapped gRPC errors;
// use errors.Is against the package sentinels (ErrConditionFailed,
// ErrNotLeader) to branch.
func (c *Client) PutItem(ctx context.Context, table string, item types.Item, opts ...PutOptions) error {
	var o PutOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := c.stub.PutItem(c.withAuth(ctx), &cefaspb.PutItemRequest{
		Table:     table,
		Item:      itemAttrMap(item),
		Condition: o.Condition,
		Binds:     itemAttrMap(types.Item(o.Binds)),
	})
	return err
}

// GetOptions toggles consistency.
type GetOptions struct {
	Strong bool
}

// GetItem returns the item, or (nil, nil) when the key is absent.
func (c *Client) GetItem(ctx context.Context, table string, key types.Item, opts ...GetOptions) (types.Item, error) {
	var o GetOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	cons := cefaspb.Consistency_CONSISTENCY_EVENTUAL
	if o.Strong {
		cons = cefaspb.Consistency_CONSISTENCY_STRONG
	}
	resp, err := c.stub.GetItem(c.withAuth(ctx), &cefaspb.GetItemRequest{
		Table:       table,
		Key:         itemAttrMap(key),
		Consistency: cons,
	})
	if err != nil {
		return nil, err
	}
	if !resp.GetFound() {
		return nil, nil
	}
	return itemFromPB(resp.GetItem()), nil
}

// DeleteOptions mirrors PutOptions for deletes.
type DeleteOptions struct {
	Condition string
	Binds     map[string]types.AttributeValue
}

// DeleteItem removes the item identified by `key`.
func (c *Client) DeleteItem(ctx context.Context, table string, key types.Item, opts ...DeleteOptions) error {
	var o DeleteOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := c.stub.DeleteItem(c.withAuth(ctx), &cefaspb.DeleteItemRequest{
		Table:     table,
		Key:       itemAttrMap(key),
		Condition: o.Condition,
		Binds:     itemAttrMap(types.Item(o.Binds)),
	})
	return err
}

// ---------- batch ----------

// BatchWriteOp is the SDK-facing batch op type. Exactly one of Item /
// Key is populated.
type BatchWriteOp struct {
	Put    types.Item
	Delete types.Item
}

// BatchWriteItem applies N puts/deletes atomically.
func (c *Client) BatchWriteItem(ctx context.Context, table string, ops []BatchWriteOp) error {
	pbOps := make([]*cefaspb.BatchWriteOp, 0, len(ops))
	for i, o := range ops {
		switch {
		case o.Put != nil:
			pbOps = append(pbOps, &cefaspb.BatchWriteOp{Kind: cefaspb.BatchWriteOp_KIND_PUT, Item: itemAttrMap(o.Put)})
		case o.Delete != nil:
			pbOps = append(pbOps, &cefaspb.BatchWriteOp{Kind: cefaspb.BatchWriteOp_KIND_DELETE, Key: itemAttrMap(o.Delete)})
		default:
			return fmt.Errorf("op %d: neither Put nor Delete set", i)
		}
	}
	_, err := c.stub.BatchWriteItem(c.withAuth(ctx), &cefaspb.BatchWriteItemRequest{Table: table, Ops: pbOps})
	return err
}

// BatchGetItem fetches multiple items by primary key.
func (c *Client) BatchGetItem(ctx context.Context, table string, keys []types.Item) ([]types.Item, error) {
	pbKeys := make([]*cefaspb.KeyMap, 0, len(keys))
	for _, k := range keys {
		pbKeys = append(pbKeys, &cefaspb.KeyMap{Attributes: itemAttrMap(k)})
	}
	resp, err := c.stub.BatchGetItem(c.withAuth(ctx), &cefaspb.BatchGetItemRequest{Table: table, Keys: pbKeys})
	if err != nil {
		return nil, err
	}
	out := make([]types.Item, len(resp.GetItems()))
	for i, it := range resp.GetItems() {
		if len(it.GetAttributes()) == 0 {
			out[i] = nil
			continue
		}
		out[i] = itemFromPB(it.GetAttributes())
	}
	return out, nil
}

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

// ---------- cluster ----------

// ClusterStatus returns membership and leadership info.
type ClusterStatus struct {
	Mode       string
	IsLeader   bool
	SelfID     string
	BindAddr   string
	LeaderHTTP string
}

// Status fetches the cluster status. Works without a token (public).
func (c *Client) Status(ctx context.Context) (ClusterStatus, error) {
	resp, err := c.stub.ClusterStatus(c.withAuth(ctx), &cefaspb.ClusterStatusRequest{})
	if err != nil {
		return ClusterStatus{}, err
	}
	return ClusterStatus{
		Mode:       resp.GetMode(),
		IsLeader:   resp.GetIsLeader(),
		SelfID:     resp.GetSelfId(),
		BindAddr:   resp.GetBindAddr(),
		LeaderHTTP: resp.GetLeaderHttp(),
	}, nil
}

// AddVoter asks the leader to add `id` at `addr` to the cluster.
// Requires cefas:cluster:admin scope.
func (c *Client) AddVoter(ctx context.Context, id, addr string) error {
	_, err := c.stub.AddVoter(c.withAuth(ctx), &cefaspb.AddVoterRequest{Id: id, Addr: addr})
	return err
}

// RemoveServer evicts a peer from the cluster. Requires
// cefas:cluster:admin scope.
func (c *Client) RemoveServer(ctx context.Context, id string) error {
	_, err := c.stub.RemoveServer(c.withAuth(ctx), &cefaspb.RemoveServerRequest{Id: id})
	return err
}
