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
	"time"

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

// TTLState mirrors the wire response of DescribeTimeToLive in a
// caller-friendly shape. Enabled is true iff Status == "ENABLED".
type TTLState struct {
	Enabled       bool
	AttributeName string
}

// UpdateTimeToLive enables or disables TTL on `table`. When enabling,
// `attribute` names the numeric epoch-seconds column the reaper uses.
// When disabling, `attribute` is ignored.
func (c *Client) UpdateTimeToLive(ctx context.Context, table, attribute string, enabled bool) (TTLState, error) {
	resp, err := c.stub.UpdateTimeToLive(c.withAuth(ctx), &cefaspb.UpdateTimeToLiveRequest{
		TableName: table,
		TimeToLiveSpecification: &cefaspb.TimeToLiveSpecification{
			Enabled:       enabled,
			AttributeName: attribute,
		},
	})
	if err != nil {
		return TTLState{}, err
	}
	spec := resp.GetTimeToLiveSpecification()
	return TTLState{Enabled: spec.GetEnabled(), AttributeName: spec.GetAttributeName()}, nil
}

// DescribeTimeToLive returns the current TTL configuration for `table`.
func (c *Client) DescribeTimeToLive(ctx context.Context, table string) (TTLState, error) {
	resp, err := c.stub.DescribeTimeToLive(c.withAuth(ctx), &cefaspb.DescribeTimeToLiveRequest{TableName: table})
	if err != nil {
		return TTLState{}, err
	}
	return TTLState{
		Enabled:       resp.GetStatus() == "ENABLED",
		AttributeName: resp.GetAttributeName(),
	}, nil
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

// UpdateOptions carries the aws-shaped UpdateItem accessories.
type UpdateOptions struct {
	UpdateExpression          string
	ConditionExpression       string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
	// ReturnValues: "" | "NONE" | "ALL_NEW" | "ALL_OLD" | "UPDATED_NEW" | "UPDATED_OLD".
	ReturnValues string
}

// UpdateItem applies the supplied UpdateExpression against the row
// keyed by `key`. Returns the requested image (NEW / OLD) when
// ReturnValues asks for one, nil otherwise.
func (c *Client) UpdateItem(ctx context.Context, table string, key types.Item, opts UpdateOptions) (types.Item, error) {
	rv := cefaspb.ReturnValues_RETURN_VALUES_NONE
	switch opts.ReturnValues {
	case "ALL_NEW":
		rv = cefaspb.ReturnValues_RETURN_VALUES_ALL_NEW
	case "ALL_OLD":
		rv = cefaspb.ReturnValues_RETURN_VALUES_ALL_OLD
	case "UPDATED_NEW":
		rv = cefaspb.ReturnValues_RETURN_VALUES_UPDATED_NEW
	case "UPDATED_OLD":
		rv = cefaspb.ReturnValues_RETURN_VALUES_UPDATED_OLD
	}
	resp, err := c.stub.UpdateItem(c.withAuth(ctx), &cefaspb.UpdateItemRequest{
		Table:                     table,
		Key:                       itemAttrMap(key),
		UpdateExpression:          opts.UpdateExpression,
		ConditionExpression:       opts.ConditionExpression,
		ExpressionAttributeNames:  opts.ExpressionAttributeNames,
		ExpressionAttributeValues: itemAttrMap(types.Item(opts.ExpressionAttributeValues)),
		ReturnValues:              rv,
	})
	if err != nil {
		return nil, err
	}
	if len(resp.GetAttributes()) == 0 {
		return nil, nil
	}
	return itemFromPB(resp.GetAttributes()), nil
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

// ---------- transactions ----------

// TransactKind selects the wire op for a TransactWriteOp.
type TransactKind uint8

const (
	TransactPut TransactKind = iota + 1
	TransactDelete
	TransactConditionCheck
)

// TransactWriteOp mirrors one entry in the AWS TransactWriteItems
// shape. Exactly one of Item / Key is set depending on Kind.
type TransactWriteOp struct {
	Kind                TransactKind
	Table               string
	Item                types.Item // Put
	Key                 types.Item // Delete / ConditionCheck
	ConditionExpression string
	Binds               map[string]types.AttributeValue
}

// TransactWriteItems applies up to 100 ops atomically. v1 requires
// every op to reference the same table — cross-table / cross-shard
// transactions are out of scope (issue #80 tracks the 2PC follow-up).
func (c *Client) TransactWriteItems(ctx context.Context, ops []TransactWriteOp) error {
	wire := make([]*cefaspb.TransactWriteOp, 0, len(ops))
	for _, op := range ops {
		w := &cefaspb.TransactWriteOp{
			ConditionExpression: op.ConditionExpression,
			Binds:               itemAttrMap(types.Item(op.Binds)),
		}
		switch op.Kind {
		case TransactPut:
			w.Op = &cefaspb.TransactWriteOp_Put_{Put: &cefaspb.TransactWriteOp_Put{
				Table: op.Table, Item: itemAttrMap(op.Item),
			}}
		case TransactDelete:
			w.Op = &cefaspb.TransactWriteOp_Delete_{Delete: &cefaspb.TransactWriteOp_Delete{
				Table: op.Table, Key: itemAttrMap(op.Key),
			}}
		case TransactConditionCheck:
			w.Op = &cefaspb.TransactWriteOp_ConditionCheck_{ConditionCheck: &cefaspb.TransactWriteOp_ConditionCheck{
				Table: op.Table, Key: itemAttrMap(op.Key),
			}}
		default:
			return fmt.Errorf("transact op: missing kind")
		}
		wire = append(wire, w)
	}
	_, err := c.stub.TransactWriteItems(c.withAuth(ctx), &cefaspb.TransactWriteItemsRequest{Ops: wire})
	return err
}

// TransactGet is one entry in TransactGetItems.
type TransactGet struct {
	Table string
	Key   types.Item
}

// TransactGetItems returns each requested item; index alignment with
// the request is preserved (nil for items that didn't exist). v1
// single-table; cross-table is rejected server-side.
func (c *Client) TransactGetItems(ctx context.Context, items []TransactGet) ([]types.Item, error) {
	wire := make([]*cefaspb.TransactGet, 0, len(items))
	for _, it := range items {
		wire = append(wire, &cefaspb.TransactGet{Table: it.Table, Key: itemAttrMap(it.Key)})
	}
	resp, err := c.stub.TransactGetItems(c.withAuth(ctx), &cefaspb.TransactGetItemsRequest{Items: wire})
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

// ---------- backups ----------

// BackupDescriptor mirrors the wire shape — name + creation time +
// table list + on-disk path.
type BackupDescriptor struct {
	Name         string
	CreatedAt    int64
	Tables       []string
	CheckpointAt string
}

// CreateBackup snapshots the live keyspace into a pebble checkpoint
// and records metadata under cefas/admin/backups/<name>. Pass nil for
// tables to back up every table the catalog currently knows.
func (c *Client) CreateBackup(ctx context.Context, name string, tables []string) (BackupDescriptor, error) {
	resp, err := c.stub.CreateBackup(c.withAuth(ctx), &cefaspb.CreateBackupRequest{Name: name, Tables: tables})
	if err != nil {
		return BackupDescriptor{}, err
	}
	return backupFromPB(resp.GetBackup()), nil
}

// ListBackups returns every admin-named backup the server knows about.
func (c *Client) ListBackups(ctx context.Context) ([]BackupDescriptor, error) {
	resp, err := c.stub.ListBackups(c.withAuth(ctx), &cefaspb.ListBackupsRequest{})
	if err != nil {
		return nil, err
	}
	out := make([]BackupDescriptor, 0, len(resp.GetBackups()))
	for _, b := range resp.GetBackups() {
		out = append(out, backupFromPB(b))
	}
	return out, nil
}

func backupFromPB(b *cefaspb.BackupDescriptor) BackupDescriptor {
	if b == nil {
		return BackupDescriptor{}
	}
	return BackupDescriptor{
		Name:         b.GetName(),
		CreatedAt:    b.GetCreatedAtUnix(),
		Tables:       b.GetTables(),
		CheckpointAt: b.GetCheckpointPath(),
	}
}

// RestoreTableFromBackup reads `sourceTable`'s descriptor from
// `backupName` and reproduces it under `targetTable` in the live
// catalog, then copies every row from the checkpoint into the new
// table. Returns the number of rows copied.
func (c *Client) RestoreTableFromBackup(ctx context.Context, backupName, sourceTable, targetTable string) (int, error) {
	resp, err := c.stub.RestoreTableFromBackup(c.withAuth(ctx), &cefaspb.RestoreTableFromBackupRequest{
		BackupName:      backupName,
		SourceTableName: sourceTable,
		TargetTableName: targetTable,
	})
	if err != nil {
		return 0, err
	}
	return int(resp.GetRowsCopied()), nil
}

type CompactionResult struct {
	Table            string
	Lower            []byte
	Upper            []byte
	StartedAtUnixNS  int64
	FinishedAtUnixNS int64
	ElapsedSeconds   float64
	Parallelized     bool
	BeforeL0Files    int64
	AfterL0Files     int64
	BeforeDebtBytes  uint64
	AfterDebtBytes   uint64
}

func (c *Client) CompactTable(ctx context.Context, table string, parallelize bool) ([]CompactionResult, error) {
	resp, err := c.stub.Compact(c.withAuth(ctx), &cefaspb.CompactRequest{Table: table, Parallelize: parallelize})
	if err != nil {
		return nil, err
	}
	out := make([]CompactionResult, 0, len(resp.GetResults()))
	for _, r := range resp.GetResults() {
		out = append(out, CompactionResult{
			Table:            r.GetTable(),
			Lower:            append([]byte(nil), r.GetLower()...),
			Upper:            append([]byte(nil), r.GetUpper()...),
			StartedAtUnixNS:  r.GetStartedAtUnixNs(),
			FinishedAtUnixNS: r.GetFinishedAtUnixNs(),
			ElapsedSeconds:   r.GetElapsedSeconds(),
			Parallelized:     r.GetParallelized(),
			BeforeL0Files:    r.GetBeforeL0Files(),
			AfterL0Files:     r.GetAfterL0Files(),
			BeforeDebtBytes:  r.GetBeforeDebtBytes(),
			AfterDebtBytes:   r.GetAfterDebtBytes(),
		})
	}
	return out, nil
}

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

// ---------- cluster ----------

// ClusterStatus returns membership and leadership info.
type ClusterStatus struct {
	Mode              string
	IsLeader          bool
	SelfID            string
	BindAddr          string
	LeaderHTTP        string
	RoutingEpoch      uint64
	PlacementVersion  uint64
	ShardCount        int
	PlacementStrategy string
	Shards            []ShardPlacement
	Nodes             []NodeDescriptor
}

type TokenRange struct {
	Start uint64
	End   uint64
}

type ShardPlacement struct {
	ID         uint32
	Ranges     []TokenRange
	State      string
	Epoch      uint64
	Voters     []string
	NonVoters  []string
	LeaderHint string
}

type NodeCapacity struct {
	Weight      int
	CPU         int
	MemoryBytes uint64
	DiskBytes   uint64
	Zone        string
	Tags        []string
}

type NodeDescriptor struct {
	ID           string
	RaftAddr     string
	HTTPAddr     string
	State        string
	Capacity     NodeCapacity
	LastSeenUnix int64
}

type MembershipOptions struct {
	ShardID   *uint32
	AllShards bool
}

type PlacementPlanRequest struct {
	Operation    string
	ShardID      uint32
	SplitToken   *uint64
	NewShardID   *uint32
	SourceNode   string
	TargetNode   string
	TargetNodes  []string
	TargetVoters []string
	NodeID       string
	MinVoters    int
}

type PlacementCatalog struct {
	Version       uint64
	Epoch         uint64
	Strategy      string
	Shards        []ShardPlacement
	Nodes         []NodeDescriptor
	UpdatedAtUnix int64
}

type PlacementPlanStep struct {
	Action  string
	ShardID *uint32
	NodeID  string
	Addr    string
	Detail  string
}

type PlacementPlan struct {
	Operation        string
	BeforeEpoch      uint64
	AfterEpoch       uint64
	Before           PlacementCatalog
	After            PlacementCatalog
	Steps            []PlacementPlanStep
	Warnings         []string
	RequiresDataCopy bool
	RequiresRestart  bool
	ApplySupported   bool
}

type PlacementApplyRequest struct {
	Plan          PlacementPlan
	ExpectedEpoch uint64
	TimeoutMS     int
}

type PlacementApplyStep struct {
	Action  string
	ShardID *uint32
	NodeID  string
	Status  string
	Detail  string
}

type PlacementApplyResult struct {
	Operation   string
	BeforeEpoch uint64
	AfterEpoch  uint64
	Steps       []PlacementApplyStep
	Placement   PlacementCatalog
}

type SplitFinalizeRequest struct {
	ParentShardID  uint32
	ChildShardID   uint32
	ExpectedEpoch  uint64
	TimeoutMS      int
	WritesQuiesced bool
}

type SplitFinalizeResult struct {
	ParentShardID     uint32
	ChildShardID      uint32
	BeforeEpoch       uint64
	AfterEpoch        uint64
	ParentRangeBefore TokenRange
	ParentRangeAfter  TokenRange
	ChildRange        TokenRange
	CopiedKeys        int64
	CopiedCatalogKeys int64
	DeletedKeys       int64
	Placement         PlacementCatalog
}

// Status fetches the cluster status. Works without a token (public).
func (c *Client) Status(ctx context.Context) (ClusterStatus, error) {
	resp, err := c.stub.ClusterStatus(c.withAuth(ctx), &cefaspb.ClusterStatusRequest{})
	if err != nil {
		return ClusterStatus{}, err
	}
	return ClusterStatus{
		Mode:              resp.GetMode(),
		IsLeader:          resp.GetIsLeader(),
		SelfID:            resp.GetSelfId(),
		BindAddr:          resp.GetBindAddr(),
		LeaderHTTP:        resp.GetLeaderHttp(),
		RoutingEpoch:      resp.GetRoutingEpoch(),
		PlacementVersion:  resp.GetPlacementVersion(),
		ShardCount:        int(resp.GetShardCount()),
		PlacementStrategy: resp.GetPlacementStrategy(),
		Shards:            shardPlacementsFromPB(resp.GetShards()),
		Nodes:             nodeDescriptorsFromPB(resp.GetNodes()),
	}, nil
}

func shardPlacementsFromPB(in []*cefaspb.ShardPlacement) []ShardPlacement {
	out := make([]ShardPlacement, 0, len(in))
	for _, sh := range in {
		out = append(out, ShardPlacement{
			ID:         sh.GetId(),
			Ranges:     tokenRangesFromPB(sh.GetRanges()),
			State:      sh.GetState(),
			Epoch:      sh.GetEpoch(),
			Voters:     append([]string(nil), sh.GetVoters()...),
			NonVoters:  append([]string(nil), sh.GetNonVoters()...),
			LeaderHint: sh.GetLeaderHint(),
		})
	}
	return out
}

func tokenRangesFromPB(in []*cefaspb.TokenRange) []TokenRange {
	out := make([]TokenRange, 0, len(in))
	for _, r := range in {
		out = append(out, TokenRange{Start: r.GetStart(), End: r.GetEnd()})
	}
	return out
}

func nodeDescriptorsFromPB(in []*cefaspb.NodeDescriptor) []NodeDescriptor {
	out := make([]NodeDescriptor, 0, len(in))
	for _, node := range in {
		capacity := NodeCapacity{}
		if c := node.GetCapacity(); c != nil {
			capacity = NodeCapacity{
				Weight:      int(c.GetWeight()),
				CPU:         int(c.GetCpu()),
				MemoryBytes: c.GetMemoryBytes(),
				DiskBytes:   c.GetDiskBytes(),
				Zone:        c.GetZone(),
				Tags:        append([]string(nil), c.GetTags()...),
			}
		}
		out = append(out, NodeDescriptor{
			ID:           node.GetId(),
			RaftAddr:     node.GetRaftAddr(),
			HTTPAddr:     node.GetHttpAddr(),
			State:        node.GetState(),
			Capacity:     capacity,
			LastSeenUnix: node.GetLastSeenUnix(),
		})
	}
	return out
}

// AddVoter asks the leader to add `id` at `addr` to the cluster.
// Requires cefas:cluster:admin scope.
func (c *Client) AddVoter(ctx context.Context, id, addr string) error {
	return c.AddVoterWithOptions(ctx, id, addr, MembershipOptions{})
}

func (c *Client) AddVoterWithOptions(ctx context.Context, id, addr string, opts MembershipOptions) error {
	req := &cefaspb.AddVoterRequest{Id: id, Addr: addr, AllShards: opts.AllShards}
	if opts.ShardID != nil {
		req.ShardId = opts.ShardID
	}
	_, err := c.stub.AddVoter(c.withAuth(ctx), req)
	return err
}

// RemoveServer evicts a peer from the cluster. Requires
// cefas:cluster:admin scope.
func (c *Client) RemoveServer(ctx context.Context, id string) error {
	return c.RemoveServerWithOptions(ctx, id, MembershipOptions{})
}

func (c *Client) RemoveServerWithOptions(ctx context.Context, id string, opts MembershipOptions) error {
	req := &cefaspb.RemoveServerRequest{Id: id, AllShards: opts.AllShards}
	if opts.ShardID != nil {
		req.ShardId = opts.ShardID
	}
	_, err := c.stub.RemoveServer(c.withAuth(ctx), req)
	return err
}

func (c *Client) PlanPlacement(ctx context.Context, req PlacementPlanRequest) (PlacementPlan, error) {
	pbReq := &cefaspb.PlanPlacementRequest{
		Operation:    req.Operation,
		ShardId:      req.ShardID,
		SourceNode:   req.SourceNode,
		TargetNode:   req.TargetNode,
		TargetNodes:  append([]string(nil), req.TargetNodes...),
		TargetVoters: append([]string(nil), req.TargetVoters...),
		NodeId:       req.NodeID,
		MinVoters:    int32(req.MinVoters),
	}
	if req.SplitToken != nil {
		pbReq.SplitToken = req.SplitToken
	}
	if req.NewShardID != nil {
		pbReq.NewShardId = req.NewShardID
	}
	resp, err := c.stub.PlanPlacement(c.withAuth(ctx), pbReq)
	if err != nil {
		return PlacementPlan{}, err
	}
	return placementPlanFromPB(resp.GetPlan()), nil
}

func (c *Client) ApplyPlacement(ctx context.Context, req PlacementApplyRequest) (PlacementApplyResult, error) {
	resp, err := c.stub.ApplyPlacement(c.withAuth(ctx), &cefaspb.ApplyPlacementRequest{
		Plan:          placementPlanToPB(req.Plan),
		ExpectedEpoch: req.ExpectedEpoch,
		TimeoutMs:     int32(req.TimeoutMS),
	})
	if err != nil {
		return PlacementApplyResult{}, err
	}
	return placementApplyResultFromPB(resp.GetResult()), nil
}

func (c *Client) FinalizeSplit(ctx context.Context, req SplitFinalizeRequest) (SplitFinalizeResult, error) {
	resp, err := c.stub.FinalizeSplit(c.withAuth(ctx), &cefaspb.FinalizeSplitRequest{
		ParentShardId:  req.ParentShardID,
		ChildShardId:   req.ChildShardID,
		ExpectedEpoch:  req.ExpectedEpoch,
		TimeoutMs:      int32(req.TimeoutMS),
		WritesQuiesced: req.WritesQuiesced,
	})
	if err != nil {
		return SplitFinalizeResult{}, err
	}
	return splitFinalizeResultFromPB(resp.GetResult()), nil
}

func placementPlanToPB(in PlacementPlan) *cefaspb.PlacementPlan {
	return &cefaspb.PlacementPlan{
		Operation:        in.Operation,
		BeforeEpoch:      in.BeforeEpoch,
		AfterEpoch:       in.AfterEpoch,
		Before:           placementCatalogToPB(in.Before),
		After:            placementCatalogToPB(in.After),
		Steps:            placementPlanStepsToPB(in.Steps),
		Warnings:         append([]string(nil), in.Warnings...),
		RequiresDataCopy: in.RequiresDataCopy,
		RequiresRestart:  in.RequiresRestart,
		ApplySupported:   in.ApplySupported,
	}
}

func placementCatalogToPB(in PlacementCatalog) *cefaspb.PlacementCatalog {
	return &cefaspb.PlacementCatalog{
		Version:       in.Version,
		Epoch:         in.Epoch,
		Strategy:      in.Strategy,
		Shards:        shardPlacementsToPB(in.Shards),
		Nodes:         nodeDescriptorsToPB(in.Nodes),
		UpdatedAtUnix: in.UpdatedAtUnix,
	}
}

func shardPlacementsToPB(in []ShardPlacement) []*cefaspb.ShardPlacement {
	out := make([]*cefaspb.ShardPlacement, 0, len(in))
	for _, sh := range in {
		out = append(out, &cefaspb.ShardPlacement{
			Id:         sh.ID,
			Ranges:     tokenRangesToPB(sh.Ranges),
			State:      sh.State,
			Epoch:      sh.Epoch,
			Voters:     append([]string(nil), sh.Voters...),
			NonVoters:  append([]string(nil), sh.NonVoters...),
			LeaderHint: sh.LeaderHint,
		})
	}
	return out
}

func tokenRangesToPB(in []TokenRange) []*cefaspb.TokenRange {
	out := make([]*cefaspb.TokenRange, 0, len(in))
	for _, r := range in {
		out = append(out, tokenRangeToPB(r))
	}
	return out
}

func tokenRangeToPB(r TokenRange) *cefaspb.TokenRange {
	return &cefaspb.TokenRange{Start: r.Start, End: r.End}
}

func nodeDescriptorsToPB(in []NodeDescriptor) []*cefaspb.NodeDescriptor {
	out := make([]*cefaspb.NodeDescriptor, 0, len(in))
	for _, node := range in {
		out = append(out, &cefaspb.NodeDescriptor{
			Id:       node.ID,
			RaftAddr: node.RaftAddr,
			HttpAddr: node.HTTPAddr,
			State:    node.State,
			Capacity: &cefaspb.NodeCapacity{
				Weight:      int32(node.Capacity.Weight),
				Cpu:         int32(node.Capacity.CPU),
				MemoryBytes: node.Capacity.MemoryBytes,
				DiskBytes:   node.Capacity.DiskBytes,
				Zone:        node.Capacity.Zone,
				Tags:        append([]string(nil), node.Capacity.Tags...),
			},
			LastSeenUnix: node.LastSeenUnix,
		})
	}
	return out
}

func placementPlanStepsToPB(in []PlacementPlanStep) []*cefaspb.PlacementPlanStep {
	out := make([]*cefaspb.PlacementPlanStep, 0, len(in))
	for _, step := range in {
		out = append(out, &cefaspb.PlacementPlanStep{
			Action:  step.Action,
			ShardId: step.ShardID,
			NodeId:  step.NodeID,
			Addr:    step.Addr,
			Detail:  step.Detail,
		})
	}
	return out
}

func placementPlanFromPB(in *cefaspb.PlacementPlan) PlacementPlan {
	if in == nil {
		return PlacementPlan{}
	}
	return PlacementPlan{
		Operation:        in.GetOperation(),
		BeforeEpoch:      in.GetBeforeEpoch(),
		AfterEpoch:       in.GetAfterEpoch(),
		Before:           placementCatalogFromPB(in.GetBefore()),
		After:            placementCatalogFromPB(in.GetAfter()),
		Steps:            placementPlanStepsFromPB(in.GetSteps()),
		Warnings:         append([]string(nil), in.GetWarnings()...),
		RequiresDataCopy: in.GetRequiresDataCopy(),
		RequiresRestart:  in.GetRequiresRestart(),
		ApplySupported:   in.GetApplySupported(),
	}
}

func placementCatalogFromPB(in *cefaspb.PlacementCatalog) PlacementCatalog {
	if in == nil {
		return PlacementCatalog{}
	}
	return PlacementCatalog{
		Version:       in.GetVersion(),
		Epoch:         in.GetEpoch(),
		Strategy:      in.GetStrategy(),
		Shards:        shardPlacementsFromPB(in.GetShards()),
		Nodes:         nodeDescriptorsFromPB(in.GetNodes()),
		UpdatedAtUnix: in.GetUpdatedAtUnix(),
	}
}

func placementPlanStepsFromPB(in []*cefaspb.PlacementPlanStep) []PlacementPlanStep {
	out := make([]PlacementPlanStep, 0, len(in))
	for _, step := range in {
		var shardID *uint32
		if step.ShardId != nil {
			id := step.GetShardId()
			shardID = &id
		}
		out = append(out, PlacementPlanStep{
			Action:  step.GetAction(),
			ShardID: shardID,
			NodeID:  step.GetNodeId(),
			Addr:    step.GetAddr(),
			Detail:  step.GetDetail(),
		})
	}
	return out
}

func placementApplyResultFromPB(in *cefaspb.PlacementApplyResult) PlacementApplyResult {
	if in == nil {
		return PlacementApplyResult{}
	}
	return PlacementApplyResult{
		Operation:   in.GetOperation(),
		BeforeEpoch: in.GetBeforeEpoch(),
		AfterEpoch:  in.GetAfterEpoch(),
		Steps:       placementApplyStepsFromPB(in.GetSteps()),
		Placement:   placementCatalogFromPB(in.GetPlacement()),
	}
}

func splitFinalizeResultFromPB(in *cefaspb.FinalizeSplitResult) SplitFinalizeResult {
	if in == nil {
		return SplitFinalizeResult{}
	}
	return SplitFinalizeResult{
		ParentShardID:     in.GetParentShardId(),
		ChildShardID:      in.GetChildShardId(),
		BeforeEpoch:       in.GetBeforeEpoch(),
		AfterEpoch:        in.GetAfterEpoch(),
		ParentRangeBefore: tokenRangeFromPB(in.GetParentRangeBefore()),
		ParentRangeAfter:  tokenRangeFromPB(in.GetParentRangeAfter()),
		ChildRange:        tokenRangeFromPB(in.GetChildRange()),
		CopiedKeys:        in.GetCopiedKeys(),
		CopiedCatalogKeys: in.GetCopiedCatalogKeys(),
		DeletedKeys:       in.GetDeletedKeys(),
		Placement:         placementCatalogFromPB(in.GetPlacement()),
	}
}

func tokenRangeFromPB(in *cefaspb.TokenRange) TokenRange {
	if in == nil {
		return TokenRange{}
	}
	return TokenRange{Start: in.GetStart(), End: in.GetEnd()}
}

func placementApplyStepsFromPB(in []*cefaspb.PlacementApplyStep) []PlacementApplyStep {
	out := make([]PlacementApplyStep, 0, len(in))
	for _, step := range in {
		var shardID *uint32
		if step.ShardId != nil {
			id := step.GetShardId()
			shardID = &id
		}
		out = append(out, PlacementApplyStep{
			Action:  step.GetAction(),
			ShardID: shardID,
			NodeID:  step.GetNodeId(),
			Status:  step.GetStatus(),
			Detail:  step.GetDetail(),
		})
	}
	return out
}
