package api

import (
	"context"
	"errors"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/core/index"
	"github.com/osvaldoandrade/cefas/pkg/core/model"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
)

func TestPluginIndexMaintenanceCallsPluginForWritePaths(t *testing.T) {
	rec := &recordingMaintenanceIndexPlugin{}
	reg := plugin.NewRegistry()
	if err := reg.Register(rec); err != nil {
		t.Fatalf("register recorder: %v", err)
	}
	srv, cleanup := newPluginIndexMaintenanceServer(t, reg)
	defer cleanup()
	ctx := context.Background()
	createMaintenanceTable(t, ctx, srv, "MaintDocs", false)
	createMaintenanceIndex(t, ctx, srv, "MaintDocs", rec.Manifest().Name)

	if _, err := srv.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "MaintDocs",
		Item: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "a"}},
			"v":  {Value: &cefaspb.AttributeValue_S{S: "one"}},
		},
	}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := srv.UpdateItem(ctx, &cefaspb.UpdateItemRequest{
		Table: "MaintDocs",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "a"}},
		},
		UpdateExpression: "SET v = :v",
		ExpressionAttributeValues: map[string]*cefaspb.AttributeValue{
			":v": {Value: &cefaspb.AttributeValue_S{S: "two"}},
		},
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := srv.DeleteItem(ctx, &cefaspb.DeleteItemRequest{
		Table: "MaintDocs",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "a"}},
		},
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := srv.BatchWriteItem(ctx, &cefaspb.BatchWriteItemRequest{
		Table: "MaintDocs",
		Ops: []*cefaspb.BatchWriteOp{
			{
				Kind: cefaspb.BatchWriteOp_KIND_PUT,
				Item: map[string]*cefaspb.AttributeValue{
					"id": {Value: &cefaspb.AttributeValue_S{S: "b"}},
					"v":  {Value: &cefaspb.AttributeValue_S{S: "batch-b"}},
				},
			},
			{
				Kind: cefaspb.BatchWriteOp_KIND_PUT,
				Item: map[string]*cefaspb.AttributeValue{
					"id": {Value: &cefaspb.AttributeValue_S{S: "c"}},
					"v":  {Value: &cefaspb.AttributeValue_S{S: "batch-c"}},
				},
			},
		},
	}); err != nil {
		t.Fatalf("batch: %v", err)
	}
	if _, err := srv.TransactWriteItems(ctx, &cefaspb.TransactWriteItemsRequest{
		Ops: []*cefaspb.TransactWriteOp{
			{Op: &cefaspb.TransactWriteOp_Put_{Put: &cefaspb.TransactWriteOp_Put{
				Table: "MaintDocs",
				Item: map[string]*cefaspb.AttributeValue{
					"id": {Value: &cefaspb.AttributeValue_S{S: "d"}},
					"v":  {Value: &cefaspb.AttributeValue_S{S: "txn-d"}},
				},
			}}},
			{Op: &cefaspb.TransactWriteOp_Delete_{Delete: &cefaspb.TransactWriteOp_Delete{
				Table: "MaintDocs",
				Key: map[string]*cefaspb.AttributeValue{
					"id": {Value: &cefaspb.AttributeValue_S{S: "b"}},
				},
			}}},
		},
	}); err != nil {
		t.Fatalf("transact: %v", err)
	}
	atomic := NewAtomicServer(srv)
	if _, err := atomic.AtomicUpdate(ctx, &cefaspb.AtomicUpdateRequest{
		Table: "MaintDocs",
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "d"}},
		},
		Actions: []*cefaspb.AtomicAction{{
			Kind:      cefaspb.AtomicActionKind_ATOMIC_SET,
			Attribute: "v",
			Value:     &cefaspb.AttributeValue{Value: &cefaspb.AttributeValue_S{S: "atomic-d"}},
		}},
	}); err != nil {
		t.Fatalf("atomic: %v", err)
	}
	if _, err := srv.Sql(ctx, &cefaspb.SqlRequest{
		Query: "INSERT INTO MaintDocs (id, v) VALUES ('e', 'sql-insert')",
	}); err != nil {
		t.Fatalf("sql insert: %v", err)
	}
	if _, err := srv.Sql(ctx, &cefaspb.SqlRequest{
		Query: "UPDATE MaintDocs SET v = 'sql-update' WHERE id = 'e'",
	}); err != nil {
		t.Fatalf("sql update: %v", err)
	}
	if _, err := srv.Sql(ctx, &cefaspb.SqlRequest{
		Query: "DELETE FROM MaintDocs WHERE id = 'e'",
	}); err != nil {
		t.Fatalf("sql delete: %v", err)
	}

	updates, deletes := rec.snapshot()
	if len(updates) != 8 {
		t.Fatalf("updates = %d, want 8: %+v", len(updates), updates)
	}
	if len(deletes) != 3 {
		t.Fatalf("deletes = %d, want 3: %+v", len(deletes), deletes)
	}
	if updates[1].oldItem["v"].S != "one" || updates[1].newItem["v"].S != "two" {
		t.Fatalf("UpdateItem delta = %+v", updates[1])
	}
	if deletes[0]["v"].S != "two" {
		t.Fatalf("DeleteItem old image = %+v", deletes[0])
	}
	if updates[5].oldItem["v"].S != "txn-d" || updates[5].newItem["v"].S != "atomic-d" {
		t.Fatalf("AtomicUpdate delta = %+v", updates[5])
	}
	if updates[7].oldItem["v"].S != "sql-insert" || updates[7].newItem["v"].S != "sql-update" {
		t.Fatalf("SQL UPDATE delta = %+v", updates[7])
	}
	if deletes[2]["v"].S != "sql-update" {
		t.Fatalf("SQL DELETE old image = %+v", deletes[2])
	}
}

func TestPluginIndexMaintenanceStrictFailureReturnsError(t *testing.T) {
	reg := plugin.NewRegistry()
	if err := reg.Register(&recordingMaintenanceIndexPlugin{updateErr: errors.New("update failed")}); err != nil {
		t.Fatalf("register failing recorder: %v", err)
	}
	srv, cleanup := newPluginIndexMaintenanceServer(t, reg)
	defer cleanup()
	ctx := context.Background()
	createMaintenanceTable(t, ctx, srv, "FailMaintDocs", false)
	createMaintenanceIndex(t, ctx, srv, "FailMaintDocs", "recordmaint")

	_, err := srv.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: "FailMaintDocs",
		Item: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "a"}},
			"v":  {Value: &cefaspb.AttributeValue_S{S: "one"}},
		},
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("status = %v, want Internal; err=%v", status.Code(err), err)
	}
}

func TestANNPluginIndexMaintenanceForGRPCAndBatch(t *testing.T) {
	srv, cleanup := newPluginIndexMaintenanceServer(t, testANNRegistry(t))
	defer cleanup()
	ctx := context.Background()
	table := "MaintANNDocs"
	createMaintenanceTable(t, ctx, srv, table, true)
	createMaintenanceANNIndex(t, ctx, srv, table)

	if _, err := srv.PutItem(ctx, &cefaspb.PutItemRequest{
		Table: table,
		Item: map[string]*cefaspb.AttributeValue{
			"id":  {Value: &cefaspb.AttributeValue_S{S: "a"}},
			"emb": testPBVec(1, 0, 0),
		},
	}); err != nil {
		t.Fatalf("put ann: %v", err)
	}
	assertTopKIDs(t, ctx, srv, table, testPBVec(1, 0, 0), "a")

	if _, err := srv.UpdateItem(ctx, &cefaspb.UpdateItemRequest{
		Table: table,
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "a"}},
		},
		UpdateExpression: "SET emb = :emb",
		ExpressionAttributeValues: map[string]*cefaspb.AttributeValue{
			":emb": testPBVec(0, 1, 0),
		},
	}); err != nil {
		t.Fatalf("update ann: %v", err)
	}
	assertTopKIDs(t, ctx, srv, table, testPBVec(0, 1, 0), "a")

	if _, err := srv.DeleteItem(ctx, &cefaspb.DeleteItemRequest{
		Table: table,
		Key: map[string]*cefaspb.AttributeValue{
			"id": {Value: &cefaspb.AttributeValue_S{S: "a"}},
		},
	}); err != nil {
		t.Fatalf("delete ann: %v", err)
	}
	assertTopKIDs(t, ctx, srv, table, testPBVec(0, 1, 0))

	if _, err := srv.BatchWriteItem(ctx, &cefaspb.BatchWriteItemRequest{
		Table: table,
		Ops: []*cefaspb.BatchWriteOp{
			{
				Kind: cefaspb.BatchWriteOp_KIND_PUT,
				Item: map[string]*cefaspb.AttributeValue{
					"id":  {Value: &cefaspb.AttributeValue_S{S: "b"}},
					"emb": testPBVec(1, 0, 0),
				},
			},
			{
				Kind: cefaspb.BatchWriteOp_KIND_PUT,
				Item: map[string]*cefaspb.AttributeValue{
					"id":  {Value: &cefaspb.AttributeValue_S{S: "c"}},
					"emb": testPBVec(0, 1, 0),
				},
			},
		},
	}); err != nil {
		t.Fatalf("batch ann: %v", err)
	}
	assertTopKIDs(t, ctx, srv, table, testPBVec(1, 0, 0), "b")
}

func TestANNPluginIndexMaintenanceForSQLDML(t *testing.T) {
	srv, cleanup := newPluginIndexMaintenanceServer(t, testANNRegistry(t))
	defer cleanup()
	ctx := context.Background()
	table := "MaintSQLANNDocs"
	createMaintenanceTable(t, ctx, srv, table, true)
	createMaintenanceANNIndex(t, ctx, srv, table)

	if _, err := srv.Sql(ctx, &cefaspb.SqlRequest{
		Query: "INSERT INTO MaintSQLANNDocs (id, emb) VALUES ('s', [1, 0, 0])",
	}); err != nil {
		t.Fatalf("sql insert ann: %v", err)
	}
	assertTopKIDs(t, ctx, srv, table, testPBVec(1, 0, 0), "s")

	if _, err := srv.Sql(ctx, &cefaspb.SqlRequest{
		Query: "UPDATE MaintSQLANNDocs SET emb = [0, 1, 0] WHERE id = 's'",
	}); err != nil {
		t.Fatalf("sql update ann: %v", err)
	}
	assertTopKIDs(t, ctx, srv, table, testPBVec(0, 1, 0), "s")

	if _, err := srv.Sql(ctx, &cefaspb.SqlRequest{
		Query: "DELETE FROM MaintSQLANNDocs WHERE id = 's'",
	}); err != nil {
		t.Fatalf("sql delete ann: %v", err)
	}
	assertTopKIDs(t, ctx, srv, table, testPBVec(0, 1, 0))
}

type recordedMaintenanceUpdate struct {
	oldItem model.Item
	newItem model.Item
}

type recordingMaintenanceIndexPlugin struct {
	mu        sync.Mutex
	updates   []recordedMaintenanceUpdate
	deletes   []model.Item
	updateErr error
	deleteErr error
}

func (p *recordingMaintenanceIndexPlugin) Manifest() plugin.Manifest {
	return plugin.Manifest{Name: "recordmaint", Kind: plugin.KindIndex, Version: "1"}
}

func (p *recordingMaintenanceIndexPlugin) Build(index.Descriptor, func(func(model.Item) bool)) error {
	return nil
}

func (p *recordingMaintenanceIndexPlugin) Update(_ index.Descriptor, oldItem, newItem model.Item) error {
	if p.updateErr != nil {
		return p.updateErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.updates = append(p.updates, recordedMaintenanceUpdate{
		oldItem: clonePluginIndexItem(oldItem),
		newItem: clonePluginIndexItem(newItem),
	})
	return nil
}

func (p *recordingMaintenanceIndexPlugin) Delete(_ index.Descriptor, key model.Item) error {
	if p.deleteErr != nil {
		return p.deleteErr
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.deletes = append(p.deletes, clonePluginIndexItem(key))
	return nil
}

func (p *recordingMaintenanceIndexPlugin) Query(index.Descriptor, plugin.IndexQuery) (plugin.CandidateSet, error) {
	return emptyMaintenanceCandidateSet{}, nil
}

func (p *recordingMaintenanceIndexPlugin) Estimate(index.Descriptor, plugin.IndexQuery) (int, error) {
	return 0, nil
}

func (p *recordingMaintenanceIndexPlugin) snapshot() ([]recordedMaintenanceUpdate, []model.Item) {
	p.mu.Lock()
	defer p.mu.Unlock()
	updates := make([]recordedMaintenanceUpdate, len(p.updates))
	for i, u := range p.updates {
		updates[i] = recordedMaintenanceUpdate{
			oldItem: clonePluginIndexItem(u.oldItem),
			newItem: clonePluginIndexItem(u.newItem),
		}
	}
	deletes := make([]model.Item, len(p.deletes))
	for i, d := range p.deletes {
		deletes[i] = clonePluginIndexItem(d)
	}
	return updates, deletes
}

type emptyMaintenanceCandidateSet struct{}

func (emptyMaintenanceCandidateSet) Next() (plugin.Candidate, bool) { return plugin.Candidate{}, false }
func (emptyMaintenanceCandidateSet) Err() error                     { return nil }
func (emptyMaintenanceCandidateSet) Close() error                   { return nil }

func newPluginIndexMaintenanceServer(t *testing.T, reg *plugin.Registry) (*GRPCServer, func()) {
	t.Helper()
	clearPluginIndexBookForTest()
	db, err := storage.Open(storage.Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	srv := NewGRPCServer(db, cat, nil)
	srv.AttachPluginRegistry(reg)
	return srv, func() {
		clearPluginIndexBookForTest()
		_ = db.Close()
	}
}

func createMaintenanceTable(t *testing.T, ctx context.Context, srv *GRPCServer, table string, vector bool) {
	t.Helper()
	td := &cefaspb.TableDescriptor{
		Name:      table,
		KeySchema: &cefaspb.KeySchema{Pk: "id"},
	}
	if vector {
		td.AttributeDefinitions = []*cefaspb.AttributeDefinition{{
			Name: "emb", Type: "V", VectorDimensions: 3,
		}}
	}
	if _, err := srv.CreateTable(ctx, &cefaspb.CreateTableRequest{Descriptor_: td}); err != nil {
		t.Fatalf("create table %s: %v", table, err)
	}
}

func createMaintenanceIndex(t *testing.T, ctx context.Context, srv *GRPCServer, table, pluginName string) {
	t.Helper()
	if _, err := srv.CreateIndex(ctx, &cefaspb.CreateIndexRequest{
		Descriptor_: &cefaspb.PluginIndexDescriptor{
			Table:      table,
			Name:       "maint_idx",
			PluginName: pluginName,
			KeySchema:  &cefaspb.KeySchema{Pk: "id"},
		},
	}); err != nil {
		t.Fatalf("create maintenance index: %v", err)
	}
}

func createMaintenanceANNIndex(t *testing.T, ctx context.Context, srv *GRPCServer, table string) {
	t.Helper()
	if _, err := srv.CreateIndex(ctx, &cefaspb.CreateIndexRequest{
		Descriptor_: annDescriptorPB(table),
	}); err != nil {
		t.Fatalf("create ann index: %v", err)
	}
}

func assertTopKIDs(t *testing.T, ctx context.Context, srv *GRPCServer, table string, target *cefaspb.AttributeValue, want ...string) {
	t.Helper()
	resp, err := srv.TopK(ctx, &cefaspb.TopKRequest{
		Table:  table,
		Field:  "emb",
		Target: target,
		K:      int32(maxInt(1, len(want))),
	})
	if err != nil {
		t.Fatalf("topk %s: %v", table, err)
	}
	if len(resp.GetRows()) != len(want) {
		t.Fatalf("topk rows = %d, want %d: %+v", len(resp.GetRows()), len(want), resp.GetRows())
	}
	for i, id := range want {
		if got := resp.GetRows()[i].GetItem().GetAttributes()["id"].GetS(); got != id {
			t.Fatalf("topk[%d] id = %q, want %q; rows=%+v", i, got, id, resp.GetRows())
		}
	}
}
