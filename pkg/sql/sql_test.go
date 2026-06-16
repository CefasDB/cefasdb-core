package sql_test

import (
	"strings"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	cefassql "github.com/osvaldoandrade/cefas/pkg/sql"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func newStorage(t *testing.T) (*pebble.DB, *catalog.Catalog) {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(pebble.Options{Path: dir})
	if err != nil {
		t.Fatalf("storage open: %v", err)
	}
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, cat
}

func TestLexerKeywords(t *testing.T) {
	toks, err := cefassql.Tokenize("SELECT * FROM events WHERE user_id = 'alice'")
	if err != nil {
		t.Fatal(err)
	}
	if len(toks) < 8 {
		t.Fatalf("not enough tokens: %d", len(toks))
	}
}

func TestParserSelectShape(t *testing.T) {
	stmt, err := cefassql.Parse("SELECT * FROM t WHERE pk = 'a' AND sk = 'b' LIMIT 5")
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := stmt.(*cefassql.SelectStmt)
	if !ok {
		t.Fatalf("got %T, want *SelectStmt", stmt)
	}
	if sel.Table != "t" || sel.Limit != 5 {
		t.Fatalf("unexpected select: %+v", sel)
	}
}

func TestParserSelectAllowScan(t *testing.T) {
	stmt, err := cefassql.Parse("SELECT id FROM t ALLOW SCAN WHERE status = 'active' LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := stmt.(*cefassql.SelectStmt)
	if !ok {
		t.Fatalf("got %T, want *SelectStmt", stmt)
	}
	if !sel.AllowScan || sel.Table != "t" || sel.Limit != 10 {
		t.Fatalf("unexpected select: %+v", sel)
	}
}

func TestParserCreateTable(t *testing.T) {
	stmt, err := cefassql.Parse("CREATE TABLE events (PRIMARY KEY (user_id, ts))")
	if err != nil {
		t.Fatal(err)
	}
	c, ok := stmt.(*cefassql.CreateTableStmt)
	if !ok || c.PK != "user_id" || c.SK != "ts" {
		t.Fatalf("got %+v", stmt)
	}
}

func TestParserCreateTableStorageAndVector(t *testing.T) {
	stmt, err := cefassql.Parse("CREATE TABLE docs (PRIMARY KEY (id), emb V<3>) WITH STORAGE = 'memory'")
	if err != nil {
		t.Fatal(err)
	}
	c, ok := stmt.(*cefassql.CreateTableStmt)
	if !ok {
		t.Fatalf("got %T", stmt)
	}
	if c.StorageClass != "memory" || len(c.AttributeDefinitions) != 1 || c.AttributeDefinitions[0].VectorDimensions != 3 {
		t.Fatalf("unexpected create stmt: %+v", c)
	}
}

func TestParserSelectANN(t *testing.T) {
	stmt, err := cefassql.Parse("SELECT id FROM docs ORDER BY emb ANN OF [1,0,0] LIMIT 5")
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := stmt.(*cefassql.SelectStmt)
	if !ok {
		t.Fatalf("got %T", stmt)
	}
	if !sel.OrderANN || sel.OrderBy != "emb" || len(sel.ANNTarget) != 3 || sel.Limit != 5 {
		t.Fatalf("unexpected select: %+v", sel)
	}
}

func TestParserSelectANNDiversify(t *testing.T) {
	stmt, err := cefassql.Parse("SELECT id FROM docs ORDER BY emb ANN OF [1,0,0] LIMIT 100 DIVERSIFY BY mmr(lambda=0.7) TO 10")
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := stmt.(*cefassql.SelectStmt)
	if !ok {
		t.Fatalf("got %T", stmt)
	}
	if sel.Diversify == nil || sel.Diversify.Method != "mmr" || sel.Diversify.Lambda != 0.7 || sel.Diversify.TargetSize != 10 {
		t.Fatalf("unexpected diversify clause: %+v", sel.Diversify)
	}
}

func TestParserSpatial(t *testing.T) {
	src := "SELECT * FROM places USE INDEX (by_loc) WHERE ST_Within(loc, BBox(40.0, -74.0, 41.0, -73.0))"
	stmt, err := cefassql.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := stmt.(*cefassql.SelectStmt)
	if !ok {
		t.Fatalf("got %T", stmt)
	}
	if sel.IndexName != "by_loc" {
		t.Fatalf("index hint lost: %+v", sel)
	}
}

func TestEndToEndCreateInsertSelect(t *testing.T) {
	db, cat := newStorage(t)
	exec := &cefassql.Executor{Storage: db, Catalog: cat}

	run := func(src string) *cefassql.Result {
		t.Helper()
		plan, err := cefassql.Compile(src, cat)
		if err != nil {
			t.Fatalf("compile %q: %v", src, err)
		}
		res, err := exec.Execute(plan)
		if err != nil {
			t.Fatalf("exec %q: %v", src, err)
		}
		return res
	}

	run("CREATE TABLE events (PRIMARY KEY (user_id, ts))")
	run("INSERT INTO events (user_id, ts, event) VALUES ('alice', '001', 'login')")
	run("INSERT INTO events (user_id, ts, event) VALUES ('alice', '002', 'click')")
	run("INSERT INTO events (user_id, ts, event) VALUES ('alice', '003', 'logout')")

	res := run("SELECT * FROM events WHERE user_id = 'alice'")
	if len(res.Rows) != 3 {
		t.Fatalf("want 3 rows, got %d", len(res.Rows))
	}

	res = run("SELECT * FROM events WHERE user_id = 'alice' AND ts BETWEEN '001' AND '002'")
	if len(res.Rows) != 2 {
		t.Fatalf("range want 2 rows, got %d", len(res.Rows))
	}

	res = run("SELECT * FROM events WHERE user_id = 'alice' AND ts = '002'")
	if len(res.Rows) != 1 || res.Rows[0]["event"].S != "click" {
		t.Fatalf("get expected click, got %+v", res.Rows)
	}

	res = run("UPDATE events SET event = 'tap' WHERE user_id = 'alice' AND ts = '002'")
	if res.AffectedRows != 1 {
		t.Fatalf("update affected %d", res.AffectedRows)
	}
	res = run("SELECT * FROM events WHERE user_id = 'alice' AND ts = '002'")
	if res.Rows[0]["event"].S != "tap" {
		t.Fatalf("update did not apply: %+v", res.Rows[0])
	}

	res = run("DELETE FROM events WHERE user_id = 'alice' AND ts = '003'")
	if res.AffectedRows != 1 {
		t.Fatalf("delete affected %d", res.AffectedRows)
	}
	res = run("SELECT * FROM events WHERE user_id = 'alice'")
	if len(res.Rows) != 2 {
		t.Fatalf("want 2 rows after delete, got %d", len(res.Rows))
	}

	res = run("SELECT * FROM events WHERE user_id = 'alice' LIMIT 1")
	if len(res.Rows) != 1 {
		t.Fatalf("limit ignored: %+v", res.Rows)
	}

	res = run("SELECT * FROM events WHERE user_id = 'alice' ORDER BY ts DESC")
	if len(res.Rows) != 2 || res.Rows[0]["ts"].S != "002" {
		t.Fatalf("DESC order broken: %+v", res.Rows)
	}
}

func TestSelectRequiresWhere(t *testing.T) {
	_, cat := newStorage(t)
	if err := cat.Create(types.TableDescriptor{Name: "t", KeySchema: types.KeySchema{PK: "id"}}); err != nil {
		t.Fatal(err)
	}
	_, err := cefassql.Compile("SELECT * FROM t", cat)
	if err == nil || !strings.Contains(err.Error(), "scan") {
		t.Fatalf("expected scan refusal, got %v", err)
	}
}

func TestAllowScanSelectsNonKeyPredicate(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (id))")
	mustExec(t, ex, cat, "INSERT INTO t (id, status, score) VALUES ('a', 'active', 10)")
	mustExec(t, ex, cat, "INSERT INTO t (id, status, score) VALUES ('b', 'inactive', 20)")
	mustExec(t, ex, cat, "INSERT INTO t (id, status, score) VALUES ('c', 'active', 30)")

	_, err := cefassql.Compile("SELECT * FROM t WHERE status = 'active' LIMIT 10", cat)
	if err == nil || !strings.Contains(err.Error(), `WHERE must equate "id"`) {
		t.Fatalf("expected key-first refusal, got %v", err)
	}

	res := mustExec(t, ex, cat, "SELECT id FROM t ALLOW SCAN WHERE status = 'active' LIMIT 10")
	if len(res.Rows) != 2 {
		t.Fatalf("scan filter want 2 rows, got %+v", res.Rows)
	}
	for _, row := range res.Rows {
		if _, ok := row["status"]; ok {
			t.Fatalf("projection leaked status: %+v", row)
		}
	}
}

func TestAllowScanRequiresLimitForRows(t *testing.T) {
	_, cat := newStorage(t)
	if err := cat.Create(types.TableDescriptor{Name: "t", KeySchema: types.KeySchema{PK: "id"}}); err != nil {
		t.Fatal(err)
	}
	_, err := cefassql.Compile("SELECT * FROM t ALLOW SCAN", cat)
	if err == nil || !strings.Contains(err.Error(), "ALLOW SCAN requires LIMIT") {
		t.Fatalf("expected ALLOW SCAN LIMIT refusal, got %v", err)
	}
}

func TestAllowScanCountDoesNotRequireLimit(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (id))")
	mustExec(t, ex, cat, "INSERT INTO t (id, status) VALUES ('a', 'active')")
	mustExec(t, ex, cat, "INSERT INTO t (id, status) VALUES ('b', 'inactive')")
	mustExec(t, ex, cat, "INSERT INTO t (id, status) VALUES ('c', 'active')")

	res := mustExec(t, ex, cat, "SELECT COUNT(*) FROM t ALLOW SCAN WHERE status = 'active'")
	if res.AffectedRows != 2 {
		t.Fatalf("scan count = %d, want 2", res.AffectedRows)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("count should not return rows: %+v", res.Rows)
	}
}

func TestUpdateRejectsKeyColumn(t *testing.T) {
	_, cat := newStorage(t)
	if err := cat.Create(types.TableDescriptor{Name: "t", KeySchema: types.KeySchema{PK: "id"}}); err != nil {
		t.Fatal(err)
	}
	_, err := cefassql.Compile("UPDATE t SET id = 'x' WHERE id = 'y'", cat)
	if err == nil || !strings.Contains(err.Error(), "key column") {
		t.Fatalf("expected key-column refusal, got %v", err)
	}
}

func TestSpatialPlanRoutesToBBox(t *testing.T) {
	_, cat := newStorage(t)
	if err := cat.Create(types.TableDescriptor{
		Name:      "places",
		KeySchema: types.KeySchema{PK: "id"},
		SpatialIndexes: []types.SpatialIndexDescriptor{{
			Name: "by_loc", Kind: "geohash", Attributes: []string{"lat", "lon"}, Precision: 6,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	plan, err := cefassql.Compile(
		"SELECT * FROM places USE INDEX (by_loc) WHERE ST_Within(lat, BBox(40.0, -74.0, 41.0, -73.0))",
		cat)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := plan.(*cefassql.PlanSpatial); !ok {
		t.Fatalf("expected *PlanSpatial, got %T", plan)
	}
}
