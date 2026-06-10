package sql_test

import (
	"errors"
	"testing"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/storage"
	cefassql "github.com/osvaldoandrade/cefas/pkg/sql"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func mustExec(t *testing.T, ex *cefassql.Executor, cat *catalog.Catalog, src string) *cefassql.Result {
	t.Helper()
	plan, err := cefassql.Compile(src, cat)
	if err != nil {
		t.Fatalf("compile %q: %v", src, err)
	}
	res, err := ex.Execute(plan)
	if err != nil {
		t.Fatalf("exec %q: %v", src, err)
	}
	return res
}

func mustFail(t *testing.T, ex *cefassql.Executor, cat *catalog.Catalog, src string, want error) {
	t.Helper()
	plan, err := cefassql.Compile(src, cat)
	if err != nil {
		t.Fatalf("compile %q: %v", src, err)
	}
	_, err = ex.Execute(plan)
	if !errors.Is(err, want) {
		t.Fatalf("%q: got %v, want %v", src, err, want)
	}
}

func newSQL(t *testing.T) (*storage.DB, *catalog.Catalog, *cefassql.Executor) {
	t.Helper()
	dir := t.TempDir()
	db, err := storage.Open(storage.Options{Path: dir})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cat, err := catalog.New(db)
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	return db, cat, &cefassql.Executor{Storage: db, Catalog: cat}
}

// ---------- IF tail (#58) ----------

func TestSQLInsertIFNotExists(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (id))")
	mustExec(t, ex, cat, "INSERT INTO t (id, v) VALUES ('a', '1') IF NOT EXISTS")
	mustFail(t, ex, cat, "INSERT INTO t (id, v) VALUES ('a', '2') IF NOT EXISTS", storage.ErrConditionFailed)
}

func TestSQLUpdateIFCheck(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (id))")
	mustExec(t, ex, cat, "INSERT INTO t (id, status) VALUES ('a', 'pending')")
	// IF on wrong value → fails
	mustFail(t, ex, cat, "UPDATE t SET status = 'done' WHERE id = 'a' IF status = 'WRONG'", storage.ErrConditionFailed)
	// IF on right value → ok
	mustExec(t, ex, cat, "UPDATE t SET status = 'done' WHERE id = 'a' IF status = 'pending'")
	res := mustExec(t, ex, cat, "SELECT * FROM t WHERE id = 'a'")
	if len(res.Rows) != 1 || res.Rows[0]["status"].S != "done" {
		t.Fatalf("status not updated: %+v", res.Rows)
	}
}

func TestSQLDeleteIFCheck(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (id))")
	mustExec(t, ex, cat, "INSERT INTO t (id, version) VALUES ('a', '1')")
	mustFail(t, ex, cat, "DELETE FROM t WHERE id = 'a' IF version = '2'", storage.ErrConditionFailed)
	mustExec(t, ex, cat, "DELETE FROM t WHERE id = 'a' IF version = '1'")
}

// ---------- Rich UPDATE (#59) ----------

func TestSQLUpdateArithmetic(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (id))")
	mustExec(t, ex, cat, "INSERT INTO t (id, score) VALUES ('a', 10)")
	mustExec(t, ex, cat, "UPDATE t SET score = score + 5 WHERE id = 'a'")
	res := mustExec(t, ex, cat, "SELECT * FROM t WHERE id = 'a'")
	if res.Rows[0]["score"].N != "15" {
		t.Fatalf("score = %q, want 15", res.Rows[0]["score"].N)
	}
	mustExec(t, ex, cat, "UPDATE t SET score = score - 3 WHERE id = 'a'")
	res = mustExec(t, ex, cat, "SELECT * FROM t WHERE id = 'a'")
	if res.Rows[0]["score"].N != "12" {
		t.Fatalf("score = %q, want 12", res.Rows[0]["score"].N)
	}
}

func TestSQLUpdateAdd(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (id))")
	mustExec(t, ex, cat, "INSERT INTO t (id, hits) VALUES ('a', 0)")
	mustExec(t, ex, cat, "UPDATE t ADD hits 1 WHERE id = 'a'")
	mustExec(t, ex, cat, "UPDATE t ADD hits 1 WHERE id = 'a'")
	mustExec(t, ex, cat, "UPDATE t ADD hits 1 WHERE id = 'a'")
	res := mustExec(t, ex, cat, "SELECT * FROM t WHERE id = 'a'")
	if res.Rows[0]["hits"].N != "3" {
		t.Fatalf("hits = %q, want 3", res.Rows[0]["hits"].N)
	}
}

func TestSQLUpdateRemove(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (id))")
	mustExec(t, ex, cat, "INSERT INTO t (id, a, b, c) VALUES ('k', '1', '2', '3')")
	mustExec(t, ex, cat, "UPDATE t REMOVE a, c WHERE id = 'k'")
	res := mustExec(t, ex, cat, "SELECT * FROM t WHERE id = 'k'")
	if _, ok := res.Rows[0]["a"]; ok {
		t.Errorf("a not removed: %+v", res.Rows[0])
	}
	if res.Rows[0]["b"].S != "2" {
		t.Errorf("b lost: %+v", res.Rows[0])
	}
	if _, ok := res.Rows[0]["c"]; ok {
		t.Errorf("c not removed: %+v", res.Rows[0])
	}
}

// ---------- WHERE functions (#60) ----------

func TestSQLWhereBeginsWith(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (uid, ts))")
	for _, ts := range []string{"001-login", "002-click", "003-logout", "004-login"} {
		mustExec(t, ex, cat, "INSERT INTO t (uid, ts) VALUES ('alice', '"+ts+"')")
	}
	res := mustExec(t, ex, cat, "SELECT * FROM t WHERE uid = 'alice' AND begins_with(ts, '00')")
	if len(res.Rows) != 4 {
		t.Fatalf("begins_with(0) want 4, got %d", len(res.Rows))
	}
	res = mustExec(t, ex, cat, "SELECT * FROM t WHERE uid = 'alice' AND begins_with(ts, '003')")
	if len(res.Rows) != 1 || res.Rows[0]["ts"].S != "003-logout" {
		t.Fatalf("begins_with(003) want 1 row, got %+v", res.Rows)
	}
}

func TestSQLWhereContains(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (uid, ts))")
	mustExec(t, ex, cat, "INSERT INTO t (uid, ts, msg) VALUES ('a', '1', 'hello world')")
	mustExec(t, ex, cat, "INSERT INTO t (uid, ts, msg) VALUES ('a', '2', 'goodbye')")
	res := mustExec(t, ex, cat, "SELECT * FROM t WHERE uid = 'a' AND contains(msg, 'world')")
	if len(res.Rows) != 1 || res.Rows[0]["ts"].S != "1" {
		t.Fatalf("contains: %+v", res.Rows)
	}
}

func TestSQLWhereAttributeExists(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (uid, ts))")
	mustExec(t, ex, cat, "INSERT INTO t (uid, ts, tag) VALUES ('a', '1', 'x')")
	mustExec(t, ex, cat, "INSERT INTO t (uid, ts) VALUES ('a', '2')")
	res := mustExec(t, ex, cat, "SELECT * FROM t WHERE uid = 'a' AND attribute_exists(tag)")
	if len(res.Rows) != 1 || res.Rows[0]["ts"].S != "1" {
		t.Fatalf("attribute_exists: %+v", res.Rows)
	}
	res = mustExec(t, ex, cat, "SELECT * FROM t WHERE uid = 'a' AND attribute_not_exists(tag)")
	if len(res.Rows) != 1 || res.Rows[0]["ts"].S != "2" {
		t.Fatalf("attribute_not_exists: %+v", res.Rows)
	}
}

func TestSQLWhereSize(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (uid, ts))")
	mustExec(t, ex, cat, "INSERT INTO t (uid, ts, msg) VALUES ('a', '1', 'hi')")
	mustExec(t, ex, cat, "INSERT INTO t (uid, ts, msg) VALUES ('a', '2', 'hello there')")
	res := mustExec(t, ex, cat, "SELECT * FROM t WHERE uid = 'a' AND size(msg) > 5")
	if len(res.Rows) != 1 || res.Rows[0]["ts"].S != "2" {
		t.Fatalf("size > 5: %+v", res.Rows)
	}
}

func TestSQLWhereCombined(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (uid, ts))")
	mustExec(t, ex, cat, "INSERT INTO t (uid, ts, status) VALUES ('a', '1', 'active')")
	mustExec(t, ex, cat, "INSERT INTO t (uid, ts, status) VALUES ('a', '2', 'inactive')")
	mustExec(t, ex, cat, "INSERT INTO t (uid, ts, status) VALUES ('a', '3', 'active')")
	res := mustExec(t, ex, cat, "SELECT * FROM t WHERE uid = 'a' AND ts BETWEEN '1' AND '3' AND status = 'active'")
	if len(res.Rows) != 2 {
		t.Fatalf("combined want 2 rows, got %+v", res.Rows)
	}
}

// Silence unused import diagnostics from older types.
var _ = types.AttrS
