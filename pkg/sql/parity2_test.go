package sql_test

import (
	"testing"

	cefassql "github.com/osvaldoandrade/cefas/pkg/sql"
)

func TestSQLCountStar(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (uid, ts))")
	for _, ts := range []string{"1", "2", "3", "4", "5"} {
		mustExec(t, ex, cat, "INSERT INTO t (uid, ts) VALUES ('a', '"+ts+"')")
	}
	res := mustExec(t, ex, cat, "SELECT COUNT(*) FROM t WHERE uid = 'a'")
	if res.AffectedRows != 5 {
		t.Fatalf("count = %d, want 5", res.AffectedRows)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("count should not return rows: %+v", res.Rows)
	}
	res = mustExec(t, ex, cat, "SELECT COUNT(*) FROM t WHERE uid = 'a' AND ts BETWEEN '2' AND '4'")
	if res.AffectedRows != 3 {
		t.Fatalf("count with range = %d, want 3", res.AffectedRows)
	}
}

func TestSQLReturningInsertNew(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (id))")
	res := mustExec(t, ex, cat, "INSERT INTO t (id, v) VALUES ('a', 'hello') RETURNING *")
	if len(res.Rows) != 1 || res.Rows[0]["v"].S != "hello" {
		t.Fatalf("RETURNING * on INSERT: %+v", res.Rows)
	}
}

func TestSQLReturningUpdateNewAndOld(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (id))")
	mustExec(t, ex, cat, "INSERT INTO t (id, v) VALUES ('a', 'before')")
	res := mustExec(t, ex, cat, "UPDATE t SET v = 'after' WHERE id = 'a' RETURNING NEW")
	if len(res.Rows) != 1 || res.Rows[0]["v"].S != "after" {
		t.Fatalf("RETURNING NEW: %+v", res.Rows)
	}
	mustExec(t, ex, cat, "UPDATE t SET v = 'newer' WHERE id = 'a'")
	res = mustExec(t, ex, cat, "UPDATE t SET v = 'newest' WHERE id = 'a' RETURNING OLD")
	if len(res.Rows) != 1 || res.Rows[0]["v"].S != "newer" {
		t.Fatalf("RETURNING OLD: %+v", res.Rows)
	}
}

func TestSQLReturningDeleteOld(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (id))")
	mustExec(t, ex, cat, "INSERT INTO t (id, v) VALUES ('a', 'gone')")
	res := mustExec(t, ex, cat, "DELETE FROM t WHERE id = 'a' RETURNING OLD")
	if len(res.Rows) != 1 || res.Rows[0]["v"].S != "gone" {
		t.Fatalf("RETURNING OLD on DELETE: %+v", res.Rows)
	}
}

func TestSQLPartiQLBind(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE t (PRIMARY KEY (id))")
	// PartiQL-style placeholder substitution converts the statement
	// to plain cefas SQL before compile, so the parser handles ?
	// values transparently.
	bound, err := cefassql.BindPartiQL("INSERT INTO t (id, v) VALUES (?, ?)", []cefassql.PartiQLParameter{
		{S: strPtr("k1")}, {S: strPtr("hello")},
	})
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	mustExec(t, ex, cat, bound)
	res := mustExec(t, ex, cat, "SELECT * FROM t WHERE id = 'k1'")
	if res.Rows[0]["v"].S != "hello" {
		t.Fatalf("partiql bind did not store value: %+v", res.Rows[0])
	}
}

func strPtr(s string) *string { return &s }
