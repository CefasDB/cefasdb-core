package sql_test

import (
	"testing"

	cefassql "github.com/osvaldoandrade/cefas/internal/sql"
)

// FuzzParse exercises Parse with arbitrary inputs to surface panics,
// out-of-bounds reads, and infinite loops. Returning an error is acceptable;
// crashing is not.
func FuzzParse(f *testing.F) {
	seeds := []string{
		// Core SELECT shapes.
		"SELECT * FROM t",
		"SELECT a, b FROM t WHERE a = 1",
		"SELECT * FROM t WHERE pk = 'a' AND sk = 'b' LIMIT 5",
		"SELECT id FROM t ALLOW SCAN WHERE status = 'active' LIMIT 10",
		"SELECT * FROM events WHERE user_id = 'alice' AND ts BETWEEN '001' AND '002'",
		"SELECT * FROM events WHERE user_id = 'alice' ORDER BY ts DESC",
		"SELECT COUNT(*) FROM t WHERE uid = 'a'",

		// DDL.
		"CREATE TABLE events (PRIMARY KEY (user_id, ts))",
		"CREATE TABLE docs (PRIMARY KEY (id), emb V<3>) WITH STORAGE = 'memory'",

		// DML.
		"INSERT INTO events (user_id, ts, event) VALUES ('alice', '001', 'login')",
		"INSERT INTO t (id, v) VALUES ('a', '1') IF NOT EXISTS",
		"UPDATE t SET v = 'after' WHERE id = 'a' RETURNING NEW",
		"UPDATE t SET status = 'done' WHERE id = 'a' IF status = 'pending'",
		"DELETE FROM t WHERE id = 'a' RETURNING OLD",

		// ANN / vector search and geospatial.
		"SELECT id FROM docs ORDER BY emb ANN OF [1,0,0] LIMIT 5",
		"SELECT id FROM docs ORDER BY emb ANN OF [1,0,0] LIMIT 100 DIVERSIFY BY mmr(lambda=0.7) TO 10",
		"SELECT * FROM places USE INDEX (by_loc) WHERE ST_Within(loc, BBox(40.0, -74.0, 41.0, -73.0))",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, in string) {
		// Contract: Parse must not panic on any input.
		_, _ = cefassql.Parse(in)
	})
}
