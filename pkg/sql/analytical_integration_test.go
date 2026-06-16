package sql_test

import (
	"strings"
	"testing"

	cefassql "github.com/osvaldoandrade/cefas/pkg/sql"
)

func TestAnalyticalSQLV1IntegratedQueries(t *testing.T) {
	_, cat, ex := newSQL(t)
	mustExec(t, ex, cat, "CREATE TABLE users (PRIMARY KEY (id))")
	mustExec(t, ex, cat, "CREATE TABLE orders (PRIMARY KEY (order_id))")
	mustExec(t, ex, cat, "INSERT INTO users (id, status, score, segment) VALUES ('u1', 'active', 90, 'gold')")
	mustExec(t, ex, cat, "INSERT INTO users (id, status, score, segment) VALUES ('u2', 'inactive', 40, 'silver')")
	mustExec(t, ex, cat, "INSERT INTO users (id, status, score, segment) VALUES ('u3', 'active', 95, 'gold')")
	mustExec(t, ex, cat, "INSERT INTO users (id, status, score) VALUES ('u4', 'active', 70)")
	mustExec(t, ex, cat, "INSERT INTO orders (order_id, user_id, status) VALUES ('o1', 'u1', 'paid')")
	mustExec(t, ex, cat, "INSERT INTO orders (order_id, user_id, status) VALUES ('o2', 'u1', 'open')")
	mustExec(t, ex, cat, "INSERT INTO orders (order_id, user_id, status) VALUES ('o3', 'u3', 'paid')")

	ordered := mustExec(t, ex, cat, "SELECT id FROM users ALLOW SCAN WHERE status = 'active' OR score >= 90 ORDER BY score DESC LIMIT 2")
	assertColumnOrder(t, ordered.Rows, "id", []string{"u3", "u1"})

	grouped := mustExec(t, ex, cat, "SELECT status, COUNT(*) FROM users ALLOW SCAN GROUP BY status")
	assertGroupCount(t, grouped.Rows, "status", "active", "count", 3)
	assertGroupCount(t, grouped.Rows, "status", "inactive", "count", 1)

	joined := mustExec(t, ex, cat, "SELECT u.id, o.order_id FROM users u INNER JOIN orders o ON u.id = o.user_id ALLOW SCAN WHERE o.status = 'paid' LIMIT 10")
	pairs := joinPairSet(joined.Rows, "u.id", "o.order_id")
	for _, want := range []string{"u1/o1", "u3/o3"} {
		if !pairs[want] {
			t.Fatalf("missing join pair %s in %+v", want, pairs)
		}
	}

	if _, err := cefassql.Compile("SELECT id FROM users WHERE status = 'active' LIMIT 10", cat); err == nil || !strings.Contains(err.Error(), `WHERE must equate "id"`) {
		t.Fatalf("expected missing scan guardrail, got %v", err)
	}
	if _, err := cefassql.Compile("SELECT u.id FROM users u INNER JOIN orders o ON u.id <> o.user_id ALLOW SCAN LIMIT 10", cat); err == nil || !strings.Contains(err.Error(), "only equality") {
		t.Fatalf("expected unsupported join-shape error, got %v", err)
	}
}
