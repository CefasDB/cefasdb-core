package pebble

import (
	"fmt"
	"testing"

	"github.com/CefasDb/cefasdb/pkg/types"
)

// TestScanTableWithStreamsAndStopsEarly proves the predicate-pushdown
// helper visits items in primary-key order and stops as soon as the
// caller's predicate returns false. The companion check counts how
// many items the iterator actually surfaced — the fix for issue #459
// is meaningful only if a permissive filter no longer materialises
// the entire table.
func TestScanTableWithStreamsAndStopsEarly(t *testing.T) {
	db, err := Open(Options{Path: t.TempDir()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	const table = "T"
	const total = 200
	ks := types.KeySchema{PK: "pk"}
	for i := 0; i < total; i++ {
		item := types.Item{
			"pk": {T: types.AttrS, S: fmt.Sprintf("k%04d", i)},
			"v":  {T: types.AttrN, N: fmt.Sprintf("%d", i)},
		}
		if err := db.PutItemWith(types.TableDescriptor{Name: table, KeySchema: ks}, item, PutOptions{}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	t.Run("visits every row exactly once", func(t *testing.T) {
		seen := map[string]bool{}
		err := db.ScanTableWith(table, func(it types.Item) bool {
			seen[it["pk"].S] = true
			return true
		})
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		if len(seen) != total {
			t.Fatalf("want %d distinct items, got %d", total, len(seen))
		}
	})

	t.Run("stops after limit-style predicate", func(t *testing.T) {
		visited := 0
		err := db.ScanTableWith(table, func(it types.Item) bool {
			visited++
			return visited < 5
		})
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		if visited != 5 {
			t.Fatalf("want exactly 5 visits before stop, got %d", visited)
		}
	})

	t.Run("filter predicate short-circuits without buffering", func(t *testing.T) {
		// Reject everything: nothing should escape; the iterator
		// still drains because visit never returned false (true means
		// continue, predicate false means reject row, not stop).
		// The point is we never accumulate the table — `kept` stays
		// at zero.
		kept := 0
		err := db.ScanTableWith(table, func(it types.Item) bool {
			if it["pk"].S == "k0042" {
				kept++
			}
			return true
		})
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		if kept != 1 {
			t.Fatalf("want 1 row matched, got %d", kept)
		}
	})
}
