package pebble

import (
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/pkg/types"
)

func TestRetentionLoopDisabledByDefault(t *testing.T) {
	db := openChangeLogTestDBWithOptions(t, Options{
		ChangeLogMode: ChangeLogModeStreamsOnly,
	})
	if db.retentionStopCh != nil {
		t.Fatalf("loop should not start without explicit positive interval")
	}
	td := streamTestTable()
	if err := db.PutItemWith(td, types.Item{
		"id":     streamS("k"),
		"status": streamS("v"),
	}, PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, ok, _ := db.loadStreamRetentionState(td.Name); ok {
		t.Fatalf("retention state should not exist without explicit apply")
	}
}

// TestRetentionLoopFiresOnExplicitInterval exercises the opt-in path: writes to
// a stream-enabled table without calling ApplyStreamRetention explicitly, then
// waits for one tick and asserts persisted state appeared.
func TestRetentionLoopFiresOnExplicitInterval(t *testing.T) {
	db := openChangeLogTestDBWithOptions(t, Options{
		ChangeLogMode: ChangeLogModeStreamsOnly,
		StreamRetention: StreamRetentionOptions{
			Interval: 50 * time.Millisecond,
		},
	})
	td := streamTestTable()
	for i := 0; i < 5; i++ {
		if err := db.PutItemWith(td, types.Item{
			"id":     streamS("k"),
			"status": streamS("v"),
		}, PutOptions{}); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	// Before the tick, nothing was persisted (per-write refresh is gone).
	if _, ok, err := db.loadStreamRetentionState(td.Name); err != nil {
		t.Fatalf("load: %v", err)
	} else if ok {
		t.Fatalf("retention state should not exist before first tick")
	}

	if err := waitForRetentionState(db, td.Name, 2*time.Second); err != nil {
		t.Fatalf("waitForRetentionState: %v", err)
	}
}

// TestRetentionLoopDisabledWhenIntervalNegative confirms a negative
// Interval skips the goroutine entirely. Persistence still works via
// the explicit ApplyStreamRetention call.
func TestRetentionLoopDisabledWhenIntervalNegative(t *testing.T) {
	db := openChangeLogTestDBWithOptions(t, Options{
		ChangeLogMode: ChangeLogModeStreamsOnly,
		StreamRetention: StreamRetentionOptions{
			Interval: -1,
		},
	})
	if db.retentionStopCh != nil {
		t.Fatalf("loop should not start with negative interval")
	}
	td := streamTestTable()
	if err := db.PutItemWith(td, types.Item{
		"id":     streamS("k"),
		"status": streamS("v"),
	}, PutOptions{}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, ok, _ := db.loadStreamRetentionState(td.Name); ok {
		t.Fatalf("retention state should not exist without explicit apply")
	}
	if _, err := db.ApplyStreamRetention(td.Name, time.Now()); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, ok, _ := db.loadStreamRetentionState(td.Name); !ok {
		t.Fatalf("explicit apply must persist state")
	}
}

// TestRetentionLoopShutdownIsClean ensures Close drains the loop
// without deadlocking — the loop must observe retentionStopCh before
// db.Close returns.
func TestRetentionLoopShutdownIsClean(t *testing.T) {
	db, err := Open(Options{
		Path:          t.TempDir(),
		ChangeLogMode: ChangeLogModeStreamsOnly,
		StreamRetention: StreamRetentionOptions{
			Interval: 10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	done := make(chan struct{})
	go func() {
		_ = db.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("Close did not return within 2s")
	}
}

func TestTrackStreamTableDedupes(t *testing.T) {
	db := openChangeLogTestDBWithOptions(t, Options{
		StreamRetention: StreamRetentionOptions{Interval: -1},
	})
	db.trackStreamTable("A")
	db.trackStreamTable("A")
	db.trackStreamTable("B")
	db.streamTablesMu.RLock()
	defer db.streamTablesMu.RUnlock()
	if len(db.streamTables) != 2 {
		t.Fatalf("streamTables size = %d, want 2", len(db.streamTables))
	}
	if _, ok := db.streamTables["A"]; !ok {
		t.Fatalf("missing A")
	}
	if _, ok := db.streamTables["B"]; !ok {
		t.Fatalf("missing B")
	}
}

func waitForRetentionState(db *DB, table string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, ok, err := db.loadStreamRetentionState(table); err != nil {
			return err
		} else if ok {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return &timeoutError{table: table, timeout: timeout}
}

type timeoutError struct {
	table   string
	timeout time.Duration
}

func (e *timeoutError) Error() string {
	return "retention state for " + e.table + " did not appear within " + e.timeout.String()
}
