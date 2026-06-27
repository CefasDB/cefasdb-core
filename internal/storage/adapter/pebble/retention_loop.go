package pebble

import (
	"time"
)

// startRetentionLoop launches the background goroutine that periodically
// removes expired CDC/changelog entries. It is a no-op when the configured
// interval is negative.
//
// The loop is bounded by StreamRetentionOptions.BatchSize. It scans only the
// time-ordered expiration prefix, so an idle database with no expired CDC
// records performs a tiny range probe instead of walking the changelog.
func (d *DB) startRetentionLoop() {
	if d == nil {
		return
	}
	interval := d.streamRetention.Interval
	if interval <= 0 {
		// Disabled — explicit ApplyStreamRetention calls still work.
		return
	}
	d.retentionStopCh = make(chan struct{})
	d.retentionStopped = make(chan struct{})
	go d.runRetentionLoop(interval)
}

func (d *DB) runRetentionLoop(interval time.Duration) {
	defer close(d.retentionStopped)
	current := d.workload.RetentionInterval(interval)
	t := time.NewTicker(current)
	defer t.Stop()
	for {
		select {
		case <-d.retentionStopCh:
			return
		case now := <-t.C:
			d.tickRetention(now)
			// Pick up any adaptive change made by workloadMode.
			next := d.workload.RetentionInterval(interval)
			if next != current && next > 0 {
				t.Reset(next)
				current = next
			}
		}
	}
}

// tickRetention applies one bounded physical cleanup pass. Errors are
// intentionally swallowed because there is no foreground caller to return them
// to; the next tick retries.
func (d *DB) tickRetention(now time.Time) {
	_, _ = d.applyExpiredChangeLogRetention(now)
}

func (d *DB) stopRetentionLoop() {
	if d == nil || d.retentionStopCh == nil {
		return
	}
	close(d.retentionStopCh)
	<-d.retentionStopped
	d.retentionStopCh = nil
	d.retentionStopped = nil
}

// trackStreamTable records that table has produced at least one
// stream-enabled change record. Called from appendChangeRecord on the
// hot path; the fast path (table already tracked) takes only an RLock.
func (d *DB) trackStreamTable(table string) {
	if table == "" {
		return
	}
	d.streamTablesMu.RLock()
	_, ok := d.streamTables[table]
	d.streamTablesMu.RUnlock()
	if ok {
		return
	}
	d.streamTablesMu.Lock()
	d.streamTables[table] = struct{}{}
	d.streamTablesMu.Unlock()
}
