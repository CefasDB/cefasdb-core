package pebble

import (
	"time"
)

// startRetentionLoop launches the background goroutine that periodically
// invokes ApplyStreamRetention for every stream-enabled table that has
// produced at least one change record. It is opt-in: the loop is a no-op unless
// the configured interval is positive.
//
// Tables are discovered lazily via trackStreamTable, called from
// appendChangeRecord when the record carries StreamRecord == true. The
// loop never blocks foreground writes — it acquires only the read side
// of streamTablesMu to snapshot the table set, then drives
// ApplyStreamRetention serially on the snapshot. ApplyStreamRetention
// takes changeMu like any other writer, so its overhead is bounded by
// the normal write-coalescer.
//
// Hot writes used to call refreshStreamRetentionAfterWrite at the end
// of every PutItem / DeleteItem / BatchWrite / Atomic / TTL evict path,
// which scanned the entire changelog prefix and committed an extra
// batch per write — O(N) on the live changelog per write. The background loop
// is still a full changelog scan per table on each tick, so production
// deployments must enable it only with an explicit interval and a bounded
// changelog.
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

// tickRetention snapshots the known stream-enabled tables and applies
// retention to each. Errors are intentionally swallowed: a transient
// failure on one table must not block the others, and there is no
// foreground caller to return the error to. The next tick will retry.
func (d *DB) tickRetention(now time.Time) {
	d.streamTablesMu.RLock()
	tables := make([]string, 0, len(d.streamTables))
	for name := range d.streamTables {
		tables = append(tables, name)
	}
	d.streamTablesMu.RUnlock()
	for _, name := range tables {
		_, _ = d.ApplyStreamRetention(name, now)
	}
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
