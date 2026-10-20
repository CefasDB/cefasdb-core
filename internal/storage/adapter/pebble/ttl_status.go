package pebble

import "time"

// TTLStatus contains bounded operational metadata, never keys or row values.
// BacklogPresent is a lower-bound observation: a tick saw more expired
// pointers than its batch budget, not an unbounded exact count.
type TTLStatus struct {
	Enabled        bool      `json:"enabled"`
	Leader         bool      `json:"leader"`
	LastAttemptAt  time.Time `json:"lastAttemptAt"`
	LastSuccessAt  time.Time `json:"lastSuccessAt"`
	Sweeps         uint64    `json:"sweeps"`
	Failures       uint64    `json:"failures"`
	Deleted        uint64    `json:"deleted"`
	PayloadDeleted uint64    `json:"payloadDeleted"`
	StalePointers  uint64    `json:"stalePointers"`
	BacklogPresent bool      `json:"backlogPresent"`
}

func (d *DB) TTLStatus() TTLStatus { d.ttlMu.RLock(); defer d.ttlMu.RUnlock(); return d.ttlStatus }

func (d *DB) ObserveTTLFailure() {
	d.ttlMu.Lock()
	defer d.ttlMu.Unlock()
	d.ttlStatus.Enabled = true
	d.ttlStatus.LastAttemptAt = time.Now().UTC()
	d.ttlStatus.Failures++
}

// ReadBarrier protects a partition-routed native read with that store's
// leader/barrier, rather than accidentally using shard zero's authority.
func (d *DB) ReadBarrier(timeout time.Duration) error {
	if d.repl == nil {
		return nil
	}
	if !d.repl.IsLeader() {
		return &NotLeaderError{LeaderURL: d.repl.LeaderHTTPAddr()}
	}
	barrier, ok := d.repl.(interface{ Barrier(time.Duration) error })
	if !ok {
		return ErrNotLeader
	}
	return barrier.Barrier(timeout)
}
