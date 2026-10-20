package server

import (
	"context"
	"time"

	"github.com/CefasDb/cefasdb/internal/catalog"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
)

// StartTTLReapers supervises current stores, including shards subsequently
// added by the manager. The metadata shard supplies the canonical catalog;
// each data-shard leader deletes through its own CommitBatch;
// a follower never deletes. The returned stop joins the worker before stores
// close. Calls are installation-opt-in; creating Server alone starts nothing.
func (s *Server) StartTTLReapers(parent context.Context, cfg pebble.ReaperConfig) func() {
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	interval := cfg.Interval
	if interval <= 0 {
		interval = time.Minute
	}
	go func() {
		defer close(done)
		tick := func() {
			canonical, err := catalog.New(s.db)
			if err != nil {
				for _, db := range s.allShards() {
					if db != nil {
						db.ObserveTTLFailure()
					}
				}
				return
			}
			sweep := func(db *pebble.DB, leader pebble.LeaderGate) {
				if db == nil || ctx.Err() != nil {
					return
				}

				// Tick records errors and progress in the store status. No row
				// data/error strings are emitted into operational logs.
				_ = pebble.NewReaper(db, canonical, leader, cfg).Tick(ctx)
			}
			if s.manager == nil {
				if s.cluster != nil {
					sweep(s.db, s.cluster)
				} else {
					sweep(s.db, nil)
				}
				return
			}
			for _, sh := range s.manager.Shards() {
				if sh == nil || sh.Storage == nil {
					continue
				}
				if sh.Raft != nil {
					sweep(sh.Storage, sh.Raft)
				} else {
					sweep(sh.Storage, nil)
				}
			}
		}
		tick()
		timer := time.NewTicker(interval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				tick()
			}
		}
	}()
	return func() { cancel(); <-done }
}
