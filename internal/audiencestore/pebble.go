// Package audiencestore bridges the storage engine and the audience
// plugin so the plugin can stay engine-agnostic (pkg/plugin/*
// forbids internal imports per pkg/plugin/plugingraph_test.go).
//
// Callers wire the bridge once at startup:
//
//	be := audiencestore.NewBackend(db)
//	store, _ := audience.NewDurableStore(audience.StoreOptions{ID: "ads", Backend: be})
//	store.Start(ctx)
//	plug.SetStore(store)
package audiencestore

import (
	"errors"

	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/plugin/audience"
)

// NewBackend returns an audience.Backend that persists state into
// the cefas storage engine. Reads stay local; writes flow through
// whatever replicator the DB was opened with (Raft in production,
// direct commit in single-node mode).
func NewBackend(db *pebble.DB) audience.Backend {
	return &backend{db: db}
}

type backend struct{ db *pebble.DB }

func (b *backend) Get(key []byte) ([]byte, bool, error) {
	v, err := b.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return v, true, nil
}

func (b *backend) Set(key, value []byte) error { return b.db.Set(key, value) }
func (b *backend) Delete(key []byte) error     { return b.db.Delete(key) }

func (b *backend) Scan(lower, upper []byte, fn func(k, v []byte) bool) error {
	it, err := b.db.Iter(lower, upper)
	if err != nil {
		return err
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		k := it.Key()
		v := it.Value()
		// Pebble reuses iterator slices across Next — copy before fn.
		kk := make([]byte, len(k))
		copy(kk, k)
		vv := make([]byte, len(v))
		copy(vv, v)
		if !fn(kk, vv) {
			break
		}
	}
	return it.Error()
}
