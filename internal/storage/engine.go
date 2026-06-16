package storage

// Engine composes Reader and Writer. The Pebble-backed adapter at
// internal/storage/adapter/pebble.DB satisfies it; consumers that
// need both read and write paths can take Engine instead of the
// concrete *pebble.DB.
//
// Engine intentionally stays narrow — backup orchestration,
// changelog stream, TTL reaper, atomic-update batching, plugin
// index maintenance, and the raw Pebble batch APIs continue to
// live on *pebble.DB. Anything those features need from the engine
// surface stays specific to the adapter.
type Engine interface {
	Reader
	Writer
}
