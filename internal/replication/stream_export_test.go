package replication

import (
	pebbledb "github.com/cockroachdb/pebble"
)

// ApplyAndPublishForTest exposes the package-private applyAndPublish
// so the cdc tests can drive it without spinning up a full raft
// instance.
func ApplyAndPublishForTest(db *pebbledb.DB, repr []byte, raftIndex uint64, pub *Publisher) error {
	return applyAndPublish(db, repr, raftIndex, pub)
}
