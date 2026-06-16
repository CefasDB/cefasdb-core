package storage

import "github.com/osvaldoandrade/cefas/pkg/types"

// Writer is the mutation surface of the cefas storage engine. The
// Pebble-backed adapter at internal/storage/adapter/pebble.DB
// implements it; tests can substitute a smaller fake without
// pulling in the Pebble engine.
//
// PutItemWith covers INSERT and UPDATE (read-modify-write through
// Reader.GetItem first when a condition is involved);
// DeleteItemWith covers DELETE.
type Writer interface {
	PutItemWith(td types.TableDescriptor, item types.Item, opts PutOptions) error
	DeleteItemWith(td types.TableDescriptor, key types.Item, opts DeleteOptions) error
}
