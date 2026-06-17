package ttl

import "github.com/CefasDb/cefasdb/internal/core/model"

// Observer is notified whenever the reaper removes an expired row.
// Implementations must be cheap + non-blocking — the reaper does not
// wait on observers.
type Observer interface {
	OnExpire(table string, key model.Item)
}

// Service exposes TTL configuration + observer registration.
type Service interface {
	// Attribute returns the attribute name configured as the TTL
	// column for `table`, or "" when TTL is disabled.
	Attribute(table string) string

	// Subscribe registers an observer for every future expiration.
	// Returns a cancel func that removes the registration.
	Subscribe(Observer) (cancel func())
}
