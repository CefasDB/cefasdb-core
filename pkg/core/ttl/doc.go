// Package ttl owns the TTL surface plugins and the engine share.
//
// The reaper that physically removes expired rows lives in
// storage; plugins (dedup, freqcap) observe expirations through
// this interface without importing storage internals. Observer
// callbacks run on the reaper's path, so implementations must be
// cheap and non-blocking — the reaper does not wait on observers.
//
// Import-direction rule: ttl imports only pkg/core/model; never
// internal/ and never the engine packages.
//
// The package boundary:
//
//   - Observer: the per-expiration callback contract.
//   - Service: TTL configuration lookup (Attribute) and observer
//     registration (Subscribe).
package ttl
