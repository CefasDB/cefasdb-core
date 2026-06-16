// Package stream owns the change-stream surface plugins use to
// maintain derived state without polling the base table.
//
// Plugins such as SimHash dedup and MinHash signatures subscribe
// against ChangeStream and observe Events in raft-log order; the
// engine routes committed mutations through the subscription ring
// buffer. Slow subscribers are dropped from the live ring and
// must fall back to a snapshot replay, so OnChange implementations
// must return quickly.
//
// Import-direction rule: stream imports only pkg/core/model;
// never internal/ and never the engine packages.
//
// The package boundary:
//
//   - Op + OpUnspecified / OpPut / OpDelete: the change-event
//     classification.
//   - Event: one committed mutation with its raft index, key, and
//     before/after item snapshots.
//   - Subscriber: the OnChange callback contract.
//   - ChangeStream: the engine-side seam plugins call Subscribe
//     against.
package stream
