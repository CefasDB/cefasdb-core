// Package model owns the stable data-model surface every cefas
// plugin depends on.
//
// It re-exports the canonical attribute / item / table types from
// pkg/types so plugins never reach into pkg/types (or any
// internal package) directly, and it declares the value-object
// ID types (ShardID, NodeID, StreamShardID, TableID) that replace
// the primitive-typed IDs used across pkg/api / internal/cluster /
// internal/metrics.
//
// Import-direction rule: model imports only pkg/types; never
// internal/ and never the engine packages.
//
// The package boundary:
//
//   - Item / AttributeValue / AttrType / KeySchema /
//     TableDescriptor: aliases for the canonical data-model types.
//   - ShardID / NodeID / StreamShardID / TableID: value-object IDs
//     with validating constructors and stable wire forms.
//   - UnroutedShardID / StreamShardIDSingle: the canonical
//     sentinel values for unrouted requests and the legacy
//     single-shard stream identifier.
package model
