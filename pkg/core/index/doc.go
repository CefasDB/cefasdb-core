// Package index owns the secondary-index lifecycle contract.
//
// Every plugin-backed index honours these verbs, and the existing
// built-in indexes (GSI, LSI, spatial) already satisfy the surface
// structurally so the engine can route both kinds through one
// implementation. The package keeps engine-side maintenance code
// (storage scans, raft proposals) out of the contract — those
// stay behind the Lifecycle implementation.
//
// Import-direction rule: index imports only pkg/core/model; never
// internal/ and never the engine packages.
//
// The package boundary:
//
//   - Descriptor: names an index and points at the plugin (or
//     built-in) that owns it.
//   - Lifecycle: the verb surface (Create / Describe / Rebuild /
//     Drop) the engine drives.
package index
