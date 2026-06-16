// Package core is the plugin-facing surface every cefas extension
// composes against. It is the single import root sub-packages live
// under so plugins never depend on internal/ packages or the
// engine's concrete types.
//
// The package itself declares no identifiers; the contract is the
// union of its sub-packages. Plugins that want to participate in
// the cefas data plane import only paths rooted here.
//
// Sub-packages:
//
//   - condition: the Evaluator seam shared by every conditional
//     write path (PutItem, DeleteItem, UpdateItem,
//     TransactWriteItems).
//   - index: the Lifecycle verbs (Create / Describe / Rebuild /
//     Drop) every plugin-backed secondary index honours.
//   - model: the stable data-model surface (Item, KeySchema,
//     AttributeValue, table-level descriptors, value-object IDs).
//   - query: the planner surface — Statement, Plan, Planner,
//     ExplainFormat — plus the Top-K and distance-operator
//     abstractions.
//   - query/mmr: the Maximal Marginal Relevance diversification
//     post-rank for TopK candidate sets.
//   - stream: the change-stream surface plugins subscribe against
//     to maintain derived state without polling the base table.
//   - ttl: the TTL surface (Service + Observer) plugins observe
//     expirations through, without importing storage internals.
//
// Import-direction rule: nothing under pkg/core/ may import
// internal/ or the engine packages (pkg/api, pkg/sql, pkg/client).
// The coregraph test pins this invariant.
package core
