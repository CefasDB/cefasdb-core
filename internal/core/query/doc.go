// Package query owns the query-planner surface and the shared
// Top-K / distance-operator abstractions.
//
// The cefas SQL package and every plugin compose against the
// interfaces declared here; no concrete planner implementation
// lives in this package. The explain rendering and the
// hybrid-ANN-+-WHERE planning helpers (strategy selection,
// overscan factor, selectivity bookkeeping) are also kept here so
// the SQL planner and plugin-driven planners share one wire
// shape.
//
// Import-direction rule: query imports only pkg/core/model;
// never internal/ and never the engine packages.
//
// The package boundary:
//
//   - Statement / CoreStatement / Plan / Planner: the planner
//     interface and the seal types statements embed.
//   - ExplainFormat / PlanNode / RenderExplain: the on-the-wire
//     explain format and the renderer that emits it.
//   - DistanceOp / DistanceRegistry / NewDistanceRegistry: the
//     distance-operator contract and its in-memory registry.
//   - TopKEngine / TopKRequest / TopKResult / NewTopK: the
//     streaming Top-K abstraction.
//   - ANNFilterPlan / ANNFilterStrategy / ChooseStrategy /
//     OverscanFactor / Predicate / Selectivity / ApplyPredicate /
//     FewerThanKWarning / FilterFirstSelectivityThreshold /
//     MaxOverscanFactor: the hybrid-ANN-+-WHERE planning helpers.
package query
