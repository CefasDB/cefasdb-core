# ADR-0004: placement-strategy pattern

- **Status**: accepted
- **Date**: 2026-06-16
- **Phases**: 4a (#326), 4b (#327), 4c (#328)
- **Tracking issue**: #317
- **Parent epic**: #307

## Context

Before phase 4 `internal/cluster/planner.go` was a single 1 191-LOC
file whose `BuildPlacementPlan` dispatcher held every placement
operation the cluster supported. Eight-plus operation paths
(`add-voter`, `remove-voter`, `rebalance`, `split`, `merge`,
`move`, `range-move`, `decommission`, `drain`, `repair`) lived as
arms of a single `switch req.Operation` block, each arm calling
into private helpers defined in the same file.

The shape produced concrete problems:

- Cyclomatic complexity of `BuildPlacementPlan` was well above the
  §9 cap (`gocyclo` / `funlen` floors documented in
  `~/.claude/skills/_shared/THRESHOLDS.md`).
- Adding a new operation required editing `planner.go` to add a
  switch arm plus a private function in the same file — every
  contributor touched the same hot file, creating merge conflicts
  every phase.
- Cross-operation helpers (token-range math, catalog cloning, voter
  diffing, apply scaffolding) were intermixed with operation logic
  so it was unclear which helpers were shared and which belonged to
  one operation only.
- Tests grew into a single `planner_test.go` table where each row
  asserted a different operation; a failure in one row carried no
  information about which operation regressed.

The §9 LOC cap on functions and the §1 cap on per-file LOC were
both being violated, blocking any further work on the placement
surface.

## Decision

Introduce a `PlanStrategy` interface, a registry keyed by
`PlacementOperation`, and one file per operation under
`internal/cluster/plan_*.go`. The dispatcher becomes a one-line
map lookup.

```go
// internal/cluster/plan_strategy.go
type PlanStrategy interface {
    Plan(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error)
}

type planFunc func(PlacementCatalog, PlacementPlanRequest) (PlacementPlan, error)

func (f planFunc) Plan(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
    return f(cat, req)
}

var defaultPlanStrategies = map[PlacementOperation]PlanStrategy{
    PlacementOperationSplit:        planFunc(planSplit),
    PlacementOperationMove:         planFunc(planMove),
    PlacementOperationRangeMove:    planFunc(planRangeMove),
    PlacementOperationDrain:        planFunc(planDrain),
    PlacementOperationDecommission: planFunc(planDecommission),
}
```

`BuildPlacementPlan` collapses to:

```go
func BuildPlacementPlan(cat PlacementCatalog, req PlacementPlanRequest) (PlacementPlan, error) {
    cat.normalize()
    if err := ValidatePlacement(cat); err != nil {
        return PlacementPlan{}, err
    }
    strategy, ok := defaultPlanStrategies[req.Operation]
    if !ok {
        return PlacementPlan{}, invalidPlan("unknown placement operation %q", req.Operation)
    }
    return strategy.Plan(cat, req)
}
```

This is Fowler's *Replace Conditional with Polymorphism* applied to
a switch over an enum.

## Cascade strategy (Strangler Fig)

The pattern landed in three phases. Each PR was small enough to
review on its own and reversible by `git revert` without any
downstream churn.

| Phase | Scope | PR |
|---|---|---|
| 4a | introduce `PlanStrategy` interface, registry, and dispatcher; move first batch of operations into per-file strategies | #326 |
| 4b | drop every `planX` function under the §9 ≤ 40-LOC cap by extracting per-operation helpers | #327 |
| 4c | final residual split: pull `apply.go`, `tokens.go`, `helpers.go` out of `planner.go` to bring the file under the §1 per-file cap | #328 |

## File layout after phase 4

```
internal/cluster/
  planner.go               # 172 LOC: types, dispatcher, shared validators
  plan_strategy.go         #  38 LOC: PlanStrategy interface + registry
  plan_strategy_test.go    #  51 LOC: registry coverage contract
  plan_split.go            # 147 LOC: split strategy
  plan_move.go             #  78 LOC: move strategy
  plan_range_move.go       # 145 LOC: range-move strategy
  plan_drain.go            # 181 LOC: drain strategy
  plan_decommission.go     #  88 LOC: decommission strategy
  apply.go                 # 406 LOC: plan application (separate concern)
  tokens.go                # 171 LOC: token-range arithmetic
  helpers.go               #  85 LOC: cross-strategy utilities
```

`planner.go` went from **1 191 → 172 LOC**. Every `planX` function
sits below the §9 ≤ 40-LOC cap. New operations land as one new file
plus one line in the registry; `planner.go` is not edited.

## Test strategy

- Each strategy file ships a sibling `plan_<op>_test.go` covering
  its own happy path, edge cases, and validation errors.
- `plan_strategy_test.go` holds a registry-coverage contract:
  `TestDefaultPlanStrategiesCoversEveryOperation` asserts that every
  `PlacementOperation*` constant declared in `planner.go` has a
  matching entry in `defaultPlanStrategies`. A new operation that
  lands without a strategy fails the build at test time rather than
  returning a generic "unknown placement operation" error at
  runtime.
- `TestBuildPlacementPlanRejectsUnknownOperation` pins the
  dispatcher behaviour for the negative case.

## Consequences

### Positive

- `planner.go` is below the §1 per-file LOC cap; every `planX`
  function is below the §9 ≤ 40-LOC cap.
- Adding a new placement operation is mechanical: write
  `plan_<op>.go`, add one line to `defaultPlanStrategies`, the
  registry test pins the coverage. No edits to `planner.go` or to
  the dispatcher.
- Per-operation tests fail with per-operation diagnostics. The old
  monolithic table is gone.
- Cross-operation helpers (`apply.go`, `tokens.go`, `helpers.go`)
  are explicitly shared: their separate files document which logic
  belongs to "any plan" versus "this plan".
- Merge conflicts on `planner.go` dropped to near zero because the
  hot file is no longer shared by every contributor.

### Negative

- One indirection at call time: the dispatcher does a map lookup
  before invoking the strategy. The cost is one allocation-free map
  read; not measurable against the planning work itself.
- Five files where there used to be one. Discovery now relies on
  the `plan_*.go` naming convention; `go doc internal/cluster` or
  `grep planSplit` recovers the previous flat view.
- The registry is a package-level `var`. Tests that want to inject
  a fake strategy must do so via a parallel registry, not by
  mutating `defaultPlanStrategies` (mutation would race with
  parallel tests).

### Neutral

- The pattern is closed for the current five operations. Future
  operations (merge, repair, rebalance) plug in without revisiting
  this ADR.

## Alternatives considered

1. **Keep the switch, extract helpers only.** Rejected: would have
   left `BuildPlacementPlan` above the §9 cap and would not have
   reduced the merge-conflict surface on `planner.go`.

2. **One struct per strategy with constructor injection.**
   Rejected for now: the strategies are pure functions of
   `(PlacementCatalog, PlacementPlanRequest)`; wrapping each in a
   struct adds boilerplate without enabling any current call site
   to inject behaviour. `planFunc` keeps the door open — a struct
   with fields satisfies the same `PlanStrategy` interface the day
   one is needed.

3. **Generate the registry from a `//go:generate` directive.**
   Rejected: five operations do not justify code generation. The
   registry-coverage test gives the same safety with no build-step
   complexity.
