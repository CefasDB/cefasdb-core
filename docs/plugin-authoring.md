# Plugin authoring guide

A CEFAS plugin is any Go type that implements one of the four
contracts in `pkg/plugin/interfaces.go` and registers itself against
`plugin.Default` via `init()`. v1 ships in-process plugins only —
out-of-process / dlopen-based loading is out of scope.

## TL;DR

1. Pick the right kind: Index, Distance, Estimator, or Audience.
2. Declare `Manifest()` + the kind-specific methods.
3. Register via `init()`.
4. Add a blank import in `pkg/plugin/builtins/builtins.go`.
5. Write a unit test using `pkg/plugin/testharness`.

## 1. Pick the kind

| Kind | Use for | Methods to implement |
|---|---|---|
| `KindIndex` | secondary indexes, membership tests | Build / Update / Delete / Query / Estimate |
| `KindDistance` | similarity / distance scalars | Name / Supports / Eval |
| `KindEstimator` | approximate aggregates (cardinality, frequency) | Observe / Estimate / Merge |
| `KindAudience` | composite ads workflows (select / dedup / freqcap) | Select / Estimate / Dedup / FreqCap |

## 2. Skeleton

```go
package mypin // pkg/plugin/mypin

import (
  "github.com/osvaldoandrade/cefas/pkg/core/model"
  "github.com/osvaldoandrade/cefas/pkg/plugin"
)

type Op struct{}

func (Op) Manifest() plugin.Manifest {
  return plugin.Manifest{
    Name: "mypin",
    Kind: plugin.KindDistance,
    Version: "1",
    Description: "what this does in one sentence",
  }
}

func (Op) Name() string                         { return "mypin" }
func (Op) Supports(a, b model.AttrType) bool    { return a == model.AttrS && b == model.AttrS }
func (Op) Eval(a, b model.AttributeValue) (float64, error) {
  // … your scoring math …
  return 0, nil
}

func init() { plugin.Default.MustRegister(Op{}) }
```

## 3. Configuration

Index plugins accept an opaque JSON blob via
`index.Descriptor.PluginConfig`. Decode it the first time the
descriptor is seen and cache the parsed form:

```go
type Config struct {
  Field string `json:"field"`
  N     int    `json:"n,omitempty"`
}

func parseConfig(raw []byte) (Config, error) {
  var c Config
  if len(raw) > 0 {
    if err := json.Unmarshal(raw, &c); err != nil {
      return c, fmt.Errorf("mypin: parse config: %w", err)
    }
  }
  if c.Field == "" {
    return c, fmt.Errorf("mypin: config.field required")
  }
  if c.N == 0 { c.N = 16 }
  return c, nil
}
```

## 4. State per descriptor

Index plugins typically keep one state object per
`(table, index_name)`:

```go
type Plugin struct {
  mu     sync.Mutex
  states map[string]*State
}

func (p *Plugin) Build(d index.Descriptor, items func(yield func(model.Item) bool)) error {
  cfg, err := parseConfig(d.PluginConfig)
  if err != nil { return err }
  fresh := newState(cfg, d.KeySchema)
  items(func(it model.Item) bool {
    id, ok := pkid.Of(it, d.KeySchema)
    if !ok { return true }
    fresh.addLocked(id, it)
    return true
  })
  key := d.Table + "/" + d.Name
  p.mu.Lock()
  p.states[key] = fresh
  p.mu.Unlock()
  return nil
}
```

Reuse `pkg/plugin/internal/pkid` for stable id extraction so every
plugin agrees on the identifier for the same item.

## 5. Persistence

v1 plugins keep state in memory. When persistence matters:

- For dedup / TTL state, subscribe to the TTL reaper via
  `pkg/core/ttl.Observer`.
- For full plugin state, serialize via the documented binary format
  alongside the catalog (this seam is partially wired through
  `plugin.IndexService`; see #122/#123 follow-up work).

## 6. Test harness

Plugin tests should not boot the server. Use `pkg/plugin/testharness`:

```go
func TestMyPluginBuildAndQuery(t *testing.T) {
  h := testharness.New(t)
  h.MustRegister(&MyIndex{})
  h.SeedTable("Users", model.Item{
    "pk":   {T: model.AttrS, S: "u1"},
    "name": {T: model.AttrS, S: "ova"},
  })
  if err := h.BuildIndex(index.Descriptor{
    Table: "Users", Name: "x", PluginName: "myidx",
    PluginConfig: []byte(`{"field":"name"}`),
    KeySchema:    model.KeySchema{PK: "pk"},
  }); err != nil {
    t.Fatalf("build: %v", err)
  }
  // … assert candidate set, estimate, etc. …
}
```

## 7. Wire into the server

Built-in plugins compile into `cefas-server` via `pkg/plugin/builtins`.
Add a blank import:

```go
// pkg/plugin/builtins/builtins.go
import (
  // … existing builtins …
  _ "github.com/osvaldoandrade/cefas/pkg/plugin/mypin"
)
```

After the next `cefas-server` build, `cefas list-plugins` surfaces it.

## 8. Boundary guard

If your plugin imports anything under `internal/*`, `pkg/api`,
`pkg/sql`, or `pkg/client`, the build fails:

```
$ go test ./pkg/plugin/...
--- FAIL: TestPluginHasNoEngineImports
    plugingraph_test.go:NN: …/mypin/mypin.go imports forbidden package …
```

Move whatever you need into `pkg/core/*` first, then re-import.
