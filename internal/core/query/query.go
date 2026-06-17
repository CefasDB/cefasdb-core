package query

import "github.com/CefasDb/cefasdb/internal/core/model"

// Statement is whatever the front-end (SQL parser, structured API,
// CLI) hands the planner. The opaque interface lets the planner
// switch on concrete types without core depending on pkg/sql.
type Statement interface {
	// stmt is a sealing method — only types in pkg/core/query (or
	// satellites that embed CoreStatement) can satisfy this.
	stmt()
}

// CoreStatement is an embeddable seal so user-defined statement
// kinds can implement Statement without re-declaring stmt().
type CoreStatement struct{}

func (CoreStatement) stmt() {}

// Plan is the planner's compiled output. Implementations carry
// engine-specific state; the universal surface is Explain.
type Plan interface {
	Explain(ExplainFormat) string
}

// ExplainFormat selects the rendering used by Plan.Explain.
type ExplainFormat uint8

// ExplainText / ExplainJSON select the explain rendering: the
// indented text outline or the machine-readable JSON document.
const (
	ExplainText ExplainFormat = iota
	ExplainJSON
)

// Planner compiles statements into plans and surfaces explain output.
type Planner interface {
	Plan(Statement) (Plan, error)
	Explain(Plan, ExplainFormat) string
}

// TopKRequest mirrors the high-level Top-K verb. `By` is a textual
// distance expression resolved against the registered distance
// operators (e.g. "cosine(embedding, :q)"). Binds fill in `:name`
// placeholders.
type TopKRequest struct {
	Table string
	By    string
	K     int
	Binds map[string]model.AttributeValue
}

// CoreTopKRequest seals the Statement contract on TopKRequest.
func (TopKRequest) stmt() {}

// DistanceOp is a registered, named distance function. The planner
// resolves operator names (e.g. "cosine", "levenshtein") against a
// registry that maps to a DistanceOp.
type DistanceOp interface {
	// Name returns the canonical operator name used in expressions.
	Name() string

	// Supports reports whether the operator accepts the given
	// attribute kinds. The planner uses Supports to fail fast at
	// plan time instead of at row time.
	Supports(a, b model.AttrType) bool

	// Eval returns the distance between a and b. Implementations
	// must be deterministic — repeated calls with the same inputs
	// must return the same result.
	Eval(a, b model.AttributeValue) (float64, error)
}

// DistanceRegistry is the lookup surface plans use to resolve
// operator names to DistanceOps.
type DistanceRegistry interface {
	// Register installs op under its Name(). Duplicate names error.
	Register(op DistanceOp) error

	// Lookup returns the operator registered under name, or false.
	Lookup(name string) (DistanceOp, bool)

	// List enumerates every registered operator in deterministic
	// (name-sorted) order.
	List() []DistanceOp
}
