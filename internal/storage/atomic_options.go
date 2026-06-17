package storage

import "github.com/CefasDb/cefasdb/pkg/types"

// AtomicActionKind selects which mutator the action applies.
type AtomicActionKind uint8

// Atomic-action kind constants.
const (
	// AtomicActionSet overwrites Attribute with Value verbatim.
	AtomicActionSet AtomicActionKind = iota + 1
	// AtomicActionIncrReturn adds Value (numeric) to Attribute and
	// returns the new value. Equivalent to Redis INCRBY.
	AtomicActionIncrReturn
	// AtomicActionAddReturn is an alias for AtomicActionIncrReturn —
	// shipped because the issue uses both names and callers that
	// already think in DynamoDB ADD-action terms can keep that
	// vocabulary.
	AtomicActionAddReturn
	// AtomicActionApply evaluates Expression server-side and assigns
	// the result to Attribute. Whitelisted grammar only.
	AtomicActionApply
)

// AtomicAction describes one mutation inside an AtomicUpdate.
type AtomicAction struct {
	Kind       AtomicActionKind
	Attribute  string
	Value      types.AttributeValue // SET / INCR_RETURN / ADD_RETURN
	Expression string               // APPLY only
}

// AtomicOptions bundles the precondition + actions for one AtomicUpdate.
type AtomicOptions struct {
	// Condition is the optional ConditionExpression — same grammar
	// as PutItem / DeleteItem.
	Condition string
	Binds     map[string]types.AttributeValue
	// Actions are applied in declaration order against the snapshot
	// of the item taken before the batch is built.
	Actions []AtomicAction
}

// AtomicResult carries the post-image of the item and a per-action
// returned value (currently populated for INCR_RETURN / ADD_RETURN /
// APPLY — the new value of the affected attribute).
type AtomicResult struct {
	Item     types.Item
	OldItem  types.Item
	Returned []types.AttributeValue
	Created  bool // true when the item did not exist prior
}
