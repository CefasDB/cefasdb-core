// Package condition defines the ConditionEvaluator surface used by
// every write path that accepts a ConditionExpression (PutItem,
// DeleteItem, UpdateItem, TransactWriteItems). Plugins and the engine
// share one evaluator implementation; this interface is the seam.
package condition

import "github.com/osvaldoandrade/cefas/pkg/core/model"

// Evaluator evaluates a DynamoDB-shaped condition expression against
// an item and a bind map. Empty expressions evaluate to true.
//
// Bind keys are stored without the leading `:` (the wire form keeps
// the colon; the gRPC handler strips it before invoking the
// evaluator).
type Evaluator interface {
	// Evaluate returns whether `expr` holds for `item` given `binds`.
	// `item` may be nil — `attribute_exists(x)` is then false.
	Evaluate(expr string, item model.Item, binds map[string]model.AttributeValue) (bool, error)
}
