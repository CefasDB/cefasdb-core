// Package condition owns the conditional-write evaluator seam.
//
// The cefas engine and every plugin that participates in a
// conditional write (PutItem, DeleteItem, UpdateItem,
// TransactWriteItems) share one Evaluator implementation routed
// through this interface. The package keeps the DynamoDB-shaped
// expression grammar at the boundary so storage backends and
// plugins never re-parse it.
//
// Import-direction rule: condition imports only pkg/core/model;
// never internal/ and never the engine packages.
//
// The package boundary:
//
//   - Evaluator: evaluates a condition expression against an item
//     and a bind map, returning the boolean result.
package condition
