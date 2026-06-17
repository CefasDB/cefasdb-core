package storage

import "github.com/CefasDb/cefasdb/pkg/types"

// PutOptions controls optional behaviour for PutItem-style writes.
type PutOptions struct {
	// Condition, when non-empty, is evaluated against the prior item
	// before the write. If it returns false the write is aborted with
	// ErrConditionFailed. Empty means "no precondition".
	Condition string
	// Binds resolves :name placeholders in Condition.
	Binds map[string]types.AttributeValue
}

// DeleteOptions mirrors PutOptions for deletes.
type DeleteOptions struct {
	Condition string
	Binds     map[string]types.AttributeValue
}

// QueryOptions configures a Query / QueryByGSI call.
type QueryOptions struct {
	// SKLow / SKHigh, when their type is not AttrNull, constrain the
	// sort key to [SKLow, SKHigh).
	SKLow  types.AttributeValue
	SKHigh types.AttributeValue
	// Limit ≤ 0 means no limit.
	Limit int
}

// BatchOp describes a single mutation inside a BatchWriteItem call.
// Exactly one of Item / Key is set; Op selects which.
type BatchOp struct {
	Op   BatchOpKind
	Item types.Item
	Key  types.Item // for delete: just the key attributes
}

// BatchOpKind enumerates the mutation kinds a BatchOp can carry.
type BatchOpKind uint8

// BatchOp kind constants used by BatchWriteItem callers.
const (
	BatchOpPut BatchOpKind = iota + 1
	BatchOpDelete
)
