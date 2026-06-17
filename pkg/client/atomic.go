package client

import (
	"context"
	"fmt"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// AtomicActionKind selects which mutator AtomicUpdate runs against
// the addressed attribute. The kinds mirror storage.AtomicActionKind
// one-to-one; see internal/storage/atomic.go for the contract.
type AtomicActionKind uint8

const (
	// AtomicSet overwrites Attribute with Value verbatim.
	AtomicSet AtomicActionKind = iota + 1
	// AtomicIncrReturn adds Value (numeric) to Attribute and reports
	// the new value in AtomicResult.Returned. Equivalent to Redis INCRBY.
	AtomicIncrReturn
	// AtomicAddReturn is the DynamoDB-style alias for AtomicIncrReturn.
	AtomicAddReturn
	// AtomicApply evaluates Expression against the prior item state
	// and assigns the result to Attribute. See storage docs for the
	// whitelisted grammar.
	AtomicApply
)

// AtomicAction is one mutation step inside an AtomicUpdate.
type AtomicAction struct {
	Kind       AtomicActionKind
	Attribute  string
	Value      types.AttributeValue
	Expression string
}

// AtomicOptions bundles the precondition + actions for one AtomicUpdate.
type AtomicOptions struct {
	Condition string
	Binds     map[string]types.AttributeValue
	Actions   []AtomicAction
}

// AtomicResult mirrors storage.AtomicResult on the wire.
type AtomicResult struct {
	Item     types.Item
	Returned []types.AttributeValue
	Created  bool
}

// AtomicUpdate runs the supplied actions against the row keyed by
// `key`. The call returns the post-image plus a per-action returned
// value so a contended counter never needs a follow-up GetItem.
//
// Example — atomic counter increment that returns the new value:
//
//	res, err := c.AtomicUpdate(ctx, "Counters",
//	    types.Item{"id": types.AttributeValue{T: types.AttrS, S: "page_views"}},
//	    client.AtomicOptions{Actions: []client.AtomicAction{{
//	        Kind:      client.AtomicIncrReturn,
//	        Attribute: "count",
//	        Value:     types.AttributeValue{T: types.AttrN, N: "1"},
//	    }}})
//	if err != nil { return err }
//	fmt.Println("new count:", res.Returned[0].N)
func (c *Client) AtomicUpdate(ctx context.Context, table string, key types.Item, opts AtomicOptions) (AtomicResult, error) {
	stub := cefaspb.NewCefasAtomicClient(c.conn)
	actions := make([]*cefaspb.AtomicAction, 0, len(opts.Actions))
	for i, a := range opts.Actions {
		kind, err := atomicKindToPB(a.Kind)
		if err != nil {
			return AtomicResult{}, fmt.Errorf("action %d: %w", i, err)
		}
		// APPLY carries no Value; skip the pb encode so the wire field
		// stays nil and the server's pbToAtomicActions short-circuits.
		var val *cefaspb.AttributeValue
		if a.Kind != AtomicApply {
			val = attrToPB(a.Value)
		}
		actions = append(actions, &cefaspb.AtomicAction{
			Kind:       kind,
			Attribute:  a.Attribute,
			Value:      val,
			Expression: a.Expression,
		})
	}
	resp, err := stub.AtomicUpdate(c.withAuth(ctx), &cefaspb.AtomicUpdateRequest{
		Table:     table,
		Key:       itemAttrMap(key),
		Condition: opts.Condition,
		Binds:     itemAttrMap(types.Item(opts.Binds)),
		Actions:   actions,
	})
	if err != nil {
		return AtomicResult{}, err
	}
	returned := make([]types.AttributeValue, len(resp.GetReturnedValues()))
	for i, v := range resp.GetReturnedValues() {
		returned[i] = attrFromPB(v)
	}
	return AtomicResult{
		Item:     itemFromPB(resp.GetItem()),
		Returned: returned,
		Created:  resp.GetCreated(),
	}, nil
}

func atomicKindToPB(k AtomicActionKind) (cefaspb.AtomicActionKind, error) {
	switch k {
	case AtomicSet:
		return cefaspb.AtomicActionKind_ATOMIC_SET, nil
	case AtomicIncrReturn:
		return cefaspb.AtomicActionKind_ATOMIC_INCR_RETURN, nil
	case AtomicAddReturn:
		return cefaspb.AtomicActionKind_ATOMIC_ADD_RETURN, nil
	case AtomicApply:
		return cefaspb.AtomicActionKind_ATOMIC_APPLY, nil
	}
	return cefaspb.AtomicActionKind_ATOMIC_ACTION_UNSPECIFIED, fmt.Errorf("unsupported AtomicActionKind %d", k)
}
