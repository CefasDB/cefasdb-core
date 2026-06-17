package client

import (
	"context"
	"fmt"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
)

// TransactKind selects the wire op for a TransactWriteOp.
type TransactKind uint8

const (
	// TransactPut puts Item into Table at the row keyed by Item's primary key.
	TransactPut TransactKind = iota + 1
	// TransactDelete deletes the row in Table keyed by Key.
	TransactDelete
	// TransactConditionCheck asserts ConditionExpression against the row
	// keyed by Key in Table without mutating it.
	TransactConditionCheck
)

// TransactWriteOp mirrors one entry in the AWS TransactWriteItems
// shape. Exactly one of Item / Key is set depending on Kind.
type TransactWriteOp struct {
	Kind                TransactKind
	Table               string
	Item                types.Item // Put
	Key                 types.Item // Delete / ConditionCheck
	ConditionExpression string
	Binds               map[string]types.AttributeValue
}

// TransactWriteItems applies up to 100 ops atomically. v1 requires
// every op to reference the same table — cross-table / cross-shard
// transactions are out of scope (issue #80 tracks the 2PC follow-up).
func (c *Client) TransactWriteItems(ctx context.Context, ops []TransactWriteOp) error {
	wire := make([]*cefaspb.TransactWriteOp, 0, len(ops))
	for _, op := range ops {
		w := &cefaspb.TransactWriteOp{
			ConditionExpression: op.ConditionExpression,
			Binds:               itemAttrMap(types.Item(op.Binds)),
		}
		switch op.Kind {
		case TransactPut:
			w.Op = &cefaspb.TransactWriteOp_Put_{Put: &cefaspb.TransactWriteOp_Put{
				Table: op.Table, Item: itemAttrMap(op.Item),
			}}
		case TransactDelete:
			w.Op = &cefaspb.TransactWriteOp_Delete_{Delete: &cefaspb.TransactWriteOp_Delete{
				Table: op.Table, Key: itemAttrMap(op.Key),
			}}
		case TransactConditionCheck:
			w.Op = &cefaspb.TransactWriteOp_ConditionCheck_{ConditionCheck: &cefaspb.TransactWriteOp_ConditionCheck{
				Table: op.Table, Key: itemAttrMap(op.Key),
			}}
		default:
			return fmt.Errorf("transact op: missing kind")
		}
		wire = append(wire, w)
	}
	_, err := c.stub.TransactWriteItems(c.withAuth(ctx), &cefaspb.TransactWriteItemsRequest{Ops: wire})
	return err
}

// TransactGet is one entry in TransactGetItems.
type TransactGet struct {
	Table string
	Key   types.Item
}

// TransactGetItems returns each requested item; index alignment with
// the request is preserved (nil for items that didn't exist). v1
// single-table; cross-table is rejected server-side.
func (c *Client) TransactGetItems(ctx context.Context, items []TransactGet) ([]types.Item, error) {
	wire := make([]*cefaspb.TransactGet, 0, len(items))
	for _, it := range items {
		wire = append(wire, &cefaspb.TransactGet{Table: it.Table, Key: itemAttrMap(it.Key)})
	}
	resp, err := c.stub.TransactGetItems(c.withAuth(ctx), &cefaspb.TransactGetItemsRequest{Items: wire})
	if err != nil {
		return nil, err
	}
	out := make([]types.Item, len(resp.GetItems()))
	for i, it := range resp.GetItems() {
		if len(it.GetAttributes()) == 0 {
			out[i] = nil
			continue
		}
		out[i] = itemFromPB(it.GetAttributes())
	}
	return out, nil
}
