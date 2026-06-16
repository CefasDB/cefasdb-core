// Item-resource client surface.
//
// PutItem / GetItem / UpdateItem / DeleteItem and the batch variants
// expose the single-row CRUD shape of the SDK on top of the typed
// pkg/types.Item model.
package client

import (
	"context"
	"fmt"

	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// PutOptions exposes the same surface as storage.PutOptions.
type PutOptions struct {
	Condition string
	Binds     map[string]types.AttributeValue
}

// PutItem upserts `item` into `table`. Returns wrapped gRPC errors;
// use errors.Is against the package sentinels (ErrConditionFailed,
// ErrNotLeader) to branch.
func (c *Client) PutItem(ctx context.Context, table string, item types.Item, opts ...PutOptions) error {
	var o PutOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := c.stub.PutItem(c.withAuth(ctx), &cefaspb.PutItemRequest{
		Table:     table,
		Item:      itemAttrMap(item),
		Condition: o.Condition,
		Binds:     itemAttrMap(types.Item(o.Binds)),
	})
	return err
}

// GetOptions toggles consistency.
type GetOptions struct {
	Strong bool
}

// GetItem returns the item, or (nil, nil) when the key is absent.
func (c *Client) GetItem(ctx context.Context, table string, key types.Item, opts ...GetOptions) (types.Item, error) {
	var o GetOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	cons := cefaspb.Consistency_CONSISTENCY_EVENTUAL
	if o.Strong {
		cons = cefaspb.Consistency_CONSISTENCY_STRONG
	}
	resp, err := c.stub.GetItem(c.withAuth(ctx), &cefaspb.GetItemRequest{
		Table:       table,
		Key:         itemAttrMap(key),
		Consistency: cons,
	})
	if err != nil {
		return nil, err
	}
	if !resp.GetFound() {
		return nil, nil
	}
	return itemFromPB(resp.GetItem()), nil
}

// UpdateOptions carries the aws-shaped UpdateItem accessories.
type UpdateOptions struct {
	UpdateExpression          string
	ConditionExpression       string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]types.AttributeValue
	// ReturnValues: "" | "NONE" | "ALL_NEW" | "ALL_OLD" | "UPDATED_NEW" | "UPDATED_OLD".
	ReturnValues string
}

// UpdateItem applies the supplied UpdateExpression against the row
// keyed by `key`. Returns the requested image (NEW / OLD) when
// ReturnValues asks for one, nil otherwise.
func (c *Client) UpdateItem(ctx context.Context, table string, key types.Item, opts UpdateOptions) (types.Item, error) {
	rv := cefaspb.ReturnValues_RETURN_VALUES_NONE
	switch opts.ReturnValues {
	case "ALL_NEW":
		rv = cefaspb.ReturnValues_RETURN_VALUES_ALL_NEW
	case "ALL_OLD":
		rv = cefaspb.ReturnValues_RETURN_VALUES_ALL_OLD
	case "UPDATED_NEW":
		rv = cefaspb.ReturnValues_RETURN_VALUES_UPDATED_NEW
	case "UPDATED_OLD":
		rv = cefaspb.ReturnValues_RETURN_VALUES_UPDATED_OLD
	}
	resp, err := c.stub.UpdateItem(c.withAuth(ctx), &cefaspb.UpdateItemRequest{
		Table:                     table,
		Key:                       itemAttrMap(key),
		UpdateExpression:          opts.UpdateExpression,
		ConditionExpression:       opts.ConditionExpression,
		ExpressionAttributeNames:  opts.ExpressionAttributeNames,
		ExpressionAttributeValues: itemAttrMap(types.Item(opts.ExpressionAttributeValues)),
		ReturnValues:              rv,
	})
	if err != nil {
		return nil, err
	}
	if len(resp.GetAttributes()) == 0 {
		return nil, nil
	}
	return itemFromPB(resp.GetAttributes()), nil
}

// DeleteOptions mirrors PutOptions for deletes.
type DeleteOptions struct {
	Condition string
	Binds     map[string]types.AttributeValue
}

// DeleteItem removes the item identified by `key`.
func (c *Client) DeleteItem(ctx context.Context, table string, key types.Item, opts ...DeleteOptions) error {
	var o DeleteOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := c.stub.DeleteItem(c.withAuth(ctx), &cefaspb.DeleteItemRequest{
		Table:     table,
		Key:       itemAttrMap(key),
		Condition: o.Condition,
		Binds:     itemAttrMap(types.Item(o.Binds)),
	})
	return err
}

// ---------- batch ----------

// BatchWriteOp is the SDK-facing batch op type. Exactly one of Item /
// Key is populated.
type BatchWriteOp struct {
	Put    types.Item
	Delete types.Item
}

// BatchWriteItem applies N puts/deletes atomically.
func (c *Client) BatchWriteItem(ctx context.Context, table string, ops []BatchWriteOp) error {
	pbOps := make([]*cefaspb.BatchWriteOp, 0, len(ops))
	for i, o := range ops {
		switch {
		case o.Put != nil:
			pbOps = append(pbOps, &cefaspb.BatchWriteOp{Kind: cefaspb.BatchWriteOp_KIND_PUT, Item: itemAttrMap(o.Put)})
		case o.Delete != nil:
			pbOps = append(pbOps, &cefaspb.BatchWriteOp{Kind: cefaspb.BatchWriteOp_KIND_DELETE, Key: itemAttrMap(o.Delete)})
		default:
			return fmt.Errorf("op %d: neither Put nor Delete set", i)
		}
	}
	_, err := c.stub.BatchWriteItem(c.withAuth(ctx), &cefaspb.BatchWriteItemRequest{Table: table, Ops: pbOps})
	return err
}

// BatchGetItem fetches multiple items by primary key.
func (c *Client) BatchGetItem(ctx context.Context, table string, keys []types.Item) ([]types.Item, error) {
	pbKeys := make([]*cefaspb.KeyMap, 0, len(keys))
	for _, k := range keys {
		pbKeys = append(pbKeys, &cefaspb.KeyMap{Attributes: itemAttrMap(k)})
	}
	resp, err := c.stub.BatchGetItem(c.withAuth(ctx), &cefaspb.BatchGetItemRequest{Table: table, Keys: pbKeys})
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
