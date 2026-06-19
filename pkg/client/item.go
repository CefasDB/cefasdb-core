package client

import (
	"context"
	"fmt"
	"time"

	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
	"github.com/CefasDb/cefasdb/pkg/types"
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
	Strong     bool
	RouteAware *bool
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
	req := &cefaspb.GetItemRequest{
		Table:       table,
		Key:         itemAttrMap(key),
		Consistency: cons,
	}
	if !o.Strong && c.routeReads != nil && routeAwareOverride(o.RouteAware) {
		return c.routeAwareGetItem(ctx, table, key, req)
	}
	resp, err := c.stub.GetItem(c.withAuth(ctx), req)
	if err != nil {
		return nil, err
	}
	if !resp.GetFound() {
		return nil, nil
	}
	return itemFromPB(resp.GetItem()), nil
}

func (c *Client) routeAwareGetItem(ctx context.Context, table string, key types.Item, req *cefaspb.GetItemRequest) (types.Item, error) {
	c.routeReads.attempts.Add(1)
	pkBytes, err := c.pkBytesForKey(ctx, table, key)
	if err != nil {
		return nil, err
	}
	if err := c.ensureRoutePlacement(ctx, false); err != nil {
		return nil, err
	}

	var last error
	refreshed := false
	for {
		target, token, err := c.routeReads.routeForPK(pkBytes)
		if err != nil {
			return nil, err
		}
		candidates, err := c.routeReads.candidatesForTarget(target, token, time.Now())
		if err != nil {
			return nil, err
		}
		for _, node := range candidates {
			start := node.begin()
			resp, err := node.stub.GetItem(c.withAuth(ctx), req)
			node.finish(start, err)
			if err == nil {
				c.routeReads.successes.Add(1)
				c.routeReads.observeServedBy(target, node.id)
				if !resp.GetFound() {
					return nil, nil
				}
				return itemFromPB(resp.GetItem()), nil
			}
			last = err
			if !c.routeReads.observeRetry(err) {
				return nil, err
			}
		}
		if refreshed {
			break
		}
		if err := c.ensureRoutePlacement(ctx, true); err != nil {
			return nil, err
		}
		refreshed = true
	}
	if last != nil {
		return nil, last
	}
	return nil, fmt.Errorf("route-aware reads: shard has no reachable candidates")
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

type BatchGetOptions struct {
	RouteAware *bool
}

// BatchGetItem fetches multiple items by primary key.
func (c *Client) BatchGetItem(ctx context.Context, table string, keys []types.Item, opts ...BatchGetOptions) ([]types.Item, error) {
	var o BatchGetOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	if c.routeReads != nil && routeAwareOverride(o.RouteAware) {
		return c.routeAwareBatchGetItem(ctx, table, keys)
	}
	return c.batchGetItemWithStub(ctx, c.stub, table, keys)
}

func (c *Client) batchGetItemWithStub(ctx context.Context, stub cefaspb.CefasClient, table string, keys []types.Item) ([]types.Item, error) {
	pbKeys := make([]*cefaspb.KeyMap, 0, len(keys))
	for _, k := range keys {
		pbKeys = append(pbKeys, &cefaspb.KeyMap{Attributes: itemAttrMap(k)})
	}
	resp, err := stub.BatchGetItem(c.withAuth(ctx), &cefaspb.BatchGetItemRequest{Table: table, Keys: pbKeys})
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

func (c *Client) routeAwareBatchGetItem(ctx context.Context, table string, keys []types.Item) ([]types.Item, error) {
	c.routeReads.attempts.Add(1)
	if len(keys) == 0 {
		c.routeReads.successes.Add(1)
		return []types.Item{}, nil
	}
	pkBytes := make([][]byte, len(keys))
	for i, key := range keys {
		b, err := c.pkBytesForKey(ctx, table, key)
		if err != nil {
			return nil, fmt.Errorf("key %d: %w", i, err)
		}
		pkBytes[i] = b
	}
	if err := c.ensureRoutePlacement(ctx, false); err != nil {
		return nil, err
	}

	var last error
	refreshed := false
	for {
		out, err := c.routeAwareBatchGetAttempt(ctx, table, keys, pkBytes)
		if err == nil {
			c.routeReads.successes.Add(1)
			return out, nil
		}
		last = err
		if !c.routeReads.observeRetry(err) || refreshed {
			break
		}
		if err := c.ensureRoutePlacement(ctx, true); err != nil {
			return nil, err
		}
		refreshed = true
	}
	return nil, last
}

func (c *Client) routeAwareBatchGetAttempt(ctx context.Context, table string, keys []types.Item, pkBytes [][]byte) ([]types.Item, error) {
	type group struct {
		node           *routeNode
		leaderServed   int
		followerServed int
		indexes        []int
		keys           []types.Item
	}
	groups := map[string]*group{}
	order := []string{}
	for i, b := range pkBytes {
		target, token, err := c.routeReads.routeForPK(b)
		if err != nil {
			return nil, err
		}
		candidates, err := c.routeReads.candidatesForTarget(target, token, time.Now())
		if err != nil {
			return nil, err
		}
		node := candidates[0]
		g := groups[node.id]
		if g == nil {
			g = &group{node: node}
			groups[node.id] = g
			order = append(order, node.id)
		}
		if target.leader != "" && target.leader == node.id {
			g.leaderServed++
		} else {
			g.followerServed++
		}
		g.indexes = append(g.indexes, i)
		g.keys = append(g.keys, keys[i])
	}

	out := make([]types.Item, len(keys))
	for _, nodeID := range order {
		g := groups[nodeID]
		start := g.node.begin()
		items, err := c.batchGetItemWithStub(ctx, g.node.stub, table, g.keys)
		g.node.finish(start, err)
		if err != nil {
			return nil, err
		}
		c.routeReads.leaderServed.Add(uint64(g.leaderServed))
		c.routeReads.followerServed.Add(uint64(g.followerServed))
		for i, item := range items {
			if i >= len(g.indexes) {
				break
			}
			out[g.indexes[i]] = item
		}
	}
	return out, nil
}
