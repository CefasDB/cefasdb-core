package server

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/auth"
	"github.com/osvaldoandrade/cefas/internal/storage"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	"github.com/osvaldoandrade/cefas/internal/tracing"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/protocol"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

// awsTransactLimit mirrors DynamoDB's hard cap on a single transaction.
const awsTransactLimit = 100

// TransactWriteItems applies up to 100 mutations atomically. v1
// restricts the batch to a single table — that path lands in one
// pebble.Batch + one raft entry (when raft is attached). Mixed-table
// or cross-shard transactions are explicitly rejected; the issue
// (#80) tracks the multi-shard 2PC design as follow-up work.
func (s *GRPCServer) TransactWriteItems(ctx context.Context, req *cefaspb.TransactWriteItemsRequest) (*cefaspb.TransactWriteItemsResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "TransactWriteItems")
	defer span.End()
	ops := req.GetOps()
	if len(ops) == 0 {
		return &cefaspb.TransactWriteItemsResponse{}, nil
	}
	if len(ops) > awsTransactLimit {
		return nil, status.Errorf(codes.InvalidArgument, "transact: %d ops exceeds the per-call limit of %d", len(ops), awsTransactLimit)
	}

	table, err := singleTransactTable(ops)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemWrite, table),
		auth.WildcardScope(auth.ScopeItemWrite)); err != nil {
		return nil, err
	}

	td, err := s.cat.Describe(table)
	if err != nil {
		return nil, mapStorageErr(err)
	}

	// Two-pass: pre-flight every ConditionExpression (Put / Delete /
	// ConditionCheck), then commit a single batch with the Puts and
	// Deletes. A TOCTOU window exists between the read and the
	// commit; single-pebble-process makes it negligible, multi-shard
	// would need 2PC anyway.
	batchOps := make([]pebble.BatchOp, 0, len(ops))
	mirrorBuckets := make(map[*pebble.DB][]pebble.BatchOp)
	var primary *pebble.DB
	var releases []func()
	defer func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}()
	for i, op := range ops {
		key, err := transactKey(op, td.KeySchema)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "ops[%d]: %v", i, err)
		}
		pkBytes, err := pkBytesFromItem(key, td.KeySchema)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "ops[%d]: %v", i, err)
		}
		targets, err := s.writeTargetsForPK(pkBytes)
		if err != nil {
			return nil, mapStorageErr(err)
		}
		releases = append(releases, targets.Release)
		if primary == nil {
			primary = targets.primary
		} else if primary != targets.primary {
			return nil, status.Errorf(codes.FailedPrecondition, "ops[%d]: cross-shard transactions not supported in v1", i)
		}

		if cond := strings.TrimSpace(op.GetConditionExpression()); cond != "" {
			c, err := storage.ParseCondition(cond)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "ops[%d].condition_expression: %v", i, err)
			}
			rawBinds, err := pbToItem(op.GetBinds())
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "ops[%d].binds: %v", i, err)
			}
			binds := make(map[string]types.AttributeValue, len(rawBinds))
			for k, v := range rawBinds {
				binds[strings.TrimPrefix(k, ":")] = v
			}
			prior, err := primary.GetItem(table, td.KeySchema, key)
			if err != nil && err != types.ErrItemNotFound {
				return nil, mapStorageErr(err)
			}
			ok, err := c.Evaluate(prior, binds)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "ops[%d]: evaluate condition: %v", i, err)
			}
			if !ok {
				return nil, status.Errorf(codes.FailedPrecondition, "ops[%d]: condition failed", i)
			}
		}
		var batchOp *pebble.BatchOp
		switch x := op.GetOp().(type) {
		case *cefaspb.TransactWriteOp_Put_:
			item, err := pbToItem(x.Put.GetItem())
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "ops[%d].put.item: %v", i, err)
			}
			next := pebble.BatchOp{Op: pebble.BatchOpPut, Item: item}
			batchOp = &next
		case *cefaspb.TransactWriteOp_Delete_:
			next := pebble.BatchOp{Op: pebble.BatchOpDelete, Key: key}
			batchOp = &next
		case *cefaspb.TransactWriteOp_ConditionCheck_:
			// already evaluated above; emits no mutation
		default:
			return nil, status.Errorf(codes.InvalidArgument, "ops[%d]: missing op", i)
		}
		if batchOp != nil {
			batchOps = append(batchOps, *batchOp)
			for _, mirror := range targets.mirrors {
				mirrorBuckets[mirror] = append(mirrorBuckets[mirror], *batchOp)
			}
		}
	}
	if len(batchOps) > 0 {
		pluginPlan, err := s.planPluginIndexBatch(primary, td, batchOps)
		if err != nil {
			return nil, mapWriteMutationErr(err)
		}
		if err := primary.BatchWriteItem(td, batchOps); err != nil {
			return nil, mapStorageErr(err)
		}
		for mirror, mirrorOps := range mirrorBuckets {
			if err := mirror.BatchWriteItem(td, mirrorOps); err != nil {
				return nil, mapStorageErr(err)
			}
		}
		if err := s.applyPluginIndexPlan(pluginPlan); err != nil {
			return nil, mapWriteMutationErr(err)
		}
	}
	return &cefaspb.TransactWriteItemsResponse{}, nil
}

// TransactGetItems fans a single round of reads. v1 single-table,
// single-shard — uses BatchGetItem so the response shape matches the
// existing batch path.
func (s *GRPCServer) TransactGetItems(ctx context.Context, req *cefaspb.TransactGetItemsRequest) (*cefaspb.TransactGetItemsResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "TransactGetItems")
	defer span.End()
	items := req.GetItems()
	if len(items) == 0 {
		return &cefaspb.TransactGetItemsResponse{}, nil
	}
	if len(items) > awsTransactLimit {
		return nil, status.Errorf(codes.InvalidArgument, "transact: %d ops exceeds the per-call limit of %d", len(items), awsTransactLimit)
	}
	table := items[0].GetTable()
	if table == "" {
		return nil, status.Error(codes.InvalidArgument, "items[0].table required")
	}
	for i, it := range items {
		if it.GetTable() != table {
			return nil, status.Errorf(codes.InvalidArgument, "items[%d].table = %q; cross-table transactions not supported in v1", i, it.GetTable())
		}
	}
	if err := requireAnyScope(ctx,
		auth.TableScope(auth.ScopeItemRead, table),
		auth.WildcardScope(auth.ScopeItemRead)); err != nil {
		return nil, err
	}
	td, err := s.cat.Describe(table)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	keys := make([]types.Item, 0, len(items))
	for i, it := range items {
		k, err := pbToItem(it.GetKey())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "items[%d].key: %v", i, err)
		}
		keys = append(keys, k)
	}
	got, err := s.db.BatchGetItem(table, td.KeySchema, keys)
	if err != nil {
		return nil, mapStorageErr(err)
	}
	out := make([]*cefaspb.Item, len(got))
	for i, item := range got {
		if item == nil {
			out[i] = &cefaspb.Item{}
			continue
		}
		out[i] = &cefaspb.Item{Attributes: itemToPB(item)}
	}
	return &cefaspb.TransactGetItemsResponse{Items: out}, nil
}

func singleTransactTable(ops []*cefaspb.TransactWriteOp) (string, error) {
	var t string
	for i, op := range ops {
		var name string
		switch x := op.GetOp().(type) {
		case *cefaspb.TransactWriteOp_Put_:
			name = x.Put.GetTable()
		case *cefaspb.TransactWriteOp_Delete_:
			name = x.Delete.GetTable()
		case *cefaspb.TransactWriteOp_ConditionCheck_:
			name = x.ConditionCheck.GetTable()
		default:
			return "", fmt.Errorf("ops[%d]: missing op", i)
		}
		if name == "" {
			return "", fmt.Errorf("ops[%d]: table required", i)
		}
		if t == "" {
			t = name
		} else if t != name {
			return "", fmt.Errorf("ops[%d].table = %q; cross-table transactions not supported in v1", i, name)
		}
	}
	return t, nil
}

func transactKey(op *cefaspb.TransactWriteOp, ks types.KeySchema) (types.Item, error) {
	var raw map[string]*cefaspb.AttributeValue
	switch x := op.GetOp().(type) {
	case *cefaspb.TransactWriteOp_Put_:
		// Put encodes the key as part of Item.
		item, err := pbToItem(x.Put.GetItem())
		if err != nil {
			return nil, err
		}
		out := types.Item{}
		if v, ok := item[ks.PK]; ok {
			out[ks.PK] = v
		}
		if ks.SK != "" {
			if v, ok := item[ks.SK]; ok {
				out[ks.SK] = v
			}
		}
		return out, nil
	case *cefaspb.TransactWriteOp_Delete_:
		raw = x.Delete.GetKey()
	case *cefaspb.TransactWriteOp_ConditionCheck_:
		raw = x.ConditionCheck.GetKey()
	default:
		return nil, fmt.Errorf("missing op")
	}
	return pbToItem(raw)
}
