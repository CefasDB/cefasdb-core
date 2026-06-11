package api

import (
	"fmt"

	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

type routedWriteTargets struct {
	primary *storage.DB
	mirrors []*storage.DB
	release func()
}

func routeWriteTargets(fallback *storage.DB, mgr *cluster.Manager, pkBytes []byte) (routedWriteTargets, error) {
	if mgr == nil {
		return routedWriteTargets{primary: fallback, release: func() {}}, nil
	}
	targets, err := mgr.WriteTargetsForPK(pkBytes, 0)
	if err != nil {
		return routedWriteTargets{}, err
	}
	out := routedWriteTargets{
		release: targets.Release,
	}
	if targets.Primary == nil || targets.Primary.Storage == nil {
		targets.Release()
		return routedWriteTargets{}, fmt.Errorf("cluster: primary write shard is not open locally")
	}
	out.primary = targets.Primary.Storage
	seen := map[*storage.DB]struct{}{out.primary: {}}
	for _, sh := range targets.Mirrors {
		if sh == nil || sh.Storage == nil {
			targets.Release()
			return routedWriteTargets{}, fmt.Errorf("cluster: mirror write shard is not open locally")
		}
		if _, ok := seen[sh.Storage]; ok {
			continue
		}
		out.mirrors = append(out.mirrors, sh.Storage)
		seen[sh.Storage] = struct{}{}
	}
	return out, nil
}

func (t routedWriteTargets) Release() {
	if t.release != nil {
		t.release()
	}
}

func (t routedWriteTargets) PutItemWith(td types.TableDescriptor, item types.Item, opts storage.PutOptions) error {
	if err := t.primary.PutItemWith(td, item, opts); err != nil {
		return err
	}
	for _, mirror := range t.mirrors {
		if err := mirror.PutItemWith(td, item, storage.PutOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (t routedWriteTargets) DeleteItemWith(td types.TableDescriptor, key types.Item, opts storage.DeleteOptions) error {
	if err := t.primary.DeleteItemWith(td, key, opts); err != nil {
		return err
	}
	for _, mirror := range t.mirrors {
		if err := mirror.DeleteItemWith(td, key, storage.DeleteOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (t routedWriteTargets) MirrorPutItem(td types.TableDescriptor, item types.Item) error {
	for _, mirror := range t.mirrors {
		if err := mirror.PutItemWith(td, item, storage.PutOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) writeTargetsForPK(pkBytes []byte) (routedWriteTargets, error) {
	return routeWriteTargets(s.db, s.manager, pkBytes)
}

func (s *GRPCServer) writeTargetsForPK(pkBytes []byte) (routedWriteTargets, error) {
	return routeWriteTargets(s.db, s.manager, pkBytes)
}
