package server

import (
	"fmt"

	"github.com/CefasDb/cefasdb/internal/cluster"
	itemhttp "github.com/CefasDb/cefasdb/internal/server/http/item"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type routedWriteTargets struct {
	primary *pebble.DB
	mirrors []*pebble.DB
	release func()
}

func routeWriteTargets(fallback *pebble.DB, mgr *cluster.Manager, pkBytes []byte) (routedWriteTargets, error) {
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
	seen := map[*pebble.DB]struct{}{out.primary: {}}
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

func (t routedWriteTargets) PutItemWith(td types.TableDescriptor, item types.Item, opts pebble.PutOptions) error {
	if err := t.primary.PutItemWith(td, item, opts); err != nil {
		return err
	}
	for _, mirror := range t.mirrors {
		if err := mirror.PutItemWith(td, item, pebble.PutOptions{AllowCounterWrite: true}); err != nil {
			return err
		}
	}
	return nil
}

func (t routedWriteTargets) DeleteItemWith(td types.TableDescriptor, key types.Item, opts pebble.DeleteOptions) error {
	if err := t.primary.DeleteItemWith(td, key, opts); err != nil {
		return err
	}
	for _, mirror := range t.mirrors {
		if err := mirror.DeleteItemWith(td, key, pebble.DeleteOptions{}); err != nil {
			return err
		}
	}
	return nil
}

func (t routedWriteTargets) MirrorPutItem(td types.TableDescriptor, item types.Item) error {
	for _, mirror := range t.mirrors {
		if err := mirror.PutItemWith(td, item, pebble.PutOptions{AllowCounterWrite: true}); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) writeTargetsForPK(pkBytes []byte) (routedWriteTargets, error) {
	return routeWriteTargets(s.db, s.manager, pkBytes)
}

// itemWriteTargetsForPK adapts (*Server).writeTargetsForPK to the
// itemhttp.WriteTargets interface. routedWriteTargets already has the
// three methods the interface declares; the wrapper just narrows the
// return type so the item package never sees the internal struct.
func (s *Server) itemWriteTargetsForPK(pkBytes []byte) (itemhttp.WriteTargets, error) {
	t, err := s.writeTargetsForPK(pkBytes)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (s *GRPCServer) writeTargetsForPK(pkBytes []byte) (routedWriteTargets, error) {
	return routeWriteTargets(s.db, s.manager, pkBytes)
}
