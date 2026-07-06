package server_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/catalog"
	"github.com/CefasDb/cefasdb/internal/cluster"
	"github.com/CefasDb/cefasdb/internal/server"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	cefaspb "github.com/CefasDb/cefasdb/pkg/protocol"
)

type multiRaftServerNode struct {
	id      string
	manager *cluster.Manager
	server  *server.GRPCServer
}

func TestMultiRaftCreateTableReplicatesMetadataShard(t *testing.T) {
	ctx := context.Background()
	nodes := startMultiRaftServerNodes(t, 4, 5, 2)
	defer func() {
		for _, node := range nodes {
			_ = node.manager.Close()
		}
	}()
	assertRemoteDataShardsStayClosed(t, nodes)

	leader := waitShardLeader(t, nodes, 0)
	done := make(chan error, 1)
	go func() {
		_, err := leader.server.CreateTable(ctx, &cefaspb.CreateTableRequest{
			Descriptor_: &cefaspb.TableDescriptor{
				Name:      "Orders",
				KeySchema: &cefaspb.KeySchema{Pk: "pk", Sk: "sk"},
			},
		})
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CreateTable via %s: %v", leader.id, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("CreateTable via %s timed out", leader.id)
	}

	for _, node := range nodes {
		shard0, ok := node.manager.Shard(0)
		if !ok || shard0 == nil {
			t.Fatalf("%s missing shard 0", node.id)
		}
		waitCatalogTable(t, node.id, shard0.Storage, "Orders")
	}
}

func assertRemoteDataShardsStayClosed(t *testing.T, nodes []*multiRaftServerNode) {
	t.Helper()
	var remote int
	for _, node := range nodes {
		for _, sh := range node.manager.Shards() {
			if sh == nil || sh.ID == 0 || sh.IsLocalVoter || sh.IsLocalNonVoter {
				continue
			}
			remote++
			if sh.Storage != nil || sh.Raft != nil || sh.RaftStorage != nil {
				t.Fatalf("%s opened remote shard %d: storage=%v raft=%v raftStorage=%v", node.id, sh.ID, sh.Storage != nil, sh.Raft != nil, sh.RaftStorage != nil)
			}
		}
	}
	if remote == 0 {
		t.Fatalf("test placement did not produce any remote data shards")
	}
}

func startMultiRaftServerNodes(t *testing.T, nodeCount, shards, replicationFactor int) []*multiRaftServerNode {
	t.Helper()
	ids := make([]string, nodeCount)
	raftAddrs := make(map[string]string, nodeCount)
	grpcAddrs := make(map[string]string, nodeCount)
	httpAddrs := make(map[string]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		id := fmt.Sprintf("n%d", i)
		ids[i] = id
		raftAddrs[id] = reserveLocalAddr(t)
		grpcAddrs[id] = reserveLocalAddr(t)
		httpAddrs[id] = "http://" + reserveLocalAddr(t)
	}

	nodes := make([]*multiRaftServerNode, 0, nodeCount)
	for _, id := range ids {
		mgr, err := cluster.Open(context.Background(), cluster.Config{
			Root:              t.TempDir(),
			Shards:            shards,
			ReplicationFactor: replicationFactor,
			SelfID:            id,
			MuxAddr:           raftAddrs[id],
			Peers:             raftAddrs,
			PeerHTTPAddrs:     httpAddrs,
			PeerGRPCAddrs:     grpcAddrs,
			Bootstrap:         true,
			HeartbeatMS:       50,
			ElectionMS:        150,
			LeaderLeaseMS:     50,
			CommitMS:          10,
			ApplyTimeout:      2 * time.Second,
			LogOutput:         io.Discard,
		})
		if err != nil {
			for _, node := range nodes {
				_ = node.manager.Close()
			}
			t.Fatalf("open manager %s: %v", id, err)
		}
		shard0, ok := mgr.Shard(0)
		if !ok || shard0 == nil {
			_ = mgr.Close()
			t.Fatalf("%s missing shard 0", id)
		}
		cat, err := catalog.New(shard0.Storage)
		if err != nil {
			_ = mgr.Close()
			t.Fatalf("%s catalog: %v", id, err)
		}
		srv := server.NewGRPCServer(shard0.Storage, cat, shard0.Raft)
		srv.AttachManager(mgr)
		nodes = append(nodes, &multiRaftServerNode{id: id, manager: mgr, server: srv})
	}
	return nodes
}

func waitShardLeader(t *testing.T, nodes []*multiRaftServerNode, shardID uint32) *multiRaftServerNode {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			shard, ok := node.manager.Shard(shardID)
			if ok && shard != nil && shard.Raft != nil && shard.Raft.IsLeader() {
				return node
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("no leader elected for shard %d", shardID)
	return nil
}

func reserveLocalAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return addr
}

func waitCatalogTable(t *testing.T, nodeID string, db *pebble.DB, table string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		cat, err := catalog.New(db)
		if err != nil {
			last = err
		} else if _, err := cat.Describe(table); err != nil {
			last = err
		} else {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s Describe replicated table %s: %v", nodeID, table, last)
}
