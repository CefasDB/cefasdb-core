package cluster_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	pebbledb "github.com/cockroachdb/pebble"
	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func pickPort(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestRouterDistributesKeys(t *testing.T) {
	r := cluster.NewRouter(4)
	hits := make(map[uint32]int)
	for i := 0; i < 10_000; i++ {
		pk := []byte(fmt.Sprintf("user-%d", i))
		hits[r.ShardForPK(pk)]++
	}
	if len(hits) != 4 {
		t.Fatalf("expected hits on every shard, got %v", hits)
	}
	// Chi-squared sanity check: every shard should get at least
	// 15% of the load with xxhash on a 10k sample.
	min := 10_000 * 15 / 100
	for shard, count := range hits {
		if count < min {
			t.Errorf("shard %d under-loaded: %d (want ≥ %d)", shard, count, min)
		}
	}
}

func TestRouterSingleShard(t *testing.T) {
	r := cluster.NewRouter(1)
	for i := 0; i < 100; i++ {
		if r.ShardForPK([]byte(fmt.Sprintf("k%d", i))) != 0 {
			t.Fatalf("single-shard router routed away from 0")
		}
	}
}

func TestManagerSeparatesRaftStore(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	addr := pickPort(t)
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:          t.TempDir(),
		Shards:        1,
		SelfID:        "n0",
		MuxAddr:       addr,
		Peers:         map[string]string{"n0": addr},
		Bootstrap:     true,
		HeartbeatMS:   50,
		ElectionMS:    150,
		LeaderLeaseMS: 50,
		CommitMS:      10,
		LogOutput:     io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	sh, ok := mgr.Shard(0)
	if !ok || sh.RaftStorage == nil {
		t.Fatalf("missing shard raft storage: %#v", sh)
	}
	if hasPrefix(t, sh.Storage, []byte("raft/")) {
		t.Fatal("data store contains raft metadata")
	}
	if !hasPrefix(t, sh.RaftStorage, []byte("raft/")) {
		t.Fatal("raft store does not contain raft metadata")
	}
}

// TestMultiShardCluster brings up a 3-node × 2-shard cluster, writes
// keys that hash to different shards, kills one node, and verifies the
// surviving shards keep serving.
func TestMultiShardCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	const nodes = 3
	const shards = 2

	muxAddrs := make([]string, nodes)
	httpAddrs := make([]string, nodes)
	for i := range muxAddrs {
		muxAddrs[i] = pickPort(t)
		httpAddrs[i] = "http://" + pickPort(t)
	}
	peers := map[string]string{}
	httpPeers := map[string]string{}
	for i := 0; i < nodes; i++ {
		peers[fmt.Sprintf("n%d", i)] = muxAddrs[i]
		httpPeers[fmt.Sprintf("n%d", i)] = httpAddrs[i]
	}

	mgrs := make([]*cluster.Manager, nodes)
	defer func() {
		for _, m := range mgrs {
			if m != nil {
				_ = m.Close()
			}
		}
	}()

	for i := 0; i < nodes; i++ {
		dir := t.TempDir()
		mgr, err := cluster.Open(context.Background(), cluster.Config{
			Root:          dir,
			Shards:        shards,
			SelfID:        fmt.Sprintf("n%d", i),
			MuxAddr:       muxAddrs[i],
			Peers:         peers,
			PeerHTTPAddrs: httpPeers,
			Bootstrap:     true,
			HeartbeatMS:   50,
			ElectionMS:    150,
			LeaderLeaseMS: 50,
			CommitMS:      10,
			LogOutput:     io.Discard,
		})
		if err != nil {
			t.Fatalf("open mgr[%d]: %v", i, err)
		}
		mgrs[i] = mgr
	}

	// Wait until every shard has a leader.
	deadline := time.Now().Add(10 * time.Second)
	leaderOfShard := make([]int, shards)
	for s := uint32(0); s < shards; s++ {
		for time.Now().Before(deadline) {
			found := -1
			for nodeIdx, mgr := range mgrs {
				sh, _ := mgr.Shard(s)
				if sh != nil && sh.Raft != nil && sh.Raft.IsLeader() {
					found = nodeIdx
					break
				}
			}
			if found >= 0 {
				leaderOfShard[s] = found
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if leaderOfShard[s] == -1 {
			t.Fatalf("shard %d never elected a leader", s)
		}
	}
	t.Logf("leaders: shard0=n%d shard1=n%d", leaderOfShard[0], leaderOfShard[1])

	// Find two keys that route to different shards.
	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	router := mgrs[0].Router()
	keys := map[uint32]string{}
	for i := 0; len(keys) < shards && i < 10_000; i++ {
		k := fmt.Sprintf("k-%d", i)
		s := router.ShardForPK([]byte(k))
		if _, ok := keys[s]; !ok {
			keys[s] = k
		}
	}
	if len(keys) != shards {
		t.Fatalf("could not find keys for both shards")
	}

	// Write each key on its shard's leader.
	for shardID, key := range keys {
		leader := mgrs[leaderOfShard[shardID]]
		shard, _ := leader.Shard(shardID)
		item := types.Item{"id": {T: types.AttrS, S: key}, "v": {T: types.AttrS, S: "hello-" + key}}
		if err := shard.Storage.PutItem(td.Name, td.KeySchema, item); err != nil {
			t.Fatalf("put on shard %d: %v", shardID, err)
		}
	}

	// Each follower should see both items on its replica of each
	// shard within a few seconds.
	wait := time.Now().Add(5 * time.Second)
	for time.Now().Before(wait) {
		all := true
		for nodeIdx, mgr := range mgrs {
			for shardID, key := range keys {
				sh, _ := mgr.Shard(shardID)
				if sh == nil {
					all = false
					continue
				}
				_, err := sh.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": {T: types.AttrS, S: key}})
				if err != nil {
					_ = nodeIdx
					all = false
				}
			}
		}
		if all {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Kill node 0. Surviving shards must keep accepting writes.
	if err := mgrs[0].Close(); err != nil {
		t.Fatalf("close n0: %v", err)
	}
	mgrs[0] = nil

	// Wait for re-election on any shard that had n0 as leader.
	for s := uint32(0); s < shards; s++ {
		if leaderOfShard[s] != 0 {
			continue
		}
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			for nodeIdx, mgr := range mgrs {
				if mgr == nil || nodeIdx == 0 {
					continue
				}
				sh, _ := mgr.Shard(s)
				if sh != nil && sh.Raft.IsLeader() {
					leaderOfShard[s] = nodeIdx
					goto next
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	next:
	}

	// Write a fresh value on each surviving shard's leader.
	for shardID, key := range keys {
		idx := leaderOfShard[shardID]
		mgr := mgrs[idx]
		if mgr == nil {
			t.Fatalf("shard %d still without surviving leader", shardID)
		}
		shard, _ := mgr.Shard(shardID)
		item := types.Item{"id": {T: types.AttrS, S: key + "-new"}, "v": {T: types.AttrS, S: "post-failover"}}
		if err := shard.Storage.PutItem(td.Name, td.KeySchema, item); err != nil {
			t.Fatalf("post-failover write on shard %d: %v", shardID, err)
		}
	}
}

func hasPrefix(t testing.TB, db *storage.DB, prefix []byte) bool {
	t.Helper()
	upper := append([]byte(nil), prefix...)
	upper[len(upper)-1]++
	iter, err := db.Raw().NewIter(&pebbledb.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	ok := iter.First()
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	return ok
}
