package cluster_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/CefasDb/cefasdb/internal/cluster"
	"github.com/CefasDb/cefasdb/internal/placement"
	"github.com/CefasDb/cefasdb/internal/routing"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/internal/testutil/wait"
	"github.com/CefasDb/cefasdb/pkg/types"
	pebbledb "github.com/cockroachdb/pebble"
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
	r := routing.NewRouter(4)
	hits := make(map[uint32]int)
	for i := 0; i < 10_000; i++ {
		pk := []byte(fmt.Sprintf("user-%d", i))
		id, err := r.ShardForPK(pk)
		if err != nil {
			t.Fatalf("ShardForPK(%q) returned error: %v", pk, err)
		}
		hits[id]++
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
	r := routing.NewRouter(1)
	for i := 0; i < 100; i++ {
		id, err := r.ShardForPK([]byte(fmt.Sprintf("k%d", i)))
		if err != nil {
			t.Fatalf("single-shard router returned error: %v", err)
		}
		if id != 0 {
			t.Fatalf("single-shard router routed away from 0")
		}
	}
}

func TestRouterUsesTokenRangePlacement(t *testing.T) {
	cat := placement.PlacementCatalog{
		Version:  1,
		Epoch:    7,
		Strategy: placement.PlacementStrategyTokenRange,
		Shards: []placement.ShardPlacement{
			{ID: 0, State: placement.ShardStateActive, Epoch: 7, Ranges: []placement.TokenRange{{Start: 0, End: 100}}},
			{ID: 1, State: placement.ShardStateActive, Epoch: 7, Ranges: []placement.TokenRange{{Start: 100, End: 0}}},
		},
	}
	r, err := routing.NewRouterFromCatalog(cat)
	if err != nil {
		t.Fatal(err)
	}
	mustShard := func(h uint64) uint32 {
		t.Helper()
		id, err := r.ShardForUint64(h)
		if err != nil {
			t.Fatalf("ShardForUint64(%d) returned error: %v", h, err)
		}
		return id
	}
	if got := mustShard(99); got != 0 {
		t.Fatalf("token 99 routed to shard %d, want 0", got)
	}
	if got := mustShard(100); got != 1 {
		t.Fatalf("token 100 routed to shard %d, want 1", got)
	}
	if got := mustShard(^uint64(0)); got != 1 {
		t.Fatalf("max token routed to shard %d, want 1", got)
	}
	if r.Epoch() != 7 {
		t.Fatalf("epoch = %d, want 7", r.Epoch())
	}
}

func TestPlacementFileRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "placement.json")
	cat := placement.DefaultPlacement(
		3,
		"n1",
		map[string]string{"n1": "127.0.0.1:9101", "n2": "127.0.0.1:9102"},
		map[string]string{"n1": "http://127.0.0.1:8081", "n2": "http://127.0.0.1:8082"},
		placement.NodeCapacity{Weight: 2, CPU: 10, Zone: "az-a", Tags: []string{"hot"}},
		placement.PlacementStrategyTokenRange,
	)
	if err := placement.SavePlacementFile(path, cat); err != nil {
		t.Fatal(err)
	}
	loaded, err := placement.LoadPlacementFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Strategy != placement.PlacementStrategyTokenRange || loaded.Epoch != 1 || len(loaded.Shards) != 3 {
		t.Fatalf("unexpected placement: %+v", loaded)
	}
	if loaded.Nodes["n1"].Capacity.Weight != 2 || loaded.Nodes["n1"].Capacity.Zone != "az-a" {
		t.Fatalf("node capacity not preserved: %+v", loaded.Nodes["n1"])
	}
}

func TestManagerCreatesTokenRangePlacementForFreshRoot(t *testing.T) {
	root := t.TempDir()
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      root,
		Shards:    2,
		SelfID:    "n0",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	cat := mgr.Placement()
	if cat.Strategy != placement.PlacementStrategyTokenRange {
		t.Fatalf("strategy = %q, want %q", cat.Strategy, placement.PlacementStrategyTokenRange)
	}
	if len(cat.Shards) != 2 || len(cat.Shards[0].Ranges) == 0 {
		t.Fatalf("missing token ranges: %+v", cat.Shards)
	}
	if _, err := os.Stat(filepath.Join(root, "placement.json")); err != nil {
		t.Fatalf("placement file not created: %v", err)
	}
}

func TestManagerCopiesLeaderHintsIntoShardHandles(t *testing.T) {
	root := t.TempDir()
	cat := placement.DefaultPlacement(
		2,
		"n0",
		map[string]string{"n0": "127.0.0.1:9100", "n1": "127.0.0.1:9101"},
		nil,
		placement.NodeCapacity{},
		placement.PlacementStrategyTokenRange,
	)
	if err := placement.SavePlacementFile(filepath.Join(root, "placement.json"), cat); err != nil {
		t.Fatal(err)
	}
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      root,
		Shards:    2,
		SelfID:    "n0",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	for _, meta := range cat.Shards {
		sh, ok := mgr.Shard(meta.ID)
		if !ok {
			t.Fatalf("missing shard %d", meta.ID)
		}
		if sh.LeaderHint != meta.LeaderHint {
			t.Fatalf("shard %d leader hint = %q, want %q", meta.ID, sh.LeaderHint, meta.LeaderHint)
		}
	}
}

func TestManagerBackfillsMissingLeaderHints(t *testing.T) {
	root := t.TempDir()
	cat := placement.DefaultPlacement(
		2,
		"n0",
		map[string]string{"n0": "127.0.0.1:9100", "n1": "127.0.0.1:9101"},
		nil,
		placement.NodeCapacity{},
		placement.PlacementStrategyTokenRange,
	)
	for i := range cat.Shards {
		cat.Shards[i].LeaderHint = ""
	}
	if err := placement.SavePlacementFile(filepath.Join(root, "placement.json"), cat); err != nil {
		t.Fatal(err)
	}
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      root,
		Shards:    2,
		SelfID:    "n0",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	got := mgr.Placement()
	want := []string{"n0", "n1"}
	for i, sh := range got.Shards {
		if sh.LeaderHint != want[i] {
			t.Fatalf("shard %d leader hint = %q, want %q", sh.ID, sh.LeaderHint, want[i])
		}
	}
}

func TestManagerPreservesLegacyModuloForExistingShardState(t *testing.T) {
	root := t.TempDir()
	st, err := pebble.Open(pebble.Options{Path: filepath.Join(root, "shards", "0", "state")})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Set([]byte("cefas/test"), []byte("present")); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      root,
		Shards:    2,
		SelfID:    "n0",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	if got := mgr.Placement().Strategy; got != placement.PlacementStrategyLegacyModulo {
		t.Fatalf("strategy = %q, want %q", got, placement.PlacementStrategyLegacyModulo)
	}
}

func TestRouteForPKRejectsStaleEpoch(t *testing.T) {
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      t.TempDir(),
		Shards:    2,
		SelfID:    "n0",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	_, err = mgr.RouteForPK([]byte("k"), mgr.RoutingEpoch()+1)
	if !errors.Is(err, cluster.ErrStaleRoute) {
		t.Fatalf("error = %v, want ErrStaleRoute", err)
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
	leaderOfShard := make([]int, shards)
	for s := uint32(0); s < shards; s++ {
		leaderOfShard[s] = -1
		shardID := s
		wait.Eventually(t, func() bool {
			for nodeIdx, mgr := range mgrs {
				sh, _ := mgr.Shard(shardID)
				if sh != nil && sh.Raft != nil && sh.Raft.IsLeader() {
					leaderOfShard[shardID] = nodeIdx
					return true
				}
			}
			return false
		}, 10*time.Second, 50*time.Millisecond, "shard %d never elected a leader", s)
	}
	t.Logf("leaders: shard0=n%d shard1=n%d", leaderOfShard[0], leaderOfShard[1])

	// Find two keys that route to different shards.
	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	router := mgrs[0].Router()
	keys := map[uint32]string{}
	for i := 0; len(keys) < shards && i < 10_000; i++ {
		k := fmt.Sprintf("k-%d", i)
		s, err := router.ShardForPK([]byte(k))
		if err != nil {
			t.Fatalf("ShardForPK(%q) returned error: %v", k, err)
		}
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
	wait.Eventually(t, func() bool {
		for _, mgr := range mgrs {
			for shardID, key := range keys {
				sh, _ := mgr.Shard(shardID)
				if sh == nil {
					return false
				}
				if _, err := sh.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": {T: types.AttrS, S: key}}); err != nil {
					return false
				}
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond, "followers never converged on the written items")

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
		shardID := s
		wait.Eventually(t, func() bool {
			for nodeIdx, mgr := range mgrs {
				if mgr == nil || nodeIdx == 0 {
					continue
				}
				sh, _ := mgr.Shard(shardID)
				if sh != nil && sh.Raft.IsLeader() {
					leaderOfShard[shardID] = nodeIdx
					return true
				}
			}
			return false
		}, 10*time.Second, 50*time.Millisecond, "shard %d had no surviving leader after n0 close", s)
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

func TestRebalanceLeadersTransfersLocalMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	const nodes = 3
	const shards = 2
	const shardID = uint32(1)

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
		mgr, err := cluster.Open(context.Background(), cluster.Config{
			Root:                            t.TempDir(),
			Shards:                          shards,
			SelfID:                          fmt.Sprintf("n%d", i),
			MuxAddr:                         muxAddrs[i],
			Peers:                           peers,
			PeerHTTPAddrs:                   httpPeers,
			Bootstrap:                       true,
			HeartbeatMS:                     50,
			ElectionMS:                      150,
			LeaderLeaseMS:                   50,
			CommitMS:                        10,
			DisableLeaderHintReconciliation: true,
			LogOutput:                       io.Discard,
		})
		if err != nil {
			t.Fatalf("open mgr[%d]: %v", i, err)
		}
		mgrs[i] = mgr
	}

	cat := mgrs[0].Placement()
	desiredLeader := cat.Shards[shardID].LeaderHint
	desiredIdx := nodeIndex(t, desiredLeader)
	targetIdx := 0
	if targetIdx == desiredIdx {
		targetIdx = 1
	}
	targetLeader := fmt.Sprintf("n%d", targetIdx)

	currentIdx := waitManagerShardLeader(t, mgrs, shardID)
	if currentIdx != targetIdx {
		currentShard, _ := mgrs[currentIdx].Shard(shardID)
		if err := currentShard.Raft.TransferLeadership(targetLeader, muxAddrs[targetIdx], 5*time.Second); err != nil {
			t.Fatalf("transfer shard %d to %s: %v", shardID, targetLeader, err)
		}
		wait.Eventually(t, func() bool {
			sh, _ := mgrs[targetIdx].Shard(shardID)
			return sh != nil && sh.Raft != nil && sh.Raft.IsLeader()
		}, 10*time.Second, 50*time.Millisecond, "shard %d did not transfer to %s", shardID, targetLeader)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := mgrs[targetIdx].RebalanceLeaders(ctx, cluster.LeaderRebalanceOptions{
		MaxConcurrent:   1,
		TransferTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("rebalance leaders: %v", err)
	}
	shardResult := findLeaderRebalanceResult(t, result.Shards, shardID)
	if shardResult.Status != cluster.LeaderRebalanceStatusTransferred {
		t.Fatalf("shard %d status = %q detail = %q", shardID, shardResult.Status, shardResult.Detail)
	}
	wait.Eventually(t, func() bool {
		sh, _ := mgrs[desiredIdx].Shard(shardID)
		return sh != nil && sh.Raft != nil && sh.Raft.IsLeader()
	}, 10*time.Second, 50*time.Millisecond, "shard %d did not rebalance to %s", shardID, desiredLeader)
}

func waitManagerShardLeader(t *testing.T, mgrs []*cluster.Manager, shardID uint32) int {
	t.Helper()
	leaderIdx := -1
	wait.Eventually(t, func() bool {
		for nodeIdx, mgr := range mgrs {
			sh, _ := mgr.Shard(shardID)
			if sh != nil && sh.Raft != nil && sh.Raft.IsLeader() {
				leaderIdx = nodeIdx
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "shard %d never elected a leader", shardID)
	return leaderIdx
}

func nodeIndex(t *testing.T, nodeID string) int {
	t.Helper()
	var idx int
	if _, err := fmt.Sscanf(nodeID, "n%d", &idx); err != nil {
		t.Fatalf("parse node id %q: %v", nodeID, err)
	}
	return idx
}

func findLeaderRebalanceResult(t *testing.T, in []cluster.LeaderRebalanceShardResult, shardID uint32) cluster.LeaderRebalanceShardResult {
	t.Helper()
	for _, sh := range in {
		if sh.ShardID == shardID {
			return sh
		}
	}
	t.Fatalf("missing rebalance result for shard %d: %+v", shardID, in)
	return cluster.LeaderRebalanceShardResult{}
}

func hasPrefix(t testing.TB, db *pebble.DB, prefix []byte) bool {
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
