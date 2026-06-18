package cluster

import (
	"errors"
	"fmt"
	"testing"

	"github.com/CefasDb/cefasdb/internal/placement"
	"github.com/CefasDb/cefasdb/internal/routing"
)

func TestReadShardForPKRequiresLocalReplica(t *testing.T) {
	cat := placement.DefaultPlacementWithReplicationFactor(
		4,
		"n1",
		map[string]string{
			"n1": "127.0.0.1:9101",
			"n2": "127.0.0.1:9102",
			"n3": "127.0.0.1:9103",
			"n4": "127.0.0.1:9104",
		},
		nil,
		placement.NodeCapacity{},
		placement.PlacementStrategyTokenRange,
		2,
	)
	mgr := managerForReadTest(t, cat, "n4")

	key, shardID := keyForShardWithoutLocalReplica(t, mgr, "n4")
	if _, err := mgr.ReadShardForPK([]byte(key), 0); !errors.Is(err, ErrNoLocalReplica) {
		t.Fatalf("ReadShardForPK shard %d error = %v, want ErrNoLocalReplica", shardID, err)
	}

	local := cat.Shards[shardID].Voters[0]
	localMgr := managerForReadTest(t, cat, local)
	sh, err := localMgr.ReadShardForPK([]byte(key), 0)
	if err != nil {
		t.Fatalf("ReadShardForPK local voter %s: %v", local, err)
	}
	if sh.ID != shardID {
		t.Fatalf("read shard = %d, want %d", sh.ID, shardID)
	}
}

func TestReadShardsRequiresCompleteLocalCoverage(t *testing.T) {
	peers := map[string]string{
		"n1": "127.0.0.1:9101",
		"n2": "127.0.0.1:9102",
		"n3": "127.0.0.1:9103",
		"n4": "127.0.0.1:9104",
	}
	partial := placement.DefaultPlacementWithReplicationFactor(
		4,
		"n1",
		peers,
		nil,
		placement.NodeCapacity{},
		placement.PlacementStrategyTokenRange,
		2,
	)
	if _, err := managerForReadTest(t, partial, "n4").ReadShards(0); !errors.Is(err, ErrNoLocalReplica) {
		t.Fatalf("ReadShards partial RF error = %v, want ErrNoLocalReplica", err)
	}

	full := placement.DefaultPlacementWithReplicationFactor(
		4,
		"n1",
		peers,
		nil,
		placement.NodeCapacity{},
		placement.PlacementStrategyTokenRange,
		0,
	)
	got, err := managerForReadTest(t, full, "n4").ReadShards(0)
	if err != nil {
		t.Fatalf("ReadShards full RF: %v", err)
	}
	if len(got) != len(full.Shards) {
		t.Fatalf("ReadShards len = %d, want %d", len(got), len(full.Shards))
	}
}

func managerForReadTest(t *testing.T, cat placement.PlacementCatalog, selfID string) *Manager {
	t.Helper()
	router, err := routing.NewRouterFromCatalog(cat)
	if err != nil {
		t.Fatal(err)
	}
	mgr := &Manager{
		cfg:    Config{SelfID: selfID},
		router: router,
		cat:    cat,
	}
	for _, meta := range cat.Shards {
		mgr.shards = append(mgr.shards, &Shard{
			ID:         meta.ID,
			State:      meta.State,
			Epoch:      meta.Epoch,
			Ranges:     append([]placement.TokenRange(nil), meta.Ranges...),
			Voters:     append([]string(nil), meta.Voters...),
			NonVoters:  append([]string(nil), meta.NonVoters...),
			LeaderHint: meta.LeaderHint,
		})
	}
	return mgr
}

func keyForShardWithoutLocalReplica(t *testing.T, mgr *Manager, selfID string) (string, uint32) {
	t.Helper()
	for i := 0; i < 10_000; i++ {
		key := fmt.Sprintf("k-%d", i)
		sh, err := mgr.RouteForPK([]byte(key), 0)
		if err != nil {
			t.Fatal(err)
		}
		if sh.ID == 0 || containsString(sh.Voters, selfID) || containsString(sh.NonVoters, selfID) {
			continue
		}
		return key, sh.ID
	}
	t.Fatalf("could not find key routed to a non-local replica for %s", selfID)
	return "", 0
}

func TestReadShardForPKRejectsStaleEpoch(t *testing.T) {
	cat := placement.DefaultPlacement(1, "n1", map[string]string{"n1": "127.0.0.1:9101"}, nil, placement.NodeCapacity{}, "")
	mgr := managerForReadTest(t, cat, "n1")
	if _, err := mgr.ReadShardForPK([]byte("k"), cat.Epoch+1); !errors.Is(err, ErrStaleRoute) {
		t.Fatalf("ReadShardForPK stale epoch error = %v, want ErrStaleRoute", err)
	}
}
