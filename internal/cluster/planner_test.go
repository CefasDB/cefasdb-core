package cluster_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/osvaldoandrade/cefas/internal/catalog"
	"github.com/osvaldoandrade/cefas/internal/cluster"
	"github.com/osvaldoandrade/cefas/internal/spatial"
	"github.com/osvaldoandrade/cefas/internal/storage"
	"github.com/osvaldoandrade/cefas/pkg/types"
)

func plannerCatalog(shards int) cluster.PlacementCatalog {
	cat := cluster.DefaultPlacement(
		shards,
		"n1",
		map[string]string{
			"n1": "127.0.0.1:9101",
			"n2": "127.0.0.1:9102",
			"n3": "127.0.0.1:9103",
		},
		map[string]string{
			"n1": "http://127.0.0.1:8081",
			"n2": "http://127.0.0.1:8082",
			"n3": "http://127.0.0.1:8083",
		},
		cluster.NodeCapacity{},
		cluster.PlacementStrategyTokenRange,
	)
	cat.Nodes["n4"] = cluster.NodeDescriptor{ID: "n4", RaftAddr: "127.0.0.1:9104", HTTPAddr: "http://127.0.0.1:8084", State: cluster.NodeStateActive}
	return cat
}

func TestPlanSplitCreatesSafeTransition(t *testing.T) {
	cat := plannerCatalog(1)
	plan, err := cluster.BuildPlacementPlan(cat, cluster.PlacementPlanRequest{
		Operation: cluster.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.RequiresDataCopy || plan.RequiresRestart || !plan.ApplySupported {
		t.Fatalf("unexpected split flags: %+v", plan)
	}
	if len(plan.Steps) != 1 || plan.Steps[0].Action != "create_shard" {
		t.Fatalf("unexpected split steps: %+v", plan.Steps)
	}
	if len(plan.After.Shards) != 2 {
		t.Fatalf("after shard count = %d, want 2", len(plan.After.Shards))
	}
	if plan.After.Shards[0].State != cluster.ShardStateSplitting {
		t.Fatalf("parent state = %s", plan.After.Shards[0].State)
	}
	if plan.After.Shards[1].State != cluster.ShardStateCreating {
		t.Fatalf("child state = %s", plan.After.Shards[1].State)
	}
	if len(plan.After.Shards[0].Ranges) != 1 || plan.After.Shards[0].Ranges[0] != cat.Shards[0].Ranges[0] {
		t.Fatalf("parent range changed before activation: %+v", plan.After.Shards[0].Ranges)
	}
	if err := cluster.ValidatePlacement(plan.After); err != nil {
		t.Fatalf("planned placement invalid: %v", err)
	}
}

func TestPlanMoveReplacesVoter(t *testing.T) {
	cat := plannerCatalog(2)
	plan, err := cluster.BuildPlacementPlan(cat, cluster.PlacementPlanRequest{
		Operation:  cluster.PlacementOperationMove,
		ShardID:    0,
		SourceNode: "n1",
		TargetNode: "n4",
		MinVoters:  3,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := plan.After.Shards[0].Voters
	if contains(got, "n1") || !contains(got, "n4") || len(got) != 3 {
		t.Fatalf("voters = %v, want n1 replaced by n4", got)
	}
	if plan.After.Shards[0].State != cluster.ShardStateActive {
		t.Fatalf("state = %s", plan.After.Shards[0].State)
	}
	if plan.RequiresDataCopy || plan.RequiresRestart || !plan.ApplySupported || len(plan.Steps) != 3 {
		t.Fatalf("unexpected move plan: %+v", plan)
	}
}

func TestPlanDrainRemovesNodeFromEveryShard(t *testing.T) {
	cat := plannerCatalog(3)
	plan, err := cluster.BuildPlacementPlan(cat, cluster.PlacementPlanRequest{
		Operation:   cluster.PlacementOperationDrain,
		NodeID:      "n1",
		TargetNodes: []string{"n4"},
		MinVoters:   3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.After.Nodes["n1"].State != cluster.NodeStateDraining {
		t.Fatalf("node state = %s", plan.After.Nodes["n1"].State)
	}
	for _, sh := range plan.After.Shards {
		if contains(sh.Voters, "n1") {
			t.Fatalf("shard %d still has n1 voter: %v", sh.ID, sh.Voters)
		}
		if !contains(sh.Voters, "n4") {
			t.Fatalf("shard %d missing replacement n4: %v", sh.ID, sh.Voters)
		}
		if sh.State != cluster.ShardStateActive {
			t.Fatalf("shard %d state = %s", sh.ID, sh.State)
		}
	}
	if !plan.ApplySupported {
		t.Fatalf("drain should be apply-supported")
	}
}

func TestPlanSplitRejectsLegacyModuloPlacement(t *testing.T) {
	cat := cluster.DefaultPlacement(2, "n1", map[string]string{"n1": "127.0.0.1:9101"}, nil, cluster.NodeCapacity{}, cluster.PlacementStrategyLegacyModulo)
	_, err := cluster.BuildPlacementPlan(cat, cluster.PlacementPlanRequest{
		Operation: cluster.PlacementOperationSplit,
		ShardID:   0,
	})
	if !errors.Is(err, cluster.ErrInvalidPlacementPlan) {
		t.Fatalf("error = %v, want ErrInvalidPlacementPlan", err)
	}
}

func TestApplyPlacementPublishesNoopMove(t *testing.T) {
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      t.TempDir(),
		Shards:    1,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	plan, err := mgr.PlanPlacement(cluster.PlacementPlanRequest{
		Operation:    cluster.PlacementOperationMove,
		ShardID:      0,
		TargetVoters: []string{"n1"},
		MinVoters:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Steps) != 0 {
		t.Fatalf("expected no membership steps, got %+v", plan.Steps)
	}
	result, err := mgr.ApplyPlacement(context.Background(), cluster.PlacementApplyRequest{
		Plan:          plan,
		ExpectedEpoch: plan.BeforeEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.AfterEpoch != plan.AfterEpoch || mgr.RoutingEpoch() != plan.AfterEpoch {
		t.Fatalf("epoch not advanced: result=%d manager=%d plan=%d", result.AfterEpoch, mgr.RoutingEpoch(), plan.AfterEpoch)
	}
}

func TestApplyPlacementPreparesSplitOnline(t *testing.T) {
	root := t.TempDir()
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      root,
		Shards:    1,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	plan, err := mgr.PlanPlacement(cluster.PlacementPlanRequest{
		Operation: cluster.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := mgr.ApplyPlacement(context.Background(), cluster.PlacementApplyRequest{
		Plan:          plan,
		ExpectedEpoch: plan.BeforeEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.AfterEpoch != plan.AfterEpoch || mgr.RoutingEpoch() != plan.AfterEpoch {
		t.Fatalf("epoch not advanced: result=%d manager=%d plan=%d", result.AfterEpoch, mgr.RoutingEpoch(), plan.AfterEpoch)
	}
	if len(mgr.Shards()) != 2 {
		t.Fatalf("open shards = %d, want 2", len(mgr.Shards()))
	}
	child, ok := mgr.Shard(1)
	if !ok || child.State != cluster.ShardStateCreating {
		t.Fatalf("child shard not creating: %#v", child)
	}
	if _, err := os.Stat(filepath.Join(root, "shards", "1", "state")); err != nil {
		t.Fatalf("child state dir not created: %v", err)
	}
	key := keyInRange(t, plan.After.Shards[1].Ranges[0])
	if got := mgr.Router().ShardForPK([]byte(key)); got != 0 {
		t.Fatalf("transition route = %d, want parent shard 0", got)
	}

	retry, err := mgr.ApplyPlacement(context.Background(), cluster.PlacementApplyRequest{
		Plan:          plan,
		ExpectedEpoch: plan.BeforeEpoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(retry.Steps) != 1 || retry.Steps[0].Status != "already_applied" {
		t.Fatalf("unexpected retry result: %+v", retry)
	}
}

func TestApplyPlacementPreparesSplitWithRaft(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	addr := pickPort(t)
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:          t.TempDir(),
		Shards:        1,
		SelfID:        "n1",
		MuxAddr:       addr,
		Peers:         map[string]string{"n1": addr},
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
	waitShardLeader(t, mgr, 0)

	plan, err := mgr.PlanPlacement(cluster.PlacementPlanRequest{
		Operation: cluster.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.ApplyPlacement(context.Background(), cluster.PlacementApplyRequest{
		Plan:          plan,
		ExpectedEpoch: plan.BeforeEpoch,
	}); err != nil {
		t.Fatal(err)
	}
	child, ok := mgr.Shard(1)
	if !ok || child.Raft == nil || child.RaftStorage == nil || child.State != cluster.ShardStateCreating {
		t.Fatalf("child raft shard not open: %#v", child)
	}
	waitShardLeader(t, mgr, 1)
}

func waitShardLeader(t *testing.T, mgr *cluster.Manager, shardID uint32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		sh, ok := mgr.Shard(shardID)
		if ok && sh != nil && sh.Raft != nil && sh.Raft.IsLeader() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("shard %d did not become leader", shardID)
}

func TestApplyPlacementRejectsBeforeMismatch(t *testing.T) {
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      t.TempDir(),
		Shards:    1,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()
	plan, err := mgr.PlanPlacement(cluster.PlacementPlanRequest{
		Operation:    cluster.PlacementOperationMove,
		ShardID:      0,
		TargetVoters: []string{"n1"},
		MinVoters:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	plan.Before.UpdatedAtUnix++
	_, err = mgr.ApplyPlacement(context.Background(), cluster.PlacementApplyRequest{Plan: plan})
	if !errors.Is(err, cluster.ErrInvalidPlacementPlan) {
		t.Fatalf("error = %v, want ErrInvalidPlacementPlan", err)
	}
}

func TestFinalizeSplitCopiesRangeAndActivatesChild(t *testing.T) {
	mgr, plan := openTransitionSplitManager(t)
	defer mgr.Close()

	parent, _ := mgr.Shard(0)
	child, _ := mgr.Shard(1)
	td := types.TableDescriptor{Name: "events", KeySchema: types.KeySchema{PK: "id"}}
	parentCatalog, err := catalog.New(parent.Storage)
	if err != nil {
		t.Fatal(err)
	}
	if err := parentCatalog.Create(td); err != nil {
		t.Fatal(err)
	}

	childRange := plan.After.Shards[1].Ranges[0]
	key := keyInRange(t, childRange)
	item := types.Item{
		"id": {T: types.AttrS, S: key},
		"v":  {T: types.AttrS, S: "moved"},
	}
	if err := parent.Storage.PutItem(td.Name, td.KeySchema, item); err != nil {
		t.Fatal(err)
	}

	result, err := mgr.FinalizeSplit(context.Background(), cluster.SplitFinalizeRequest{
		ParentShardID:  0,
		ChildShardID:   1,
		ExpectedEpoch:  plan.AfterEpoch,
		WritesQuiesced: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CopiedKeys != 1 || result.DeletedKeys != 1 || result.CopiedCatalogKeys != 1 {
		t.Fatalf("unexpected copy/delete counts: %+v", result)
	}
	if result.Placement.Shards[0].State != cluster.ShardStateActive || result.Placement.Shards[1].State != cluster.ShardStateActive {
		t.Fatalf("unexpected shard states: %+v", result.Placement.Shards)
	}
	if got := mgr.Router().ShardForPK([]byte(key)); got != 1 {
		t.Fatalf("key routes to shard %d, want child shard 1", got)
	}
	got, err := child.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": {T: types.AttrS, S: key}})
	if err != nil {
		t.Fatalf("child get: %v", err)
	}
	if got["v"].S != "moved" {
		t.Fatalf("child value = %q", got["v"].S)
	}
	if _, err := parent.Storage.GetItem(td.Name, td.KeySchema, types.Item{"id": {T: types.AttrS, S: key}}); !errors.Is(err, types.ErrItemNotFound) {
		t.Fatalf("parent get error = %v, want ErrItemNotFound", err)
	}
}

func TestFinalizeSplitAllowsLiveWriteBarrier(t *testing.T) {
	mgr, plan := openTransitionSplitManager(t)
	defer mgr.Close()

	if _, err := mgr.FinalizeSplit(context.Background(), cluster.SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	}); err != nil {
		t.Fatalf("finalize without external quiesce: %v", err)
	}
}

func TestFinalizeSplitMigratesIndexesAndTTL(t *testing.T) {
	mgr, plan := openTransitionSplitManager(t)
	defer mgr.Close()

	parent, _ := mgr.Shard(0)
	child, _ := mgr.Shard(1)
	td := types.TableDescriptor{
		Name:      "events",
		KeySchema: types.KeySchema{PK: "user_id", SK: "ts"},
		GSIs: []types.GSIDescriptor{{
			Name:      "by_event",
			KeySchema: types.KeySchema{PK: "event", SK: "ts"},
		}},
		LSIs: []types.LSIDescriptor{{
			Name: "by_author",
			SK:   "author",
		}},
		SpatialIndexes: []types.SpatialIndexDescriptor{{
			Name:       "by_location",
			Kind:       storage.SpatialKindGeohash,
			Attributes: []string{"lat", "lon"},
			Precision:  5,
		}},
		TTLAttribute: "expires_at",
	}
	parentCatalog, err := catalog.New(parent.Storage)
	if err != nil {
		t.Fatal(err)
	}
	if err := parentCatalog.Create(td); err != nil {
		t.Fatal(err)
	}

	keys := keysInRange(t, plan.After.Shards[1].Ranges[0], 3)
	now := time.Now()
	moved := types.Item{
		"user_id":    splitSAttr(keys[0]),
		"ts":         splitSAttr("001"),
		"event":      splitSAttr("signup"),
		"author":     splitSAttr("old-author"),
		"lat":        splitNAttr("40.7128"),
		"lon":        splitNAttr("-74.0060"),
		"expires_at": splitNAttr(fmt.Sprintf("%d", now.Add(time.Hour).Unix())),
	}
	if err := parent.Storage.PutItemWith(td, moved, storage.PutOptions{}); err != nil {
		t.Fatalf("put seed: %v", err)
	}
	moved["event"] = splitSAttr("login")
	moved["author"] = splitSAttr("alice")
	if err := parent.Storage.PutItemWith(td, moved, storage.PutOptions{}); err != nil {
		t.Fatalf("update indexed row: %v", err)
	}

	deleted := types.Item{
		"user_id": splitSAttr(keys[1]),
		"ts":      splitSAttr("002"),
		"event":   splitSAttr("login"),
		"author":  splitSAttr("deleted"),
		"lat":     splitNAttr("40.7300"),
		"lon":     splitNAttr("-73.9900"),
	}
	if err := parent.Storage.PutItemWith(td, deleted, storage.PutOptions{}); err != nil {
		t.Fatalf("put deleted seed: %v", err)
	}
	if err := parent.Storage.DeleteItemWith(td, deleted, storage.DeleteOptions{}); err != nil {
		t.Fatalf("delete seed: %v", err)
	}

	expired := types.Item{
		"user_id":    splitSAttr(keys[2]),
		"ts":         splitSAttr("003"),
		"event":      splitSAttr("expired"),
		"author":     splitSAttr("ttl"),
		"lat":        splitNAttr("40.7000"),
		"lon":        splitNAttr("-74.0100"),
		"expires_at": splitNAttr(fmt.Sprintf("%d", now.Add(-time.Hour).Unix())),
	}
	if err := parent.Storage.PutItemWith(td, expired, storage.PutOptions{}); err != nil {
		t.Fatalf("put expired row: %v", err)
	}

	result, err := mgr.FinalizeSplit(context.Background(), cluster.SplitFinalizeRequest{
		ParentShardID: 0,
		ChildShardID:  1,
		ExpectedEpoch: plan.AfterEpoch,
	})
	if err != nil {
		t.Fatalf("finalize indexed split: %v", err)
	}
	if result.CopiedKeys != 2 || result.DeletedKeys != 2 || result.CopiedCatalogKeys != 1 {
		t.Fatalf("unexpected copy/delete counts: %+v", result)
	}

	gsiHits, err := child.Storage.QueryByGSI(td, "by_event", splitSAttr("login"), storage.QueryOptions{})
	if err != nil {
		t.Fatalf("child GSI query: %v", err)
	}
	if len(gsiHits) != 1 || gsiHits[0]["user_id"].S != keys[0] || gsiHits[0]["author"].S != "alice" {
		t.Fatalf("child GSI hits = %+v", gsiHits)
	}
	lsiHits, err := child.Storage.QueryByLSI(td, "by_author", splitSAttr(keys[0]), storage.QueryOptions{})
	if err != nil {
		t.Fatalf("child LSI query: %v", err)
	}
	if len(lsiHits) != 1 || lsiHits[0]["author"].S != "alice" {
		t.Fatalf("child LSI hits = %+v", lsiHits)
	}
	box := spatial.BBox{MinLat: 40.6, MinLon: -74.1, MaxLat: 40.8, MaxLon: -73.9}
	spatialHits, err := child.Storage.SpatialQueryItems(td, "by_location", storage.SpatialQuery{BBox: &box})
	if err != nil {
		t.Fatalf("child spatial query: %v", err)
	}
	if len(spatialHits) != 2 {
		t.Fatalf("child spatial hits = %+v, want moved and expired rows", spatialHits)
	}

	if got := countTableDataKeys(t, parent.Storage, td.Name); got != 0 {
		t.Fatalf("parent table data keys = %d, want 0", got)
	}

	reaper := storage.NewReaper(child.Storage, splitCatalog{tables: []types.TableDescriptor{td}}, nil, storage.ReaperConfig{
		BatchSize: 1024,
		Now:       func() time.Time { return now },
	})
	if err := reaper.Tick(context.Background()); err != nil {
		t.Fatalf("child TTL reaper: %v", err)
	}
	_, err = child.Storage.GetItem(td.Name, td.KeySchema, types.Item{
		"user_id": splitSAttr(keys[2]),
		"ts":      splitSAttr("003"),
	})
	if !errors.Is(err, types.ErrItemNotFound) {
		t.Fatalf("expired child row error = %v, want ErrItemNotFound", err)
	}
}

func openTransitionSplitManager(t *testing.T) (*cluster.Manager, cluster.PlacementPlan) {
	t.Helper()
	root := t.TempDir()
	cat := cluster.DefaultPlacement(1, "n1", nil, nil, cluster.NodeCapacity{}, cluster.PlacementStrategyTokenRange)
	plan, err := cluster.BuildPlacementPlan(cat, cluster.PlacementPlanRequest{
		Operation: cluster.PlacementOperationSplit,
		ShardID:   0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.SavePlacementFile(filepath.Join(root, "placement.json"), plan.After); err != nil {
		t.Fatal(err)
	}
	mgr, err := cluster.Open(context.Background(), cluster.Config{
		Root:      root,
		Shards:    2,
		SelfID:    "n1",
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	return mgr, plan
}

func keyInRange(t *testing.T, rng cluster.TokenRange) string {
	t.Helper()
	keys := keysInRange(t, rng, 1)
	return keys[0]
}

func keysInRange(t *testing.T, rng cluster.TokenRange, n int) []string {
	t.Helper()
	router := cluster.NewRouter(1)
	keys := make([]string, 0, n)
	for i := 0; i < 100_000; i++ {
		key := fmt.Sprintf("split-key-%d", i)
		if rng.Contains(router.TokenForPK([]byte(key))) {
			keys = append(keys, key)
			if len(keys) == n {
				return keys
			}
		}
	}
	t.Fatalf("could not find %d keys in range", n)
	return nil
}

func splitSAttr(s string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrS, S: s}
}

func splitNAttr(n string) types.AttributeValue {
	return types.AttributeValue{T: types.AttrN, N: n}
}

type splitCatalog struct {
	tables []types.TableDescriptor
}

func (c splitCatalog) List() []types.TableDescriptor { return c.tables }

func countTableDataKeys(t *testing.T, db *storage.DB, table string) int {
	t.Helper()
	lower, upper := storage.PrefixTable(table)
	iter, err := db.Iter(lower, upper)
	if err != nil {
		t.Fatal(err)
	}
	defer iter.Close()
	var count int
	for valid := iter.First(); valid; valid = iter.Next() {
		count++
	}
	if err := iter.Error(); err != nil {
		t.Fatal(err)
	}
	return count
}

func contains(in []string, v string) bool {
	for _, existing := range in {
		if existing == v {
			return true
		}
	}
	return false
}
