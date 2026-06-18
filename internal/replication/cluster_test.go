package replication_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	craft "github.com/CefasDb/cefasdb/internal/replication"
	pebble "github.com/CefasDb/cefasdb/internal/storage/adapter/pebble"
	"github.com/CefasDb/cefasdb/internal/testutil/wait"
	"github.com/CefasDb/cefasdb/pkg/types"
)

type node struct {
	t       testing.TB
	id      string
	bind    string
	storage *pebble.DB
	raft    *craft.DB
}

func (n *node) close() {
	if n.raft != nil {
		_ = n.raft.Close()
	}
	if n.storage != nil {
		_ = n.storage.Close()
	}
}

// pickPort grabs an ephemeral TCP port the test can hand to raft. The
// listener closes immediately; raft re-binds the same port a moment
// later. Race-prone in theory, fine in practice for tests.
func pickPort(t testing.TB) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

// startCluster spins up `count` nodes on ephemeral ports. The first
// node bootstraps the cluster from the full peer set; the others wait
// to be joined (they would normally join via AddVoter, but for the
// bootstrap path we pass the same Configuration to every node so they
// agree on initial membership).
func startCluster(t testing.TB, count int) []*node {
	return startClusterWithStorageOptions(t, count, func(path string) pebble.Options {
		return pebble.Options{Path: path}
	})
}

func startClusterWithStorageOptions(t testing.TB, count int, storageOptions func(path string) pebble.Options) []*node {
	t.Helper()
	ids := make([]string, count)
	addrs := make([]string, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("n%d", i)
		addrs[i] = pickPort(t)
	}
	peers := map[string]string{}
	for i := range ids {
		peers[ids[i]] = addrs[i]
	}
	httpPeers := map[string]string{}
	for i := range ids {
		httpPeers[ids[i]] = fmt.Sprintf("http://%s", addrs[i]) // placeholder
	}

	nodes := make([]*node, count)
	for i := range nodes {
		dir := t.TempDir()
		opts := pebble.Options{Path: dir + "/state"}
		if storageOptions != nil {
			opts = storageOptions(dir + "/state")
		}
		st, err := pebble.Open(opts)
		if err != nil {
			t.Fatalf("open storage[%d]: %v", i, err)
		}
		cfg := craft.Config{
			Path:          dir,
			SelfID:        ids[i],
			BindAddr:      addrs[i],
			Bootstrap:     true, // every node sees the same peer set
			PeerAddrs:     peers,
			PeerHTTPAddrs: httpPeers,
			HeartbeatMS:   50,
			ElectionMS:    150,
			LeaderLeaseMS: 50,
			CommitMS:      10,
			ApplyTimeout:  3 * time.Second,
			LogOutput:     io.Discard,
		}
		r, err := craft.Open(context.Background(), cfg, st.Raw())
		if err != nil {
			_ = st.Close()
			t.Fatalf("open raft[%d]: %v", i, err)
		}
		st.AttachReplicator(r)
		nodes[i] = &node{t: t, id: ids[i], bind: addrs[i], storage: st, raft: r}
	}
	return nodes
}

// waitLeader returns the index of the node that's currently the raft
// leader. Polls for up to 10 s. The first leader wins under tight
// election timeouts.
func waitLeader(t testing.TB, nodes []*node) int {
	t.Helper()
	leader := -1
	wait.Eventually(t, func() bool {
		for i, n := range nodes {
			if n.raft.IsLeader() {
				leader = i
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "no leader after 10s")
	return leader
}

func waitNewLeader(t testing.TB, nodes []*node, exclude int) int {
	t.Helper()
	leader := -1
	wait.Eventually(t, func() bool {
		for i, n := range nodes {
			if i == exclude {
				continue
			}
			if n.raft.IsLeader() {
				leader = i
				return true
			}
		}
		return false
	}, 10*time.Second, 50*time.Millisecond, "no replacement leader after 10s")
	return leader
}

func waitItem(t testing.TB, n *node, table string, ks types.KeySchema, key types.Item, deadline time.Duration) types.Item {
	t.Helper()
	var got types.Item
	wait.Eventually(t, func() bool {
		out, err := n.storage.GetItem(table, ks, key)
		if err == nil {
			got = out
			return true
		}
		if !errors.Is(err, types.ErrItemNotFound) {
			t.Fatalf("getItem on %s: %v", n.id, err)
		}
		return false
	}, deadline, 20*time.Millisecond, "item not replicated to %s within %v", n.id, deadline)
	return got
}

func sAttr(s string) types.AttributeValue { return types.AttributeValue{T: types.AttrS, S: s} }

func TestRaftReplicatesAcrossNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	nodes := startCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.close()
		}
	}()

	leader := waitLeader(t, nodes)
	td := types.TableDescriptor{Name: "tbl", KeySchema: types.KeySchema{PK: "id"}}

	// Write on the leader.
	item := types.Item{"id": sAttr("k1"), "data": sAttr("hello")}
	if err := nodes[leader].storage.PutItemWith(td, item, pebble.PutOptions{}); err != nil {
		t.Fatalf("put on leader: %v", err)
	}

	// Every follower must see the same value.
	for i, n := range nodes {
		if i == leader {
			continue
		}
		got := waitItem(t, n, "tbl", td.KeySchema, types.Item{"id": sAttr("k1")}, 5*time.Second)
		if got["data"].S != "hello" {
			t.Fatalf("follower %s has data=%q, want hello", n.id, got["data"].S)
		}
	}
}

func TestRaftWriteConsistencyOneReplicatesAsyncAcrossNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	nodes := startClusterWithStorageOptions(t, 3, func(path string) pebble.Options {
		return pebble.Options{Path: path, WriteConsistency: "one"}
	})
	defer func() {
		for _, n := range nodes {
			n.close()
		}
	}()

	leader := waitLeader(t, nodes)
	td := types.TableDescriptor{Name: "tbl", KeySchema: types.KeySchema{PK: "id"}}

	item := types.Item{"id": sAttr("async-1"), "data": sAttr("eventual")}
	if err := nodes[leader].storage.PutItemWith(td, item, pebble.PutOptions{}); err != nil {
		t.Fatalf("put on leader: %v", err)
	}
	if stats := nodes[leader].raft.AsyncReplicationStats(); stats.Submitted == 0 {
		t.Fatalf("async submitted = %d, want > 0", stats.Submitted)
	}

	for i, n := range nodes {
		if i == leader {
			continue
		}
		got := waitItem(t, n, "tbl", td.KeySchema, types.Item{"id": sAttr("async-1")}, 5*time.Second)
		if got["data"].S != "eventual" {
			t.Fatalf("follower %s has data=%q, want eventual", n.id, got["data"].S)
		}
	}
}

func TestRaftFollowerWritesAreRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	nodes := startCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.close()
		}
	}()

	leader := waitLeader(t, nodes)
	follower := (leader + 1) % len(nodes)
	td := types.TableDescriptor{Name: "tbl", KeySchema: types.KeySchema{PK: "id"}}

	err := nodes[follower].storage.PutItemWith(td, types.Item{"id": sAttr("x"), "v": sAttr("y")}, pebble.PutOptions{})
	if !errors.Is(err, pebble.ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestRaftTransfersLeadershipToSpecificNode(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	nodes := startCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.close()
		}
	}()

	leader := waitLeader(t, nodes)
	target := (leader + 1) % len(nodes)
	if err := nodes[leader].raft.TransferLeadership(nodes[target].id, nodes[target].bind, 5*time.Second); err != nil {
		t.Fatalf("transfer leadership: %v", err)
	}
	wait.Eventually(t, func() bool {
		return nodes[target].raft.IsLeader()
	}, 10*time.Second, 50*time.Millisecond, "leadership did not transfer to %s", nodes[target].id)
}

func TestRaftSurvivesLeaderLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("short")
	}
	nodes := startCluster(t, 3)
	defer func() {
		for _, n := range nodes {
			n.close()
		}
	}()

	leader := waitLeader(t, nodes)
	td := types.TableDescriptor{Name: "tbl", KeySchema: types.KeySchema{PK: "id"}}

	if err := nodes[leader].storage.PutItemWith(td, types.Item{"id": sAttr("pre"), "data": sAttr("before")}, pebble.PutOptions{}); err != nil {
		t.Fatalf("pre-failover write: %v", err)
	}
	// Wait until at least one follower has applied the entry — this
	// guarantees the survivors' logs cover the pre-failover write.
	other := (leader + 1) % len(nodes)
	waitItem(t, nodes[other], "tbl", td.KeySchema, types.Item{"id": sAttr("pre")}, 5*time.Second)

	// Kill the leader hard.
	if err := nodes[leader].raft.Close(); err != nil {
		t.Fatalf("close leader: %v", err)
	}

	newLeader := waitNewLeader(t, nodes, leader)
	if err := nodes[newLeader].storage.PutItemWith(td, types.Item{"id": sAttr("post"), "data": sAttr("after")}, pebble.PutOptions{}); err != nil {
		t.Fatalf("post-failover write: %v", err)
	}

	// All surviving followers see both writes.
	for i, n := range nodes {
		if i == leader {
			continue
		}
		pre := waitItem(t, n, "tbl", td.KeySchema, types.Item{"id": sAttr("pre")}, 5*time.Second)
		post := waitItem(t, n, "tbl", td.KeySchema, types.Item{"id": sAttr("post")}, 5*time.Second)
		if pre["data"].S != "before" || post["data"].S != "after" {
			t.Fatalf("survivor %s has wrong state: pre=%q post=%q", n.id, pre["data"].S, post["data"].S)
		}
	}
}
