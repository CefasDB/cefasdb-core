package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/CefasDb/cefasdb/internal/placement"
)

const (
	leaderHintReconcileWindow   = 30 * time.Second
	leaderHintReconcileInterval = 250 * time.Millisecond
	leaderHintTransferTimeout   = 5 * time.Second
)

func (m *Manager) startLeaderHintReconciliation() {
	if !m.hasTransferableLeaderHints() {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), leaderHintReconcileWindow)
	go func() {
		defer cancel()
		ticker := time.NewTicker(leaderHintReconcileInterval)
		defer ticker.Stop()
		for {
			_ = m.ApplyLeaderHints(ctx)
			if m.leaderHintsSatisfied() {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// ApplyLeaderHints asks any locally-led shard to transfer leadership to
// the placement catalog's hinted voter. Followers skip the shard; the
// same reconciliation runs on every node during startup, so whichever
// process currently leads a shard performs the handoff.
func (m *Manager) ApplyLeaderHints(ctx context.Context) error {
	var errs []error
	result, err := m.RebalanceLeaders(ctx, LeaderRebalanceRequest{
		IncludeShardZero: true,
		MaxConcurrent:    1,
		Timeout:          leaderHintTransferTimeout,
	})
	if err != nil {
		return err
	}
	for _, step := range result.Steps {
		if step.Status == "failed" {
			errs = append(errs, fmt.Errorf("shard %d: %s: %s", step.ShardID, step.Reason, step.Detail))
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) hasTransferableLeaderHints() bool {
	for _, meta := range m.Placement().Shards {
		if !transferableLeaderHint(meta) {
			continue
		}
		sh, ok := m.Shard(meta.ID)
		if ok && sh != nil && sh.Raft != nil {
			return true
		}
	}
	return false
}

func (m *Manager) leaderHintsSatisfied() bool {
	for _, meta := range m.Placement().Shards {
		if !transferableLeaderHint(meta) {
			continue
		}
		sh, ok := m.Shard(meta.ID)
		if !ok || sh == nil || sh.Raft == nil {
			continue
		}
		leaderID, _ := sh.Raft.LeaderInfo()
		if leaderID != meta.LeaderHint {
			return false
		}
	}
	return true
}

func transferableLeaderHint(sh placement.ShardPlacement) bool {
	if sh.LeaderHint == "" || !containsString(sh.Voters, sh.LeaderHint) {
		return false
	}
	return len(uniqueStrings(sh.Voters)) > 1
}

func leaderHintRaftAddr(cat placement.PlacementCatalog, nodeID string) string {
	if cat.Nodes == nil {
		return ""
	}
	node := cat.Nodes[nodeID]
	return node.RaftAddr
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func containsString(in []string, want string) bool {
	for _, v := range in {
		if v == want {
			return true
		}
	}
	return false
}
