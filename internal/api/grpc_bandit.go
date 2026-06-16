// Handler set for the bandit operator (issue #246). Wires the
// BanditCreate / BanditSample / BanditReward / BanditDescribe RPCs
// against the in-tree bandit plugin (pkg/plugin/bandit).
//
// Posterior persistence:
//   - Records live under raw pebble keys prefixed "cefas/internal/bandit/…"
//     so they sit in the cefas internal namespace and never collide
//     with user tables.
//   - Writes go through DB.Set (single-key) — which already flows
//     through the replicator on a multi-node setup, so posteriors are
//     replicated linearizably.
//   - Optimistic concurrency is implemented in pkg/plugin/bandit via
//     a Get/CompareVersion/Set loop. The atomic read-modify-write
//     primitive from #242 is being built in a sibling branch and is
//     NOT available here; once it lands the dbBanditStore.PutArm
//     implementation should swap the loop for one atomic call.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/osvaldoandrade/cefas/internal/auth"
	pebble "github.com/osvaldoandrade/cefas/internal/storage/adapter/pebble"
	"github.com/osvaldoandrade/cefas/internal/tracing"
	cefaspb "github.com/osvaldoandrade/cefas/pkg/api/proto"
	"github.com/osvaldoandrade/cefas/pkg/plugin"
	"github.com/osvaldoandrade/cefas/pkg/plugin/bandit"
)

// Posterior records live under the cefas internal namespace so they
// never collide with user tables.
const (
	banditMetaPrefix = "cefas/internal/bandit/meta/"
	banditArmPrefix  = "cefas/internal/bandit/arm/"
)

// banditBindRegistry tracks which (server, plugin) pairs have already
// had a storage-backed Store bound. Keyed by plugin pointer because
// the plugin registry is process-global and tests often share it.
// One entry per plugin instance keeps the binding idempotent without
// adding a field to GRPCServer.
var (
	banditBindMu       sync.Mutex
	banditBindRegistry = map[*bandit.Plugin]struct{}{}
)

// ensureBanditStore lazily binds a pebble-backed Store onto the
// bandit plugin so RPCs serve out of persistent storage.
func (s *GRPCServer) ensureBanditStore() (*bandit.Plugin, error) {
	plug, ok := s.pluginRegistry().Lookup("bandit")
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "bandit plugin not registered")
	}
	bp, ok := plug.(*bandit.Plugin)
	if !ok {
		return nil, status.Error(codes.Internal, "registered bandit plugin has unexpected type")
	}
	banditBindMu.Lock()
	if _, bound := banditBindRegistry[bp]; !bound {
		bp.Bind(newDBBanditStore(s.db))
		banditBindRegistry[bp] = struct{}{}
	}
	banditBindMu.Unlock()
	return bp, nil
}

// BanditCreate registers a bandit + its arms with the plugin.
func (s *GRPCServer) BanditCreate(ctx context.Context, req *cefaspb.BanditCreateRequest) (*cefaspb.BanditCreateResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "BanditCreate")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableCreate); err != nil {
		return nil, err
	}
	bp, err := s.ensureBanditStore()
	if err != nil {
		return nil, err
	}
	if req.GetBanditId() == "" {
		return nil, status.Error(codes.InvalidArgument, "bandit_id required")
	}
	if len(req.GetArms()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "arms required")
	}
	spec := plugin.BanditSpec{
		BanditID: req.GetBanditId(),
		Strategy: req.GetStrategy(),
		Epsilon:  req.GetEpsilon(),
		C:        req.GetC(),
	}
	for _, a := range req.GetArms() {
		spec.Arms = append(spec.Arms, plugin.BanditArmSpec{
			ArmID:  a.GetArmId(),
			Family: a.GetFamily(),
			Alpha:  a.GetAlpha(),
			Beta:   a.GetBeta(),
			Mu:     a.GetMu(),
			Sigma:  a.GetSigma(),
		})
	}
	if err := bp.Init(spec); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "bandit create: %v", err)
	}
	return &cefaspb.BanditCreateResponse{}, nil
}

// BanditSample returns one or more arm IDs from the named bandit.
func (s *GRPCServer) BanditSample(ctx context.Context, req *cefaspb.BanditSampleRequest) (*cefaspb.BanditSampleResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "BanditSample")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeItemRead); err != nil {
		return nil, err
	}
	bp, err := s.ensureBanditStore()
	if err != nil {
		return nil, err
	}
	n := int(req.GetN())
	if n <= 1 {
		arm, err := bp.Sample(req.GetBanditId(), req.GetContext())
		if err != nil {
			return nil, mapBanditErr(err)
		}
		return &cefaspb.BanditSampleResponse{ArmId: []string{arm}}, nil
	}
	arms, err := bp.BatchSample(req.GetBanditId(), req.GetContext(), n)
	if err != nil {
		return nil, mapBanditErr(err)
	}
	return &cefaspb.BanditSampleResponse{ArmId: arms}, nil
}

// BanditReward records one reward observation. The plugin handles the
// optimistic-lock retry loop internally.
func (s *GRPCServer) BanditReward(ctx context.Context, req *cefaspb.BanditRewardRequest) (*cefaspb.BanditRewardResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "BanditReward")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeItemWrite); err != nil {
		return nil, err
	}
	bp, err := s.ensureBanditStore()
	if err != nil {
		return nil, err
	}
	if err := bp.Reward(req.GetBanditId(), req.GetArmId(), req.GetReward(), req.GetContext()); err != nil {
		return nil, mapBanditErr(err)
	}
	return &cefaspb.BanditRewardResponse{}, nil
}

// BanditDescribe returns the live posterior for every arm under
// bandit_id. Read-only.
func (s *GRPCServer) BanditDescribe(ctx context.Context, req *cefaspb.BanditDescribeRequest) (*cefaspb.BanditDescribeResponse, error) {
	ctx, span := tracing.Tracer().Start(ctx, "BanditDescribe")
	defer span.End()
	if err := requireScope(ctx, auth.ScopeTableDescribe); err != nil {
		return nil, err
	}
	bp, err := s.ensureBanditStore()
	if err != nil {
		return nil, err
	}
	snap, err := bp.Snapshot(req.GetBanditId())
	if err != nil {
		return nil, mapBanditErr(err)
	}
	resp := &cefaspb.BanditDescribeResponse{
		BanditId: snap.BanditID,
		Strategy: snap.Strategy,
	}
	for _, a := range snap.Arms {
		resp.Arms = append(resp.Arms, &cefaspb.BanditArmStats{
			ArmId:   a.ArmID,
			Family:  a.Family,
			Alpha:   a.Alpha,
			Beta:    a.Beta,
			Mu:      a.Mu,
			Sigma:   a.Sigma,
			Pulls:   a.Pulls,
			Rewards: a.Rewards,
			Mean:    a.Mean,
		})
	}
	return resp, nil
}

func mapBanditErr(err error) error {
	switch {
	case errors.Is(err, bandit.ErrUnknownBandit):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, bandit.ErrUnknownArm):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, bandit.ErrNoArms), errors.Is(err, bandit.ErrBadStrategy):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, bandit.ErrNoEligibleArms):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, bandit.ErrTooManyRetries):
		return status.Error(codes.Aborted, err.Error())
	}
	return status.Errorf(codes.Internal, "bandit: %v", err)
}

// ---------- pebble-backed Store ----------

// dbBanditStore persists posterior records as raw key/values under
// the cefas internal namespace. PutArm implements the
// expected-version contract via a Get + version check + Set sequence
// — this is correct under a single-writer leader (Set goes through
// the replicator); under raw concurrent writes from the same node we
// rely on the bandit plugin retry loop to handle losers.
type dbBanditStore struct {
	db *pebble.DB

	locksMu sync.Mutex
	locks   map[string]*sync.Mutex
}

func newDBBanditStore(db *pebble.DB) *dbBanditStore {
	return &dbBanditStore{db: db, locks: map[string]*sync.Mutex{}}
}

func metaKey(banditID string) []byte { return []byte(banditMetaPrefix + banditID) }

func armKey(banditID, armID string) []byte {
	return []byte(banditArmPrefix + banditID + "/" + armID)
}

func armPrefix(banditID string) []byte {
	return []byte(banditArmPrefix + banditID + "/")
}

func (s *dbBanditStore) GetMeta(banditID string) (*bandit.MetaRecord, bool, error) {
	v, err := s.db.Get(metaKey(banditID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("bandit db get meta: %w", err)
	}
	var rec bandit.MetaRecord
	if err := json.Unmarshal(v, &rec); err != nil {
		return nil, false, fmt.Errorf("bandit db decode meta: %w", err)
	}
	return &rec, true, nil
}

func (s *dbBanditStore) PutMeta(rec bandit.MetaRecord) error {
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.db.Set(metaKey(rec.BanditID), b)
}

func (s *dbBanditStore) GetArm(banditID, armID string) (*bandit.ArmRecord, bool, error) {
	v, err := s.db.Get(armKey(banditID, armID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("bandit db get arm: %w", err)
	}
	var rec bandit.ArmRecord
	if err := json.Unmarshal(v, &rec); err != nil {
		return nil, false, fmt.Errorf("bandit db decode arm: %w", err)
	}
	return &rec, true, nil
}

// PutArm performs a read-compare-set under a short critical section
// per arm. The arm-level lock is sufficient because a single cefas
// shard owns the key prefix; in a multi-shard layout the arm key
// hashes to one shard and writes are serialised by the replicator.
// Once #242 lands this loop collapses into one atomic call.
func (s *dbBanditStore) PutArm(rec bandit.ArmRecord, expectedVersion int64) error {
	lock := s.lockFor(rec.BanditID, rec.ArmID)
	lock.Lock()
	defer lock.Unlock()

	if expectedVersion >= 0 {
		cur, err := s.db.Get(armKey(rec.BanditID, rec.ArmID))
		switch {
		case errors.Is(err, pebble.ErrNotFound):
			if expectedVersion != 0 {
				return bandit.ErrConditionFailed
			}
		case err != nil:
			return fmt.Errorf("bandit db read for cas: %w", err)
		default:
			var existing bandit.ArmRecord
			if err := json.Unmarshal(cur, &existing); err != nil {
				return fmt.Errorf("bandit db decode for cas: %w", err)
			}
			if existing.Version != expectedVersion {
				return bandit.ErrConditionFailed
			}
		}
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return s.db.Set(armKey(rec.BanditID, rec.ArmID), b)
}

func (s *dbBanditStore) ListArms(banditID string) ([]bandit.ArmRecord, error) {
	prefix := armPrefix(banditID)
	upper := append([]byte(nil), prefix...)
	upper[len(upper)-1]++
	it, err := s.db.Iter(prefix, upper)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	out := make([]bandit.ArmRecord, 0)
	for it.First(); it.Valid(); it.Next() {
		if !bytes.HasPrefix(it.Key(), prefix) {
			continue
		}
		var rec bandit.ArmRecord
		if err := json.Unmarshal(it.Value(), &rec); err != nil {
			return nil, fmt.Errorf("bandit db decode arm: %w", err)
		}
		out = append(out, rec)
	}
	return out, it.Error()
}

// lockFor returns the per-arm mutex used to serialise the
// read-compare-set sequence inside PutArm. The map grows monotonically
// with the live arm set — bounded by the bandit cardinality, which is
// expected to be small (tens to thousands).
func (s *dbBanditStore) lockFor(banditID, armID string) *sync.Mutex {
	k := banditID + "/" + armID
	s.locksMu.Lock()
	defer s.locksMu.Unlock()
	m, ok := s.locks[k]
	if !ok {
		m = &sync.Mutex{}
		s.locks[k] = m
	}
	return m
}
