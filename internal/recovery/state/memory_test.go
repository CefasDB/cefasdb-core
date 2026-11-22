package state_test

import (
	"context"
	"sync"

	"github.com/CefasDb/cefasdb/internal/recovery/state"
)

type memoryBacking struct {
	mu   sync.Mutex
	runs map[string]state.RecoveryRun
	key  []byte
}
type memoryStore struct {
	scope string
	data  *memoryBacking
}

func newMemory(scope string) (*memoryStore, func() state.RunStore) {
	data := &memoryBacking{runs: map[string]state.RecoveryRun{}, key: []byte("state-test-cursor-key")}
	open := func() state.RunStore { return &memoryStore{scope: scope, data: data} }
	return open().(*memoryStore), open
}
func (s *memoryStore) Durable() bool { return false }
func (s *memoryStore) allowed(scope, op string) error {
	if scope != s.scope {
		return &state.Error{Code: state.CodeScopeDenied, Operation: op, Field: "scopeId"}
	}
	return nil
}

func (s *memoryStore) Create(ctx context.Context, run state.RecoveryRun) (state.RecoveryRun, error) {
	if err := s.allowed(run.ScopeID, "Create"); err != nil {
		return state.RecoveryRun{}, err
	}
	if err := ctx.Err(); err != nil {
		return state.RecoveryRun{}, err
	}
	if err := state.ValidateRun(run); err != nil {
		return state.RecoveryRun{}, err
	}
	if run.Revision != 1 {
		return state.RecoveryRun{}, &state.Error{Code: state.CodeInvalidRun, Operation: "Create", Field: "revision"}
	}
	run = state.CopyRun(run)
	s.data.mu.Lock()
	defer s.data.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return state.RecoveryRun{}, err
	}
	if old, ok := s.data.runs[run.RunID]; ok {
		if old.PayloadSHA256 != run.PayloadSHA256 {
			return state.RecoveryRun{}, &state.Error{Code: state.CodeRunPayloadConflict, Operation: "Create", Revision: old.Revision}
		}
		return state.CopyRun(old), nil
	}
	s.data.runs[run.RunID] = run
	return state.CopyRun(run), nil
}

func (s *memoryStore) Get(ctx context.Context, scope, runID string) (state.RecoveryRun, error) {
	if err := s.allowed(scope, "Get"); err != nil {
		return state.RecoveryRun{}, err
	}
	if err := ctx.Err(); err != nil {
		return state.RecoveryRun{}, err
	}
	s.data.mu.Lock()
	defer s.data.mu.Unlock()
	run, ok := s.data.runs[runID]
	if !ok {
		return state.RecoveryRun{}, &state.Error{Code: state.CodeRunNotFound, Operation: "Get", Field: "runId"}
	}
	return state.CopyRun(run), nil
}

func (s *memoryStore) CompareAndSwap(ctx context.Context, scope, runID string, expected uint64, next state.RecoveryRun) (state.RecoveryRun, error) {
	if err := s.allowed(scope, "CompareAndSwap"); err != nil {
		return state.RecoveryRun{}, err
	}
	if err := ctx.Err(); err != nil {
		return state.RecoveryRun{}, err
	}
	s.data.mu.Lock()
	defer s.data.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return state.RecoveryRun{}, err
	}
	current, ok := s.data.runs[runID]
	if !ok {
		return state.RecoveryRun{}, &state.Error{Code: state.CodeRunNotFound, Operation: "CompareAndSwap", Field: "runId"}
	}
	if current.Revision == expected+1 && state.SameRun(current, next) {
		return state.CopyRun(current), nil
	}
	if current.Revision != expected {
		return state.RecoveryRun{}, &state.Error{Code: state.CodeRevisionConflict, Operation: "CompareAndSwap", Revision: current.Revision}
	}
	if err := state.ValidateAdvance(current, next, expected); err != nil {
		return state.RecoveryRun{}, err
	}
	s.data.runs[runID] = state.CopyRun(next)
	return state.CopyRun(next), nil
}

func (s *memoryStore) List(ctx context.Context, scope string, size int, token string) (state.RunPage, error) {
	if err := s.allowed(scope, "List"); err != nil {
		return state.RunPage{}, err
	}
	if err := ctx.Err(); err != nil {
		return state.RunPage{}, err
	}
	s.data.mu.Lock()
	defer s.data.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return state.RunPage{}, err
	}
	return state.Paginate(s.data.runs, scope, size, token, s.data.key)
}
