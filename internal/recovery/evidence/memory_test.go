package evidence_test

import (
	"bytes"
	"context"
	"sync"
	"time"

	"github.com/CefasDb/cefasdb/internal/recovery/evidence"
)

type memoryValue struct {
	record    evidence.QualificationRecord
	canonical []byte
	ref       evidence.EvidenceRef
}
type memoryBacking struct {
	mu       sync.Mutex
	records  map[string]memoryValue
	key      []byte
	failNext bool
	now      func() time.Time
}
type memoryStore struct {
	scope string
	data  *memoryBacking
}

func newMemory(scope string) (*memoryStore, func() evidence.EvidenceStore) {
	at := time.Date(2026, 11, 22, 4, 0, 0, 0, time.UTC)
	data := &memoryBacking{records: map[string]memoryValue{}, key: []byte("evidence-test-cursor-key"), now: func() time.Time { return at }}
	open := func() evidence.EvidenceStore { return &memoryStore{scope: scope, data: data} }
	return open().(*memoryStore), open
}
func (s *memoryStore) Durable() bool { return false }
func (s *memoryStore) allowed(scope, op string) error {
	if scope != s.scope {
		return &evidence.Error{Code: evidence.CodeScopeDenied, Operation: op, Field: "scopeId"}
	}
	return nil
}

func (s *memoryStore) PutTerminal(ctx context.Context, input evidence.QualificationRecord) (evidence.EvidenceRef, error) {
	if err := s.allowed(input.ScopeID, "PutTerminal"); err != nil {
		return evidence.EvidenceRef{}, err
	}
	if err := ctx.Err(); err != nil {
		return evidence.EvidenceRef{}, err
	}
	record, canonical, digest, err := evidence.PrepareRecord(input)
	if err != nil {
		return evidence.EvidenceRef{}, err
	}
	s.data.mu.Lock()
	defer s.data.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return evidence.EvidenceRef{}, err
	}
	if old, ok := s.data.records[record.RunID]; ok {
		if bytes.Equal(old.canonical, canonical) {
			return old.ref, nil
		}
		return evidence.EvidenceRef{}, &evidence.Error{Code: evidence.CodeEvidenceConflict, Operation: "PutTerminal"}
	}
	if s.data.failNext {
		s.data.failNext = false
		return evidence.EvidenceRef{}, &evidence.Error{Code: evidence.CodeEvidenceNotDurable, Operation: "PutTerminal"}
	}
	ref := evidence.EvidenceRef{Key: "scopes/" + record.ScopeID + "/runs/" + record.RunID + "/qualification.v1.json", SHA256: digest, CreatedAt: s.data.now()}
	s.data.records[record.RunID] = memoryValue{record: evidence.CopyRecord(record), canonical: append([]byte(nil), canonical...), ref: ref}
	return ref, nil
}

func (s *memoryStore) Get(ctx context.Context, scope, runID string) (evidence.QualificationRecord, error) {
	if err := s.allowed(scope, "Get"); err != nil {
		return evidence.QualificationRecord{}, err
	}
	if err := ctx.Err(); err != nil {
		return evidence.QualificationRecord{}, err
	}
	s.data.mu.Lock()
	defer s.data.mu.Unlock()
	value, ok := s.data.records[runID]
	if !ok {
		return evidence.QualificationRecord{}, &evidence.Error{Code: evidence.CodeEvidenceNotFound, Operation: "Get", Field: "runId"}
	}
	return evidence.CopyRecord(value.record), nil
}

func (s *memoryStore) List(ctx context.Context, scope string, size int, token string) (evidence.EvidencePage, error) {
	if err := s.allowed(scope, "List"); err != nil {
		return evidence.EvidencePage{}, err
	}
	if err := ctx.Err(); err != nil {
		return evidence.EvidencePage{}, err
	}
	s.data.mu.Lock()
	defer s.data.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return evidence.EvidencePage{}, err
	}
	records := make(map[string]evidence.QualificationRecord, len(s.data.records))
	for id, value := range s.data.records {
		records[id] = evidence.CopyRecord(value.record)
	}
	return evidence.Paginate(records, scope, size, token, s.data.key)
}

func (s *memoryStore) failPersistence() { s.data.mu.Lock(); s.data.failNext = true; s.data.mu.Unlock() }
