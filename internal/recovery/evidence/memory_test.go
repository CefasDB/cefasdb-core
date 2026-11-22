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
	// mu protects records and failNext for every handle sharing this backing.
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

func newMemoryBacking() *memoryBacking {
	at := time.Date(2026, 11, 22, 4, 0, 0, 0, time.UTC)
	return &memoryBacking{
		records: map[string]memoryValue{},
		key:     []byte("evidence-test-cursor-key"),
		now:     func() time.Time { return at },
	}
}

func openMemory(data *memoryBacking, scope string) (*memoryStore, func() evidence.EvidenceStore) {
	open := func() evidence.EvidenceStore { return &memoryStore{scope: scope, data: data} }
	return open().(*memoryStore), open
}

func newMemory(scope string) (*memoryStore, func() evidence.EvidenceStore) {
	return openMemory(newMemoryBacking(), scope)
}

func memoryKey(scope, runID string) string { return scope + "\x00" + runID }

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
	key := memoryKey(record.ScopeID, record.RunID)
	if old, ok := s.data.records[key]; ok {
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
	s.data.records[key] = memoryValue{record: evidence.CopyRecord(record), canonical: append([]byte(nil), canonical...), ref: ref}
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
	if err := ctx.Err(); err != nil {
		return evidence.QualificationRecord{}, err
	}
	value, ok := s.data.records[memoryKey(scope, runID)]
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
	for _, value := range s.data.records {
		if value.record.ScopeID == scope {
			records[value.record.RunID] = evidence.CopyRecord(value.record)
		}
	}
	return evidence.Paginate(records, scope, size, token, s.data.key)
}

func (s *memoryStore) failPersistence() {
	s.data.mu.Lock()
	defer s.data.mu.Unlock()
	s.data.failNext = true
}
