package bandit

import (
	"sort"
	"sync"
)

// MemoryStore is the in-memory Store the plugin defaults to. Tests +
// the no-storage fast path use it directly; the cefas server binds a
// storage-backed Store before the bandit plugin starts taking
// traffic. Safe for concurrent use.
type MemoryStore struct {
	mu   sync.Mutex
	meta map[string]*MetaRecord           // banditID → meta
	arms map[string]map[string]*ArmRecord // banditID → armID → arm
}

// NewMemoryStore returns a Store backed by Go maps.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		meta: map[string]*MetaRecord{},
		arms: map[string]map[string]*ArmRecord{},
	}
}

// GetMeta implements Store.
func (m *MemoryStore) GetMeta(banditID string) (*MetaRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.meta[banditID]
	if !ok {
		return nil, false, nil
	}
	cp := *r
	cp.ArmIDs = append([]string(nil), r.ArmIDs...)
	return &cp, true, nil
}

// PutMeta implements Store.
func (m *MemoryStore) PutMeta(rec MetaRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := rec
	cp.ArmIDs = append([]string(nil), rec.ArmIDs...)
	m.meta[rec.BanditID] = &cp
	if _, ok := m.arms[rec.BanditID]; !ok {
		m.arms[rec.BanditID] = map[string]*ArmRecord{}
	}
	return nil
}

// GetArm implements Store.
func (m *MemoryStore) GetArm(banditID, armID string) (*ArmRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bag, ok := m.arms[banditID]
	if !ok {
		return nil, false, nil
	}
	r, ok := bag[armID]
	if !ok {
		return nil, false, nil
	}
	cp := *r
	return &cp, true, nil
}

// PutArm implements Store with the optimistic-lock contract. When
// expectedVersion < 0 the write is unconditional; otherwise the
// on-disk record's version must match.
func (m *MemoryStore) PutArm(rec ArmRecord, expectedVersion int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	bag, ok := m.arms[rec.BanditID]
	if !ok {
		bag = map[string]*ArmRecord{}
		m.arms[rec.BanditID] = bag
	}
	if expectedVersion >= 0 {
		if cur, ok := bag[rec.ArmID]; ok {
			if cur.Version != expectedVersion {
				return ErrConditionFailed
			}
		} else if expectedVersion != 0 {
			return ErrConditionFailed
		}
	}
	cp := rec
	bag[rec.ArmID] = &cp
	return nil
}

// ListArms implements Store.
func (m *MemoryStore) ListArms(banditID string) ([]ArmRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	bag, ok := m.arms[banditID]
	if !ok {
		return nil, nil
	}
	out := make([]ArmRecord, 0, len(bag))
	for _, r := range bag {
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ArmID < out[j].ArmID })
	return out, nil
}
