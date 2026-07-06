package server

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type LifecycleState string

const (
	LifecycleStarting LifecycleState = "starting"
	LifecycleServing  LifecycleState = "serving"
	LifecycleDraining LifecycleState = "draining"
)

type LifecycleSnapshot struct {
	State     LifecycleState `json:"state"`
	Started   bool           `json:"started"`
	Draining  bool           `json:"draining"`
	Reason    string         `json:"reason,omitempty"`
	UpdatedAt time.Time      `json:"updatedAt"`
}

type ReadinessCheck func(context.Context) error

type namedReadinessCheck struct {
	name string
	fn   ReadinessCheck
}

type Lifecycle struct {
	mu     sync.RWMutex
	checks []namedReadinessCheck

	started   bool
	draining  bool
	reason    string
	updatedAt time.Time
}

func NewLifecycle() *Lifecycle {
	return &Lifecycle{updatedAt: time.Now().UTC()}
}

func (l *Lifecycle) MarkStarted() {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.started = true
	l.updatedAt = time.Now().UTC()
}

func (l *Lifecycle) StartDraining(reason string) {
	if l == nil {
		return
	}
	if reason == "" {
		reason = "draining"
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.draining = true
	l.reason = reason
	l.updatedAt = time.Now().UTC()
}

func (l *Lifecycle) Snapshot() LifecycleSnapshot {
	if l == nil {
		return LifecycleSnapshot{State: LifecycleStarting, UpdatedAt: time.Now().UTC()}
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	state := LifecycleStarting
	if l.draining {
		state = LifecycleDraining
	} else if l.started {
		state = LifecycleServing
	}
	return LifecycleSnapshot{
		State:     state,
		Started:   l.started,
		Draining:  l.draining,
		Reason:    l.reason,
		UpdatedAt: l.updatedAt,
	}
}

func (l *Lifecycle) AddReadinessCheck(name string, fn ReadinessCheck) {
	if l == nil || fn == nil {
		return
	}
	if name == "" {
		name = "readiness"
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.checks = append(l.checks, namedReadinessCheck{name: name, fn: fn})
}

func (l *Lifecycle) readinessChecks() []namedReadinessCheck {
	if l == nil {
		return nil
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	return append([]namedReadinessCheck(nil), l.checks...)
}

func (l *Lifecycle) servingError() error {
	snap := l.Snapshot()
	if !snap.Started {
		return fmt.Errorf("startup not complete")
	}
	if snap.Draining {
		if snap.Reason != "" {
			return fmt.Errorf("draining: %s", snap.Reason)
		}
		return fmt.Errorf("draining")
	}
	return nil
}

func (s *Server) Lifecycle() *Lifecycle {
	if s.lifecycle == nil {
		s.lifecycle = NewLifecycle()
	}
	return s.lifecycle
}

func (s *Server) AttachLifecycle(l *Lifecycle) {
	if l == nil {
		l = NewLifecycle()
	}
	s.lifecycle = l
}

func (s *Server) MarkStarted() { s.Lifecycle().MarkStarted() }

func (s *Server) StartDraining(reason string) { s.Lifecycle().StartDraining(reason) }

func (s *Server) AddReadinessCheck(name string, fn ReadinessCheck) {
	s.Lifecycle().AddReadinessCheck(name, fn)
}

func (s *GRPCServer) AttachLifecycle(l *Lifecycle) {
	if l == nil {
		l = NewLifecycle()
	}
	s.lifecycle = l
}
