package identitylease

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFileLeaseExcludesDuplicateAndIncrementsEpoch(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	first, err := Acquire(ctx, Options{
		NodeID:        "cefas-0",
		LeasePath:     dir,
		Backend:       BackendFile,
		HolderID:      "holder-a",
		LeaseTTL:      time.Second,
		RenewInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if got := first.Epoch(); got != 1 {
		t.Fatalf("first epoch = %d, want 1", got)
	}
	_, err = Acquire(ctx, Options{
		NodeID:        "cefas-0",
		LeasePath:     dir,
		Backend:       BackendFile,
		HolderID:      "holder-b",
		LeaseTTL:      time.Second,
		RenewInterval: 100 * time.Millisecond,
	})
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("second acquire error = %v, want ErrLeaseHeld", err)
	}
	if err := first.Close(ctx); err != nil {
		t.Fatalf("close first: %v", err)
	}
	second, err := Acquire(ctx, Options{
		NodeID:        "cefas-0",
		LeasePath:     dir,
		Backend:       BackendFile,
		HolderID:      "holder-c",
		LeaseTTL:      time.Second,
		RenewInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("acquire after close: %v", err)
	}
	defer second.Close(ctx)
	if got := second.Epoch(); got != 2 {
		t.Fatalf("second epoch = %d, want 2", got)
	}
}

func TestKubernetesLeaseBlocksActiveHolderAndAllowsExpiredTakeover(t *testing.T) {
	store := &leaseStore{}
	api := httptest.NewServer(store)
	defer api.Close()

	ctx := context.Background()
	first, err := Acquire(ctx, Options{
		NodeID:                "cefas-0",
		Backend:               BackendKubernetes,
		LeaseName:             "cefas-cefas-0",
		KubernetesNamespace:   "cefasdb",
		KubernetesAPIURL:      api.URL,
		KubernetesBearerToken: "token",
		HTTPClient:            api.Client(),
		HolderID:              "holder-a",
		LeaseTTL:              time.Second,
		RenewInterval:         100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("acquire first: %v", err)
	}
	if got := first.Epoch(); got != 1 {
		t.Fatalf("epoch = %d, want 1", got)
	}

	_, err = Acquire(ctx, Options{
		NodeID:                "cefas-0",
		Backend:               BackendKubernetes,
		LeaseName:             "cefas-cefas-0",
		KubernetesNamespace:   "cefasdb",
		KubernetesAPIURL:      api.URL,
		KubernetesBearerToken: "token",
		HTTPClient:            api.Client(),
		HolderID:              "holder-b",
		LeaseTTL:              time.Second,
		RenewInterval:         100 * time.Millisecond,
	})
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("duplicate acquire error = %v, want ErrLeaseHeld", err)
	}

	store.expire(2 * time.Second)
	second, err := Acquire(ctx, Options{
		NodeID:                "cefas-0",
		Backend:               BackendKubernetes,
		LeaseName:             "cefas-cefas-0",
		KubernetesNamespace:   "cefasdb",
		KubernetesAPIURL:      api.URL,
		KubernetesBearerToken: "token",
		HTTPClient:            api.Client(),
		HolderID:              "holder-b",
		LeaseTTL:              time.Second,
		RenewInterval:         100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("expired takeover: %v", err)
	}
	if got := second.Epoch(); got != 2 {
		t.Fatalf("takeover epoch = %d, want 2", got)
	}

	_, err = first.backend.Renew(ctx, first.HolderID(), first.Record())
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("stale renew error = %v, want ErrLeaseHeld", err)
	}
}

type leaseStore struct {
	mu    sync.Mutex
	lease *k8sLease
	rv    int
}

func (s *leaseStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer token" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/apis/coordination.k8s.io/v1/namespaces/cefasdb/leases") {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		if s.lease == nil {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, http.StatusOK, s.lease)
	case http.MethodPost:
		if s.lease != nil {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		var lease k8sLease
		if err := json.NewDecoder(r.Body).Decode(&lease); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.rv++
		lease.Metadata.ResourceVersion = strconvItoa(s.rv)
		s.lease = &lease
		writeJSON(w, http.StatusCreated, s.lease)
	case http.MethodPut:
		if s.lease == nil {
			http.NotFound(w, r)
			return
		}
		var lease k8sLease
		if err := json.NewDecoder(r.Body).Decode(&lease); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if lease.Metadata.ResourceVersion != s.lease.Metadata.ResourceVersion {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		s.rv++
		lease.Metadata.ResourceVersion = strconvItoa(s.rv)
		s.lease = &lease
		writeJSON(w, http.StatusOK, s.lease)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *leaseStore) expire(age time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease != nil && s.lease.Spec.RenewTime != nil {
		s.lease.Spec.RenewTime.Time = time.Now().UTC().Add(-age)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func strconvItoa(v int) string {
	const digits = "0123456789"
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = digits[v%10]
		v /= 10
	}
	return string(buf[i:])
}
