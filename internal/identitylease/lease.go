package identitylease

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	BackendAuto       = "auto"
	BackendOff        = "off"
	BackendFile       = "file"
	BackendKubernetes = "kubernetes"

	defaultTTL           = 30 * time.Second
	defaultRenewInterval = 10 * time.Second

	annotationRaftID = "cefasdb.io/raft-id"
	annotationEpoch  = "cefasdb.io/identity-epoch"
)

var (
	ErrLeaseHeld = errors.New("raft identity lease held")
	ErrLeaseLost = errors.New("raft identity lease lost")
)

// Options configure the per-raft-id process guard. Kubernetes fields
// are only used when Backend is "kubernetes" or auto detects an
// in-cluster service account.
type Options struct {
	NodeID        string
	DataDir       string
	Backend       string
	LeasePath     string
	LeaseName     string
	LeaseTTL      time.Duration
	RenewInterval time.Duration
	HolderID      string

	KubernetesNamespace   string
	KubernetesAPIURL      string
	KubernetesBearerToken string
	KubernetesCAFile      string
	HTTPClient            *http.Client
}

// Record is the durable identity lease state for a raft ServerID.
type Record struct {
	NodeID    string    `json:"nodeId"`
	HolderID  string    `json:"holderId"`
	Epoch     uint64    `json:"epoch"`
	Backend   string    `json:"backend"`
	Resource  string    `json:"resource"`
	RenewedAt time.Time `json:"renewedAt"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// HeldError includes the current lease owner so operators know that a
// replacement must wait for expiry or fence the old process.
type HeldError struct {
	Record Record
}

func (e *HeldError) Error() string {
	rec := e.Record
	return fmt.Sprintf("%v: node=%s holder=%s epoch=%d backend=%s resource=%s renewed=%s expires=%s; fence the current holder or wait for lease expiry before replacement",
		ErrLeaseHeld, rec.NodeID, rec.HolderID, rec.Epoch, rec.Backend, rec.Resource,
		rec.RenewedAt.Format(time.RFC3339Nano), rec.ExpiresAt.Format(time.RFC3339Nano))
}

func (e *HeldError) Unwrap() error { return ErrLeaseHeld }

type backend interface {
	Acquire(ctx context.Context, holderID string) (Record, error)
	Renew(ctx context.Context, holderID string, prev Record) (Record, error)
	Release(ctx context.Context, holderID string, prev Record) error
}

// Guard owns the lease while the process is serving. If RenewLoop
// reports loss, callers must stop accepting reads/writes and shut down
// Raft before releasing process resources.
type Guard struct {
	backend backend
	holder  string
	renew   time.Duration

	mu     sync.RWMutex
	record Record
}

func Acquire(ctx context.Context, opts Options) (*Guard, error) {
	if strings.TrimSpace(opts.NodeID) == "" {
		return nil, fmt.Errorf("raft identity lease: node id is required")
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = defaultTTL
	}
	if opts.RenewInterval <= 0 {
		opts.RenewInterval = defaultRenewInterval
	}
	if opts.RenewInterval >= opts.LeaseTTL {
		return nil, fmt.Errorf("raft identity lease: renew interval %s must be less than ttl %s", opts.RenewInterval, opts.LeaseTTL)
	}
	holder := opts.HolderID
	if holder == "" {
		holder = defaultHolderID(opts.NodeID)
	}
	be, err := newBackend(opts)
	if err != nil {
		return nil, err
	}
	rec, err := be.Acquire(ctx, holder)
	if err != nil {
		return nil, err
	}
	return &Guard{backend: be, holder: holder, renew: opts.RenewInterval, record: rec}, nil
}

func (g *Guard) Record() Record {
	if g == nil {
		return Record{}
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.record
}

func (g *Guard) Epoch() uint64 { return g.Record().Epoch }

func (g *Guard) HolderID() string {
	if g == nil {
		return ""
	}
	return g.holder
}

func (g *Guard) RenewLoop(ctx context.Context, onLost func(error)) {
	if g == nil {
		return
	}
	t := time.NewTicker(g.renew)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			prev := g.Record()
			rec, err := g.backend.Renew(ctx, g.holder, prev)
			if err != nil {
				if onLost != nil {
					onLost(fmt.Errorf("%w: %v", ErrLeaseLost, err))
				}
				return
			}
			g.mu.Lock()
			g.record = rec
			g.mu.Unlock()
		}
	}
}

func (g *Guard) Close(ctx context.Context) error {
	if g == nil {
		return nil
	}
	return g.backend.Release(ctx, g.holder, g.Record())
}

func newBackend(opts Options) (backend, error) {
	backendName := strings.TrimSpace(opts.Backend)
	if backendName == "" {
		backendName = BackendFile
	}
	if backendName == BackendAuto {
		if inKubernetes(opts) {
			backendName = BackendKubernetes
		} else {
			backendName = BackendFile
		}
	}
	switch backendName {
	case BackendOff:
		return nil, fmt.Errorf("raft identity lease: backend off cannot acquire a guard")
	case BackendFile:
		return newFileBackend(opts)
	case BackendKubernetes:
		return newKubernetesBackend(opts)
	default:
		return nil, fmt.Errorf("raft identity lease: unsupported backend %q", opts.Backend)
	}
}

func defaultHolderID(nodeID string) string {
	host, _ := os.Hostname()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s/%s/%d/%d", nodeID, host, os.Getpid(), time.Now().UnixNano())
	}
	return fmt.Sprintf("%s/%s/%d/%s", nodeID, host, os.Getpid(), hex.EncodeToString(b[:]))
}

func inKubernetes(opts Options) bool {
	if opts.KubernetesAPIURL != "" {
		return true
	}
	return os.Getenv("KUBERNETES_SERVICE_HOST") != "" && serviceAccountToken() != ""
}

type fileBackend struct {
	nodeID string
	dir    string
	ttl    time.Duration

	mu   sync.Mutex
	file *os.File
}

func newFileBackend(opts Options) (*fileBackend, error) {
	dir := opts.LeasePath
	if dir == "" {
		data := opts.DataDir
		if data == "" {
			data = "."
		}
		dir = filepath.Join(data, "raft-identity")
	}
	return &fileBackend{nodeID: opts.NodeID, dir: dir, ttl: opts.LeaseTTL}, nil
}

func (b *fileBackend) Acquire(ctx context.Context, holderID string) (Record, error) {
	_ = ctx
	if err := os.MkdirAll(b.dir, 0o755); err != nil {
		return Record{}, fmt.Errorf("raft identity lease file mkdir: %w", err)
	}
	lockPath := filepath.Join(b.dir, fileNameForNode(b.nodeID)+".lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return Record{}, fmt.Errorf("raft identity lease file open: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		rec := b.readRecord()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return Record{}, &HeldError{Record: rec}
		}
		return Record{}, fmt.Errorf("raft identity lease file lock: %w", err)
	}
	now := time.Now().UTC()
	rec := b.readRecord()
	rec.NodeID = b.nodeID
	rec.HolderID = holderID
	rec.Epoch++
	if rec.Epoch == 0 {
		rec.Epoch = 1
	}
	rec.Backend = BackendFile
	rec.Resource = b.dir
	rec.RenewedAt = now
	rec.ExpiresAt = now.Add(b.ttl)
	if err := b.writeRecord(rec); err != nil {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
		return Record{}, err
	}
	b.mu.Lock()
	b.file = f
	b.mu.Unlock()
	return rec, nil
}

func (b *fileBackend) Renew(ctx context.Context, holderID string, prev Record) (Record, error) {
	_ = ctx
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.file == nil {
		return Record{}, fmt.Errorf("file lease is not held")
	}
	now := time.Now().UTC()
	prev.NodeID = b.nodeID
	prev.HolderID = holderID
	prev.Backend = BackendFile
	prev.Resource = b.dir
	prev.RenewedAt = now
	prev.ExpiresAt = now.Add(b.ttl)
	if err := b.writeRecord(prev); err != nil {
		return Record{}, err
	}
	return prev, nil
}

func (b *fileBackend) Release(ctx context.Context, holderID string, prev Record) error {
	_ = ctx
	_ = holderID
	_ = prev
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.file == nil {
		return nil
	}
	err := syscall.Flock(int(b.file.Fd()), syscall.LOCK_UN)
	if closeErr := b.file.Close(); err == nil {
		err = closeErr
	}
	b.file = nil
	return err
}

func (b *fileBackend) recordPath() string {
	return filepath.Join(b.dir, fileNameForNode(b.nodeID)+".json")
}

func (b *fileBackend) readRecord() Record {
	p := b.recordPath()
	raw, err := os.ReadFile(p)
	if err != nil {
		now := time.Now().UTC()
		return Record{NodeID: b.nodeID, Backend: BackendFile, Resource: b.dir, RenewedAt: now, ExpiresAt: now}
	}
	var rec Record
	if err := json.Unmarshal(raw, &rec); err != nil {
		now := time.Now().UTC()
		return Record{NodeID: b.nodeID, Backend: BackendFile, Resource: b.dir, RenewedAt: now, ExpiresAt: now}
	}
	return rec
}

func (b *fileBackend) writeRecord(rec Record) error {
	raw, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("raft identity lease file marshal: %w", err)
	}
	tmp := b.recordPath() + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("raft identity lease file write: %w", err)
	}
	if err := os.Rename(tmp, b.recordPath()); err != nil {
		return fmt.Errorf("raft identity lease file rename: %w", err)
	}
	return nil
}

func fileNameForNode(nodeID string) string {
	sum := sha1.Sum([]byte(nodeID))
	name := strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(nodeID)
	name = strings.Trim(name, "._- ")
	if name == "" {
		name = "node"
	}
	if len(name) > 48 {
		name = name[:48]
	}
	return fmt.Sprintf("%s-%s", name, hex.EncodeToString(sum[:4]))
}

type kubernetesBackend struct {
	nodeID    string
	name      string
	namespace string
	apiURL    string
	token     string
	ttl       time.Duration
	client    *http.Client
}

func newKubernetesBackend(opts Options) (*kubernetesBackend, error) {
	ns := opts.KubernetesNamespace
	if ns == "" {
		ns = serviceAccountNamespace()
	}
	if ns == "" {
		return nil, fmt.Errorf("raft identity lease kubernetes: namespace is required")
	}
	apiURL := opts.KubernetesAPIURL
	if apiURL == "" {
		host := os.Getenv("KUBERNETES_SERVICE_HOST")
		port := os.Getenv("KUBERNETES_SERVICE_PORT")
		if port == "" {
			port = "443"
		}
		if host == "" {
			return nil, fmt.Errorf("raft identity lease kubernetes: KUBERNETES_SERVICE_HOST is not set")
		}
		apiURL = "https://" + netJoinHostPort(host, port)
	}
	if _, err := url.Parse(apiURL); err != nil {
		return nil, fmt.Errorf("raft identity lease kubernetes api url: %w", err)
	}
	token := opts.KubernetesBearerToken
	if token == "" {
		token = serviceAccountToken()
	}
	if token == "" {
		return nil, fmt.Errorf("raft identity lease kubernetes: bearer token is required")
	}
	client := opts.HTTPClient
	if client == nil {
		var err error
		client, err = kubernetesHTTPClient(opts.KubernetesCAFile)
		if err != nil {
			return nil, err
		}
	}
	name := opts.LeaseName
	if name == "" {
		name = "cefas-" + dnsLabel(opts.NodeID)
	}
	return &kubernetesBackend{
		nodeID:    opts.NodeID,
		name:      dnsLabel(name),
		namespace: ns,
		apiURL:    strings.TrimRight(apiURL, "/"),
		token:     token,
		ttl:       opts.LeaseTTL,
		client:    client,
	}, nil
}

func (b *kubernetesBackend) Acquire(ctx context.Context, holderID string) (Record, error) {
	for attempt := 0; attempt < 3; attempt++ {
		lease, status, err := b.get(ctx)
		if err != nil {
			return Record{}, err
		}
		now := time.Now().UTC()
		if status == http.StatusNotFound {
			rec, err := b.create(ctx, holderID, now, 1)
			if isConflict(err) {
				continue
			}
			return rec, err
		}
		if status != http.StatusOK {
			return Record{}, fmt.Errorf("raft identity lease kubernetes get: status %d", status)
		}
		if b.activeHeldByOther(lease, holderID, now) {
			return Record{}, &HeldError{Record: b.recordFromLease(lease, now)}
		}
		nextEpoch := leaseEpoch(lease) + 1
		rec, err := b.update(ctx, lease, holderID, now, nextEpoch)
		if isConflict(err) {
			continue
		}
		return rec, err
	}
	return Record{}, fmt.Errorf("raft identity lease kubernetes acquire: conflicted after retries")
}

func (b *kubernetesBackend) Renew(ctx context.Context, holderID string, prev Record) (Record, error) {
	_ = prev
	for attempt := 0; attempt < 3; attempt++ {
		lease, status, err := b.get(ctx)
		if err != nil {
			return Record{}, err
		}
		now := time.Now().UTC()
		if status == http.StatusNotFound {
			return Record{}, fmt.Errorf("lease %s/%s disappeared", b.namespace, b.name)
		}
		if status != http.StatusOK {
			return Record{}, fmt.Errorf("raft identity lease kubernetes get: status %d", status)
		}
		if leaseHolder(lease) != holderID {
			return Record{}, &HeldError{Record: b.recordFromLease(lease, now)}
		}
		rec, err := b.update(ctx, lease, holderID, now, leaseEpoch(lease))
		if isConflict(err) {
			continue
		}
		return rec, err
	}
	return Record{}, fmt.Errorf("raft identity lease kubernetes renew: conflicted after retries")
}

func (b *kubernetesBackend) Release(ctx context.Context, holderID string, prev Record) error {
	_ = prev
	lease, status, err := b.get(ctx)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound {
		return nil
	}
	if status != http.StatusOK {
		return fmt.Errorf("raft identity lease kubernetes release get: status %d", status)
	}
	if leaseHolder(lease) != holderID {
		return nil
	}
	now := time.Now().UTC()
	_, err = b.update(ctx, lease, "", now, leaseEpoch(lease))
	return err
}

func (b *kubernetesBackend) activeHeldByOther(lease *k8sLease, holderID string, now time.Time) bool {
	holder := leaseHolder(lease)
	if holder == "" || holder == holderID {
		return false
	}
	renewed := leaseRenewedAt(lease)
	if renewed.IsZero() {
		return true
	}
	ttl := time.Duration(leaseDurationSeconds(lease)) * time.Second
	if ttl <= 0 {
		ttl = b.ttl
	}
	return now.Before(renewed.Add(ttl))
}

func (b *kubernetesBackend) recordFromLease(lease *k8sLease, now time.Time) Record {
	renewed := leaseRenewedAt(lease)
	if renewed.IsZero() {
		renewed = now
	}
	ttl := time.Duration(leaseDurationSeconds(lease)) * time.Second
	if ttl <= 0 {
		ttl = b.ttl
	}
	return Record{
		NodeID:    annotation(lease, annotationRaftID, b.nodeID),
		HolderID:  leaseHolder(lease),
		Epoch:     leaseEpoch(lease),
		Backend:   BackendKubernetes,
		Resource:  b.namespace + "/" + b.name,
		RenewedAt: renewed,
		ExpiresAt: renewed.Add(ttl),
	}
}

func (b *kubernetesBackend) create(ctx context.Context, holderID string, now time.Time, epoch uint64) (Record, error) {
	lease := newLease(b.namespace, b.name, b.nodeID, holderID, now, b.ttl, epoch)
	status, err := b.do(ctx, http.MethodPost, b.collectionPath(), lease, lease)
	if err != nil {
		return Record{}, err
	}
	if status != http.StatusCreated && status != http.StatusOK {
		return Record{}, statusError{status: status}
	}
	return b.recordFromLease(lease, now), nil
}

func (b *kubernetesBackend) update(ctx context.Context, lease *k8sLease, holderID string, now time.Time, epoch uint64) (Record, error) {
	applyLeaseUpdate(lease, b.nodeID, holderID, now, b.ttl, epoch)
	status, err := b.do(ctx, http.MethodPut, b.resourcePath(), lease, lease)
	if err != nil {
		return Record{}, err
	}
	if status != http.StatusOK {
		return Record{}, statusError{status: status}
	}
	return b.recordFromLease(lease, now), nil
}

func (b *kubernetesBackend) get(ctx context.Context) (*k8sLease, int, error) {
	var lease k8sLease
	status, err := b.do(ctx, http.MethodGet, b.resourcePath(), nil, &lease)
	if status == http.StatusNotFound {
		return nil, status, nil
	}
	if err != nil {
		return nil, status, err
	}
	return &lease, status, nil
}

func (b *kubernetesBackend) collectionPath() string {
	return fmt.Sprintf("/apis/coordination.k8s.io/v1/namespaces/%s/leases", b.namespace)
}

func (b *kubernetesBackend) resourcePath() string {
	return b.collectionPath() + "/" + b.name
}

func (b *kubernetesBackend) do(ctx context.Context, method, path string, in any, out any) (int, error) {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return 0, err
		}
		body = strings.NewReader(string(raw))
	}
	req, err := http.NewRequestWithContext(ctx, method, b.apiURL+path, body)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+b.token)
	resp, err := b.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 && out != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, out); err != nil {
			return resp.StatusCode, err
		}
	}
	if readErr != nil {
		return resp.StatusCode, readErr
	}
	if resp.StatusCode >= 400 && resp.StatusCode != http.StatusNotFound && resp.StatusCode != http.StatusConflict {
		return resp.StatusCode, fmt.Errorf("kubernetes api %s %s: status %d: %s", method, path, resp.StatusCode, string(raw))
	}
	return resp.StatusCode, nil
}

type statusError struct {
	status int
}

func (e statusError) Error() string { return fmt.Sprintf("kubernetes api status %d", e.status) }

func isConflict(err error) bool {
	var st statusError
	return errors.As(err, &st) && st.status == http.StatusConflict
}

type k8sLease struct {
	APIVersion string        `json:"apiVersion,omitempty"`
	Kind       string        `json:"kind,omitempty"`
	Metadata   k8sObjectMeta `json:"metadata"`
	Spec       k8sLeaseSpec  `json:"spec"`
}

type k8sObjectMeta struct {
	Name            string            `json:"name,omitempty"`
	Namespace       string            `json:"namespace,omitempty"`
	ResourceVersion string            `json:"resourceVersion,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	Annotations     map[string]string `json:"annotations,omitempty"`
}

type k8sLeaseSpec struct {
	HolderIdentity       *string  `json:"holderIdentity,omitempty"`
	LeaseDurationSeconds *int32   `json:"leaseDurationSeconds,omitempty"`
	AcquireTime          *k8sTime `json:"acquireTime,omitempty"`
	RenewTime            *k8sTime `json:"renewTime,omitempty"`
	LeaseTransitions     *int32   `json:"leaseTransitions,omitempty"`
}

type k8sTime struct {
	time.Time
}

func (t k8sTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.UTC().Format(time.RFC3339Nano))
}

func (t *k8sTime) UnmarshalJSON(raw []byte) error {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return err
	}
	if s == "" {
		t.Time = time.Time{}
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return err
	}
	t.Time = parsed
	return nil
}

func newLease(namespace, name, nodeID, holderID string, now time.Time, ttl time.Duration, epoch uint64) *k8sLease {
	lease := &k8sLease{
		APIVersion: "coordination.k8s.io/v1",
		Kind:       "Lease",
		Metadata: k8sObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name": "cefas",
				"cefasdb.io/raft-id":     dnsLabel(nodeID),
			},
		},
	}
	applyLeaseUpdate(lease, nodeID, holderID, now, ttl, epoch)
	return lease
}

func applyLeaseUpdate(lease *k8sLease, nodeID, holderID string, now time.Time, ttl time.Duration, epoch uint64) {
	oldHolder := leaseHolder(lease)
	if lease.Metadata.Annotations == nil {
		lease.Metadata.Annotations = map[string]string{}
	}
	lease.Metadata.Annotations[annotationRaftID] = nodeID
	lease.Metadata.Annotations[annotationEpoch] = strconv.FormatUint(epoch, 10)
	seconds := int32(ttl / time.Second)
	if seconds <= 0 {
		seconds = int32(defaultTTL / time.Second)
	}
	lease.Spec.HolderIdentity = stringPtr(holderID)
	lease.Spec.LeaseDurationSeconds = &seconds
	kt := &k8sTime{Time: now.UTC()}
	if holderID == "" {
		lease.Spec.HolderIdentity = nil
		lease.Spec.RenewTime = nil
		return
	}
	if lease.Spec.AcquireTime == nil || oldHolder != holderID {
		lease.Spec.AcquireTime = kt
	}
	lease.Spec.RenewTime = kt
	transitions := int32(epoch)
	lease.Spec.LeaseTransitions = &transitions
}

func leaseHolder(lease *k8sLease) string {
	if lease == nil || lease.Spec.HolderIdentity == nil {
		return ""
	}
	return *lease.Spec.HolderIdentity
}

func leaseRenewedAt(lease *k8sLease) time.Time {
	if lease == nil || lease.Spec.RenewTime == nil {
		return time.Time{}
	}
	return lease.Spec.RenewTime.Time
}

func leaseDurationSeconds(lease *k8sLease) int32 {
	if lease == nil || lease.Spec.LeaseDurationSeconds == nil {
		return 0
	}
	return *lease.Spec.LeaseDurationSeconds
}

func leaseEpoch(lease *k8sLease) uint64 {
	if lease == nil {
		return 0
	}
	if lease.Metadata.Annotations != nil {
		if v := lease.Metadata.Annotations[annotationEpoch]; v != "" {
			if parsed, err := strconv.ParseUint(v, 10, 64); err == nil {
				return parsed
			}
		}
	}
	if lease.Spec.LeaseTransitions != nil && *lease.Spec.LeaseTransitions > 0 {
		return uint64(*lease.Spec.LeaseTransitions)
	}
	return 0
}

func annotation(lease *k8sLease, key, fallback string) string {
	if lease != nil && lease.Metadata.Annotations != nil {
		if v := lease.Metadata.Annotations[key]; v != "" {
			return v
		}
	}
	return fallback
}

func stringPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func serviceAccountNamespace() string {
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	raw, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

func serviceAccountToken() string {
	raw, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

func kubernetesHTTPClient(caFile string) (*http.Client, error) {
	if caFile == "" {
		caFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	if raw, err := os.ReadFile(caFile); err == nil && len(raw) > 0 {
		pool.AppendCertsFromPEM(raw)
	}
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		},
	}, nil
}

func netJoinHostPort(host, port string) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return "[" + host + "]:" + port
	}
	return host + ":" + port
}

func dnsLabel(s string) string {
	s = strings.ToLower(s)
	var b strings.Builder
	lastDash := false
	for _, r := range s {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if ok {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "node"
	}
	if len(out) <= 63 {
		return out
	}
	sum := sha1.Sum([]byte(out))
	suffix := "-" + hex.EncodeToString(sum[:4])
	return strings.Trim(out[:63-len(suffix)], "-") + suffix
}
