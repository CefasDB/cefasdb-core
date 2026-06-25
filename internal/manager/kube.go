package manager

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

type Kubernetes interface {
	Snapshot(ctx context.Context, opts KubeSnapshotOptions) (KubernetesSnapshot, error)
}

type KubeSnapshotOptions struct {
	Namespace string
	Selector  string
}

type KubernetesSnapshot struct {
	Namespace string                  `json:"namespace"`
	Selector  string                  `json:"selector,omitempty"`
	ListedAt  time.Time               `json:"listedAt"`
	Pods      []Pod                   `json:"pods,omitempty"`
	Nodes     []Node                  `json:"nodes,omitempty"`
	Leases    []Lease                 `json:"leases,omitempty"`
	Endpoints []Endpoint              `json:"endpoints,omitempty"`
	PVCs      []PersistentVolumeClaim `json:"persistentVolumeClaims,omitempty"`
}

type ObjectMeta struct {
	Name              string            `json:"name,omitempty"`
	Namespace         string            `json:"namespace,omitempty"`
	UID               string            `json:"uid,omitempty"`
	ResourceVersion   string            `json:"resourceVersion,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	CreationTimestamp string            `json:"creationTimestamp,omitempty"`
}

type Pod struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     PodSpec    `json:"spec,omitempty"`
	Status   PodStatus  `json:"status,omitempty"`
}

type PodSpec struct {
	NodeName string `json:"nodeName,omitempty"`
}

type PodStatus struct {
	Phase             string            `json:"phase,omitempty"`
	PodIP             string            `json:"podIP,omitempty"`
	HostIP            string            `json:"hostIP,omitempty"`
	Conditions        []PodCondition    `json:"conditions,omitempty"`
	ContainerStatuses []ContainerStatus `json:"containerStatuses,omitempty"`
}

type PodCondition struct {
	Type    string `json:"type,omitempty"`
	Status  string `json:"status,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type ContainerStatus struct {
	Name         string         `json:"name,omitempty"`
	Ready        bool           `json:"ready,omitempty"`
	RestartCount int            `json:"restartCount,omitempty"`
	State        ContainerState `json:"state,omitempty"`
}

type ContainerState struct {
	Waiting    *ContainerStateWaiting    `json:"waiting,omitempty"`
	Running    *ContainerStateRunning    `json:"running,omitempty"`
	Terminated *ContainerStateTerminated `json:"terminated,omitempty"`
}

type ContainerStateWaiting struct {
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type ContainerStateRunning struct {
	StartedAt string `json:"startedAt,omitempty"`
}

type ContainerStateTerminated struct {
	Reason     string `json:"reason,omitempty"`
	Message    string `json:"message,omitempty"`
	ExitCode   int    `json:"exitCode,omitempty"`
	FinishedAt string `json:"finishedAt,omitempty"`
}

type Node struct {
	Metadata ObjectMeta `json:"metadata"`
	Status   NodeStatus `json:"status,omitempty"`
}

type NodeStatus struct {
	Conditions []NodeCondition `json:"conditions,omitempty"`
}

type NodeCondition struct {
	Type    string `json:"type,omitempty"`
	Status  string `json:"status,omitempty"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

type Lease struct {
	Metadata ObjectMeta `json:"metadata"`
	Spec     LeaseSpec  `json:"spec,omitempty"`
}

type LeaseSpec struct {
	HolderIdentity       string `json:"holderIdentity,omitempty"`
	LeaseDurationSeconds int    `json:"leaseDurationSeconds,omitempty"`
	AcquireTime          string `json:"acquireTime,omitempty"`
	RenewTime            string `json:"renewTime,omitempty"`
	LeaseTransitions     int    `json:"leaseTransitions,omitempty"`
}

type Endpoint struct {
	Metadata ObjectMeta       `json:"metadata"`
	Subsets  []EndpointSubset `json:"subsets,omitempty"`
}

type EndpointSubset struct {
	Addresses         []EndpointAddress `json:"addresses,omitempty"`
	NotReadyAddresses []EndpointAddress `json:"notReadyAddresses,omitempty"`
}

type EndpointAddress struct {
	IP        string           `json:"ip,omitempty"`
	Hostname  string           `json:"hostname,omitempty"`
	TargetRef *ObjectReference `json:"targetRef,omitempty"`
}

type ObjectReference struct {
	Kind      string `json:"kind,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
	UID       string `json:"uid,omitempty"`
}

type PersistentVolumeClaim struct {
	Metadata ObjectMeta `json:"metadata"`
	Status   PVCStatus  `json:"status,omitempty"`
}

type PVCStatus struct {
	Phase string `json:"phase,omitempty"`
}

type HTTPKubeClient struct {
	base       *url.URL
	httpClient *http.Client
	token      string
	now        func() time.Time
}

func NewInClusterKubeClient() (*HTTPKubeClient, string, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, "", errors.New("KUBERNETES_SERVICE_HOST/PORT not set")
	}
	tokenBytes, err := os.ReadFile(path.Join(serviceAccountDir, "token"))
	if err != nil {
		return nil, "", fmt.Errorf("read service account token: %w", err)
	}
	nsBytes, err := os.ReadFile(path.Join(serviceAccountDir, "namespace"))
	if err != nil {
		return nil, "", fmt.Errorf("read service account namespace: %w", err)
	}
	pool := x509.NewCertPool()
	if caBytes, err := os.ReadFile(path.Join(serviceAccountDir, "ca.crt")); err == nil {
		pool.AppendCertsFromPEM(caBytes)
	}
	u, err := url.Parse("https://" + netJoinHostPort(host, port))
	if err != nil {
		return nil, "", err
	}
	return &HTTPKubeClient{
		base: u,
		httpClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs:    pool,
			MinVersion: tls.VersionTLS12,
		}}},
		token: strings.TrimSpace(string(tokenBytes)),
		now:   time.Now,
	}, strings.TrimSpace(string(nsBytes)), nil
}

func NewHTTPKubeClient(baseURL, bearer string, httpClient *http.Client) (*HTTPKubeClient, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	u, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return nil, err
	}
	return &HTTPKubeClient{base: u, httpClient: httpClient, token: bearer, now: time.Now}, nil
}

func (c *HTTPKubeClient) Snapshot(ctx context.Context, opts KubeSnapshotOptions) (KubernetesSnapshot, error) {
	if opts.Namespace == "" {
		return KubernetesSnapshot{}, errors.New("namespace is required")
	}
	snap := KubernetesSnapshot{Namespace: opts.Namespace, Selector: opts.Selector, ListedAt: c.clock().UTC()}
	var err error
	if snap.Pods, err = c.listPods(ctx, opts.Namespace, opts.Selector); err != nil {
		return snap, err
	}
	if snap.Endpoints, err = c.listEndpoints(ctx, opts.Namespace, opts.Selector); err != nil {
		return snap, err
	}
	if snap.PVCs, err = c.listPVCs(ctx, opts.Namespace, opts.Selector); err != nil {
		return snap, err
	}
	if snap.Leases, err = c.listLeases(ctx, opts.Namespace, leaseSelector(opts.Selector)); err != nil {
		return snap, err
	}
	if snap.Nodes, err = c.listNodes(ctx); err != nil {
		return snap, err
	}
	return snap, nil
}

func (c *HTTPKubeClient) listPods(ctx context.Context, namespace, selector string) ([]Pod, error) {
	var out struct {
		Items []Pod `json:"items"`
	}
	err := c.getJSON(ctx, namespacePath(namespace, "pods"), selector, &out)
	return out.Items, err
}

func (c *HTTPKubeClient) listEndpoints(ctx context.Context, namespace, selector string) ([]Endpoint, error) {
	var out struct {
		Items []Endpoint `json:"items"`
	}
	err := c.getJSON(ctx, namespacePath(namespace, "endpoints"), selector, &out)
	return out.Items, err
}

func (c *HTTPKubeClient) listPVCs(ctx context.Context, namespace, selector string) ([]PersistentVolumeClaim, error) {
	var out struct {
		Items []PersistentVolumeClaim `json:"items"`
	}
	err := c.getJSON(ctx, namespacePath(namespace, "persistentvolumeclaims"), selector, &out)
	return out.Items, err
}

func (c *HTTPKubeClient) listLeases(ctx context.Context, namespace, selector string) ([]Lease, error) {
	var out struct {
		Items []Lease `json:"items"`
	}
	err := c.getJSON(ctx, "/apis/coordination.k8s.io/v1/namespaces/"+url.PathEscape(namespace)+"/leases", selector, &out)
	return out.Items, err
}

func (c *HTTPKubeClient) listNodes(ctx context.Context) ([]Node, error) {
	var out struct {
		Items []Node `json:"items"`
	}
	err := c.getJSON(ctx, "/api/v1/nodes", "", &out)
	return out.Items, err
}

func (c *HTTPKubeClient) getJSON(ctx context.Context, apiPath, selector string, out any) error {
	u := c.apiURL(apiPath)
	if selector != "" {
		q := u.Query()
		q.Set("labelSelector", selector)
		u.RawQuery = q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	return c.doJSON(req, http.StatusOK, out)
}

func (c *HTTPKubeClient) doJSON(req *http.Request, wantStatus int, out any) error {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("kubernetes %s %s: status=%d body=%s", req.Method, req.URL.Path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *HTTPKubeClient) apiURL(apiPath string) *url.URL {
	u := *c.base
	u.Path = apiPath
	return &u
}

func namespacePath(namespace, resource string) string {
	return "/api/v1/namespaces/" + url.PathEscape(namespace) + "/" + resource
}

func leaseSelector(selector string) string {
	for _, part := range strings.Split(selector, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "app.kubernetes.io/name=") {
			return part
		}
	}
	return ""
}

func (c *HTTPKubeClient) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func podReady(p Pod) bool {
	if p.Status.Phase != "Running" {
		return false
	}
	for _, cond := range p.Status.Conditions {
		if cond.Type == "Ready" {
			return cond.Status == "True"
		}
	}
	return false
}

func nodeReady(n Node) bool {
	for _, cond := range n.Status.Conditions {
		if cond.Type == "Ready" {
			return cond.Status == "True"
		}
	}
	return false
}

func leaseExpired(l Lease, now time.Time) bool {
	if l.Spec.LeaseDurationSeconds <= 0 {
		return false
	}
	raw := l.Spec.RenewTime
	if raw == "" {
		raw = l.Spec.AcquireTime
	}
	if raw == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return false
	}
	return now.After(t.Add(time.Duration(l.Spec.LeaseDurationSeconds) * time.Second))
}

func (c *HTTPKubeClient) getLease(ctx context.Context, namespace, name string) (Lease, int, error) {
	var lease Lease
	apiPath := "/apis/coordination.k8s.io/v1/namespaces/" + url.PathEscape(namespace) + "/leases/" + url.PathEscape(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiURL(apiPath).String(), nil)
	if err != nil {
		return lease, 0, err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return lease, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return lease, resp.StatusCode, nil
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return lease, resp.StatusCode, fmt.Errorf("kubernetes GET lease %s/%s: status=%d body=%s", namespace, name, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if err := json.NewDecoder(resp.Body).Decode(&lease); err != nil {
		return lease, resp.StatusCode, err
	}
	return lease, resp.StatusCode, nil
}

func (c *HTTPKubeClient) putLease(ctx context.Context, namespace string, lease Lease, create bool) (Lease, error) {
	var out Lease
	apiPath := "/apis/coordination.k8s.io/v1/namespaces/" + url.PathEscape(namespace) + "/leases"
	method := http.MethodPost
	want := http.StatusCreated
	if !create {
		apiPath += "/" + url.PathEscape(lease.Metadata.Name)
		method = http.MethodPut
		want = http.StatusOK
	}
	body, err := json.Marshal(lease)
	if err != nil {
		return out, err
	}
	req, err := http.NewRequestWithContext(ctx, method, c.apiURL(apiPath).String(), bytes.NewReader(body))
	if err != nil {
		return out, err
	}
	req.Header.Set("Content-Type", "application/json")
	return out, c.doJSON(req, want, &out)
}

func netJoinHostPort(host, port string) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return "[" + host + "]:" + port
	}
	return host + ":" + port
}
