package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/CefasDb/cefasdb/internal/placement"
	"github.com/CefasDb/cefasdb/pkg/client"
)

type Cefas interface {
	Status(ctx context.Context) (client.ClusterStatus, error)
	AuditPlacement(ctx context.Context, req placement.PlacementAuditRequest) (placement.PlacementAuditReport, error)
	PlanPlacement(ctx context.Context, req client.PlacementPlanRequest) (client.PlacementPlan, error)
	ApplyPlacement(ctx context.Context, req client.PlacementApplyRequest) (client.PlacementApplyResult, error)
}

type SDKCefas struct {
	GRPC  *client.Client
	Audit *HTTPAuditClient
}

func (s *SDKCefas) Status(ctx context.Context) (client.ClusterStatus, error) {
	if s == nil || s.GRPC == nil {
		return client.ClusterStatus{}, fmt.Errorf("cefas gRPC client is not configured")
	}
	return s.GRPC.Status(ctx)
}

func (s *SDKCefas) AuditPlacement(ctx context.Context, req placement.PlacementAuditRequest) (placement.PlacementAuditReport, error) {
	if s == nil || s.Audit == nil {
		return placement.PlacementAuditReport{}, fmt.Errorf("cefas audit HTTP client is not configured")
	}
	return s.Audit.AuditPlacement(ctx, req)
}

func (s *SDKCefas) PlanPlacement(ctx context.Context, req client.PlacementPlanRequest) (client.PlacementPlan, error) {
	if s == nil || s.GRPC == nil {
		return client.PlacementPlan{}, fmt.Errorf("cefas gRPC client is not configured")
	}
	return s.GRPC.PlanPlacement(ctx, req)
}

func (s *SDKCefas) ApplyPlacement(ctx context.Context, req client.PlacementApplyRequest) (client.PlacementApplyResult, error) {
	if s == nil || s.GRPC == nil {
		return client.PlacementApplyResult{}, fmt.Errorf("cefas gRPC client is not configured")
	}
	return s.GRPC.ApplyPlacement(ctx, req)
}

type HTTPAuditClient struct {
	base       *url.URL
	token      string
	httpClient *http.Client
}

func NewHTTPAuditClient(baseURL, bearer string, httpClient *http.Client) (*HTTPAuditClient, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if baseURL == "" {
		return nil, fmt.Errorf("audit base URL is required")
	}
	u, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		return nil, err
	}
	return &HTTPAuditClient{base: u, token: bearer, httpClient: httpClient}, nil
}

func (c *HTTPAuditClient) AuditPlacement(ctx context.Context, req placement.PlacementAuditRequest) (placement.PlacementAuditReport, error) {
	if c == nil {
		return placement.PlacementAuditReport{}, fmt.Errorf("audit client is nil")
	}
	req.IncludeRepairPlan = true
	body, err := json.Marshal(req)
	if err != nil {
		return placement.PlacementAuditReport{}, err
	}
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + "/v1/cluster/placement/audit"
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return placement.PlacementAuditReport{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	if c.token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return placement.PlacementAuditReport{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return placement.PlacementAuditReport{}, fmt.Errorf("placement audit status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var report placement.PlacementAuditReport
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		return placement.PlacementAuditReport{}, err
	}
	return report, nil
}
