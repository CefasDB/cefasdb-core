package manager

import (
	"time"

	"github.com/CefasDb/cefasdb/internal/placement"
	"github.com/CefasDb/cefasdb/pkg/client"
)

type HealthClass string

const (
	HealthHealthy    HealthClass = "healthy"
	HealthDegraded   HealthClass = "degraded"
	HealthRepairable HealthClass = "repairable"
	HealthUnsafe     HealthClass = "unsafe"
)

type Signal struct {
	Component string         `json:"component"`
	Name      string         `json:"name"`
	Status    string         `json:"status"`
	Class     HealthClass    `json:"class"`
	Detail    string         `json:"detail,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

type DoctorReport struct {
	GeneratedAt    time.Time                       `json:"generatedAt"`
	Classification HealthClass                     `json:"classification"`
	Cluster        *client.ClusterStatus           `json:"cluster,omitempty"`
	Kubernetes     *KubernetesSnapshot             `json:"kubernetes,omitempty"`
	PlacementAudit *placement.PlacementAuditReport `json:"placementAudit,omitempty"`
	Signals        []Signal                        `json:"signals"`
}

type PreconditionStatus string

const (
	PreconditionPass    PreconditionStatus = "pass"
	PreconditionFail    PreconditionStatus = "fail"
	PreconditionWarning PreconditionStatus = "warning"
)

type Precondition struct {
	Name   string             `json:"name"`
	Status PreconditionStatus `json:"status"`
	Detail string             `json:"detail,omitempty"`
}

type RepairAction struct {
	ID            string                       `json:"id"`
	Type          string                       `json:"type"`
	Description   string                       `json:"description"`
	Supported     bool                         `json:"supported"`
	Sensitive     bool                         `json:"sensitive"`
	Preconditions []string                     `json:"preconditions,omitempty"`
	Placement     *client.PlacementPlanRequest `json:"placement,omitempty"`
	Payload       map[string]any               `json:"payload,omitempty"`
}

type RepairPlan struct {
	GeneratedAt    time.Time      `json:"generatedAt"`
	Mode           string         `json:"mode"`
	Classification HealthClass    `json:"classification"`
	Preconditions  []Precondition `json:"preconditions"`
	Actions        []RepairAction `json:"actions"`
}

type ActionResult struct {
	ActionID string                       `json:"actionId"`
	Status   string                       `json:"status"`
	Detail   string                       `json:"detail,omitempty"`
	Plan     *client.PlacementPlan        `json:"plan,omitempty"`
	Result   *client.PlacementApplyResult `json:"result,omitempty"`
}

type RepairResult struct {
	Plan         RepairPlan     `json:"plan"`
	Applied      []ActionResult `json:"applied,omitempty"`
	AuditLogPath string         `json:"auditLogPath,omitempty"`
	StoppedAt    string         `json:"stoppedAt,omitempty"`
	Error        string         `json:"error,omitempty"`
}

func worstHealth(a, b HealthClass) HealthClass {
	if healthRank(b) > healthRank(a) {
		return b
	}
	return a
}

func healthRank(v HealthClass) int {
	switch v {
	case HealthUnsafe:
		return 4
	case HealthRepairable:
		return 3
	case HealthDegraded:
		return 2
	case HealthHealthy:
		return 1
	default:
		return 0
	}
}
