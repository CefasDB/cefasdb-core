package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/CefasDb/cefasdb/pkg/client"
)

type RepairOptions struct {
	Cefas          Cefas
	Report         DoctorReport
	Mode           string
	Leader         LeaderLease
	ApproveFencing bool
	AuditLogPath   string
	Timeout        time.Duration
	Now            func() time.Time
}

func BuildRepairPlan(opts RepairOptions) RepairPlan {
	now := time.Now
	if opts.Now != nil {
		now = opts.Now
	}
	mode := opts.Mode
	if mode == "" {
		mode = "dry-run"
	}
	plan := RepairPlan{GeneratedAt: now().UTC(), Mode: mode, Classification: opts.Report.Classification}
	clusterReady := opts.Report.Cluster != nil
	plan.Preconditions = append(plan.Preconditions, Precondition{Name: "cluster_status_read", Status: boolPrecondition(clusterReady), Detail: detailIf(!clusterReady, "doctor could not read Cefas cluster status")})
	auditComplete := opts.Report.PlacementAudit == nil || !opts.Report.PlacementAudit.Truncated
	plan.Preconditions = append(plan.Preconditions, Precondition{Name: "placement_audit_complete", Status: boolPrecondition(auditComplete), Detail: detailIf(!auditComplete, "placement audit is truncated; repair plan is incomplete")})
	if mode == "execute" {
		plan.Preconditions = append(plan.Preconditions, Precondition{Name: "manager_leader_election", Status: boolPrecondition(opts.Leader.Acquired), Detail: detailIf(!opts.Leader.Acquired, fmt.Sprintf("leader lease is held by %q", opts.Leader.Holder))})
	} else {
		plan.Preconditions = append(plan.Preconditions, Precondition{Name: "manager_leader_election", Status: PreconditionWarning, Detail: "not required for dry-run output"})
	}
	plan.Actions = append(plan.Actions, placementAuditActions(opts.Report)...)
	if opts.Report.Cluster != nil {
		plan.Actions = append(plan.Actions, placementNodeActions(*opts.Report.Cluster)...)
	}
	if opts.Report.Kubernetes != nil {
		plan.Actions = append(plan.Actions, kubernetesRepairActions(*opts.Report.Kubernetes, plan.GeneratedAt)...)
	}
	if hasSensitiveAction(plan.Actions) {
		status := PreconditionWarning
		detail := "sensitive actions require --approve-fencing before execution"
		if mode == "execute" {
			status = boolPrecondition(opts.ApproveFencing)
			detail = detailIf(!opts.ApproveFencing, detail)
		}
		plan.Preconditions = append(plan.Preconditions, Precondition{Name: "fencing_approved", Status: status, Detail: detail})
	}
	return plan
}

func ExecuteRepair(ctx context.Context, opts RepairOptions) (RepairResult, error) {
	opts.Mode = "execute"
	plan := BuildRepairPlan(opts)
	result := RepairResult{Plan: plan, AuditLogPath: opts.AuditLogPath}
	if err := appendAudit(opts.AuditLogPath, "plan", "", plan, ""); err != nil {
		return result, err
	}
	if err := preconditionsPassed(plan); err != nil {
		result.Error = err.Error()
		result.StoppedAt = "preconditions"
		_ = appendAudit(opts.AuditLogPath, "stop", "preconditions", nil, err.Error())
		return result, err
	}
	if opts.Cefas == nil {
		err := errors.New("cefas client is required for execute")
		result.Error = err.Error()
		result.StoppedAt = "cefas_client"
		_ = appendAudit(opts.AuditLogPath, "stop", "cefas_client", nil, err.Error())
		return result, err
	}
	timeoutMS := int((5 * time.Second) / time.Millisecond)
	if opts.Timeout > 0 {
		timeoutMS = int(opts.Timeout / time.Millisecond)
	}
	for _, action := range plan.Actions {
		if action.Sensitive && !opts.ApproveFencing {
			err := fmt.Errorf("action %s is fencing-sensitive and --approve-fencing was not set", action.ID)
			result.Error = err.Error()
			result.StoppedAt = action.ID
			_ = appendAudit(opts.AuditLogPath, "stop", action.ID, action, err.Error())
			return result, err
		}
		if !action.Supported {
			err := fmt.Errorf("action %s requires manual review: %s", action.ID, action.Description)
			result.Error = err.Error()
			result.StoppedAt = action.ID
			_ = appendAudit(opts.AuditLogPath, "stop", action.ID, action, err.Error())
			return result, err
		}
		if action.Placement == nil {
			err := fmt.Errorf("action %s is marked supported but has no placement request", action.ID)
			result.Error = err.Error()
			result.StoppedAt = action.ID
			_ = appendAudit(opts.AuditLogPath, "stop", action.ID, action, err.Error())
			return result, err
		}
		_ = appendAudit(opts.AuditLogPath, "start", action.ID, action, "")
		placementPlan, err := opts.Cefas.PlanPlacement(ctx, *action.Placement)
		if err != nil {
			result.Error = err.Error()
			result.StoppedAt = action.ID
			_ = appendAudit(opts.AuditLogPath, "stop", action.ID, action, err.Error())
			return result, err
		}
		if !placementPlan.ApplySupported {
			err := fmt.Errorf("placement plan for action %s is not apply-supported", action.ID)
			result.Error = err.Error()
			result.StoppedAt = action.ID
			_ = appendAudit(opts.AuditLogPath, "stop", action.ID, placementPlan, err.Error())
			return result, err
		}
		applyResult, err := opts.Cefas.ApplyPlacement(ctx, client.PlacementApplyRequest{Plan: placementPlan, ExpectedEpoch: placementPlan.BeforeEpoch, TimeoutMS: timeoutMS})
		actionResult := ActionResult{ActionID: action.ID, Status: "applied", Plan: &placementPlan, Result: &applyResult}
		if err != nil {
			actionResult.Status = "failed"
			actionResult.Detail = err.Error()
			result.Applied = append(result.Applied, actionResult)
			result.Error = err.Error()
			result.StoppedAt = action.ID
			_ = appendAudit(opts.AuditLogPath, "stop", action.ID, actionResult, err.Error())
			return result, err
		}
		result.Applied = append(result.Applied, actionResult)
		_ = appendAudit(opts.AuditLogPath, "applied", action.ID, actionResult, "")
	}
	return result, nil
}

func DryRunRepair(opts RepairOptions) RepairResult {
	opts.Mode = "dry-run"
	return RepairResult{Plan: BuildRepairPlan(opts), AuditLogPath: opts.AuditLogPath}
}

func placementAuditActions(report DoctorReport) []RepairAction {
	if report.PlacementAudit == nil || report.PlacementAudit.RepairPlan == nil {
		return nil
	}
	var actions []RepairAction
	for i, action := range report.PlacementAudit.RepairPlan.Actions {
		actions = append(actions, RepairAction{
			ID:            fmt.Sprintf("placement-audit-%03d", i+1),
			Type:          "placement_audit_manual_repair",
			Description:   action.Detail,
			Supported:     report.PlacementAudit.RepairPlan.ApplySupported,
			Sensitive:     true,
			Preconditions: []string{"cluster_status_read", "placement_audit_complete", "fencing_approved"},
			Payload:       map[string]any{"action": action},
		})
	}
	return actions
}

func placementNodeActions(st client.ClusterStatus) []RepairAction {
	var actions []RepairAction
	for _, node := range st.Nodes {
		if node.State != "draining" {
			continue
		}
		refs := nodeActiveReferences(st, node.ID)
		if len(refs) > 0 {
			actions = append(actions, RepairAction{
				ID:            "placement-drain-" + sanitizeID(node.ID),
				Type:          "placement_drain",
				Description:   "continue draining placement references away from " + node.ID,
				Supported:     true,
				Preconditions: []string{"cluster_status_read", "manager_leader_election"},
				Placement:     &client.PlacementPlanRequest{Operation: "drain", NodeID: node.ID},
				Payload:       map[string]any{"activeReferences": refs},
			})
			continue
		}
		actions = append(actions, RepairAction{
			ID:            "placement-decommission-" + sanitizeID(node.ID),
			Type:          "placement_decommission",
			Description:   "mark drained node " + node.ID + " as decommissioned",
			Supported:     true,
			Sensitive:     true,
			Preconditions: []string{"cluster_status_read", "manager_leader_election", "fencing_approved"},
			Placement:     &client.PlacementPlanRequest{Operation: "decommission", NodeID: node.ID},
		})
	}
	return actions
}

func kubernetesRepairActions(snap KubernetesSnapshot, now time.Time) []RepairAction {
	var actions []RepairAction
	for _, pod := range snap.Pods {
		if podReady(pod) {
			continue
		}
		actions = append(actions, RepairAction{
			ID:            "kubernetes-pod-review-" + sanitizeID(pod.Metadata.Name),
			Type:          "kubernetes_manual_review",
			Description:   "pod is not ready; verify replacement/fencing before any data-plane repair",
			Supported:     false,
			Sensitive:     true,
			Preconditions: []string{"fencing_approved"},
			Payload:       map[string]any{"pod": pod.Metadata.Name, "phase": pod.Status.Phase, "nodeName": pod.Spec.NodeName, "namespace": pod.Metadata.Namespace},
		})
	}
	for _, lease := range snap.Leases {
		if !leaseExpired(lease, now) {
			continue
		}
		actions = append(actions, RepairAction{
			ID:            "identity-lease-review-" + sanitizeID(lease.Metadata.Name),
			Type:          "identity_lease_manual_review",
			Description:   "identity lease expired; confirm the old holder is fenced before deleting or replacing it",
			Supported:     false,
			Sensitive:     true,
			Preconditions: []string{"fencing_approved"},
			Payload:       map[string]any{"lease": lease.Metadata.Name, "holder": lease.Spec.HolderIdentity},
		})
	}
	return actions
}

func preconditionsPassed(plan RepairPlan) error {
	for _, pre := range plan.Preconditions {
		if pre.Status == PreconditionFail {
			if pre.Detail != "" {
				return fmt.Errorf("precondition %s failed: %s", pre.Name, pre.Detail)
			}
			return fmt.Errorf("precondition %s failed", pre.Name)
		}
	}
	return nil
}

func boolPrecondition(ok bool) PreconditionStatus {
	if ok {
		return PreconditionPass
	}
	return PreconditionFail
}

func detailIf(ok bool, detail string) string {
	if ok {
		return detail
	}
	return ""
}

func hasSensitiveAction(actions []RepairAction) bool {
	for _, action := range actions {
		if action.Sensitive {
			return true
		}
	}
	return false
}

func sanitizeID(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range v {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	return b.String()
}

type auditEntry struct {
	At       time.Time `json:"at"`
	Stage    string    `json:"stage"`
	ActionID string    `json:"actionId,omitempty"`
	Object   any       `json:"object,omitempty"`
	Error    string    `json:"error,omitempty"`
}

func appendAudit(path, stage, actionID string, object any, errText string) error {
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	entry := auditEntry{At: time.Now().UTC(), Stage: stage, ActionID: actionID, Object: object, Error: errText}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = f.Write(append(data, '\n'))
	return err
}
