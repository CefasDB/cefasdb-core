package manager

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/CefasDb/cefasdb/internal/placement"
	"github.com/CefasDb/cefasdb/pkg/client"
)

type DoctorOptions struct {
	Cefas      Cefas
	Kubernetes Kubernetes
	Kube       KubeSnapshotOptions
	Audit      placement.PlacementAuditRequest
	Now        func() time.Time
}

func Doctor(ctx context.Context, opts DoctorOptions) (DoctorReport, error) {
	now := time.Now
	if opts.Now != nil {
		now = opts.Now
	}
	report := DoctorReport{GeneratedAt: now().UTC(), Classification: HealthHealthy}
	if opts.Cefas == nil {
		report.addSignal(Signal{Component: "cefas", Name: "client", Status: "missing", Class: HealthUnsafe, Detail: "cefas client is not configured"})
		return report, nil
	}
	status, err := opts.Cefas.Status(ctx)
	if err != nil {
		report.addSignal(Signal{Component: "cefas", Name: "cluster_status", Status: "unreachable", Class: HealthUnsafe, Detail: err.Error()})
		return report, nil
	}
	report.Cluster = &status
	evaluateClusterStatus(&report, status)

	auditReq := opts.Audit
	auditReq.IncludeRepairPlan = true
	audit, err := opts.Cefas.AuditPlacement(ctx, auditReq)
	if err != nil {
		report.addSignal(Signal{Component: "cefas", Name: "placement_audit", Status: "unavailable", Class: HealthDegraded, Detail: err.Error()})
	} else {
		report.PlacementAudit = &audit
		evaluatePlacementAudit(&report, audit)
	}

	if opts.Kubernetes == nil {
		report.addSignal(Signal{Component: "kubernetes", Name: "snapshot", Status: "skipped", Class: HealthDegraded, Detail: "kubernetes client is not configured"})
		return report, nil
	}
	snap, err := opts.Kubernetes.Snapshot(ctx, opts.Kube)
	if err != nil {
		report.addSignal(Signal{Component: "kubernetes", Name: "snapshot", Status: "unavailable", Class: HealthDegraded, Detail: err.Error()})
		return report, nil
	}
	report.Kubernetes = &snap
	evaluateKubernetes(&report, status, snap, report.GeneratedAt)
	return report, nil
}

func (r *DoctorReport) addSignal(sig Signal) {
	r.Signals = append(r.Signals, sig)
	r.Classification = worstHealth(r.Classification, sig.Class)
}

func evaluateClusterStatus(report *DoctorReport, st client.ClusterStatus) {
	if st.Mode == "" {
		report.addSignal(Signal{Component: "cefas", Name: "mode", Status: "unknown", Class: HealthDegraded})
	}
	if st.ShardCount == 0 && len(st.Shards) == 0 {
		report.addSignal(Signal{Component: "cefas", Name: "placement", Status: "empty", Class: HealthUnsafe, Detail: "cluster reports no shards"})
		return
	}
	for _, shard := range st.Shards {
		if len(shard.Voters) == 0 {
			report.addSignal(Signal{Component: "cefas", Name: fmt.Sprintf("shard/%d/quorum", shard.ID), Status: "no_voters", Class: HealthUnsafe, Detail: "shard has no voting replicas"})
			continue
		}
		if len(shard.Voters) < 3 {
			report.addSignal(Signal{
				Component: "cefas",
				Name:      fmt.Sprintf("shard/%d/quorum", shard.ID),
				Status:    "low_replication_factor",
				Class:     HealthDegraded,
				Detail:    fmt.Sprintf("shard has %d voters; RF=3 is the expected resilient default", len(shard.Voters)),
				Metadata:  map[string]any{"voters": shard.Voters},
			})
		}
		if shard.State != "" && shard.State != "active" {
			report.addSignal(Signal{Component: "cefas", Name: fmt.Sprintf("shard/%d/state", shard.ID), Status: shard.State, Class: HealthRepairable, Detail: "non-active shard state requires reconciliation"})
		}
	}
	for _, node := range st.Nodes {
		switch node.State {
		case "", "active":
		case "draining":
			report.addSignal(Signal{Component: "cefas", Name: "node/" + node.ID, Status: "draining", Class: HealthRepairable, Detail: strings.Join(nodeActiveReferences(st, node.ID), "; ")})
		case "decommissioned":
			report.addSignal(Signal{Component: "cefas", Name: "node/" + node.ID, Status: "decommissioned", Class: HealthHealthy})
		default:
			report.addSignal(Signal{Component: "cefas", Name: "node/" + node.ID, Status: node.State, Class: HealthRepairable, Detail: "unknown placement node state"})
		}
	}
	if report.Classification == HealthHealthy {
		report.addSignal(Signal{Component: "cefas", Name: "cluster_status", Status: "ok", Class: HealthHealthy})
	}
}

func evaluatePlacementAudit(report *DoctorReport, audit placement.PlacementAuditReport) {
	if audit.Truncated {
		report.addSignal(Signal{Component: "cefas", Name: "placement_audit", Status: "truncated", Class: HealthUnsafe, Detail: "audit hit max issue limit; repair plan is incomplete"})
		return
	}
	if audit.ConsistencyVerdict == "pass" && len(audit.Issues) == 0 {
		report.addSignal(Signal{Component: "cefas", Name: "placement_audit", Status: "pass", Class: HealthHealthy})
		return
	}
	for _, issue := range audit.Issues {
		class := HealthRepairable
		if issue.Severity == placement.PlacementAuditSeverityWarning {
			class = HealthDegraded
		}
		report.addSignal(Signal{
			Component: "cefas",
			Name:      "placement_audit/" + issue.Kind,
			Status:    issue.Severity,
			Class:     class,
			Detail:    issue.Detail,
			Metadata:  map[string]any{"table": issue.Table, "keyHex": issue.KeyHex, "shardId": issue.ShardID},
		})
	}
}

func evaluateKubernetes(report *DoctorReport, st client.ClusterStatus, snap KubernetesSnapshot, now time.Time) {
	if len(snap.Pods) == 0 {
		report.addSignal(Signal{Component: "kubernetes", Name: "pods", Status: "empty", Class: HealthDegraded, Detail: "no pods matched the manager selector"})
	}
	for _, pod := range snap.Pods {
		if !podReady(pod) {
			report.addSignal(Signal{Component: "kubernetes", Name: "pod/" + pod.Metadata.Name, Status: pod.Status.Phase, Class: HealthRepairable, Detail: podFailureDetail(pod), Metadata: map[string]any{"nodeName": pod.Spec.NodeName}})
		}
		for _, cs := range pod.Status.ContainerStatuses {
			if cs.RestartCount >= 5 {
				report.addSignal(Signal{Component: "kubernetes", Name: "pod/" + pod.Metadata.Name + "/container/" + cs.Name, Status: "high_restarts", Class: HealthRepairable, Detail: fmt.Sprintf("restartCount=%d", cs.RestartCount)})
			}
			if cs.State.Waiting != nil && crashLoopReason(cs.State.Waiting.Reason) {
				report.addSignal(Signal{Component: "kubernetes", Name: "pod/" + pod.Metadata.Name + "/container/" + cs.Name, Status: cs.State.Waiting.Reason, Class: HealthUnsafe, Detail: cs.State.Waiting.Message})
			}
		}
	}
	for _, node := range snap.Nodes {
		if !nodeReady(node) {
			report.addSignal(Signal{Component: "kubernetes", Name: "node/" + node.Metadata.Name, Status: "not_ready", Class: HealthDegraded, Detail: nodeFailureDetail(node)})
		}
	}
	for _, pvc := range snap.PVCs {
		if pvc.Status.Phase != "" && pvc.Status.Phase != "Bound" {
			report.addSignal(Signal{Component: "kubernetes", Name: "pvc/" + pvc.Metadata.Name, Status: pvc.Status.Phase, Class: HealthUnsafe, Detail: "persistent volume claim is not bound"})
		}
	}
	for _, ep := range snap.Endpoints {
		notReady := 0
		ready := 0
		for _, subset := range ep.Subsets {
			ready += len(subset.Addresses)
			notReady += len(subset.NotReadyAddresses)
		}
		if ready == 0 || notReady > 0 {
			class := HealthDegraded
			if ready == 0 {
				class = HealthUnsafe
			}
			report.addSignal(Signal{Component: "kubernetes", Name: "endpoints/" + ep.Metadata.Name, Status: "not_ready", Class: class, Detail: fmt.Sprintf("ready=%d notReady=%d", ready, notReady)})
		}
	}
	clusterNodeIDs := clusterNodeSet(st)
	for _, lease := range snap.Leases {
		if leaseExpired(lease, now) {
			class := HealthRepairable
			if clusterNodeIDs[lease.Spec.HolderIdentity] {
				class = HealthUnsafe
			}
			report.addSignal(Signal{Component: "kubernetes", Name: "lease/" + lease.Metadata.Name, Status: "expired", Class: class, Detail: fmt.Sprintf("holder=%s", lease.Spec.HolderIdentity)})
		}
	}
	if report.Classification == HealthHealthy {
		report.addSignal(Signal{Component: "kubernetes", Name: "snapshot", Status: "ok", Class: HealthHealthy})
	}
}

func nodeActiveReferences(st client.ClusterStatus, nodeID string) []string {
	var refs []string
	for _, sh := range st.Shards {
		if containsString(sh.Voters, nodeID) {
			refs = append(refs, fmt.Sprintf("shard %d voter", sh.ID))
		}
		if containsString(sh.NonVoters, nodeID) {
			refs = append(refs, fmt.Sprintf("shard %d non-voter", sh.ID))
		}
		if sh.LeaderHint == nodeID {
			refs = append(refs, fmt.Sprintf("shard %d leaderHint", sh.ID))
		}
	}
	sort.Strings(refs)
	return refs
}

func containsString(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func podFailureDetail(pod Pod) string {
	var parts []string
	for _, cond := range pod.Status.Conditions {
		if cond.Status != "True" && (cond.Reason != "" || cond.Message != "") {
			parts = append(parts, strings.TrimSpace(cond.Type+" "+cond.Reason+" "+cond.Message))
		}
	}
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil {
			parts = append(parts, strings.TrimSpace(cs.Name+" waiting "+cs.State.Waiting.Reason+" "+cs.State.Waiting.Message))
		}
		if cs.State.Terminated != nil {
			parts = append(parts, strings.TrimSpace(fmt.Sprintf("%s terminated %s exit=%d %s", cs.Name, cs.State.Terminated.Reason, cs.State.Terminated.ExitCode, cs.State.Terminated.Message)))
		}
	}
	if len(parts) == 0 {
		return "pod is not ready"
	}
	return strings.Join(parts, "; ")
}

func nodeFailureDetail(node Node) string {
	for _, cond := range node.Status.Conditions {
		if cond.Type == "Ready" && cond.Status != "True" {
			return strings.TrimSpace(cond.Reason + " " + cond.Message)
		}
	}
	return "node is not ready"
}

func crashLoopReason(reason string) bool {
	switch reason {
	case "CrashLoopBackOff", "RunContainerError", "CreateContainerConfigError", "ImagePullBackOff", "ErrImagePull":
		return true
	default:
		return false
	}
}

func clusterNodeSet(st client.ClusterStatus) map[string]bool {
	out := map[string]bool{}
	for _, node := range st.Nodes {
		out[node.ID] = true
	}
	return out
}
