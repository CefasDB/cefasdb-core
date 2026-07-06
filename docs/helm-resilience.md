# Helm Resilience Profile

The default chart remains a single-Pod development install. Enable the
resilience profile for an RF=3 Kubernetes deployment:

```sh
helm upgrade --install cefas dist/helm/cefas \
  --set resilience.enabled=true \
  --set cluster.shards=24
```

With `resilience.enabled=true`, the chart renders:

- at least three `cefasdb` StatefulSet replicas
- `cluster.replicationFactor=3` unless explicitly set to a higher safe value
- generated Raft, HTTP, and gRPC peer maps for every StatefulSet ordinal
- PVC-backed storage unless `resilience.allowEphemeralStorage=true` is explicit
- a PodDisruptionBudget with one voluntary disruption at a time
- preferred pod anti-affinity by `kubernetes.io/hostname`
- topology spread constraints for `kubernetes.io/hostname` and
  `cefasdb.io/failure-domain`
- a longer startup probe window for bootstrap and Raft log replay

## Failure Domains

Label physical hosts when the cluster spans local machines:

```sh
kubectl label node m1-node cefasdb.io/failure-domain=m1
kubectl label node m4-node cefasdb.io/failure-domain=m4
```

The default spread rule is `ScheduleAnyway`: it prefers distribution when the
labels exist, but it does not make local test clusters unschedulable while a
node label is missing.

## Resource Policy

Database Pods should have explicit memory safeguards. CPU limits are optional:
they are useful for shared clusters, but they can hide available hardware and
make performance tests misleading through throttling.

Production baseline:

```yaml
resilience:
  enabled: true
resources:
  requests:
    cpu: "2000m"
    memory: "8Gi"
  limits:
    cpu: "4000m"
    memory: "8Gi"
resourcePolicy:
  disableCPULimits: false
  requireMemoryLimit: true
```

Dedicated performance or local M1/M4 test nodes:

```yaml
resilience:
  enabled: true
cluster:
  shards: 24
resources:
  requests:
    cpu: "1000m"
    memory: "4Gi"
  limits:
    memory: "8Gi"
resourcePolicy:
  disableCPULimits: true
  requireMemoryLimit: true
```

When CPU limits are disabled, use metrics and alerts to detect saturation
instead of relying on throttling as the control plane. Watch Kubernetes CPU
usage/throttling, `process_cpu_seconds_total`, Go runtime metrics,
`cefas_storage_lane_*`, `cefas_backpressure_*`, and Raft lag/leadership
metrics.

## Validation

Unsafe combinations fail during Helm rendering:

- `resilience.replicas < 3`
- replication factor below 3
- replication factor greater than rendered replicas
- disabled persistence without `resilience.allowEphemeralStorage=true`
- missing memory limit when `resourcePolicy.requireMemoryLimit=true`

Run the chart smoke tests:

```sh
scripts/test_helm_resilience.sh
```

Run the Kubernetes resilience acceptance suite in CI-safe dry-run mode:

```sh
MODE=dry-run scripts/k8s_resilience_suite.sh
```
