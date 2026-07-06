# Kubernetes Resilience Acceptance Matrix

This is the RF=3 acceptance matrix for Kubernetes and Talos-style failure
testing. The suite is implemented by `scripts/k8s_resilience_suite.sh`.

Run the CI-safe render check:

```sh
MODE=dry-run scripts/k8s_resilience_suite.sh
```

Run against a real cluster:

```sh
MODE=live \
KUBE_CONTEXT=hack \
NAMESPACE=cefas-resilience \
RELEASE=cefas-resilience \
KEEP_CLUSTER=1 \
scripts/k8s_resilience_suite.sh
```

Destructive node and provider faults require an explicit opt-in:

```sh
MODE=live \
KUBE_CONTEXT=hack \
ALLOW_DESTRUCTIVE=1 \
NODE_SHUTDOWN_COMMAND='./ops/talos-shutdown-one-node "$TARGET_NODE"' \
NODE_RESTORE_COMMAND='./ops/talos-power-on-one-node "$TARGET_NODE"' \
scripts/k8s_resilience_suite.sh
```

The hook commands run with `TARGET_POD`, `TARGET_NODE`, `NAMESPACE`,
`RELEASE`, `APP`, `SELECTOR`, and `KUBE_CONTEXT` exported.

| Scenario | Fault | Expected Service Behavior | Recovery | Stop Condition |
| --- | --- | --- | --- | --- |
| `healthy-baseline` | None | All database Pods are ready; `cefas-manager doctor` is healthy or degraded-but-serving. | Not applicable. | Doctor fails or reports unsafe. |
| `pod-kill` | Delete one CefasDB Pod and keep PVCs. | StatefulSet recreates the Pod and the RF=3 cluster remains serving. | Wait for rollout and startup probe completion. | Ready quorum is not restored or doctor reports unsafe. |
| `node-drain` | Cordon one node hosting a database Pod and drain CefasDB Pods from it. | One failure domain can be unavailable while the remaining majority stays serving. | Uncordon the node and let Kubernetes reschedule. | Quorum-ready Pod count is lost or doctor reports unsafe. |
| `node-shutdown` | Provider/Talos hook shuts down one node. | One physical host loss remains serving or explicitly degraded-but-serving. | Provider/Talos hook powers the node back on. | Doctor reports unsafe for a one-node loss. |
| `orphan-process` | Provider/Talos hook leaves or simulates a stale process with an old raft identity. | The stale process is fenced by the Kubernetes lease and the active cluster remains serving. | Provider/Talos hook removes the stale process. | The stale process can serve or CPU-spins without a valid lease. |
| `disk-pressure` | Provider/Talos hook applies disk pressure on one database node. | Manager reports pressure and the database stays serving or degraded-but-serving. | Provider/Talos hook removes pressure and confirms PVC health. | Pods spin indefinitely or doctor reports unsafe for a one-node fault. |
| `network-partition` | Provider/Talos hook partitions one database node from the cluster. | The majority side remains serving and the isolated node is not accepted as healthy. | Provider/Talos hook removes the partition and raft catches up. | Split brain is observed or manager cannot identify unsafe state. |
| `two-node-failure` | Two failure domains unavailable at the same time. | RF=3 fails safely with clear quorum or unsafe reporting. | Restore nodes/hooks and wait for raft catch-up. | Pods CPU-spin without functional quorum or health reporting is ambiguous. |

## Artifacts

Each run writes:

- `summary.md`: scenario table and statuses
- `matrix.json`: machine-readable acceptance matrix
- `rendered.yaml`: Helm render used for the run
- `helm-lint.txt`: chart lint output
- `scenarios/<name>/doctor.json`: manager health report when available
- `scenarios/<name>/*-pods.txt`, `*-events.txt`, `*-nodes.txt`, and logs for failures

## Acceptance Rules

One failure domain loss must keep the cluster serving or explicitly
degraded-but-serving. Two failure domain loss is not required to serve with
RF=3, but it must fail safely: the manager must report quorum loss or unsafe
state, and Pods must not remain CPU-bound while nonfunctional.
