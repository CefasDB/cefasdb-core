# ADR 0009: RF3 qualification acceptance contract

Status: accepted (2026-11-22 UTC)

## Decision

Team CefasDB accepts the schema 1.0 RF3 qualification contract for contract
`rf3.acceptance.rf3-p1.v1`. The product owner, database operator, and
platform owner approved the profile IDs and exact profile digests at
2026-11-22T16:09:32.817Z through the protected decision records listed below.
This decision authorizes the checked-in targets; it does not claim that an RF3
environment has passed qualification. Publication remains gated on a host
`make test` receipt for the final source bytes.

## Context

RF3-P2 and RF3-P4 need one immutable identity for the workload, dataset,
topology, observation windows, service limits, recovery rules, variance rule,
capacity method, and eight resilience scenarios. The repository matrix defines
the scenario names and the one-domain versus two-domain safety boundary, but it
does not supply approved numeric limits.

The core decoder accepts exactly three caller-supplied JSON sources. It
strictly decodes the acceptance root, workload profile, and topology profile;
checks the root-to-profile IDs and SHA-256 digests; validates the aggregate;
and returns `Bundle{}` on any error. [SRC-36566995ee042ac5]
[SRC-d96069f0ad8ff1c6] The filesystem adapter opens those three named paths,
closes every opened input, preserves path failures, and delegates semantics to
the core. [SRC-3b1d1359eddeca9b]

## Exact-byte contract

The accepted bundle contains these semantic inputs:

| Role | Repository path | Identity | SHA-256 |
| --- | --- | --- | --- |
| Acceptance root | `contracts/rf3-acceptance/v1.json` | `rf3.acceptance.rf3-p1.v1` | `10004b1885b3cbbe5d75cd9431d48449d5d6305d72c925448aba13ed9ab09853` |
| Workload profile | `contracts/rf3-acceptance/workload-v1.json` | `rf3.workload.synthetic-mixed-one-kops.v1` | `d5bba0f8c03acc85b7190a15386acaba42167d6f42825b46f0847b198a9e3d66` |
| Topology profile | `contracts/rf3-acceptance/topology-v1.json` | `rf3.topology.three-node-standard.v1` | `33628e501c43019dce38209a37ca42c8cde9e56c90e23af955e2878f49e33b78` |

A digest is lowercase `sha256:<64-hex>` over exact file bytes. The loader
does not canonicalize JSON, normalize whitespace, reorder fields, or normalize
values before hashing. The root binds both profile paths, IDs, and digests.
[SRC-b7e8b815921c6624]

`contracts/rf3-acceptance/publication-v1.json` is the audit binding for the
published root. Its SHA-256 is
`e2b1c429f2cd2031de2bed134cf962c3e3d2b3d3502c281fa1b623cc6fde2fea`.
It binds the root digest, both profile digests, the protected review manifest,
the platform-input receipt, the decision policy, each role decision, and the
approved attempt semantics. It is not a fourth input to `DecodeBundle`.
[SRC-08db62aad3d95d42]

## Protected owner decisions

The Code Forge records below are the authorization evidence. The checked-in
publication record indexes them; it does not replace their protected storage.

| Decision | Protected record |
| --- | --- |
| Review manifest | `codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/drafts/approval-review-plan-implementation--1--publish-material/4775c160088eb62e695f3da2dbe1cdc28855aeecb058427b59297f795ded07f6/manifest.json`, SHA-256 `706b59e4729641c03de05ac8b1d4db96e78630cccb560ecb123d2eced87c6740` |
| Decision policy | `codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-decision-policy-v1.json` |
| Product owner | `codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-product-owner-approval-v1.json` |
| Database operator | `codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-database-operator-approval-v1.json` |
| Platform owner | `codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-platform-owner-approval-v1.json` |
| Platform literals | `codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/input-approval-plan-implementation--1--platform-d69b6f54ee47816284499670211f92456ef5e58c0a118cdf10a89313f89ccc14.json` |

Each role decision names the contract ID, both profile IDs, both profile
digests, the approver `github:432802 (@osvaldoandrade)`, and the approval
timestamp. The publication record additionally binds every role decision to
the final acceptance-root digest. Changing the root or either profile requires
a new publication record and a new verification receipt.

The platform-input record authorizes exactly
`rf3q-zone-a`, `rf3q-zone-b`, and `rf3q-zone-c`, plus storage class
`cefasdb-rf3-durable-rwo-v1`.

## Attempt and error semantics

Schema 1.0 retains the encoded unit `requests` for
`defaultRule.minimumAttempts`. For this accepted contract, the publication
record defines that denominator as logical item operations:

- one attempted write item counts as one attempt, even when the runner sends
  several items in one batch RPC;
- one point read counts as one attempt;
- a failed batch write charges every item in the batch as failed unless
  per-item evidence proves a narrower failure set;
- error ppm equals failed logical item operations divided by attempted logical
  item operations, multiplied by 1,000,000.

The root requires 300,000 logical attempts in the 300,000 ms serving window.
A consumer that only reports RPC attempts cannot evaluate this gate and must
return an incomplete result. The source rationale and the publication record
carry the same mapping, so the pre-approval review note no longer remains in
the accepted root. [SRC-b7e8b815921c6624] [SRC-08db62aad3d95d42]

## Fixed bundle boundary

This design is a fixed three-source aggregate, not a dynamic microkernel. The
core constructs concrete `AcceptanceContractV1`, `WorkloadProfileV1`, and
`TopologyProfileV1` values. The caller supplies all three sources in one
`DecodeBundle` call. Any missing, unreadable, malformed, unsupported, or
inconsistent source rejects the entire aggregate; the design provides no
independent source lifecycle or failure isolation. Adding a fourth role
requires a contract version, decoder change, loader change, and tests.

The checked-in JSON documents are data inputs. They do not implement
`pkg/plugin.Plugin`, register executable behavior, support directory
discovery, or load shared objects.

## Component evidence

| Component | Accountable owner | Business responsibility | Source roots | Provides | Consumes | Owned data and reason to change |
| --- | --- | --- | --- | --- | --- | --- |
| RF3 acceptance contract data | Team CefasDB, from task `TASK-RF3-P1-APPROVAL` | Publish exact workload, topology, acceptance, and authorization bindings for downstream qualification | `contracts/rf3-acceptance` | Versioned JSON inputs and publication evidence | Protected Code Forge decision locators | Owns the checked-in contract bytes; this change removes the unresolved attempt mapping and binds the final root to approvals |
| RF3 acceptance core | Team CefasDB, from the task owner; repository CODEOWNERS evidence was not inspected | Strictly decode and validate the three-source aggregate | `internal/core/rf3acceptance` | `DecodeBundle`, `Bundle`, typed contract values | Three immutable `Source` values | Owns semantic validation and exact-byte digest checks; this resolution does not change its fixed schema 1.0 API |
| RF3 contractbuild adapter | Team CefasDB, from the task owner; repository CODEOWNERS evidence was not inspected | Load caller-selected files and verify published artifacts | `internal/contractbuild` | `LoadRF3AcceptanceFiles` and end-to-end contract tests | RF3 acceptance core and checked-in JSON | Owns filesystem error handling and publication verification; this change adds exact publication and decision-binding assertions |
| RF3 contract ADR | Team CefasDB, from the task owner | Record the accepted boundary, authorization, semantics, verification gate, and rollback | `docs/adr/0009-rf3-qualification-acceptance-contract.md` | Decision record | Inspected source and protected record locators | Owns the decision narrative; this change removes contradictory proposed-state and microkernel claims |

The captured architecture assessment is incomplete and reports no passed
fitness result. This ADR does not convert its incomplete metrics into a quality
claim.

## Verification gate

`TestLoadPublishedRF3AcceptanceBundle` loads the exact three semantic files
and pins their digests. [SRC-39537e4be70c23c9]
`TestRF3PublicationBindsProtectedDecisionsAndFinalBundle` strictly decodes
the publication record, pins its digest, compares all bundle digests, checks
the protected evidence locators, and enforces the logical-attempt mapping.
[SRC-785addbca2c07085]

The source definitions are not an execution receipt. The host must run
`make test` against the final recorded source version and retain an exit-code
0 receipt. A failed or missing host receipt blocks publication even when the
test files exist. The repository's warn-only CI job does not replace this
task-specific host gate.

## Compatibility

Readers accept schema 1.0 only and fail closed on unsupported versions,
unknown fields, unknown enums, missing required fields, path mismatches, and
digest mismatches. The publication record resolves the schema 1.0 attempt-unit
label for this contract without changing the three-input decoder API.

A future change to a field name, type, unit, enum, digest rule, owner rule, or
invariant requires a new major schema version. Any workload, topology, target,
method, approval, or publication binding change produces new bytes and a new
root or publication version.

## Consequences

- RF3-P2 and RF3-P4 can identify the exact authorized contract and protected
  decisions without treating embedded approver strings as authorization.
- Evaluators use one logical-operation denominator for batched writes and
  point reads.
- One invalid semantic input rejects the whole bundle; callers receive no
  partial aggregate or isolated source recovery.
- The accepted contract does not prove live availability, latency,
  throughput, recovery, variance, capacity, or fault behavior.
- Formatting-only edits change identity and require a new publication
  binding.

## Alternatives considered

1. Treat the three JSON files as independent plug-ins. Rejected because the
   implementation has one fixed call, one aggregate, and no independent
   lifecycle or failure boundary.
2. Treat the embedded approver strings as authorization. Rejected because
   canonical text and profile digest validation do not prove role authority.
3. Count batch RPCs as attempts. Rejected because the approved denominator is
   logical item operations and batching would change both the attempt floor and
   error ppm.
4. Change schema 1.0 and the core decoder in this resolution. Rejected because
   this review step can edit only its recorded source files; the publication
   record resolves the approved mapping without claiming a decoder API change.
5. Use the warn-only CI test as the publication receipt. Rejected because a
   continued test failure leaves the workflow green.

## Rollback

Before a downstream manifest consumes the acceptance-root digest, Team
CefasDB may revert the root, publication record, tests, and this ADR.

After a run, result, or release record consumes a digest, Team CefasDB must
retain those exact bytes. A correction publishes a new root and publication
version for new runs; rollback may stop new qualification runs and restore the
last supported reader, but it must not edit or relabel consumed artifacts.
