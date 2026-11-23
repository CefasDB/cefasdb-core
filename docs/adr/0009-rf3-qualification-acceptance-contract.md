# ADR 0009: RF3 qualification acceptance contract

Status: accepted (2026-11-22 UTC)

## Decision

Team CefasDB accepts the schema 1.0 RF3 qualification contract identified by
rf3.acceptance.rf3-p1.v1. The accepted authorization boundary consists of the
acceptance root, workload profile, topology profile, publication record, review
manifest, decision policy, platform-input decision, and three role approvals.

The product owner, database operator, and platform owner approved the frozen
profile IDs and digests at 2026-11-22T16:09:32.817Z. This decision authorizes
contract bytes. It does not claim that an RF3 environment passed qualification.
Publication remains blocked until the host records an exit-code-0 make test
receipt for the final source hashes.

## Context

The core RF3 decoder is a fixed three-source semantic boundary. It decodes the
acceptance root, workload profile, and topology profile, verifies the
root-to-profile paths, IDs, and digests, validates the aggregate, and returns no
partial bundle on error.

Authorization is a separate four-file adapter boundary. Production qualification
consumers call LoadRF3PublishedAcceptanceFiles with the three semantic files,
publication-v1.json, and a protected-evidence verifier. P2 environment consumers
call LoadAuthorizedRF3EnvironmentFiles. The older LoadRF3EnvironmentFiles API
is a semantic fixture loader: it composes a decoded bundle with an environment
manifest but does not establish owner authorization. It must not be used as the
P2 or P4 production authorization entry point. The enforced adapters and their
zero-value-on-error behavior are defined in
internal/contractbuild/rf3_publication.go. [SRC-28c24ee85d66ac87]
[SRC-781539152d991d4b]

## Exact-byte contract

The authorization adapter binds these repository artifacts to exact bytes:

| Role | Repository path | Identity | SHA-256 |
| --- | --- | --- | --- |
| Acceptance root | contracts/rf3-acceptance/v1.json | rf3.acceptance.rf3-p1.v1 | 10004b1885b3cbbe5d75cd9431d48449d5d6305d72c925448aba13ed9ab09853 |
| Workload profile | contracts/rf3-acceptance/workload-v1.json | rf3.workload.synthetic-mixed-one-kops.v1 | d5bba0f8c03acc85b7190a15386acaba42167d6f42825b46f0847b198a9e3d66 |
| Topology profile | contracts/rf3-acceptance/topology-v1.json | rf3.topology.three-node-standard.v1 | 33628e501c43019dce38209a37ca42c8cde9e56c90e23af955e2878f49e33b78 |
| Publication record | contracts/rf3-acceptance/publication-v1.json | rf3-acceptance-publication schema 1.0 | c0e288f375f710e09125a85544f0461aa4069da37ae598a7c5feb6b8c3439498 |

A digest is lowercase sha256:<64-hex> over exact stored bytes. The adapter does
not canonicalize JSON, normalize whitespace, reorder fields, or normalize
values before hashing. The root binds both profile paths, IDs, and digests.
The publication record binds all three semantic artifacts, the review manifest,
the decision policy, the platform-input record, the role decisions, and the
attempt semantics. [SRC-67e9649873ef4b3b] [SRC-892ceec743916368]

The core decoder still receives three sources. The publication adapter loads the
fourth file, validates its artifact bindings against the decoded bundle, and
only then verifies protected evidence. This separation preserves the core
semantic API while making authorization mandatory at the production adapter.

## Protected owner decisions

The publication record content-binds every protected authorization input:

| Decision | Protected locator | Whole-document SHA-256 | Authority-record SHA-256 |
| --- | --- | --- | --- |
| Review manifest | codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/drafts/approval-review-plan-implementation--1--publish-material/4775c160088eb62e695f3da2dbe1cdc28855aeecb058427b59297f795ded07f6/manifest.json | 706b59e4729641c03de05ac8b1d4db96e78630cccb560ecb123d2eced87c6740 | not applicable |
| Decision policy | codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-decision-policy-v1.json | 57c2e3335faa142886f36e66d11422dfae7606bd47db6b661ae4843976ba3049 | not applicable |
| Platform inputs | codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/input-approval-plan-implementation--1--platform-d69b6f54ee47816284499670211f92456ef5e58c0a118cdf10a89313f89ccc14.json | 1a345953d6c7be8adca1b74a45868094188e3dde2a92e19eabb4c6f91a4ce082 | not applicable |
| Product owner | codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-product-owner-approval-v1.json | 4db83b1bb7d39f4ead7179b5b366ae97689b16f91366d3f1bcb84a82593d13ea | 5a017b0a1ff25b7f54d96d55ec416d9347de8051a3e1e738ea4b782caf0f1475 |
| Database operator | codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-database-operator-approval-v1.json | 607cd0d98105adb0c4a2cef8fa16bd8e7aa84e613d48cace44ac3eab983d862f | 64fcdea372ec661c056132374bb13feeed4a8787a414f336d8e57f87a245eda3 |
| Platform owner | codetask/automations/runs/RUN-e345cdf3-13d3-432e-8912-1f37f742825d/decisions/rf3-p1-platform-owner-approval-v1.json | 01ab42ec93219ef2bfbea0798ed5e7cbdc28bf4c5b98b0e8fee34f8cbb4b02ab | b989ff11bd44aa3230af7b361c07ab9a8b3391e88db12e1c13f9a8313eca8395 |

The verifier is a required dependency. For each protected locator it resolves
the exact stored bytes and compares the whole-document digest. For a role
approval, the publication record also supplies the exact JSON pointer
/authorityRecord; the verifier selects that value, serializes it with
JSON.stringify(value, null, 2) plus one LF, and compares the authority digest.
A missing verifier, lookup failure, digest mismatch, pointer mismatch, authority
mismatch, duplicate locator, or malformed digest fails closed and returns no
published contract. The consumer-side verifier contract is
defined beside the production loader. [SRC-28c24ee85d66ac87]
The publication model constructs exactly five protected verifier requests:
decision policy, platform inputs, and three role approvals.
[SRC-ea5a126fb6c96413] [SRC-be83b80fc2329ca9]

The platform-input record authorizes exactly rf3q-zone-a, rf3q-zone-b, and
rf3q-zone-c, plus storage class cefasdb-rf3-durable-rwo-v1. The publication
adapter compares those literals with the decoded topology before returning.

## Attempt and error semantics

Schema 1.0 retains the encoded unit requests for
defaultRule.minimumAttempts. The publication record makes the denominator
operational and the production adapter exposes it as the typed
RF3AttemptSemanticsV1 value in RF3PublishedAcceptanceV1.Publication:

- one logical item submitted in a batch write is one attempt;
- one logical point read is one attempt;
- every item in a failed batch is charged unless per-item evidence proves a
  narrower failure set;
- error ppm is failed logical item operations divided by attempted logical item
  operations, multiplied by 1,000,000.

The adapter rejects any publication whose root field, encoded unit, denominator,
write mapping, point-read mapping, or failed-batch mapping differs from the
accepted constants. A consumer that reports only RPC attempts cannot evaluate
the gate and must return an incomplete result. The root requires 300,000
logical attempts in a 300,000 ms serving window. [SRC-ea5a126fb6c96413]

All loader failures are fail-closed. Nil context, cancellation, unreadable or
oversized input, close failure, malformed JSON, unknown field, identity mismatch,
artifact mismatch, protected-evidence failure, or environment-manifest failure
returns the corresponding zero value plus an error.

## Fixed bundle boundary

This design is a fixed semantic aggregate plus an authorization envelope. The
core constructs concrete acceptance, workload, and topology values from exactly
three sources. The publication adapter adds one fixed publication source and
five fixed protected-evidence checks. It does not implement executable plug-ins,
directory discovery, shared-object loading, or independent source lifecycles.

Adding another semantic document, protected role, or evidence kind requires a
versioned contract and corresponding model, validation, loader, and test changes.

## Component evidence

| Component | Accountable owner | Business responsibility | Exact source roots | Provides | Consumes | Owned data and reason to change |
| --- | --- | --- | --- | --- | --- | --- |
| RF3 contract data | Team CefasDB from TASK-RF3-P1-APPROVAL | Publish exact workload, topology, acceptance, and authorization bindings | contracts/rf3-acceptance | Schema 1.0 JSON inputs and publication evidence | Protected decision locators and approved literals | Owns contract bytes; this change adds immutable decision and authority digests |
| RF3 acceptance core | Team CefasDB from the task owner; CODEOWNERS not inspected | Strictly decode and validate the three-source semantic aggregate | internal/core/rf3acceptance | DecodeBundle, Bundle, typed semantic values | Three immutable Source values | Owns semantic validation and profile digest checks; unchanged by this resolution |
| RF3 publication adapter | Team CefasDB from the task owner; CODEOWNERS not inspected | Establish production authorization before exposing a contract | internal/contractbuild/rf3_publication.go; internal/contractbuild/rf3_publication_model.go | LoadRF3PublishedAcceptanceFiles, RF3ProtectedEvidenceVerifier, full typed publication/root/workload projections | RF3 acceptance core, four checked-in files, five protected evidence checks | Owns authorization, attempt mapping, full consumer projection, and fail-closed behavior |
| RF3 environment adapter | Team CefasDB from the task owner; CODEOWNERS not inspected | Compose an authorized P1 contract with a P2 environment manifest | internal/contractbuild/rf3_environment.go; internal/contractbuild/rf3_publication.go | LoadAuthorizedRF3EnvironmentFiles; legacy semantic-only LoadRF3EnvironmentFiles | Published acceptance adapter and RF3 environment core | Owns environment composition; this change adds the authorized production entry point without changing fixture semantics |
| RF3 publication tests | Team CefasDB from the task owner | Pin exact bundle/evidence bindings and error behavior | internal/contractbuild/rf3_publication_test.go | Regression cases for authorization, drift, typed values, and P2 composition | Checked-in contract files and adapter APIs | Owns executable specifications; this change replaces locator-only assertions |
| RF3 contract ADR | Team CefasDB from the task owner | Record the accepted boundary, provenance, semantics, gate, compatibility, and rollback | docs/adr/0009-rf3-qualification-acceptance-contract.md | Decision record | Inspected sources and protected receipts | Owns the decision narrative; this change aligns prose with the enforced boundary |

The published typed boundary exposes dataset quantities, all workload phase
parameters, root profile references, root approvals, default rules, scenarios,
variance, capacity method, publication bindings, and attempt semantics.
Topology remains available through the validated core Bundle. The complete
projections are defined in internal/contractbuild/rf3_publication_model.go.
[SRC-8e25fe5f06394b29] [SRC-ea5a126fb6c96413]

The captured source architecture assessment is incomplete: 82 of 88 components
are omitted from the displayed component set, capture limitations remain, and
no fitness result passed. This ADR does not turn incomplete metrics into a
quality claim. Component ownership above is task-scoped evidence, not a
repository-wide CODEOWNERS claim.

## Verification gate

The source defines tests that load the checked-in four-file boundary with all
five protected bindings, reject a missing or failing verifier, reject
publication drift, compose an authorized P2 environment, and assert the
consumer-visible dataset, workload, references, approvals, variance, capacity,
and attempt values. [SRC-c34328d4ce95d678]
[SRC-74db6980c599a831] [SRC-f32b948614f21ad2]

Test source is not an execution receipt. This resolution records formatted
source bytes only. The host must run make test against those exact hashes and
retain an exit-code-0 receipt. A failed or missing receipt blocks publication.
The warn-only CI job does not satisfy this task-specific gate.

## Compatibility

The acceptance root, workload profile, and topology profile bytes and schema 1.0
semantic API are unchanged. The publication record is a pre-consumption
authorization contract: decision-policy, platform-input, role-decision,
authority-pointer, and authority digests are now required. Any locator-only
reader must update before adopting this publication version.

The legacy LoadRF3EnvironmentFiles function remains available for semantic
fixtures and compatibility, but it is not an authorization API. New P2 and P4
qualification consumers must use LoadAuthorizedRF3EnvironmentFiles or
LoadRF3PublishedAcceptanceFiles with a verifier.

A change to a field, type, unit, enum, digest rule, owner rule, attempt mapping,
or invariant requires a versioned contract. Any accepted workload, topology,
target, method, approval, or publication-binding change produces new bytes and
requires a new authorization and host verification receipt.

## Consequences

- P2 and P4 have a production entry point that consumes publication-v1.json and
  refuses to return a contract until five protected records are verified.
- The publication file content-binds the decision policy, platform-input
  decision, three role decisions, and each role authority proof.
- Consumers receive the full typed dataset, workload, profile-reference,
  approval, variance, capacity, and attempt contract instead of an ID-only
  projection.
- Evaluators use one logical-item-operation denominator for batched writes and
  point reads.
- Any invalid semantic source, publication source, or protected record rejects
  the complete result; no partially authorized contract is returned.
- Callers must implement the protected-evidence verifier at their authority
  boundary and must treat the legacy environment loader as semantic-only.
- The accepted contract does not prove live availability, latency, throughput,
  recovery, variance, capacity, or fault behavior.
- Formatting-only edits change identity and require a new publication binding.

## Alternatives considered

1. Keep locator-only protected evidence. Rejected because mutable locator
   contents could change without changing publication-v1.json.
2. Treat embedded approver strings as authorization. Rejected because strings
   do not prove role authority or protected-record identity.
3. Make the core three-source decoder query protected storage. Rejected because
   that would couple deterministic semantic decoding to an external authority
   boundary.
4. Count batch RPCs as attempts. Rejected because batching would change the
   approved attempt floor and error ppm.
5. Leave LoadRF3EnvironmentFiles as the production P2 entry point. Rejected
   because it cannot consume publication-v1.json or verify protected records.
6. Treat test source or the warn-only CI job as the publication receipt.
   Rejected because neither proves that the final recorded bytes passed.

## Rollback

Before any downstream manifest, result, or release consumes the new publication
digest, Team CefasDB may revert publication-v1.json, the publication adapter,
its tests, and this ADR. The unchanged three semantic files remain valid under
their existing core decoder.

After a downstream record consumes the publication digest, Team CefasDB must
retain those exact bytes. A correction publishes a new publication version and,
when semantic inputs change, a new acceptance root. Rollback may stop new
qualification runs and restore the last supported reader, but it must not edit
or relabel consumed artifacts.
