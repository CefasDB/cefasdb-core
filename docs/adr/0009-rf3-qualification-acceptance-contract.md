# ADR 0009: RF3 qualification acceptance contract

Status: proposed

## Decision request

The product owner, database operator, and platform owner must approve their
fields before this ADR can become accepted or an RF3 acceptance bundle can
qualify a run. No approval deadline or approved numeric target is recorded.
The decision fixes a schema 1.0, exact-byte contract boundary now, at the cost
of rejecting incomplete or unsupported inputs instead of inheriting tool
defaults. Team CefasDB owns implementation after those approvals.

## Context

Later RF3 qualification work needs one immutable identity for the workload,
dataset, topology, observation windows, service limits, recovery rules,
variance rule, capacity-method status, and all eight resilience scenarios.
The repository matrix names those scenarios and distinguishes one-domain
serving from two-domain safe rejection, but it does not define numeric
acceptance limits. [SRC-07a4546899ec536a]

The current load runner exposes write, point-read, and mixed controls and
reports request counts, errors, throughput, and latency percentiles; it does
not supply an approved scan profile, topology, recovery limit, variance rule,
or acceptance decision. [SRC-1be63d7f43fcea03] [SRC-2b616f55cdac4c46]
[SRC-edae006394a40450] Those executable fields constrain future mappings but
do not become approved defaults.

The repository already separates semantic decoding from filesystem access.
`compatibility.DecodeRegistry` returns a zero aggregate on every error,
strictly rejects unknown fields and trailing documents, and delegates file
opening and closing to `internal/contractbuild`.
[SRC-de2b78731714dde2] [SRC-6ac1702831b79eb7]
[SRC-93538d64f389a8f3] The RF3 contract follows that boundary.

## Decision

### Schema 1.0 and exact-byte identity

The initial bundle uses schema 1.0 and contains three explicitly supplied
file-backed plug-ins:

1. `contracts/rf3-acceptance/v1.json`, the acceptance-root plug-in.
2. `contracts/rf3-acceptance/workload-v1.json`, the workload-profile plug-in.
3. `contracts/rf3-acceptance/topology-v1.json`, the topology-profile plug-in.

`contracts/rf3-acceptance/schema-v1.json` is a closed JSON Schema draft
2020-12 mirror for editor and CI feedback. It is not a fourth plug-in and is
not the semantic authority. The repository uses the same mirror-versus-Go
authority split for compatibility contracts and already includes the schema
compiler dependency. [SRC-3cf0b81f580ab8c2] [SRC-8ea0a15e87dd8d3e]

A digest is lowercase `sha256:<64-hex>` over the exact file bytes. JSON
canonicalization, whitespace normalization, field reordering, or value
normalization does not occur before hashing. The acceptance root binds the
relative identity, profile ID, and digest of the workload and topology files.
A downstream run also records the exact acceptance-root digest. A path,
contract ID, or profile ID alone never establishes identity; mutable aliases
such as `latest` are invalid.

### Ownership and provenance

The product owner and database operator jointly approve the dataset, four
workload profiles, latency and error limits, throughput, recovery, variance,
and any capacity gate. The platform owner approves the non-production RF=3
topology, three distinct failure domains, durable-volume and anti-colocation
rules, and resource units. The acceptance root records all three roles and
binds only the profile digests they reviewed.

Every decision-driving number records its value, unit, provenance kind,
accountable owner, and evidence locator or proposal rationale.
`measured` requires reproducible evidence. `proposed-target` requires an
owner and rationale; approval makes it an authorized target, not a measured
baseline. Missing, unresolved, ownerless, unitless, or unlabelled values are
invalid. Each approval timestamp must follow the point at which the exact
referenced bytes were fixed. Changing any referenced bytes invalidates the old
approval set and requires a new root version and approval timestamp.

This ADR remains proposed until the product owner, database operator, and
platform owner approve their fields. The proposed 30 percent headroom and any
three-run capacity method remain non-binding until the product owner and
database operator approve their unit, population, window, formula, rounding,
and variance rule.

### Profile separation

The acceptance root owns approvals, profile references, the default rule,
exactly eight scenario overrides, the variance rule, and capacity-method
status. The workload profile owns the synthetic dataset plus separate write,
point-read, scan, and mixed profiles. The topology profile owns the
non-production RF=3 requirements. Separate digests preserve each owner's
boundary while the root provides one bundle identity.

Profiles contain no credentials, bearer tokens, kubeconfigs, provider tokens,
raw environments, or customer data. Provider inventory, fault hooks, live
results, recurring cadence, and evidence retention remain outside schema 1.0.

### RF3 microkernel mapping

The core owns one `Bundle` aggregate containing `AcceptanceContractV1`,
`WorkloadProfileV1`, and `TopologyProfileV1`. It constructs validated
`ContractID`, `ProfileID`, `Digest`, `DurationMillis`, `RatePPM`, and
`Provenance` values before the aggregate becomes observable.

| Microkernel role | Proposed RF3 element | Contract |
| --- | --- | --- |
| Core | `internal/core/rf3acceptance` | Filesystem-free typed values, schema 1.0 semantics, aggregate invariants, and zero partial output. |
| Plug-in contract | `rf3acceptance.DecodeBundle(root, workload, topology Source) (Bundle, error)` | Consumes the three explicit sources and returns one complete immutable bundle or `Bundle{}` plus an error. |
| Registry | `internal/contractbuild` RF3 acceptance registry | Opens only the explicitly supplied root, workload, and topology paths, preserves source identities and path errors, closes inputs, and delegates all semantics to `DecodeBundle`. |
| Plug-ins | Acceptance root, workload profile, topology profile | Versioned checked-in data files; each implements the contract by conforming to schema 1.0 and the core invariants. |
| Contract tests | RF3 schema, semantic, loader, property, and fuzz suite | Runs against every plug-in and proves version rejection, digest binding, failure isolation, and zero partial output. |

These are data plug-ins in a bounded acceptance microkernel. They do not
implement or extend `pkg/plugin.Plugin`. The existing runtime registry
stores executable `Plugin` implementations registered by name and serves
index, distance, estimator, and audience behavior.
[SRC-be60138e3310e9d0] [SRC-63437f3b77f625c6]
RF3 acceptance files need neither runtime registration nor executable
dispatch.

### Operation inventory

| Operation | Owner and sequence | Result and failure contract |
| --- | --- | --- |
| `DecodeBundle(root, workload, topology Source)` | The core reads each exact byte stream once; strictly decodes one document per source; verifies schema 1.0, IDs, root-to-profile paths and SHA-256 digests; constructs typed values; validates approvals, provenance, profiles, topology, and the exact eight scenarios. | Returns a complete `Bundle` only after every check passes. Nil input, read failure, malformed or trailing JSON, unknown field, unsupported version, digest mismatch, missing approval, invalid provenance, or invariant failure returns `Bundle{}` and an error. It performs no filesystem or network I/O beyond reading the supplied sources. |
| Registry load | `internal/contractbuild` opens the three caller-selected files, attaches their stable identities, invokes `DecodeBundle`, and closes every opened file. It adds no semantic defaults and performs no directory discovery. | Preserves the failing path and underlying open or close error. It returns no partial bundle and performs no mutation. |
| Schema feedback | Tooling compiles `schema-v1.json` and validates each authoring document before semantic decoding. | Schema success never overrides a core rejection; a schema/core disagreement fails verification. |

### Compatibility policy

Readers initially accept schema 1.0 only and fail closed on unsupported
versions, unknown fields, unknown enums, and missing required fields. They do
not merge documents, select `latest`, or ignore data they do not understand.

A future minor schema may add optional fields without changing existing
meaning, but a reader accepts that minor version only after its code and
contract tests declare support. Removing or renaming a field, making an
optional field required, changing a type, unit, enum, digest rule, owner rule,
or invariant requires a new major schema version. Any workload, topology,
target, method, or approval change produces new profile bytes and a new
acceptance-root version even when the schema version remains supported.

Published bytes are immutable. Errata use a new version; consumers never
reinterpret an old result under a corrected file.

### Explicit non-decision: no dynamic registry

This change does not extend `pkg/plugin`, add `init` registration, scan a
directory, load shared objects, select a plug-in by mutable name, or introduce
a general dynamic registry. The contractbuild registry has exactly three
caller-supplied file roles. A new role requires a new contract version and a
recorded need, not a generic discovery mechanism.

## Consequences

- Later runs can bind every decision to exact root, workload, and topology
  bytes and reject drift before mutation.
- Owners can revise one profile without copying unrelated fields, but every
  revision requires a new digest, root version, and approvals.
- Formatting-only edits change identity. Authors must treat checked-in
  contract bytes as published artifacts.
- Strict readers reject unimplemented minor versions; this trades automatic
  forward compatibility for explicit qualification semantics.
- This ADR approves the boundary only. It does not approve numeric targets,
  profile values, topology resources, a capacity gate, or RF3-P1 acceptance.

## Alternatives considered

1. One monolithic JSON file. Rejected because it merges product, database,
   and platform ownership and prevents independent profile identity.
2. JSON Schema as the only validator. Rejected because digest relationships,
   approval timing, provenance, exact scenario membership, and zero partial
   output require semantic validation.
3. Current load-tool defaults as the approved workload. Rejected because
   executable defaults are not owner decisions and omit scan, topology,
   recovery, variance, and capacity contracts.
4. `pkg/plugin` or a dynamic plug-in host. Rejected because the existing
   registry dispatches executable runtime behavior, while RF3 inputs are
   immutable data with exactly three known roles.
5. A service or planning database. Rejected because checked-in files, code
   review, strict validation, and content digests satisfy this single-writer
   authoring boundary without a network or persistence dependency.

## Rollback

Before any downstream run consumes an acceptance-root digest, rollback may
revert the additive ADR, schema, profiles, core, registry, and tests.

After any run, manifest, result, or release record consumes a digest, that
version is immutable. Rollback may restore the last supported reader and stop
new qualification runs, but it must retain the consumed bytes and their
evidence. A correction or new target publishes a new schema/profile/root
version for new runs; it never edits or re-labels the consumed version.

## Source evidence

- The existing semantic decoder returns no partial aggregate and separates
  strict document decoding from aggregate validation.
  [SRC-de2b78731714dde2] [SRC-6ac1702831b79eb7]
- The existing filesystem adapter opens named inputs, delegates semantics,  closes inputs, and preserves path failures. [SRC-93538d64f389a8f3]
- The compatibility schema is a closed draft-2020-12 feedback mirror, while
  Go validation remains authoritative. [SRC-3cf0b81f580ab8c2]
- The runtime plug-in registry stores executable implementations by manifest
  name; its interfaces cover runtime behaviors rather than contract files.
  [SRC-be60138e3310e9d0] [SRC-63437f3b77f625c6]
- The resilience matrix defines eight scenarios and the one-domain versus
  two-domain safety boundary without numeric service limits.
  [SRC-07a4546899ec536a]
- The load runner and report expose executable workload inputs and measured
  request fields, not approved acceptance values.
  [SRC-1be63d7f43fcea03] [SRC-2b616f55cdac4c46]
  [SRC-edae006394a40450]
