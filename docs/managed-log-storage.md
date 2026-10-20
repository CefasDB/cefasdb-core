# Native partition text search and TTL (CFP-115)

This is the CefasDB wire contract used by the managed logs runtime. HTTP
requests use the existing authenticated `/v1` service and DynamoDB attribute
values. The database is not exposed to Console browsers. The runtime verifies
tenant authority and always supplies an exact partition key. The database
checks the actual primary-key value again when materializing candidates.

## Bootstrap and idempotent writes

`POST /v1/tables` (existing operation):

```json
{"name":"logs-events","keySchema":{"pk":"tenantId","sk":"orderKey"},"ttlAttribute":"expiresAt","textIndexes":[{"name":"message","field":"message"}]}
```

Success is HTTP 201 with the normalized descriptor. If the table already
exists, describe it with `GET /v1/tables/logs-events` and compare key schema,
TTL attribute, storage class and text index. A mismatched schema is an error;
the runtime must not replace a table or silently query without its index.
Text indexes are disk-backed and declared at table creation; adding/removing
one on an existing table is rejected, rather than pretending a backfill ran.

`POST /v1/UpdateTimeToLive` is an additive HTTP equivalent of the existing RPC:

```json
{"table":"logs-events","attributeName":"expiresAt","enabled":true}
```

Success is HTTP 200 with `{"ttlAttribute":"expiresAt"}`. Repeating the same
enabled attribute is idempotent. This does not backfill old rows. The runtime
must fail closed on a different existing TTL attribute.

`POST /v1/PutItem` (existing operation):

```json
{"table":"logs-events","item":{"tenantId":{"S":"tenant-a"},"orderKey":{"S":"2026-10-19T12:00:00.000000000Z#evt-1"},"id":{"S":"evt-1"},"timestamp":{"S":"2026-10-19T12:00:00.000000000Z"},"acceptedAt":{"S":"2026-10-19T12:00:01.000000000Z"},"expiresAt":{"N":"1793016001"},"message":{"S":"Connection failed"},"level":{"S":"error"},"fields":{"M":{"attempt":{"N":"2"}}}},"condition":"attribute_not_exists(tenantId)"}
```

`orderKey` is the fixed-nine-digit UTC event time followed by `#` and the
stable event ID. `acceptedAt` and `expiresAt` originate at durable acceptance;
replay never resets them. A condition failure returns the existing conditional
error. The worker then reads the exact key and compares the immutable payload
hash, source, acceptance time and expiry: an identical retry succeeds without
writing; different content is a conflict. A changed timestamp under an event
ID must already be rejected by the durable batch/event identity contract.

## Query

`POST /v1/TextQuery`:

```json
{"table":"logs-events","indexName":"message","pkValue":{"S":"tenant-a"},"skLow":{"S":"2026-10-19T11:00:00.000000000Z"},"skHigh":{"S":"2026-10-19T13:00:00.000000000Z"},"search":"connection failed","phrase":"connection failed","fields":{"fields.attempt":{"N":"2"},"level":{"S":"error"}},"minValues":{"acceptedAt":{"S":"2026-10-12T12:00:00.000000000Z"}},"descending":true,"limit":100,"cursor":""}
```

Response (HTTP 200):

```json
{"items":[{"id":{"S":"evt-1"},"message":{"S":"Connection failed"}}],"nextCursor":"opaque-position-or-omitted"}
```

The response contains the complete stored item, abbreviated above. `pkValue`
and `indexName` are required. Sort-key bounds are lower-inclusive and
upper-exclusive. `search` is whitespace-separated, case-insensitive substring
terms combined with AND. `phrase` is a case-insensitive contiguous substring;
both predicates combine with AND. No regex or query DSL is accepted. `fields`
is an AND of exact scalar values at dotted map paths; `minValues` is an AND
of inclusive scalar lower bounds (numbers compare numerically). `maxValues`
provides the corresponding inclusive scalar upper bounds. The runtime
uses `minValues.acceptedAt` for a shortened tenant retention policy, in
addition to the record's immutable `expiresAt`. Acceptance time and its cutoff
must use the same fixed-nine-digit UTC format. The runtime owns a retained,
monotonic visibility floor so increasing a policy never reveals earlier
policy-expired records. Missing/expired records never
appear. Empty text searches still apply partition, time and field predicates.

Rows sort by the encoded primary sort key. Limit defaults to 100 and is at
most 1,000. The opaque database cursor binds the table/index/partition/filter,
range and direction; changing one rejects the cursor. The public runtime
adds its own authenticated cursor envelope, tenant binding and expiry.
Queries use a consistent local snapshot after the existing strong-read
barrier when `consistency:"strong"` is supplied. Pagination is not a retained
cross-request snapshot; concurrent expiry/removal can shorten later pages.

## Exact aggregate

`POST /v1/TextAggregate` accepts the same filter without pagination, plus:

```json
{"table":"logs-events","indexName":"message","pkValue":{"S":"tenant-a"},"skLow":{"S":"2026-10-19T11:00:00.000000000Z"},"skHigh":{"S":"2026-10-19T13:00:00.000000000Z"},"timeField":"timestamp","intervalSeconds":60,"groupBy":"service","errorField":"level","errorValues":["error","fatal","panic"]}
```

Response:

```json
{"buckets":[{"timestamp":"2026-10-19T12:00:00Z","count":1,"errors":1,"groups":{"appreport":1}}]}
```

Counts cover every matching unexpired row in the request snapshot, never
just a page. `timeField` defaults to `timestamp` (RFC3339Nano string or Unix
seconds number); intervals are 1..86400 seconds; `groupBy` is an optional
dotted scalar path. Error matching is case-insensitive; defaults are shown.
Requests have a 30-second deadline and at most 1,000,000 candidate rows,
10,000 buckets and 10,000 bucket/group pairs. Query pages cap encoded rows at
4 MiB and at most 1,000 records. HTTP requests are at most 1 MiB.
An explicit bounded scan/deadline or bucket/group limit returns an error,
not successful partial counts. Only bounded iterator/page/bucket state is
held in RAM; neither the corpus nor the posting lists are rebuilt in RAM.
Long/high-entropy strings use one persistent overflow reference instead of
unbounded postings. An ordered iterator union and residual verification keep
matching exact, including short/Unicode searches. Message strings are valid
UTF-8 and at most 256 KiB. Index/storage amplification and broad-range
aggregate latency require measured capacity acceptance before enabling
production collection. A resource-limit error is never successful empty data.

## Expiration, replication and rollback

New text-indexed TTL tables hide rows at the numeric Unix deadline on all
native reads, independently of asynchronous cleanup. The production reaper
is enabled explicitly with `storage.ttlReaperEnabled`; its interval and batch
are bounded. Only the leader of each shard deletes. Expiry cleanup rechecks
the current row under the same primary-write lock, removes secondary/text
postings and TTL pointers in one replicated batch, and reports progress and
errors. Stale TTL pointers cannot delete a renewed row. YAML/env settings
`ttlReaperInterval` (default 60s) and `ttlReaperBatchSize` (default 1024), or
corresponding `--storage-ttl-reaper-*` flags, control the runner.
`CEFAS_STORAGE_TTL_REAPER_ENABLED` defaults false. Tick budgets cap pointers
and batch bytes; a single row's atomic mutations are never split. Cluster
status contains `ttlCleanup`; `cefas_ttl_cleanup_*` metrics report progress,
failures, last success and observed backlog (a lower bound, not an SLA).

The primary row and native postings are one Pebble/Raft mutation, so indexes
survive restart and replica application without a corpus rebuild. There is
no external index service. Existing tables without `textIndexes` retain
their current descriptor and TTL compatibility semantics. Older binaries do
not understand native text indexes: disable new collection/query before
rollback, retain the accepted rows, and do not write this table with an older
binary. Backups have a separately governed retention policy.

## Conditional cleanup and acceptance-time index

`POST /v1/DeleteItem` accepts `{table,key,condition,binds}`;
`HTTPClient.DeleteItem` exposes the operation. `condition` references `:name`,
while `binds` keys omit the colon (`{"name":{"S":"value"}}`), matching the
existing condition evaluator. Cleanup verifies the actual
row tenant ID, event ID, acceptance time and immutable payload hash and
includes those identity fields in the delete condition.

An optional existing GSI supports incremental policy cleanup without a
periodic full scan. Add this to `gsis` when creating the table:

```json
{"name":"accepted-time","keySchema":{"pk":"tenantId","sk":"acceptedKey"},"projection":{"mode":"KEYS_ONLY"}}
```

Set `acceptedKey` to fixed-nine-digit UTC acceptance time plus `#eventID`.
The existing `POST /v1/Query` accepts `indexName:"accepted-time"`, `pkValue`,
exclusive `skHigh` equal to the acceptance cutoff (without the ID suffix),
and `limit`. Repeated bounded read/conditional-delete batches drain the older
range. Because the legacy GSI uses hashed partitions, the cleanup worker
must recheck actual tenant identity before deleting any materialized row.
