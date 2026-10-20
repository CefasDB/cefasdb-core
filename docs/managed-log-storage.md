# Native partition text search and TTL (CFP-115)

This is the CefasDB wire contract used by the managed logs runtime. HTTP
requests use the existing authenticated `/v1` service and DynamoDB attribute
values. The database is not exposed to Console browsers. The runtime verifies
tenant authority and always supplies an exact partition key. The database
checks the actual primary-key value again when materializing candidates.

## Bootstrap and idempotent writes

`POST /v1/tables` (existing operation):

```json
{"name":"logs-events","keySchema":{"pk":"tenantId","sk":"orderKey"},"ttlAttribute":"expiresAt","textIndexes":[{"name":"message","field":"message"}],"gsis":[{"name":"accepted-time","keySchema":{"pk":"tenantId","sk":"acceptedKey"},"projection":{"mode":"KEYS_ONLY"}}],"streamSpecification":{"streamEnabled":true,"streamViewType":"NEW_IMAGE","retentionSeconds":86400}}
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

## Commit cursor and live tail

V1 requires a dedicated single logical data store: a single Raft group may
have three voters; tests may use standalone fsync. CDC APIs reject Managers
with multiple stores with HTTP 503 / gRPC Unavailable, never a successful
shard-zero subset. Every CDC read checks the actual store's leader barrier.
A known leader uses HTTP 307 + Location; retry only explicitly trusted
installation endpoints. The SDK never follows redirects automatically.

All four operations are POST and require `cefas:table:describe`. Only the
gateway receives that internal scope and filters images by exact tenant,
immutable expiry and policy visibility floor. Iterators are positions, not
tenant authorization, and must not reach browsers.

`/v1/ListStreams` accepts `{tableName?,limit?,exclusiveStartStreamArn?}` and
returns `{streams:[{streamArn,streamLabel,tableName}],lastEvaluatedStreamArn?}`.
`/v1/DescribeStream` accepts `{streamArn,limit?,exclusiveStartShardId?}`:

```json
{"streamDescription":{"streamArn":"descriptor-latestStreamArn","shards":[{"shardId":"shardId-000000000000","sequenceNumberRange":{"startingSequenceNumber":"1"},"latestSequenceNumber":"0"}]}}
```

The full existing descriptor additionally contains streamLabel/status/view,
tableName, creationRequestDateTime, keySchema and optional lastEvaluatedShardId.
The additive `latestSequenceNumber` string includes `"0"` for an empty store
and observes committed storage, never an allocated in-flight sequence.
`endingSequenceNumber` still means closed and is absent on open streams.
Bootstrap this checkpoint **before** the history query, then consume later
commits. Deduplicate history/live overlap by immutable event identity. Late
event timestamps and delayed queue persistence cannot fall behind this
commit cursor. Ordering is per-store commit order, not event/acceptance time.

`/v1/GetShardIterator` request:

```json
{"streamArn":"descriptor-latestStreamArn","shardId":"shardId-000000000000","shardIteratorType":"AFTER_SEQUENCE_NUMBER","sequenceNumber":"0"}
```

It returns `{"shardIterator":"opaque-token"}`. Types are TRIM_HORIZON,
LATEST, AT_SEQUENCE_NUMBER and AFTER_SEQUENCE_NUMBER. AFTER 0 is valid if
the readable floor is 1; AT retains its stricter floor check. Tokens expire
after 15 minutes. The runtime stores stream/shard/sequence inside its own
signed tenant/filter/floor cursor and obtains AFTER on each poll; it never
interprets the database token.

`/v1/GetRecords` accepts `{"shardIterator":"opaque-token","limit":1000}`:

```json
{"records":[{"eventID":"streamArn:1","eventName":"INSERT","dynamodb":{"keys":{"tenantId":{"S":"tenant-a"},"orderKey":{"S":"event-time#id"}},"newImage":{"id":{"S":"evt-1"}},"sequenceNumber":"1","streamViewType":"NEW_IMAGE"}}],"nextShardIterator":"opaque-next-token","lastScannedSequenceNumber":"1"}
```

The existing full envelope also contains eventVersion/source/sourceARN,
awsRegion, approximateCreationDateTime and sizeBytes. The additive
`lastScannedSequenceNumber` string is next position minus one, including zero,
even after skipped other-table or withdrawn-image entries. Advance on empty
and metadata-only pages. Limits are 1,000 records / 1 MiB per response (one
oversized record is not split), and 10,000 scanned metadata entries / 8 MiB
per call. A trimmed sequence returns HTTP 412: show a gap/history recovery,
never silently reset to LATEST. An expired token can be recreated from the
compact sequence. Stream retention is 86,400 seconds independently of rows.
Raw `/v1/Stream` SSE has no resume contract and is not used.

Native images live outside the changelog under a full SHA-256 PK/SK custody
prefix plus sequence. Primary/index/metadata/image writes are one durable
batch. Reads validate exact identity and expiry in the same snapshot and
remove expired images again before returning. DeleteItem (including policy
cleanup) and TTL delete the whole row custody prefix in the same batch,
using DeleteRange without scanning versions. Separate per-image expiry
pointers and CDC trimming remove image keys in bounded cleanup passes;
sequence/key metadata may remain to preserve cursor continuity.
`ttlCleanup.payloadDeleted` and
`cefas_ttl_cleanup_cdc_payload_deleted_total` count processed expiry pointers.
This deletes the operational keyspace; immediate byte erasure from old
SST/WAL/Raft segments is not promised. Compaction/snapshot truncation and
backup retention have separate measured cleanup requirements before
production acceptance. Old binaries must not write these native tables.
