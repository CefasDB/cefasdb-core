# ADR 0008: native partition text indexes and bounded row expiration

Status: accepted for implementation; production activation requires measured
acceptance. Scope: CFP-115 managed logs, additive and collection default off.

## Context and evidence

The existing trigram plugin stores a corpus and posting lists in RAM and
rebuilds them after restart. It cannot be the managed-log storage contract.
The existing row reaper was not started by the server. Executable pre-change
tests showed three independent faults: a stale pointer deleted a renewed row,
TTL left an ALL-projected GSI reference, and 24 concurrent conditional writes
to the same absent key all succeeded. HTTP table creation also silently
discarded the intended text/TTL schema. A cold catalog cache could also
overwrite an existing native schema; the new creation path checks the
barrier-backed stored descriptor, not only the cache. These are stored-data correctness
requirements, separate from production installation/capacity acceptance.
The final race run also reproduced a pre-existing plugin-registry pointer
publication race on an untouched baseline archive; an RWMutex now serializes
that publication/read without changing the legacy plugin algorithm.

## Decision

### Native stream continuity and payload custody

Managed-log tables use the existing `NEW_IMAGE` stream with a 24-hour
checkpoint window, independently of row retention. Native text stream writes
serialize sequence allocation through their commit on each store, before
primary-key locks. Stream reads take that same fence while opening/reading a
committed snapshot, so a concurrent legacy-table write cannot advance a
checkpoint past an in-flight native write. The throughput cost is measured
before activation, not hidden behind a lossless claim.

The initial logs backing-store topology is dedicated single-Raft (which may
have multiple voters), or standalone fsync in tests. CDC refuses Managers with
more than one logical data store rather than claiming a complete shard-zero
stream. Native text query/aggregate already route by the exact tenant PK and
use the actual store's barrier. DescribeStream exposes additive
`latestSequenceNumber` (including `"0"`) from committed storage after a leader
barrier; `endingSequenceNumber` continues to mean closed. Iterator/records
requests bind the requested shard to the real local descriptor; missing
leader or incarnation proof fails rather than returning a successful subset.
GetRecords additionally exposes `lastScannedSequenceNumber`, including skipped
or redacted records, so a compact stream/shard/sequence cursor can resume via
AFTER_SEQUENCE_NUMBER without interpreting opaque iterators.

Native stream images live in separate payload keys under a full SHA-256
primary-key custody prefix plus sequence; changelog metadata contains no image.
The primary/index/change/payload write is one durable batch. DeleteItem and TTL
cleanup delete that row's entire custody prefix with DeleteRange in the same
batch, without scanning versions. Stream retention deletes individual payloads.
Reads resolve payloads under the same snapshot and suppress expired/withdrawn
images while advancing the scanned checkpoint. This is logical physical
deletion from the operational keyspace: SST compaction and separately governed
backups can retain older bytes until their own lifecycle completes.

Declare native immutable `textIndexes` on new disk tables with a sort key.
Maintain trigram candidate references beside primary data in the same
Pebble/Raft batch, including Put, Delete, BatchWrite and AtomicUpdate. Store
only primary sort-key references, not duplicate row bodies. Partition queries
seek a gram and stream its references; long/high-entropy rows use one overflow
reference. Merge ordered iterators with constant candidate state, re-read the
snapshot primary, and verify exact partition/text/scalar filters/expiration.
Short searches stream a bounded partition/range. No corpus is rebuilt in RAM.

Use the existing per-primary mutex pool for all item write paths and TTL
rechecks; multi-key batches acquire stripes in a fixed order. Conditional
replicated puts and expiry rechecks use the real store's leader barrier before
reading prior state. A successful commit contains the complete primary/index
delta. Failed/lost acknowledgements never synthesize a new expiration.

Native text-table TTL is immutable. Point, range, scan, secondary/spatial and
native text reads exclude expired primary rows before asynchronous physical
cleanup. Legacy non-text tables retain their previous lazy-read compatibility.
Views and independent legacy plugin state are separate derived-data contracts;
managed logs query the native base table, not those surfaces.

The opt-in server reaper uses the canonical metadata-shard catalog, current
data-shard leadership, a bounded pointer/byte budget and replicated atomic
index deletion. A stale TTL pointer is removed without deleting a different
current row. Shutdown joins the runner. Per-store status/metrics expose
failures, progress, last success and observed backlog without keys or payloads.

Native `/v1/TextQuery`, `/v1/TextAggregate` and HTTP TTL configuration extend
the existing bearer/table-scope API. The canonical HTTP SDK and server share
one public attribute codec; the legacy internal codec path delegates to it.
Descriptors gain additive protobuf fields; older clients ignore them, but old
binaries must not write native-indexed tables. Wire details and limits are in
`docs/managed-log-storage.md`.

## Retention, ownership and rollback

The logs gateway/runtime owns tenant identity, fixed accepted-at/expiry,
replay payload comparison, retained policy visibility floors, and
acceptance-time GSI cleanup after policy shortening. The database never turns
an index hash or cursor into tenant authority. All results are revalidated;
resource/deadline errors cannot masquerade as successful empty/partial counts.

No table backfill, existing table index mutation, automatic capacity increase,
or production deployment is part of this change. Index amplification,
replicated throughput, broad-range aggregate limits and cleanup catch-up must
be measured with the installation's actual capacity before activation.
Backups have separately controlled retention.

Rollback first disables new collection/query and drains/retains accepted
queue/store data. Preserve primary rows and TTL policy. Do not run an old
writer against a native-indexed table: it cannot maintain the postings.
Changing an existing index/TTL schema requires an explicit migration, never
a silent descriptor edit or an invented historical backfill.
