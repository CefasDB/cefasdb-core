# DynamoDB Streams Compatibility

CefasDB Streams exposes table change events through DynamoDB Streams compatible
APIs while keeping the durable source of truth inside the local CefasDB
changelog. The same physical changelog also supports point-in-time restore; the
Streams layer applies DynamoDB-style projection, iterator, retention, and
pagination semantics on top of those records.

Use this guide when migrating consumers from `aws dynamodbstreams` to the
`cefas` CLI or to the Go client in `pkg/client`.

## Model

A table has no stream by default. Enable one at create time or through table
updates with a `StreamSpecification`:

```text
StreamEnabled=true,StreamViewType=<KEYS_ONLY|NEW_IMAGE|OLD_IMAGE|NEW_AND_OLD_IMAGES>
```

If `StreamEnabled=false`, CefasDB may still append physical changelog records
for PITR, but those records are not visible through Streams APIs. If
`StreamEnabled=true` and `StreamViewType` is omitted, the server defaults to
`NEW_AND_OLD_IMAGES`.

Create a table with Streams disabled by omitting `--stream-specification` or by
passing `--stream-specification StreamEnabled=false`. A disabled stream
specification is normalized away from the active table descriptor.

Each stream has:

- one local ARN in the form
  `arn:cefas:dynamodb:local:000000000000:table/<table>/stream/<label>`
- one v1 shard, `shardId-000000000000`
- numeric sequence numbers derived from the local changelog index
- per-table logical retention state

Lifecycle rules:

- enabling Streams creates a stream descriptor and stores its ARN on the table
  as `LatestStreamArn`
- changing `StreamViewType` while the stream is enabled is rejected
- disabling Streams clears active stream metadata from the table and marks the
  old stream descriptor `DISABLED`
- re-enabling Streams creates a new stream ARN and keeps the old disabled stream
  discoverable through `ListStreams` and `DescribeStream`

## Supported APIs And Flags

The CLI binary is `cefas`; the source lives under `cmd/cefas-cli`.

| DynamoDB Streams operation | CefasDB CLI |
| --- | --- |
| `ListStreams` | `cefas list-streams [--table-name <name>] [--limit <n>] [--exclusive-start-stream-arn <arn>]` |
| `DescribeStream` | `cefas describe-stream --stream-arn <arn> [--limit <n>] [--exclusive-start-shard-id <id>]` |
| `GetShardIterator` | `cefas get-shard-iterator --stream-arn <arn> --shard-id <id> --shard-iterator-type <type> [--sequence-number <n>]` |
| `GetRecords` | `cefas get-records --shard-iterator <token> [--limit <n>]` |

The same surface is available through gRPC and HTTP JSON endpoints:
`/v1/ListStreams`, `/v1/DescribeStream`, `/v1/GetShardIterator`, and
`/v1/GetRecords`.

Legacy CDC remains available separately through gRPC `StreamChanges` and HTTP
SSE `/v1/Stream`. Those endpoints are raft-log change feeds that emit
`ChangeEvent` records with `RaftIndex`, `Op`, raw storage key bytes, and raw
value bytes. They are intentionally not DynamoDB Streams compatible: they do
not use table stream ARNs, shard iterators, `StreamViewType` projection,
DynamoDB event names, or Streams retention. New AWS-style consumers should use
the APIs in this guide. Existing internal CDC consumers can keep using
`StreamChanges` or `/v1/Stream` as the legacy CDC surface.

Supported iterator types:

- `TRIM_HORIZON`: first readable sequence in the stream after retention.
- `LATEST`: next sequence after the newest readable record.
- `AT_SEQUENCE_NUMBER`: starts at the supplied sequence number.
- `AFTER_SEQUENCE_NUMBER`: starts after the supplied sequence number.

`GetRecords` returns `Records` plus `NextShardIterator`. Empty polls are valid:
callers should keep the returned `NextShardIterator` and poll again.

## Copy-Paste Workflow

Start a local server with gRPC enabled:

```sh
cefas-server -data ./cefas-data -grpc :9090 -http :8080
```

Set the common CLI connection flags:

```sh
export CEFAS_FLAGS="--endpoint localhost:9090 --insecure --output json"
```

Create a table with `NEW_AND_OLD_IMAGES` enabled:

```sh
cefas $CEFAS_FLAGS create-table \
  --table-name Events \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --stream-specification StreamEnabled=true,StreamViewType=NEW_AND_OLD_IMAGES
```

Write an insert, a modify, and a remove:

```sh
cefas $CEFAS_FLAGS put-item \
  --table-name Events \
  --item '{"pk":{"S":"event-1"},"status":{"S":"new"}}'

cefas $CEFAS_FLAGS put-item \
  --table-name Events \
  --item '{"pk":{"S":"event-1"},"status":{"S":"updated"}}'

cefas $CEFAS_FLAGS delete-item \
  --table-name Events \
  --key '{"pk":{"S":"event-1"}}'
```

Discover the stream and shard:

```sh
STREAM_ARN=$(cefas $CEFAS_FLAGS list-streams --table-name Events \
  | jq -r '.Streams[0].StreamArn')

SHARD_ID=$(cefas $CEFAS_FLAGS describe-stream --stream-arn "$STREAM_ARN" \
  | jq -r '.StreamDescription.Shards[0].ShardId')
```

Read from the beginning:

```sh
ITERATOR=$(cefas $CEFAS_FLAGS get-shard-iterator \
  --stream-arn "$STREAM_ARN" \
  --shard-id "$SHARD_ID" \
  --shard-iterator-type TRIM_HORIZON \
  | jq -r '.ShardIterator')

cefas $CEFAS_FLAGS get-records --shard-iterator "$ITERATOR" --limit 10
```

## Event Shapes

All records include the DynamoDB Streams envelope fields:

```json
{
  "eventID": "1",
  "eventName": "INSERT",
  "eventVersion": "1.1",
  "eventSource": "cefas:dynamodb-compatible",
  "eventSourceARN": "arn:cefas:dynamodb:local:000000000000:table/Events/stream/2026-06-14T00:00:00Z",
  "awsRegion": "local",
  "dynamodb": {
    "ApproximateCreationDateTime": 1781395200,
    "Keys": {"pk": {"S": "event-1"}},
    "SequenceNumber": "1",
    "SizeBytes": 180,
    "StreamViewType": "NEW_AND_OLD_IMAGES"
  }
}
```

Projection depends on `StreamViewType`.

`KEYS_ONLY`:

```json
{
  "eventName": "MODIFY",
  "dynamodb": {
    "Keys": {"pk": {"S": "event-1"}},
    "SequenceNumber": "2",
    "StreamViewType": "KEYS_ONLY"
  }
}
```

`NEW_IMAGE`:

```json
{
  "eventName": "MODIFY",
  "dynamodb": {
    "Keys": {"pk": {"S": "event-1"}},
    "NewImage": {"pk": {"S": "event-1"}, "status": {"S": "updated"}},
    "SequenceNumber": "2",
    "StreamViewType": "NEW_IMAGE"
  }
}
```

`OLD_IMAGE`:

```json
{
  "eventName": "REMOVE",
  "dynamodb": {
    "Keys": {"pk": {"S": "event-1"}},
    "OldImage": {"pk": {"S": "event-1"}, "status": {"S": "updated"}},
    "SequenceNumber": "3",
    "StreamViewType": "OLD_IMAGE"
  }
}
```

`NEW_AND_OLD_IMAGES`:

```json
{
  "eventName": "MODIFY",
  "dynamodb": {
    "Keys": {"pk": {"S": "event-1"}},
    "OldImage": {"pk": {"S": "event-1"}, "status": {"S": "new"}},
    "NewImage": {"pk": {"S": "event-1"}, "status": {"S": "updated"}},
    "SequenceNumber": "2",
    "StreamViewType": "NEW_AND_OLD_IMAGES"
  }
}
```

Compatibility behavior:

- successful `PutItem` on a new key emits `INSERT`
- successful `PutItem` on an existing key emits `MODIFY`
- successful `DeleteItem` on an existing key emits `REMOVE`
- failed conditional writes emit no stream record
- deleting a missing item emits no stream record
- batch writes preserve the committed changelog order

## Retention

Streams retention is logical and per table. CefasDB keeps physical changelog
entries for PITR, while Streams reads reject sequences below the logical trim
floor with a trimmed-stream error.

Default retention is 24 hours. Configure it with flags, environment variables,
or YAML:

```sh
cefas-server \
  -storage-stream-retention 24h \
  -storage-stream-retention-max-bytes 1073741824
```

```sh
export CEFAS_STORAGE_STREAM_RETENTION=24h
export CEFAS_STORAGE_STREAM_RETENTION_MAX_BYTES=1073741824
```

```yaml
storage:
  streamRetention: 24h
  streamRetentionMaxBytes: 1073741824
```

`storage.streamRetentionMaxBytes` is optional. A value of `0` disables the byte
cap and keeps only the time window.

## Migration From aws dynamodbstreams

Replace the AWS command prefix and keep the operation-level shape:

```sh
aws dynamodbstreams list-streams \
  --table-name Events

cefas list-streams \
  --table-name Events
```

```sh
aws dynamodbstreams describe-stream \
  --stream-arn "$STREAM_ARN"

cefas describe-stream \
  --stream-arn "$STREAM_ARN"
```

```sh
aws dynamodbstreams get-shard-iterator \
  --stream-arn "$STREAM_ARN" \
  --shard-id "$SHARD_ID" \
  --shard-iterator-type TRIM_HORIZON

cefas get-shard-iterator \
  --stream-arn "$STREAM_ARN" \
  --shard-id "$SHARD_ID" \
  --shard-iterator-type TRIM_HORIZON
```

```sh
aws dynamodbstreams get-records \
  --shard-iterator "$ITERATOR" \
  --limit 100

cefas get-records \
  --shard-iterator "$ITERATOR" \
  --limit 100
```

Use the normal global CefasDB CLI flags (`--endpoint`, `--token`,
`--token-file`, `--ca`, `--insecure`, `--output`, and `--timeout`) instead of
AWS region, profile, and credentials flags.

## Known v1 Differences

- Streams are local to the CefasDB deployment and do not integrate with AWS IAM,
  Kinesis, Lambda, or CloudWatch.
- Each table stream has one shard in v1: `shardId-000000000000`.
- ARNs use the local CefasDB format:
  `arn:cefas:dynamodb:local:000000000000:table/<table>/stream/<label>`.
- `eventSource` is `cefas:dynamodb-compatible`, and `awsRegion` is `local`.
- Shard iterators are CefasDB-local signed tokens. They are not AWS iterator
  tokens and are only valid against the CefasDB cluster that issued them.
- Sequence numbers are local changelog indexes. They are ordered within the
  CefasDB changelog and should be treated as opaque by consumers.
- Retention trims the logical Streams read window but keeps the physical
  changelog available for PITR according to the database's PITR policy.

## Compatibility Coverage

The repository locks the compatibility scenarios with focused tests:

| Scenario | Coverage |
| --- | --- |
| streams disabled, create with each view type, enable/disable/re-enable lifecycle | `internal/catalog` and `internal/storage` |
| global and table-filtered `ListStreams` | `pkg/api` |
| `DescribeStream` metadata and shard sequence ranges | `pkg/api` |
| iterator types and validation | `pkg/api` |
| `GetRecords` pagination, empty polling, expired iterators, and trimmed reads | `pkg/api` |
| `INSERT`, `MODIFY`, and `REMOVE` event shapes | `internal/storage`, `pkg/api`, and `cmd/cefas-cli` |
| batch write ordering | `internal/storage` |
| failed conditional writes and missing-item deletes emitting no stream record | `internal/storage` |
| CLI copy-paste workflow | `cmd/cefas-cli` |
