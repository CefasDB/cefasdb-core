// Package client is the cefas gRPC and HTTP/JSON SDK.
//
// Dial returns a *Client wired to a cefas server. Every operation is
// a method on *Client; the resource categories are:
//
//   - Table schema: CreateTable, DescribeTable, ListTables, DropTable,
//     UpdateTimeToLive, DescribeTimeToLive.
//   - Items and batches: PutItem, GetItem, UpdateItem, DeleteItem,
//     BatchWriteItem, BatchGetItem.
//   - Transactions: TransactWriteItems, TransactGetItems.
//   - Queries: Query / QueryBuilder, Scan, ScanStream, Sql,
//     SpatialQueryByBBox, SpatialQueryByRadius, SpatialQueryByZ.
//   - Backup and storage admin: CreateBackup, ListBackups, DeleteBackup,
//     ApplyBackupRetention, RestoreTableFromBackup,
//     RestoreTableFromBackupWithOptions, CompactTable.
//   - Plugins and analytics: ListPlugins, DescribePlugin, CreateIndex,
//     DescribeIndex, RebuildIndex, Explain, TopK, CohortCreate,
//     CohortEstimate, GeoAudience, Dedup, FreqCap, Aggregate.
//   - Cluster admin: Status, AddVoter, AddVoterWithOptions,
//     RemoveServer, RemoveServerWithOptions, PlanPlacement,
//     ApplyPlacement, FinalizeSplit, FinalizeRangeMove.
//   - Atomic, bandit, pipeline, rerank and streams surfaces:
//     AtomicUpdate, BanditCreate / BanditSample / BanditBatchSample /
//     BanditReward / BanditDescribe, Recommend, NextBestAction,
//     RecordReward, GetDecision, Rerank, ListStreams, DescribeStream,
//     GetShardIterator, GetRecords.
//
// NewHTTPClient provides context-bound table/item/native-text operations over
// the public JSON protocol. It uses pkg/encoding/ddbjson, the same canonical
// attribute codec as the server and CLI, without a fork of the wire format.
package client
