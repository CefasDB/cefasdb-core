// Package stream owns the HTTP handlers for the DynamoDB-Streams-
// compatible endpoints (/v1/ListStreams, /v1/DescribeStream,
// /v1/GetShardIterator, /v1/GetRecords) and the SSE endpoint
// /v1/Stream.
//
// Handlers are exposed as methods on *Handlers so the composition
// root (internal/api.Server) can wrap each handler with its standard
// auth + metrics middleware via the same register helper it uses
// for every other route. HandleStream is deliberately registered
// without that wrapper — it is a long-lived SSE connection that
// must bypass per-request instrumentation.
//
// The package depends only on:
//
//   - internal/api/streamcore      — shared stream helpers and types
//   - internal/auth                — scope checks
//   - internal/catalog             — stream descriptor lookup
//   - internal/storage             — change-record retrieval
//   - internal/api/http/httpx      — JSON write helpers
//   - pkg/core/model               — shard-id parsing
//   - pkg/ddbjson                  — attribute encoding
//   - pkg/types                    — wire types + sentinel errors
//
// It deliberately does not import internal/api so the import graph
// stays one-way (internal/api → internal/api/http/stream, never the
// reverse).
package stream
